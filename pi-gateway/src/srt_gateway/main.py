"""main.py — the SRT Pi gateway supervisor.

Wires together every subsystem and supervises their background threads:

    serial_io.SerialReader   reads frames off the base node (or a replay file)
       -> on_frame dispatch:
            T_UPLINK      -> store.insert_fix(decoded)
            T_HEALTH      -> cache base health + store.upsert_node
            T_ACK         -> downlinker.record_ack(decoded)
            T_NODE_STATS  -> store.upsert_node per entry + log
            T_LOG         -> log
    uplink.CloudUplinker     forwards un-synced fixes up to the cloud
    downlink.CloudDownlinker pulls race state down and programs the base
    health.HealthServer      serves GET /health aggregating all of the above

Crash isolation
---------------
Each background loop is wrapped in :func:`_supervised` so that one loop raising
an unexpected exception logs and (optionally) restarts WITHOUT taking down the
others or the process. The serial reader and health server own their own
threads already; the uplink and downlink loops get supervised wrappers here.

Shutdown
--------
SIGINT / SIGTERM set a global stop event; all loops observe it and exit, then
threads are joined and the store/serial/health are closed cleanly.

CLI
---
    python -m srt_gateway.main --config /etc/srt-gateway/config.toml
    python -m srt_gateway.main --replay capture.bin     # no hardware needed
    python -m srt_gateway.main --once                   # one cycle, then exit
"""

import argparse
import logging
import signal
import sys
import threading
import time

from . import config as config_mod
from . import health as health_mod
from . import http_client
from . import protocol
from .downlink import CloudDownlinker
from .serial_io import SerialReader
from .store import Store
from .uplink import CloudUplinker

log = logging.getLogger("srt_gateway.main")


# --------------------------------------------------------------------------- #
# Frame dispatch                                                              #
# --------------------------------------------------------------------------- #

class FrameDispatcher:
    """Routes decoded serial frames to the store / downlinker / base cache.

    Constructed with the store and downlinker; ``on_frame`` is the callback
    handed to :class:`SerialReader`. Keeps the last base HEALTH frame so
    /health can surface it. Every branch is defensive — a single bad frame must
    never break the read loop (SerialReader already guards, but we double up).
    """

    def __init__(self, store, downlinker=None):
        self.store = store
        self.downlinker = downlinker
        self.base_health = None        # last decoded T_HEALTH dict
        self.counts = {}               # per-type frame counts (observability)

    def get_base_health(self):
        return self.base_health

    def on_frame(self, frame_type, payload, decoded):
        self.counts[frame_type] = self.counts.get(frame_type, 0) + 1
        try:
            if frame_type == protocol.T_UPLINK:
                self._on_uplink(decoded)
            elif frame_type == protocol.T_HEALTH:
                self._on_health(decoded)
            elif frame_type == protocol.T_ACK:
                self._on_ack(decoded)
            elif frame_type == protocol.T_NODE_STATS:
                self._on_node_stats(decoded)
            elif frame_type == protocol.T_LOG:
                self._on_log(decoded)
            else:
                log.debug("unhandled frame type 0x%02x: %r", frame_type, decoded)
        except Exception:
            log.exception("frame dispatch failed for type 0x%02x", frame_type)

    def _on_uplink(self, decoded):
        if "raw" in decoded:
            log.warning("undecodable UPLINK: %s", decoded["raw"])
            return
        inserted = self.store.insert_fix(decoded)
        if inserted:
            log.debug("fix node=%s seq=%s ts=%s", decoded["node_id"],
                      decoded["sequence"], decoded["ts_ms"])

    def _on_health(self, decoded):
        self.base_health = decoded
        # Mirror slot_count/armed into the node summary is base-wide, not a
        # node — keep base health cached; node rows come from fixes/node_stats.
        log.debug("base health: fw=%s sats=%s armed=%s",
                  decoded.get("fw_version"), decoded.get("sats"),
                  decoded.get("armed"))

    def _on_ack(self, decoded):
        if self.downlinker is not None:
            self.downlinker.record_ack(decoded)
        else:
            log.info("ACK (no downlinker): %r", decoded)

    def _on_node_stats(self, decoded):
        for entry in decoded.get("entries", []):
            try:
                self.store.upsert_node(
                    entry["node_id"],
                    last_rssi=entry.get("last_rssi_dbm"),
                    last_seen=entry.get("last_heard_ms"),
                )
            except Exception:
                log.debug("upsert_node from node_stats failed", exc_info=True)
        log.debug("node_stats: %d nodes", decoded.get("count", 0))

    def _on_log(self, decoded):
        log.info("base log: %s", decoded.get("text", decoded))


# --------------------------------------------------------------------------- #
# Supervised background loops                                                 #
# --------------------------------------------------------------------------- #

def _supervised(name, fn, stop_event, restart_delay=2.0):
    """Run ``fn()`` in a loop, restarting it if it raises, until ``stop_event``.

    ``fn`` is expected to be a long-running loop that itself watches
    ``stop_event``; if it returns normally we exit. If it raises, we log and
    restart after ``restart_delay`` — crash isolation so one subsystem dying
    does not kill the gateway.
    """
    while not stop_event.is_set():
        try:
            fn()
            return  # clean return -> loop is done
        except Exception:
            log.exception("background loop %r crashed; restarting in %.1fs",
                          name, restart_delay)
            if stop_event.wait(restart_delay):
                return


class Gateway:
    """Owns all subsystems and their threads for one gateway process."""

    def __init__(self, cfg, *, replay_path=None,
                 http_post=None, http_get=None):
        self.cfg = cfg
        self.replay_path = replay_path
        self._http_post = http_post or http_client.http_post
        self._http_get = http_get or http_client.http_get

        self.stop_event = threading.Event()
        self._threads = []

        # -- store ----------------------------------------------------------
        self.store = Store(cfg.db_path)

        # -- uplink / downlink (need store + injected http) -----------------
        self.uplinker = CloudUplinker(
            self.store, self._http_post, cfg.cloud_url, cfg.cloud_api_key,
        )
        # downlinker.send_frame is wired after the serial reader exists.
        self.downlinker = CloudDownlinker(
            self.store, self._http_get, self._noop_send,
            cfg.cloud_url, cfg.cloud_api_key,
        )

        # -- serial reader --------------------------------------------------
        self.dispatcher = FrameDispatcher(self.store, self.downlinker)
        self.serial = SerialReader(
            self.dispatcher.on_frame,
            port=cfg.serial_port,            # None => auto-detect
            baud=cfg.serial_baud,
            replay_path=replay_path,
        )
        # Now the reader exists, point the downlinker at its real send().
        self.downlinker.send_frame = self.serial.send

        # -- health ---------------------------------------------------------
        self.health_state = health_mod.HealthState(
            serial_reader=self.serial,
            store=self.store,
            uplinker=self.uplinker,
            downlinker=self.downlinker,
            base_health_getter=self.dispatcher.get_base_health,
        )
        self.health_server = health_mod.HealthServer(
            self.health_state, port=cfg.health_port,
        )

    @staticmethod
    def _noop_send(_frame):
        raise RuntimeError("send_frame not yet wired")

    # -- background loop bodies ---------------------------------------------

    def _downlink_loop(self):
        """Poll the cloud for race state every ``downlink_interval_s``.

        Forces a (re)apply on the very first poll so a freshly-booted base is
        always programmed even if our cached fingerprint says "already applied".
        """
        first = True
        while not self.stop_event.is_set():
            try:
                self.downlinker.poll_once(force=first)
            except Exception:
                log.exception("downlink poll_once raised")
            first = False
            if self.stop_event.wait(self.cfg.downlink_interval_s):
                return

    # -- lifecycle ----------------------------------------------------------

    def run_once(self):
        """Run a single ingest + downlink cycle and return (for --once/tests).

        In replay mode this streams the whole capture through the pipeline
        first (so fixes land in the store), then does one uplink drain and one
        forced downlink poll. With no cloud configured the cloud calls simply
        fail/idle harmlessly.
        """
        if self.replay_path is not None:
            self.serial.run()  # replay streams the file and returns

        up = self.uplinker.run_once()
        try:
            down = self.downlinker.poll_once(force=True)
        except Exception:
            log.exception("downlink poll_once raised in --once")
            down = {"status": "error"}
        snap = self.health_state.snapshot()
        log.info("once: uplink=%s downlink=%s fixes=%s",
                 up.get("status"), down.get("status"),
                 snap.get("store", {}).get("total_fixes"))
        return {"uplink": up, "downlink": down, "health": snap}

    def start(self):
        """Start all background threads (serial, uplink, downlink, health)."""
        # Health server first so probes work even while the link comes up.
        try:
            self.health_server.start()
        except Exception:
            log.exception("health server failed to start (continuing)")

        # Serial reader owns its own thread.
        self.serial.start()

        # Uplink loop (supervised).
        t_up = threading.Thread(
            target=_supervised,
            args=("uplink", lambda: self.uplinker.run_forever(
                interval=self.cfg.ingest_interval_s), self.stop_event),
            name="srt-uplink", daemon=True,
        )
        t_up.start()
        self._threads.append(t_up)

        # Downlink loop (supervised).
        t_down = threading.Thread(
            target=_supervised,
            args=("downlink", self._downlink_loop, self.stop_event),
            name="srt-downlink", daemon=True,
        )
        t_down.start()
        self._threads.append(t_down)

        log.info("gateway started (db=%s, cloud=%s, health:%d)",
                 self.cfg.db_path, self.cfg.cloud_url, self.cfg.health_port)

    def wait(self):
        """Block until stop_event is set (SIGINT/SIGTERM)."""
        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.stop_event.set()

    def shutdown(self):
        """Signal stop, join threads, close everything cleanly."""
        log.info("gateway shutting down")
        self.stop_event.set()
        try:
            self.serial.stop()
        except Exception:
            log.debug("serial stop error", exc_info=True)
        for t in self._threads:
            t.join(timeout=3.0)
        try:
            self.health_server.stop()
        except Exception:
            log.debug("health stop error", exc_info=True)
        try:
            self.store.close()
        except Exception:
            log.debug("store close error", exc_info=True)


# --------------------------------------------------------------------------- #
# Entrypoint                                                                  #
# --------------------------------------------------------------------------- #

def _install_signal_handlers(stop_event):
    def _handler(signum, _frame):
        log.info("received signal %s; stopping", signum)
        stop_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            # Not on the main thread (e.g. under tests) — skip silently.
            pass


def build_arg_parser():
    p = argparse.ArgumentParser(
        prog="srt-gateway",
        description="Sail Race Tracker Raspberry Pi gateway supervisor.",
    )
    p.add_argument("--config", help="path to a TOML config file")
    p.add_argument("--replay", metavar="BYTELOG",
                   help="replay a captured serial byte-log instead of live "
                        "serial (proves the pipeline with no hardware)")
    p.add_argument("--once", action="store_true",
                   help="run a single ingest+downlink cycle then exit (testing)")
    p.add_argument("--log-level", help="override log level (DEBUG/INFO/...)")
    return p


def main(argv=None):
    args = build_arg_parser().parse_args(argv)

    cfg = config_mod.load_config(args.config)
    level = (args.log_level or cfg.log_level or "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    gw = Gateway(cfg, replay_path=args.replay)

    if args.once:
        try:
            gw.run_once()
        finally:
            gw.shutdown()
        return 0

    if args.replay:
        # Replay mode: stream the capture through the live pipeline, run one
        # uplink/downlink pass, then exit (no live serial to wait on).
        gw.run_once()
        gw.shutdown()
        return 0

    _install_signal_handlers(gw.stop_event)
    gw.start()
    gw.wait()
    gw.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
