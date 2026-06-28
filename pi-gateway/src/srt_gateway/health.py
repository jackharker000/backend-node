"""health.py — dependency-free /health HTTP endpoint for the SRT gateway.

A tiny ``http.server``-based JSON status endpoint so an operator (or a
monitoring probe / the cloud) can ask the Pi gateway "are you alive, is the
serial link up, are fixes flowing, is the cloud sync caught up?" without any
web framework.

It aggregates, on each request, the live health of every subsystem:

  * serial     — ``SerialReader.health()`` (connected, port, frames, reconnects)
  * store      — total fixes, unsynced backlog, per-node last-heard summary
  * uplink     — ``CloudUplinker.health()`` (pending, last sync, dead-letters)
  * downlink   — ``CloudDownlinker.health()`` (last applied state, ACKs)
  * base       — last base-node HEALTH frame the read loop cached (or None)
  * uptime_s   — gateway process uptime

Design:

* **Stdlib only.** ``http.server.ThreadingHTTPServer`` + ``BaseHTTPRequestHandler``.
* **Pull, don't push.** The handler holds a reference to a single
  :class:`HealthState` aggregator and calls ``snapshot()`` per request, so the
  data is always fresh and there is no shared mutable JSON to keep in sync.
* **Never crashes the gateway.** Runs in a daemon thread; a handler exception
  returns 500 with a JSON error and is logged, but the process lives on.
"""

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

__all__ = ["HealthState", "HealthServer", "make_handler"]

log = logging.getLogger("srt_gateway.health")


class HealthState:
    """Aggregates subsystem health into one JSON-able snapshot.

    Holds references to the live subsystem objects (serial reader, store,
    uplinker, downlinker) and a callable returning the last cached base HEALTH
    dict. Each is optional so partial wiring (e.g. tests) still works.
    """

    def __init__(self, *, serial_reader=None, store=None, uplinker=None,
                 downlinker=None, base_health_getter=None, clock=time.time):
        self.serial_reader = serial_reader
        self.store = store
        self.uplinker = uplinker
        self.downlinker = downlinker
        self.base_health_getter = base_health_getter
        self._clock = clock
        self._start = clock()

    def _safe(self, fn, default=None):
        try:
            return fn()
        except Exception as exc:  # one bad subsystem must not break /health
            log.debug("health subsystem error: %s", exc, exc_info=True)
            return {"error": repr(exc)} if default is None else default

    def snapshot(self):
        """Return the aggregated health dict (called per HTTP request)."""
        now = self._clock()
        snap = {
            "ok": True,
            "uptime_s": round(now - self._start, 1),
            "ts_ms": int(now * 1000),
        }

        if self.serial_reader is not None:
            snap["serial"] = self._safe(self.serial_reader.health)

        if self.store is not None:
            snap["store"] = self._safe(self._store_health)

        if self.uplinker is not None:
            snap["uplink"] = self._safe(self.uplinker.health)

        if self.downlinker is not None:
            snap["downlink"] = self._safe(self.downlinker.health)

        if self.base_health_getter is not None:
            snap["base"] = self._safe(self.base_health_getter)

        # Overall ok flag: serial down or cloud backlog growing is still "up"
        # (we are storing-and-forwarding); only an internal error flips ok.
        snap["ok"] = "error" not in snap.get("store", {})
        return snap

    def _store_health(self):
        store = self.store
        nodes = []
        for row in store.latest_per_node():
            nodes.append({
                "node_id": row["node_id"],
                "ts_ms": row["ts_ms"],
                "lat": row["lat"],
                "lon": row["lon"],
                "battery_mv": row["battery_mv"],
                "rssi": row["rssi"],
            })
        return {
            "total_fixes": store.total_fixes(),
            "unsynced": store.unsynced_count(),
            "nodes": nodes,
            "node_count": len(nodes),
        }


def make_handler(state):
    """Build a BaseHTTPRequestHandler subclass bound to a HealthState."""

    class _Handler(BaseHTTPRequestHandler):
        # Quiet the default stderr logging; route through our logger.
        def log_message(self, fmt, *args):
            log.debug("health %s - %s", self.address_string(), fmt % args)

        def _write_json(self, code, obj):
            body = json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?", 1)[0].rstrip("/")
            if path in ("", "/health", "/healthz"):
                try:
                    snap = state.snapshot()
                    self._write_json(200, snap)
                except Exception as exc:
                    log.exception("health snapshot failed")
                    self._write_json(500, {"ok": False, "error": repr(exc)})
            else:
                self._write_json(404, {"error": "not found", "path": self.path})

    return _Handler


class HealthServer:
    """Run the /health HTTP server in a background daemon thread.

    ``start()`` binds and serves; ``stop()`` shuts it down. Binding failures
    (port in use) are raised from ``start`` so the supervisor can decide,
    but serving runs in its own thread and never blocks the gateway.
    """

    def __init__(self, state, host="0.0.0.0", port=8080):
        self.state = state
        self.host = host
        self.port = port
        self._httpd = None
        self._thread = None

    def start(self):
        handler = make_handler(self.state)
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="srt-health", daemon=True,
        )
        self._thread.start()
        log.info("health server listening on %s:%d", self.host, self.port)
        return self._thread

    def stop(self):
        if self._httpd is not None:
            try:
                self._httpd.shutdown()
                self._httpd.server_close()
            except Exception:
                log.debug("error shutting down health server", exc_info=True)
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
