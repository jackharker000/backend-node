"""downlink.py — cloud -> base-node control plane for the SRT Pi gateway.

This is the inverse of ``uplink.py``. Where the uplink forwards boat fixes
*up* to the cloud, the downlink pulls the authoritative race state *down* from
the cloud and translates it into the serial ``SET_*`` control frames the base
node (ESP32) understands. The cloud is the source of truth for "what race is
armed, with which slot map and timing"; this module makes the radio match it.

Cloud contract (GET /race/current)
-----------------------------------
Returns the currently-active race configuration the base should enforce::

    {
      "race_id":        int,
      "state":          str,            # e.g. "idle"|"armed"|"running"
      "armed":          bool,
      "slot_count":     int,            # TDMA slots in a frame
      "toa_ms":         int,            # time-on-air budget per slot (ms)
      "guard_ms":       int,            # guard interval per slot (ms)
      "frame_epoch_ms": int (optional), # 0 / absent => let base self-anchor
      "slots":          [ {"node_id": int, "slot": int}, ... ]
    }

Translation (order matters)
---------------------------
We emit exactly these four control frames, IN THIS ORDER, on each *change*:

  1. SET_TIMING  (slot_count, toa_ms, guard_ms)  — define the frame geometry
  2. SET_EPOCH   (frame_epoch_ms or 0)           — anchor time (0=self-anchor)
  3. SET_SLOTMAP (node_id->slot pairs)           — who transmits when
  4. SET_ARMED   (armed)                         — go/no-go LAST

Arming last is deliberate: the base must know the geometry, time anchor and
slot assignments *before* it starts gating transmissions. Disarm is also sent
last; sending the (possibly disarming) armed flag after the rest is harmless.

Change detection (don't spam the base)
--------------------------------------
Re-sending four frames every poll would needlessly load the serial link and
the ESP32. So we compute a canonical fingerprint of the applied state
(slot_count, toa_ms, guard_ms, frame_epoch_ms, armed, sorted slot pairs) and
cache the last-applied fingerprint in the store's ``downlink_state`` table
(created lazily here, like uplink's ``dead_letter`` — we never touch the shared
``schema.sql``). ``poll_once`` only emits frames when the freshly-fetched
fingerprint differs from the cached one, OR when called with ``force=True``
(used on startup / after a base reboot to guarantee the base is re-programmed).

The cache is persisted so a gateway restart does not blindly re-push state the
base already has — but ``force=True`` on boot is the safe default the
supervisor uses, because the *base* may have rebooted while the gateway's cache
says "already applied".

ACK handling
------------
Each ``SET_*`` frame is ACKed asynchronously by the base via a ``T_ACK`` frame
that arrives on the serial *read* loop (not here). The supervisor wires the
read loop's ``on_frame`` to call :meth:`record_ack` for every ``T_ACK``. We do
not block ``poll_once`` waiting for ACKs (the read loop runs in another
thread); instead we track the last ACK per control type and surface
applied/rejected in :meth:`health`. A rejected ACK is logged loudly.

Failure handling
----------------
If the serial port is down, ``send_frame`` raises
:class:`~srt_gateway.serial_io.NotConnectedError`. We catch it, log, do NOT
advance the cached fingerprint (so the next poll re-attempts the same state),
and return — the poll loop simply retries on its next tick once the link is
back.
"""

import json
import logging

from . import protocol

__all__ = ["CloudDownlinker"]

log = logging.getLogger("srt_gateway.downlink")

# Order is contractually significant — see module docstring.
_SET_TYPES = (
    protocol.T_SET_TIMING,
    protocol.T_SET_EPOCH,
    protocol.T_SET_SLOTMAP,
    protocol.T_SET_ARMED,
)


def _canonical_state(race):
    """Normalise a /race/current dict into a hashable, comparable form.

    Slots are sorted by (node_id, slot) so the same logical map always yields
    the same fingerprint regardless of cloud ordering. Missing/None
    ``frame_epoch_ms`` collapses to 0 (self-anchor).
    """
    slots = sorted(
        (int(s["node_id"]), int(s["slot"])) for s in race.get("slots", [])
    )
    return {
        "race_id": race.get("race_id"),
        "state": race.get("state"),
        "armed": bool(race.get("armed", False)),
        "slot_count": int(race.get("slot_count", 0)),
        "toa_ms": int(race.get("toa_ms", 0)),
        "guard_ms": int(race.get("guard_ms", 0)),
        "frame_epoch_ms": int(race.get("frame_epoch_ms") or 0),
        "slots": slots,
    }


def _fingerprint(state):
    """Stable string fingerprint of a canonical state (for change detection)."""
    return json.dumps(state, sort_keys=True, separators=(",", ":"))


class CloudDownlinker:
    """Pull race state from the cloud and program the base node over serial.

    Parameters
    ----------
    store:
        A ``srt_gateway.store.Store`` (or compatible). Used to persist the
        last-applied fingerprint (``downlink_state`` table, created lazily) and
        to mirror the race into ``races`` via ``upsert_race``.
    http_get:
        Dependency-injected ``http_get(url, headers)`` returning a
        response-like object with ``.status_code`` and ``.json()``. Transport
        failures must raise (e.g. ConnectionError/TimeoutError).
    send_frame:
        Dependency-injected ``send_frame(frame_bytes)`` — typically
        ``SerialReader.send``. Raises ``NotConnectedError`` when the port is
        down; we catch and retry next poll.
    cloud_url:
        Base cloud URL; ``/race/current`` is appended.
    api_key:
        Bearer token sent as ``Authorization: Bearer <api_key>``.
    """

    def __init__(self, store, http_get, send_frame, cloud_url, api_key):
        self.store = store
        self.http_get = http_get
        self.send_frame = send_frame
        self.cloud_url = cloud_url.rstrip("/")
        self.api_key = api_key

        self._ensure_state_table()

        # Health / observability.
        self.last_downlink_ok = None      # canonical state of last good apply
        self.last_error = None
        self.last_poll_status = "idle"    # "idle"|"unchanged"|"applied"|"error"
        # last ACK seen per SET_* type: {ref_type: {"result":..,"code":..}}
        self.last_acks = {}

    # -- lazy local state table ---------------------------------------------

    def _conn(self):
        return getattr(self.store, "_conn", None)

    def _ensure_state_table(self):
        conn = self._conn()
        if conn is None:
            return
        conn.execute(
            """CREATE TABLE IF NOT EXISTS downlink_state (
                   key         TEXT PRIMARY KEY,
                   fingerprint TEXT,
                   applied_ms  INTEGER
               )"""
        )
        conn.commit()

    def _load_cached_fingerprint(self):
        conn = self._conn()
        if conn is None:
            return None
        row = conn.execute(
            "SELECT fingerprint FROM downlink_state WHERE key='applied'"
        ).fetchone()
        if row is None:
            return None
        # row may be sqlite3.Row or tuple
        try:
            return row["fingerprint"]
        except (KeyError, IndexError, TypeError):
            return row[0]

    def _store_cached_fingerprint(self, fp):
        conn = self._conn()
        if conn is None:
            return
        import time
        conn.execute(
            """INSERT INTO downlink_state (key, fingerprint, applied_ms)
               VALUES ('applied', ?, ?)
               ON CONFLICT(key) DO UPDATE SET
                   fingerprint = excluded.fingerprint,
                   applied_ms  = excluded.applied_ms""",
            (fp, int(time.time() * 1000)),
        )
        conn.commit()

    # -- cloud fetch ---------------------------------------------------------

    def _headers(self):
        return {
            "Authorization": "Bearer " + self.api_key,
            "Content-Type": "application/json",
        }

    def _fetch_race(self):
        """GET /race/current. Returns the dict, or None on failure (logged)."""
        url = self.cloud_url + "/race/current"
        try:
            resp = self.http_get(url, self._headers())
        except Exception as exc:
            self.last_error = "fetch failed: %r" % (exc,)
            log.warning("downlink GET /race/current failed: %s", exc)
            return None
        status = resp.status_code
        if status == 204:
            return None  # no active race
        if not (200 <= status < 300):
            self.last_error = "HTTP %d" % status
            log.warning("downlink GET /race/current -> HTTP %d", status)
            return None
        try:
            return resp.json()
        except Exception as exc:
            self.last_error = "bad json: %r" % (exc,)
            log.warning("downlink response not JSON: %s", exc)
            return None

    # -- frame building ------------------------------------------------------

    @staticmethod
    def _build_frames(state):
        """Return the four SET_* frames in contractual order for a state."""
        slots = [(nid, slot) for nid, slot in state["slots"]]
        return [
            protocol.encode_set_timing(
                state["slot_count"], state["toa_ms"], state["guard_ms"]
            ),
            protocol.encode_set_epoch(state["frame_epoch_ms"]),
            protocol.encode_set_slotmap(slots),
            protocol.encode_set_armed(state["armed"]),
        ]

    # -- the testable unit ---------------------------------------------------

    def poll_once(self, force=False):
        """Fetch race state and, if changed (or ``force``), program the base.

        Returns a stats dict::

            {"status": "idle"|"unchanged"|"applied"|"error",
             "sent": int, "race_id": int|None, "error": str|None}

        * ``idle``      — no active race / fetch failed (nothing to do).
        * ``unchanged`` — state matches the cached fingerprint; emitted nothing.
        * ``applied``   — emitted the four SET_* frames (state changed/forced).
        * ``error``     — a send failed (serial down); cache NOT advanced.
        """
        stats = {"status": "idle", "sent": 0, "race_id": None, "error": None}

        race = self._fetch_race()
        if race is None:
            stats["status"] = "idle"
            self.last_poll_status = "idle"
            return stats

        state = _canonical_state(race)
        stats["race_id"] = state["race_id"]
        fp = _fingerprint(state)

        # Mirror race metadata into the store (best effort, never fatal).
        try:
            if state["race_id"] is not None:
                self.store.upsert_race(
                    state["race_id"], state=state["state"]
                )
        except Exception:
            log.debug("upsert_race failed", exc_info=True)

        cached = self._load_cached_fingerprint()
        if fp == cached and not force:
            stats["status"] = "unchanged"
            self.last_poll_status = "unchanged"
            return stats

        # State changed (or forced) -> emit the four SET_* frames in order.
        frames = self._build_frames(state)
        sent = 0
        try:
            for frame in frames:
                self.send_frame(frame)
                sent += 1
        except Exception as exc:
            # Serial down (NotConnectedError) or write failure. Do NOT advance
            # the cache: the next poll re-attempts the full state once the link
            # is back. Partial sends are fine — re-sending all four is safe.
            self.last_error = "send failed: %r" % (exc,)
            log.warning(
                "downlink send failed after %d/%d frames: %s",
                sent, len(frames), exc,
            )
            stats.update(status="error", sent=sent, error=str(exc))
            self.last_poll_status = "error"
            return stats

        # All four sent. Cache the new fingerprint so we don't re-send until it
        # changes again. ACKs arrive asynchronously via record_ack().
        self._store_cached_fingerprint(fp)
        self.last_downlink_ok = state
        self.last_poll_status = "applied"
        stats.update(status="applied", sent=sent)
        log.info(
            "downlink applied race %s: timing(%d,%d,%d) epoch=%d slots=%d armed=%s",
            state["race_id"], state["slot_count"], state["toa_ms"],
            state["guard_ms"], state["frame_epoch_ms"], len(state["slots"]),
            state["armed"],
        )
        return stats

    # -- ACK intake (called from the serial read loop) -----------------------

    def record_ack(self, decoded):
        """Record a decoded T_ACK frame (from ``protocol.decode_ack``).

        ``decoded`` carries ``ref_type`` (the SET_* type being ACKed),
        ``code`` (0 = applied), and ``result`` ("applied"/"rejected(N)").
        We keep the last ACK per ref_type for /health and log rejections.
        """
        ref_type = decoded.get("ref_type")
        if ref_type is None:
            return
        self.last_acks[ref_type] = {
            "ref_name": decoded.get("ref_name"),
            "code": decoded.get("code"),
            "result": decoded.get("result"),
        }
        if decoded.get("code", 0) != 0:
            self.last_error = "ACK rejected: %s %s" % (
                decoded.get("ref_name"), decoded.get("result")
            )
            log.warning(
                "base REJECTED %s: %s",
                decoded.get("ref_name"), decoded.get("result"),
            )
        else:
            log.debug("base ACK applied: %s", decoded.get("ref_name"))

    # -- health --------------------------------------------------------------

    def health(self):
        """Snapshot for the gateway /health endpoint."""
        acks = {
            protocol.TYPE_NAMES.get(t, "0x%02x" % t): v
            for t, v in self.last_acks.items()
        }
        return {
            "last_poll_status": self.last_poll_status,
            "last_downlink_ok": self.last_downlink_ok,
            "last_error": self.last_error,
            "last_acks": acks,
        }
