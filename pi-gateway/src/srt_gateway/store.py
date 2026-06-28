"""store.py — Sail Race Tracker gateway authoritative local store.

This is the durable, append-only system of record for decoded boat position
fixes arriving from the serial link. A separate uplink process later reads
un-synced rows and forwards them to the cloud, then calls ``mark_synced``.

Backed by SQLite via the stdlib ``sqlite3`` module only. Opened in WAL mode
(``journal_mode=WAL``, ``synchronous=NORMAL``) so a reader (the uplink
process) and the writer (the serial ingest loop) don't block each other and
writes survive a process crash (WAL+NORMAL is durable across application
crashes; only an OS/power loss can lose the last in-flight commit).

Sequence-wrap dedup policy
--------------------------
The node ``sequence`` is a u16 that wraps 65534 -> 65535 -> 0 -> 1 -> ....
Our dedup key is ``UNIQUE (node_id, seq)``. That alone only distinguishes
fixes within a single 65536-wide window: after a full wrap, seq 0 from the
*next* cycle collides with seq 0 from the *previous* cycle and would be
silently dropped as a "duplicate".

To stay correct across many wraps we gate the dedup on recency:

    A new fix is treated as a DUPLICATE only if a row already exists with the
    same (node_id, seq) AND that existing row's ts_ms is within
    ``DEDUP_WINDOW_MS`` of the incoming fix's ts_ms.

If the existing same-(node_id, seq) row is *older* than the window, the
incoming frame is from a later wrap-cycle and is a genuinely NEW fix: we
retire the stale row's unique slot (set its ``seq`` to NULL so it leaves the
UNIQUE(node_id, seq) index -- the row itself is kept, the store is
append-only) and insert the new fix. The window is chosen far larger than the time to emit 65536 fixes at
any realistic rate, but far smaller than the time to wrap all the way around
again at that rate, so the two never alias.

In practice nodes emit at ~1 Hz, so a full 65536-fix wrap is ~18 hours.
``DEDUP_WINDOW_MS`` defaults to 1 hour: a true resend arrives within seconds
(dup), while the same seq value from the next wrap arrives ~18 h later (new).

Key tested properties:
  * Replaying the exact same frame twice in a row -> exactly 1 row.
  * A genuine new fix after a full 0..65535..0 wrap -> a new row.
"""

import os
import sqlite3
import threading

__all__ = ["Store", "DEDUP_WINDOW_MS"]

# A same-(node_id, seq) row older than this (by ts_ms) is from a previous
# wrap-cycle and must NOT shadow a new fix. See module docstring.
DEDUP_WINDOW_MS = 60 * 60 * 1000  # 1 hour

_SCHEMA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "schema.sql",
)


class Store:
    """Authoritative local SQLite store for the gateway.

    Public API:
        Store(db_path, schema_path=None)
        insert_fix(uplink) -> bool          # True if newly inserted, False if dup
        get_unsynced(limit) -> list[sqlite3.Row]
        mark_synced(fix_ids) -> None
        get_sync_cursor(key, default=0) -> int
        set_sync_cursor(key, value) -> None
        total_fixes() -> int
        unsynced_count() -> int
        latest_per_node() -> list[sqlite3.Row]
        upsert_node(node_id, *, slot=None, last_seen=None,
                    last_battery_mv=None, last_rssi=None, fw_version=None)
        upsert_boat(boat_id, *, node_id=None, name=None, sail_no=None)
        upsert_race(race_id, *, name=None, state=None, started_ms=None)
        close()
    """

    def __init__(self, db_path, schema_path=None):
        self.db_path = db_path
        # check_same_thread=False: the serial reader thread and the main thread
        # both touch the store. We serialize every access with self._lock below,
        # so sharing one connection across threads is safe (writes never overlap).
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.RLock()
        self._conn.row_factory = sqlite3.Row
        # WAL: concurrent reader/writer; NORMAL: durable across app crashes.
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._apply_schema(schema_path or _SCHEMA_PATH)

    # -- setup ---------------------------------------------------------------

    def _apply_schema(self, schema_path):
        with open(schema_path, "r", encoding="utf-8") as fh:
            self._conn.executescript(fh.read())
        self._conn.commit()

    # -- fix ingest ----------------------------------------------------------

    def insert_fix(self, uplink):
        """Idempotently insert one decoded uplink fix.

        Returns True if a new row was inserted, False if it was a duplicate
        resend (same node_id+seq within the recency window). Also updates the
        ``nodes`` row for this node with the latest seen/battery/rssi.

        ``uplink`` is the dict from ``protocol.decode_uplink`` (which already
        carries ``ts_ms``). cm/s -> m/s and centideg -> deg conversions are
        applied here.
        """
        node_id = uplink["node_id"]
        seq = uplink["sequence"]
        ts_ms = uplink["ts_ms"]

        sog = uplink["speed_cms"] / 100.0
        cog = uplink["course_cdeg"] / 100.0
        battery_mv = uplink["battery_mv"]
        rssi = uplink["rssi_dbm"]
        snr = uplink["snr_db"]
        flags = uplink["flags"]
        rx_time_ms = uplink["rx_time_ms"]
        lat = uplink["lat"]
        lon = uplink["lon"]

        with self._lock:
            cur = self._conn.cursor()
            try:
                # Wrap-aware dedup: an existing same-(node_id, seq) row only counts
                # as a duplicate if it is recent. A stale row (older than the
                # window) is from a previous wrap-cycle -> retire it so the new fix
                # can take the unique slot.
                row = cur.execute(
                    "SELECT id, ts_ms FROM fixes WHERE node_id=? AND seq=?",
                    (node_id, seq),
                ).fetchone()

                inserted = False
                if row is not None:
                    if abs(ts_ms - row["ts_ms"]) <= DEDUP_WINDOW_MS:
                        # Recent same-(node_id, seq) -> a true duplicate resend.
                        inserted = False
                    else:
                        # From a previous/next wrap-cycle -> genuinely new fix.
                        # Retire the stale row's seq slot (set NULL so it leaves the
                        # UNIQUE(node_id, seq) index; SQLite treats NULLs as
                        # distinct) WITHOUT deleting it -- the store is append-only,
                        # so that old fix's position/time history is preserved.
                        cur.execute(
                            "UPDATE fixes SET seq=NULL WHERE id=?", (row["id"],)
                        )
                        cur.execute(
                            """INSERT INTO fixes
                                   (node_id, boat_id, race_id, ts_ms, lat, lon,
                                    sog, cog, battery_mv, seq, rssi, snr, flags,
                                    synced, rx_time_ms)
                               VALUES (?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                       0, ?)""",
                            (node_id, ts_ms, lat, lon, sog, cog, battery_mv, seq,
                             rssi, snr, flags, rx_time_ms),
                        )
                        inserted = True
                else:
                    # Fast path: ON CONFLICT DO NOTHING guards against a racing
                    # writer that inserted the same (node_id, seq) since our SELECT.
                    cur.execute(
                        """INSERT INTO fixes
                               (node_id, boat_id, race_id, ts_ms, lat, lon,
                                sog, cog, battery_mv, seq, rssi, snr, flags,
                                synced, rx_time_ms)
                           VALUES (?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                   0, ?)
                           ON CONFLICT(node_id, seq) DO NOTHING""",
                        (node_id, ts_ms, lat, lon, sog, cog, battery_mv, seq,
                         rssi, snr, flags, rx_time_ms),
                    )
                    inserted = cur.rowcount > 0

                # Always refresh the node summary from the newest fix we've seen.
                self._upsert_node_from_fix(cur, node_id, ts_ms, battery_mv, rssi)

                self._conn.commit()
                return inserted
            except Exception:
                self._conn.rollback()
                raise

    @staticmethod
    def _upsert_node_from_fix(cur, node_id, ts_ms, battery_mv, rssi):
        # Only advance last_seen/battery/rssi when this fix is newer than what
        # we already recorded (out-of-order resends must not regress it).
        cur.execute(
            """INSERT INTO nodes (node_id, last_seen, last_battery_mv, last_rssi)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(node_id) DO UPDATE SET
                   last_battery_mv = CASE WHEN excluded.last_seen >= nodes.last_seen
                                          THEN excluded.last_battery_mv
                                          ELSE nodes.last_battery_mv END,
                   last_rssi       = CASE WHEN excluded.last_seen >= nodes.last_seen
                                          THEN excluded.last_rssi
                                          ELSE nodes.last_rssi END,
                   last_seen       = MAX(nodes.last_seen, excluded.last_seen)""",
            (node_id, ts_ms, battery_mv, rssi),
        )

    # -- uplink / sync -------------------------------------------------------

    def get_unsynced(self, limit):
        """Return up to ``limit`` un-synced fix rows, oldest id first."""
        return self._conn.execute(
            "SELECT * FROM fixes WHERE synced=0 ORDER BY id ASC LIMIT ?",
            (limit,),
        ).fetchall()

    def mark_synced(self, fix_ids):
        """Mark the given fix ids as synced (synced=1)."""
        ids = list(fix_ids)
        if not ids:
            return
        self._conn.executemany(
            "UPDATE fixes SET synced=1 WHERE id=?",
            [(i,) for i in ids],
        )
        self._conn.commit()

    def get_sync_cursor(self, key, default=0):
        """Read an integer cursor from sync_state, or ``default`` if unset."""
        row = self._conn.execute(
            "SELECT value FROM sync_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row is not None else default

    def set_sync_cursor(self, key, value):
        """Set an integer cursor in sync_state."""
        self._conn.execute(
            """INSERT INTO sync_state (key, value) VALUES (?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, int(value)),
        )
        self._conn.commit()

    # -- counts / health -----------------------------------------------------

    def total_fixes(self):
        return self._conn.execute("SELECT COUNT(*) AS c FROM fixes").fetchone()["c"]

    def unsynced_count(self):
        return self._conn.execute(
            "SELECT COUNT(*) AS c FROM fixes WHERE synced=0"
        ).fetchone()["c"]

    def latest_per_node(self):
        """Most recent fix per node (by ts_ms, id as tiebreak) for /health."""
        return self._conn.execute(
            """SELECT f.* FROM fixes f
               JOIN (SELECT node_id, MAX(ts_ms) AS m FROM fixes GROUP BY node_id) g
                 ON f.node_id = g.node_id AND f.ts_ms = g.m
               GROUP BY f.node_id
               HAVING f.id = MAX(f.id)"""
        ).fetchall()

    # -- downlink-side upserts (used later by control plane) -----------------

    def upsert_node(self, node_id, *, slot=None, last_seen=None,
                    last_battery_mv=None, last_rssi=None, fw_version=None):
        """Upsert a nodes row; only non-None fields overwrite existing values."""
        self._conn.execute(
            """INSERT INTO nodes
                   (node_id, slot, last_seen, last_battery_mv, last_rssi, fw_version)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(node_id) DO UPDATE SET
                   slot            = COALESCE(excluded.slot, nodes.slot),
                   last_seen       = COALESCE(excluded.last_seen, nodes.last_seen),
                   last_battery_mv = COALESCE(excluded.last_battery_mv, nodes.last_battery_mv),
                   last_rssi       = COALESCE(excluded.last_rssi, nodes.last_rssi),
                   fw_version      = COALESCE(excluded.fw_version, nodes.fw_version)""",
            (node_id, slot, last_seen, last_battery_mv, last_rssi, fw_version),
        )
        self._conn.commit()

    def upsert_boat(self, boat_id, *, node_id=None, name=None, sail_no=None):
        self._conn.execute(
            """INSERT INTO boats (boat_id, node_id, name, sail_no)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(boat_id) DO UPDATE SET
                   node_id = COALESCE(excluded.node_id, boats.node_id),
                   name    = COALESCE(excluded.name, boats.name),
                   sail_no = COALESCE(excluded.sail_no, boats.sail_no)""",
            (boat_id, node_id, name, sail_no),
        )
        self._conn.commit()

    def upsert_race(self, race_id, *, name=None, state=None, started_ms=None):
        self._conn.execute(
            """INSERT INTO races (race_id, name, state, started_ms)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(race_id) DO UPDATE SET
                   name       = COALESCE(excluded.name, races.name),
                   state      = COALESCE(excluded.state, races.state),
                   started_ms = COALESCE(excluded.started_ms, races.started_ms)""",
            (race_id, name, state, started_ms),
        )
        self._conn.commit()

    # -- lifecycle -----------------------------------------------------------

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False
