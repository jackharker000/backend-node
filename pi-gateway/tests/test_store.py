"""Tests for the gateway SQLite store (srt_gateway.store.Store)."""

import os
import sqlite3
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
)

from srt_gateway.store import Store, DEDUP_WINDOW_MS  # noqa: E402


def make_uplink(node_id=7, sequence=0, ts_ms=1_700_000_000_000,
                speed_cms=532, course_cdeg=18000, battery_mv=4012,
                rssi_dbm=-95, snr_db=7.5, flags=0, lat=51.5, lon=-1.25,
                rx_time_ms=123456789):
    """Build a decoded-uplink dict shaped like protocol.decode_uplink output."""
    # gps_time/subsec are split such that gps_time*1000+subsec == ts_ms,
    # matching how protocol.decode_uplink derives ts_ms.
    return {
        "version": 1,
        "node_id": node_id,
        "sequence": sequence,
        "lat": lat,
        "lon": lon,
        "speed_cms": speed_cms,
        "course_cdeg": course_cdeg,
        "battery_mv": battery_mv,
        "gps_time": ts_ms // 1000,
        "subsec_ms": ts_ms % 1000,
        "flags": flags,
        "rssi_dbm": rssi_dbm,
        "snr_db": snr_db,
        "rx_time_ms": rx_time_ms,
        "ts_ms": ts_ms,
    }


@pytest.fixture
def store(tmp_path):
    s = Store(str(tmp_path / "gw.db"))
    yield s
    s.close()


# 1. Exact-resend idempotency.
def test_insert_then_reinsert_same_is_one_row(store):
    up = make_uplink(node_id=7, sequence=42)
    assert store.insert_fix(up) is True
    assert store.insert_fix(dict(up)) is False  # same frame again
    assert store.total_fixes() == 1


# 2. Sequence wrap dedup policy.
def test_seq_wrap_dedup_policy(store):
    node = 7
    base = 1_700_000_000_000
    # Four consecutive seqs across the u16 wrap, 1s apart -> 4 distinct rows.
    for i, seq in enumerate((65534, 65535, 0, 1)):
        assert store.insert_fix(
            make_uplink(node_id=node, sequence=seq, ts_ms=base + i * 1000)
        ) is True
    assert store.total_fixes() == 4

    # Immediate resend of 65535 (same ts within window) -> duplicate, no row.
    assert store.insert_fix(
        make_uplink(node_id=node, sequence=65535, ts_ms=base + 1 * 1000)
    ) is False
    assert store.total_fixes() == 4

    # Full second wrap: seq 0 again, but MUCH later (well beyond the dedup
    # window). Policy: stale same-(node,seq) row is a previous wrap-cycle, so
    # this is a NEW fix.
    later = base + DEDUP_WINDOW_MS + 60_000
    assert store.insert_fix(
        make_uplink(node_id=node, sequence=0, ts_ms=later)
    ) is True
    assert store.total_fixes() == 5

    # And re-sending that later seq-0 immediately is again a dup.
    assert store.insert_fix(
        make_uplink(node_id=node, sequence=0, ts_ms=later)
    ) is False
    assert store.total_fixes() == 5


# 3. WAL mode actually enabled.
def test_wal_mode_enabled(tmp_path):
    db = str(tmp_path / "wal.db")
    s = Store(db)
    mode = sqlite3.connect(db).execute(
        "PRAGMA journal_mode"
    ).fetchone()[0]
    s.close()
    assert mode.lower() == "wal"


# 4. get_unsynced / mark_synced round-trip.
def test_unsynced_marksynced_roundtrip(store):
    n = 10
    base = 1_700_000_000_000
    for i in range(n):
        store.insert_fix(
            make_uplink(node_id=7, sequence=i, ts_ms=base + i * 1000)
        )
    rows = store.get_unsynced(100)
    assert len(rows) == n
    assert store.unsynced_count() == n

    half = [r["id"] for r in rows[: n // 2]]
    store.mark_synced(half)

    rest = store.get_unsynced(100)
    assert len(rest) == n - len(half)
    assert store.unsynced_count() == n - len(half)
    assert all(r["id"] not in half for r in rest)
    # Ordered by id ascending.
    assert [r["id"] for r in rest] == sorted(r["id"] for r in rest)


# 5. sog/cog unit conversion.
def test_sog_cog_conversion(store):
    store.insert_fix(make_uplink(speed_cms=532, course_cdeg=18000))
    row = store.get_unsynced(1)[0]
    assert row["sog"] == pytest.approx(5.32)
    assert row["cog"] == pytest.approx(180.0)


# 6. nodes table reflects newest fix.
def test_nodes_table_updated_on_insert(store):
    base = 1_700_000_000_000
    store.insert_fix(make_uplink(
        node_id=7, sequence=1, ts_ms=base,
        battery_mv=4012, rssi_dbm=-95,
    ))
    store.insert_fix(make_uplink(
        node_id=7, sequence=2, ts_ms=base + 5000,
        battery_mv=3990, rssi_dbm=-88,
    ))
    conn = sqlite3.connect(store.db_path)
    conn.row_factory = sqlite3.Row
    node = conn.execute("SELECT * FROM nodes WHERE node_id=7").fetchone()
    conn.close()
    assert node["last_seen"] == base + 5000
    assert node["last_battery_mv"] == 3990
    assert node["last_rssi"] == -88


# 7. sync_state cursor get/set.
def test_sync_cursor_get_set(store):
    assert store.get_sync_cursor("uplink_hwm", default=0) == 0
    store.set_sync_cursor("uplink_hwm", 12345)
    assert store.get_sync_cursor("uplink_hwm") == 12345
    store.set_sync_cursor("uplink_hwm", 99999)
    assert store.get_sync_cursor("uplink_hwm") == 99999


# Bonus: latest_per_node returns one newest fix per node.
def test_latest_per_node(store):
    base = 1_700_000_000_000
    store.insert_fix(make_uplink(node_id=7, sequence=1, ts_ms=base))
    store.insert_fix(make_uplink(node_id=7, sequence=2, ts_ms=base + 2000))
    store.insert_fix(make_uplink(node_id=9, sequence=1, ts_ms=base + 1000))
    latest = {r["node_id"]: r for r in store.latest_per_node()}
    assert set(latest) == {7, 9}
    assert latest[7]["ts_ms"] == base + 2000
    assert latest[9]["ts_ms"] == base + 1000
