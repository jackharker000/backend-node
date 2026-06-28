"""Full-pipeline smoke test for the gateway supervisor (srt_gateway.main).

Proves the whole ingest pipeline works in --replay mode with NO hardware and
NO cloud:

  1. Build a byte-log of encoded UPLINK frames with ``protocol.encode_frame``.
  2. Run the Gateway in replay mode against that byte-log into a tmp SQLite DB.
  3. Assert the decoded fixes landed in the DB and that the in-process health
     aggregator (the same data /health serves) reports them.

The cloud is a no-op mock (POST/GET that never succeed) so the uplink/downlink
passes are exercised but require no network.
"""

import os
import struct
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
)

from srt_gateway import protocol            # noqa: E402
from srt_gateway import config as config_mod  # noqa: E402
from srt_gateway.main import Gateway        # noqa: E402


def _make_uplink_frame(node_id, sequence, ts_s, lat, lon):
    """Encode one UPLINK frame matching protocol.decode_uplink's struct.

    struct "<BHHiiHHHIHBhhQ":
      version u8, node_id u16, sequence u16, lat_e7 i32, lon_e7 i32,
      speed_cms u16, course_cdeg u16, battery_mv u16, gps_time u32,
      subsec_ms u16, flags u8, rssi_dbm i16, snr_cdb i16, rx_time_ms u64
    """
    payload = struct.pack(
        "<BHHiiHHHIHBhhQ",
        1,                       # version
        node_id,
        sequence,
        int(lat * 1e7),
        int(lon * 1e7),
        250,                     # speed_cms = 2.5 m/s
        18000,                   # course_cdeg = 180.00 deg
        3700,                    # battery_mv
        ts_s,                    # gps_time (s)
        0,                       # subsec_ms
        0,                       # flags
        -90,                     # rssi_dbm
        750,                     # snr_cdb = 7.5 dB
        ts_s * 1000,             # rx_time_ms
    )
    return protocol.encode_frame(protocol.T_UPLINK, payload)


def _make_bytelog(path):
    """Write a byte-log of several UPLINK frames (plus a stray HEALTH)."""
    frames = bytearray()
    # 3 nodes, 2 fixes each = 6 fixes.
    base_ts = 1_700_000_000
    for node_id in (7, 12, 99):
        for seq in (1, 2):
            frames += _make_uplink_frame(
                node_id, seq, base_ts + seq,
                51.5 + node_id * 0.001, -0.1 + seq * 0.001,
            )
    # A HEALTH frame too, to exercise that branch (40-byte struct).
    health_payload = struct.pack(
        "<IIIIHBBBBBBQQ",
        0x010203,   # fw_version 1.2.3
        1234,       # uptime_s
        6,          # packets_rx_total
        0,          # crc_errors_total
        100,        # pps_chz
        1,          # gps_fix
        1,          # pps_locked
        9,          # sats
        50,         # slot_count
        1,          # armed
        0,          # reserved
        0,          # frame_epoch_ms
        0,          # last_beacon_ms
    )
    frames += protocol.encode_frame(protocol.T_HEALTH, health_payload)

    with open(path, "wb") as fh:
        fh.write(frames)
    return 6  # number of fixes


class NoopCloud:
    """POST/GET that always fail (no network) — uplink/downlink stay idle."""

    def post(self, url, json_body, headers):
        raise ConnectionError("no cloud in smoke test")

    def get(self, url, headers):
        raise ConnectionError("no cloud in smoke test")


def test_replay_pipeline_end_to_end(tmp_path):
    bytelog = tmp_path / "capture.bin"
    n_fixes = _make_bytelog(str(bytelog))

    cfg = config_mod.Config(
        serial_device="auto",
        cloud_url="http://localhost:1",   # nothing listening
        cloud_api_key="test",
        db_path=str(tmp_path / "srt.db"),
        health_port=0,                    # not actually bound in run_once
    )

    cloud = NoopCloud()
    gw = Gateway(
        cfg, replay_path=str(bytelog),
        http_post=cloud.post, http_get=cloud.get,
    )
    try:
        result = gw.run_once()
    finally:
        gw.shutdown()

    # Fixes landed in the DB.
    health = result["health"]
    assert health["store"]["total_fixes"] == n_fixes
    assert health["store"]["node_count"] == 3

    # The base HEALTH frame was cached and surfaced.
    assert health["base"] is not None
    assert health["base"]["fw_version"] == "1.2.3"
    assert health["base"]["sats"] == 9

    # Per-node last-heard present for all three nodes.
    node_ids = {n["node_id"] for n in health["store"]["nodes"]}
    assert node_ids == {7, 12, 99}

    # Uplink tried and failed gracefully (no cloud) -> rows still unsynced.
    assert health["store"]["unsynced"] == n_fixes
    # Downlink with no cloud -> idle (GET raised).
    assert result["downlink"]["status"] == "idle"


def test_replay_reports_via_healthstate_snapshot(tmp_path):
    """The same data /health serves comes from HealthState.snapshot()."""
    bytelog = tmp_path / "capture.bin"
    _make_bytelog(str(bytelog))

    cfg = config_mod.Config(
        db_path=str(tmp_path / "srt.db"),
        cloud_url="http://localhost:1",
        serial_device="auto",
    )
    cloud = NoopCloud()
    gw = Gateway(cfg, replay_path=str(bytelog),
                 http_post=cloud.post, http_get=cloud.get)
    try:
        gw.serial.run()  # stream replay into the store
        snap = gw.health_state.snapshot()
    finally:
        gw.shutdown()

    assert snap["ok"] is True
    assert snap["store"]["total_fixes"] == 6
    assert "serial" in snap
    assert "uplink" in snap
    assert "downlink" in snap
    assert snap["uptime_s"] >= 0
