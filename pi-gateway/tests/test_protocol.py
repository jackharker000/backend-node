"""pytest suite for srt_gateway.protocol — the USB serial wire contract."""

import os
import struct
import sys

import pytest

# Make src/ importable without an installed package.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), os.pardir, "src"),
)

from srt_gateway import protocol as p  # noqa: E402


# --- Test 1: CRC check vector ----------------------------------------------

def test_crc_check_vector():
    assert p.crc16_ccitt_false(b"123456789") == 0x29B1


def test_crc_start_end_slice():
    data = b"\x00\x00123456789\x00"
    # CRC over the "123456789" slice only.
    assert p.crc16_ccitt_false(data, 2, 11) == 0x29B1


# --- Sample payload builders -----------------------------------------------

def _sample_uplink_payload():
    return struct.pack(
        "<BHHiiHHHIHBhhQ",
        1,            # version
        0x1234,       # node_id
        7,            # sequence
        515000000,    # lat_e7  -> 51.5
        -1280000,     # lon_e7  -> -0.128
        342,          # speed_cms
        1800,         # course_cdeg
        4011,         # battery_mv
        1751000000,   # gps_time (s)
        250,          # subsec_ms
        0x03,         # flags
        -77,          # rssi_dbm
        925,          # snr_cdb -> 9.25 dB
        123456789,    # rx_time_ms
    )


def _sample_health_payload():
    return struct.pack(
        "<IIIIHBBBBBBQQ",
        0x00010203,   # fw_version -> 1.2.3
        86400,        # uptime_s
        100000,       # packets_rx_total
        12,           # crc_errors_total
        100,          # pps_chz -> 1.00 Hz
        3,            # gps_fix
        1,            # pps_locked
        9,            # sats
        50,           # slot_count
        1,            # armed
        0,            # _reserved
        1751000000000,  # frame_epoch_ms
        1751000050000,  # last_beacon_ms
    )


def _sample_node_stats_payload():
    body = bytearray([2])  # count = 2
    body += struct.pack("<HhIQ", 7, -80, 1234, 1751000000000)
    body += struct.pack("<HhIQ", 12, -65, 99, 1751000050000)
    return bytes(body)


# --- Test 2: round-trip every frame type -----------------------------------

ROUND_TRIP_FRAMES = [
    ("uplink", p.T_UPLINK, _sample_uplink_payload()),
    ("health", p.T_HEALTH, _sample_health_payload()),
    ("node_stats", p.T_NODE_STATS, _sample_node_stats_payload()),
    ("ack", p.T_ACK, bytes([p.T_SET_ARMED, 0])),
    ("log", p.T_LOG, "hello base node ⚓".encode("utf-8")),
    ("set_armed", p.T_SET_ARMED, bytes([1])),
    ("set_epoch", p.T_SET_EPOCH, struct.pack("<Q", 1751000000000)),
    ("set_slotmap", p.T_SET_SLOTMAP, bytes([1]) + struct.pack("<HB", 7, 0)),
    ("set_timing", p.T_SET_TIMING, struct.pack("<BHH", 50, 57, 20)),
    ("ping", p.T_PING, b""),
]


@pytest.mark.parametrize("name,ftype,payload", ROUND_TRIP_FRAMES,
                         ids=[r[0] for r in ROUND_TRIP_FRAMES])
def test_encode_parse_round_trip(name, ftype, payload):
    frame = p.encode_frame(ftype, payload)
    parser = p.FrameParser()
    out = parser.push(frame)
    assert out == [(ftype, payload)]
    assert parser.crc_errors == 0


# --- Test 3: resync after garbage ------------------------------------------

def test_resync_after_garbage():
    parser = p.FrameParser()
    frame = p.encode_frame(p.T_UPLINK, _sample_uplink_payload())
    # Garbage that includes a stray MAGIC byte to force a false resync attempt.
    garbage = bytes([0x00, 0xFF, 0xA5, 0x42, 0x13, 0x37, 0x99])
    stream = garbage + frame + bytes([0x11, 0x22])
    out = parser.push(stream)
    assert out == [(p.T_UPLINK, _sample_uplink_payload())]
    # The stray MAGIC in the garbage must have produced at least one crc error.
    assert parser.crc_errors >= 1


def test_two_frames_with_garbage_between():
    parser = p.FrameParser()
    f1 = p.encode_frame(p.T_PING, b"")
    f2 = p.encode_frame(p.T_HEALTH, _sample_health_payload())
    stream = b"\xde\xad" + f1 + b"\xa5\xa5\x00garbage" + f2
    out = parser.push(stream)
    assert (p.T_PING, b"") in out
    assert (p.T_HEALTH, _sample_health_payload()) in out
    assert parser.crc_errors >= 1


# --- Test 4: partial reads, byte by byte -----------------------------------

def test_partial_reads_byte_by_byte():
    parser = p.FrameParser()
    payload = _sample_uplink_payload()
    frame = p.encode_frame(p.T_UPLINK, payload)
    collected = []
    for i, b in enumerate(frame):
        out = parser.push(bytes([b]))
        # Frame must only emerge on the very last byte, exactly once.
        if i < len(frame) - 1:
            assert out == []
        collected.extend(out)
    assert collected == [(p.T_UPLINK, payload)]
    assert parser.crc_errors == 0


# --- Test 5: decode known samples to field values --------------------------

def test_decode_uplink_fields():
    d = p.decode_uplink(_sample_uplink_payload())
    assert d["version"] == 1
    assert d["node_id"] == 0x1234
    assert d["sequence"] == 7
    assert d["lat"] == pytest.approx(51.5)
    assert d["lon"] == pytest.approx(-0.128)
    assert d["rssi_dbm"] == -77
    assert d["snr_db"] == pytest.approx(9.25)
    assert d["gps_time"] == 1751000000
    assert d["subsec_ms"] == 250
    # ts_ms = gps_time * 1000 + subsec_ms
    assert d["ts_ms"] == 1751000000 * 1000 + 250


def test_decode_health_fields():
    d = p.decode_health(_sample_health_payload())
    assert d["fw_version"] == "1.2.3"
    assert d["uptime_s"] == 86400
    assert d["packets_rx_total"] == 100000
    assert d["crc_errors_total"] == 12
    assert d["pps_hz"] == pytest.approx(1.0)
    assert d["gps_fix"] == 3
    assert d["pps_locked"] == 1
    assert d["sats"] == 9
    assert d["slot_count"] == 50
    assert d["armed"] == 1
    assert d["frame_epoch_ms"] == 1751000000000
    assert d["last_beacon_ms"] == 1751000050000


def test_decode_node_stats_fields():
    d = p.decode_node_stats(_sample_node_stats_payload())
    assert d["count"] == 2
    assert d["entries"][0] == {
        "node_id": 7,
        "last_rssi_dbm": -80,
        "packets": 1234,
        "last_heard_ms": 1751000000000,
    }
    assert d["entries"][1]["node_id"] == 12
    assert d["entries"][1]["last_rssi_dbm"] == -65


def test_decode_ack_and_log_via_decode_payload():
    ack = p.decode_payload(p.T_ACK, bytes([p.T_SET_ARMED, 0]))
    assert ack["ref_type"] == p.T_SET_ARMED
    assert ack["code"] == 0
    assert ack["result"] == "applied"

    log = p.decode_payload(p.T_LOG, "boot ok".encode("utf-8"))
    assert log["text"] == "boot ok"


# --- Test 6: encode_set_* produce the right TYPE and payload ---------------

def _split(frame):
    """Return (type, payload) from a full wire frame, ignoring framing/CRC."""
    assert frame[0] == p.MAGIC
    ftype = frame[1]
    length = frame[2] | (frame[3] << 8)
    payload = frame[4:4 + length]
    return ftype, payload


def test_encode_set_armed_payload():
    ftype, payload = _split(p.encode_set_armed(True))
    assert ftype == p.T_SET_ARMED
    assert payload == bytes([1])
    ftype, payload = _split(p.encode_set_armed(False))
    assert ftype == p.T_SET_ARMED
    assert payload == bytes([0])


def test_encode_set_epoch_payload():
    ftype, payload = _split(p.encode_set_epoch(1751000000000))
    assert ftype == p.T_SET_EPOCH
    assert payload == struct.pack("<Q", 1751000000000)


def test_encode_set_slotmap_payload():
    ftype, payload = _split(p.encode_set_slotmap([(7, 0), (12, 5), (99, 49)]))
    assert ftype == p.T_SET_SLOTMAP
    expected = bytes([3]) + struct.pack("<HB", 7, 0) \
        + struct.pack("<HB", 12, 5) + struct.pack("<HB", 99, 49)
    assert payload == expected


def test_encode_set_timing_payload():
    ftype, payload = _split(p.encode_set_timing(50, 57, 20))
    assert ftype == p.T_SET_TIMING
    assert payload == struct.pack("<BHH", 50, 57, 20)


def test_encode_ping_payload():
    ftype, payload = _split(p.encode_ping())
    assert ftype == p.T_PING
    assert payload == b""


def test_known_wire_vectors():
    # Cross-checked against base_control.py committed vectors.
    assert p.encode_set_armed(True).hex() == "a5810100015d08"
    assert p.encode_set_epoch(1751000000000).hex() == "a582080000a6bbaf970100006604"
    assert (
        p.encode_set_slotmap([(7, 0), (12, 5), (99, 49)]).hex()
        == "a5830a00030700000c0005630031c01c"
    )
    assert p.encode_set_timing(50, 57, 20).hex() == "a5840500323900140031e2"


# --- Test 7: impossible length after false MAGIC is skipped ----------------

def test_impossible_length_does_not_eat_following_frame():
    parser = p.FrameParser()
    # False MAGIC declaring an impossible length (> SERIAL_MAX_PAYLOAD).
    bogus_len = p.SERIAL_MAX_PAYLOAD + 100
    false_frame = bytes([p.MAGIC, 0xEE]) + struct.pack("<H", bogus_len) \
        + bytes([0xAA, 0xBB])
    real = p.encode_frame(p.T_PING, b"")
    out = parser.push(false_frame + real)
    # The real PING frame must survive even though it followed a bogus length.
    assert out == [(p.T_PING, b"")]
    assert parser.crc_errors >= 1


def test_impossible_length_split_then_real_frame():
    # Same, but the bogus length only resolves once enough bytes arrive,
    # and the following real frame must still be recovered intact.
    parser = p.FrameParser()
    bogus_len = 0xFFFF
    false_hdr = bytes([p.MAGIC, 0x55]) + struct.pack("<H", bogus_len)
    real = p.encode_frame(p.T_HEALTH, _sample_health_payload())
    out = parser.push(false_hdr)
    assert out == []  # impossible length consumed, nothing valid yet
    out = parser.push(real)
    assert out == [(p.T_HEALTH, _sample_health_payload())]
    assert parser.crc_errors >= 1


# --- Bonus: overhead / max payload constants exist -------------------------

def test_overhead_constants():
    assert p.SERIAL_OVERHEAD == 6
    assert p.SERIAL_MAX_PAYLOAD == 2048
