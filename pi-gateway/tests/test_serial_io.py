"""Tests for srt_gateway.serial_io — all hardware-free (FakeSerial / replay)."""

import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
)

from srt_gateway import protocol  # noqa: E402
from srt_gateway import serial_io  # noqa: E402
from srt_gateway.serial_io import (  # noqa: E402
    FakeSerial,
    SerialReader,
    NotConnectedError,
    find_base_port,
    replay_bytelog,
)


# --- frame-building helpers -------------------------------------------------

def make_health_frame(uptime_s=42, sats=9, armed=1):
    """A valid 40-byte HEALTH frame."""
    import struct
    payload = struct.pack(
        "<IIIIHBBBBBBQQ",
        0x010203,        # fw_version -> 1.2.3
        uptime_s,        # uptime_s
        1000,            # packets_rx_total
        2,               # crc_errors_total
        10000,           # pps_chz -> 100.0 Hz
        1,               # gps_fix
        1,               # pps_locked
        sats,            # sats
        8,               # slot_count
        armed,           # armed
        0,               # reserved
        1_700_000_000_000,  # frame_epoch_ms
        123456,          # last_beacon_ms
    )
    return protocol.encode_frame(protocol.T_HEALTH, payload)


def make_log_frame(text="hello"):
    return protocol.encode_frame(protocol.T_LOG, text.encode("utf-8"))


def make_ack_frame(ref_type=protocol.T_SET_ARMED, code=0):
    return protocol.encode_frame(protocol.T_ACK, bytes([ref_type, code]))


def drain(reader, max_cycles=50):
    """Poll until no frame comes back for a couple of cycles (FakeSerial)."""
    total = 0
    idle = 0
    for _ in range(max_cycles):
        got = reader.poll_once()
        total += got
        idle = 0 if got else idle + 1
        if idle >= 2:
            break
    return total


# --- 1. several valid frames ------------------------------------------------

def test_valid_frames_dispatched_with_decoded():
    stream = make_health_frame(uptime_s=7) + make_log_frame("ping") + \
        make_ack_frame(code=0)
    fake = FakeSerial(stream)
    got = []
    reader = SerialReader(
        on_frame=lambda t, p, d: got.append((t, d)),
        serial_obj=fake,
        sleep=lambda s: None,
    )

    drain(reader)

    types = [t for t, _ in got]
    assert types == [protocol.T_HEALTH, protocol.T_LOG, protocol.T_ACK]
    # decoded dicts are real protocol output, not raw passthrough
    health = got[0][1]
    assert health["uptime_s"] == 7
    assert health["fw_version"] == "1.2.3"
    assert got[1][1]["text"] == "ping"
    assert got[2][1]["result"] == "applied"
    assert reader.frames_total == 3
    assert reader.connected is True


# --- 2. garbage interleaved -------------------------------------------------

def test_garbage_interleaved_still_delivers_valid_frames():
    stream = (
        b"\x00\xffjunk"
        + make_health_frame()
        + b"\xa5\xa5 not a frame \x13\x37"
        + make_log_frame("after-garbage")
        + b"\xde\xad\xbe\xef"
    )
    fake = FakeSerial(stream)
    got = []
    reader = SerialReader(
        on_frame=lambda t, p, d: got.append((t, d)),
        serial_obj=fake,
        sleep=lambda s: None,
    )

    drain(reader)

    types = [t for t, _ in got]
    assert protocol.T_HEALTH in types
    assert protocol.T_LOG in types
    assert got[-1][1]["text"] == "after-garbage"
    # the loop never crashed and stayed connected
    assert reader.connected is True


# --- 3. disconnect / reconnect ---------------------------------------------

def test_disconnect_then_reconnect_resumes():
    # First FakeSerial: one valid frame, then read() raises -> link lost.
    first = FakeSerial(make_health_frame(uptime_s=1), raise_after=2)
    # Second FakeSerial (what reconnect produces): more frames.
    second = FakeSerial(make_log_frame("back") + make_ack_frame())

    box = {"opened": 0}

    def open_serial():
        box["opened"] += 1
        # first open returns `first`, every subsequent open returns `second`
        return first if box["opened"] == 1 else second

    got = []
    states = []
    reader = SerialReader(
        on_frame=lambda t, p, d: got.append((t, d)),
        open_serial=open_serial,
        sleep=lambda s: None,  # no real backoff delay in tests
        backoff_base=0.01,
    )

    # cycle 1: opens `first`, reads the HEALTH frame
    reader.poll_once()
    states.append(reader.connected)
    assert got and got[0][0] == protocol.T_HEALTH

    # keep polling: `first` drains, then read() raises -> reconnect to `second`
    for _ in range(8):
        reader.poll_once()
        states.append(reader.connected)

    types = [t for t, _ in got]
    # frames from BEFORE and AFTER the reconnect were both delivered
    assert protocol.T_HEALTH in types
    assert protocol.T_LOG in types
    assert protocol.T_ACK in types
    # connected flipped False at some point (disconnect) then True again
    assert False in states
    assert reader.connected is True
    assert reader.reconnects >= 1
    assert box["opened"] >= 2


def test_disconnect_marks_connected_false_immediately():
    fake = FakeSerial(b"", raise_after=0)  # first read raises
    reader = SerialReader(
        on_frame=lambda *a: None,
        serial_obj=fake,
        # reconnect would loop forever opening the same dead object; stop it
        open_serial=None,
        sleep=lambda s: None,
    )
    # No open_serial and serial_obj already consumed -> _reconnect re-adopts
    # the (now closed) object. We only care that connected flips False on the
    # read failure path, so stop the reader right after to avoid a busy loop.
    reader._stop.set()
    reader.poll_once()
    assert reader.connected is False


# --- 4. a decoder that raises does not kill the loop ------------------------

def test_decoder_exception_does_not_kill_loop(monkeypatch):
    stream = make_health_frame() + make_log_frame("survivor")
    fake = FakeSerial(stream)

    real_decode = protocol.decode_payload
    calls = {"n": 0}

    def flaky_decode(ftype, payload):
        calls["n"] += 1
        if ftype == protocol.T_HEALTH:
            raise RuntimeError("boom in decoder")
        return real_decode(ftype, payload)

    monkeypatch.setattr(protocol, "decode_payload", flaky_decode)

    got = []
    errors = []
    reader = SerialReader(
        on_frame=lambda t, p, d: got.append((t, d)),
        serial_obj=fake,
        on_error=lambda t, p, e: errors.append((t, e)),
        sleep=lambda s: None,
    )

    drain(reader)

    # HEALTH decode blew up (forwarded to on_error), LOG still delivered
    types = [t for t, _ in got]
    assert protocol.T_HEALTH not in types
    assert protocol.T_LOG in types
    assert got[-1][1]["text"] == "survivor"
    assert errors and errors[0][0] == protocol.T_HEALTH
    assert reader.connected is True


# --- 5. replay mode ---------------------------------------------------------

def test_replay_bytelog_recovers_all_valid_frames(tmp_path):
    blob = (
        b"garbage-prefix"
        + make_health_frame(uptime_s=99)
        + b"\x00\x00"
        + make_log_frame("from-replay")
        + b"\xa5\xa5bad"
        + make_ack_frame(code=0)
    )
    path = tmp_path / "capture.bin"
    path.write_bytes(blob)

    got = []
    n = replay_bytelog(str(path), lambda t, p, d: got.append((t, d)),
                       chunk_size=7)  # tiny chunks to exercise reassembly

    assert n == 3
    types = [t for t, _ in got]
    assert types == [protocol.T_HEALTH, protocol.T_LOG, protocol.T_ACK]
    assert got[0][1]["uptime_s"] == 99
    assert got[1][1]["text"] == "from-replay"


def test_replay_mode_via_serialreader_run(tmp_path):
    blob = make_health_frame() + make_log_frame("x") + make_ack_frame()
    path = tmp_path / "cap.bin"
    path.write_bytes(blob)

    got = []
    reader = SerialReader(
        on_frame=lambda t, p, d: got.append(t),
        replay_path=str(path),
        sleep=lambda s: None,
    )
    delivered = reader.run()  # streams file then returns

    assert delivered == 3
    assert got == [protocol.T_HEALTH, protocol.T_LOG, protocol.T_ACK]
    assert reader.frames_total == 3


# --- 6. find_base_port ------------------------------------------------------

def test_find_base_port_prefers_configured():
    assert find_base_port("/dev/ttyACM9") == "/dev/ttyACM9"


def test_find_base_port_globs_acm_before_usb():
    def fake_glob(pattern):
        if pattern == "/dev/ttyACM*":
            return ["/dev/ttyACM3", "/dev/ttyACM0"]
        if pattern == "/dev/ttyUSB*":
            return ["/dev/ttyUSB0"]
        return []

    # configured=None -> auto-detect; sorted() picks ACM0 over ACM3, ACM over USB
    assert find_base_port(None, _globber=fake_glob) == "/dev/ttyACM0"


def test_find_base_port_falls_through_to_usb():
    def fake_glob(pattern):
        return ["/dev/ttyUSB1"] if pattern == "/dev/ttyUSB*" else []

    assert find_base_port(None, _globber=fake_glob) == "/dev/ttyUSB1"


def test_find_base_port_returns_none_when_nothing():
    assert find_base_port(None, _globber=lambda p: []) is None


# --- bonus: send() behaviour ------------------------------------------------

def test_send_writes_when_connected():
    fake = FakeSerial(b"")
    reader = SerialReader(on_frame=lambda *a: None, serial_obj=fake,
                          sleep=lambda s: None)
    reader._open()  # adopt + mark connected
    frame = protocol.encode_set_armed(True)
    reader.send(frame)
    assert bytes(fake.written) == frame


def test_send_raises_when_disconnected():
    reader = SerialReader(on_frame=lambda *a: None, sleep=lambda s: None)
    # never opened -> not connected
    with pytest.raises(NotConnectedError):
        reader.send(protocol.encode_ping())
