"""Tests for the cloud -> base control plane (srt_gateway.downlink).

We drive ``CloudDownlinker`` with:
  * a mock ``http_get`` returning a canned /race/current response,
  * a recording ``send_frame`` that captures every emitted frame,
and assert:
  * the four SET_* frames are emitted IN ORDER, with correct encoded bytes
    (decoded back with ``protocol`` to verify slot_count / slots / armed),
  * change detection: identical state twice -> second poll emits nothing;
    a changed slot map -> re-emits; force=True always re-emits,
  * ACK handling: applied vs rejected ACKs are recorded,
  * a disconnected serial link (send raises NotConnectedError) doesn't crash
    the poll, logs, and retries on the next poll once the link is back.
"""

import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
)

from srt_gateway import protocol                       # noqa: E402
from srt_gateway.store import Store                     # noqa: E402
from srt_gateway.downlink import CloudDownlinker        # noqa: E402
from srt_gateway.serial_io import NotConnectedError     # noqa: E402


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class MockCloudGet:
    """Serves a sequence of /race/current responses (cycling on the last)."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def __call__(self, url, headers):
        self.calls.append((url, headers))
        idx = min(len(self.calls) - 1, len(self._responses) - 1)
        status, payload = self._responses[idx]
        return FakeResponse(status, payload)


class RecordingSend:
    """Records every frame; can be set to raise NotConnectedError."""

    def __init__(self):
        self.frames = []
        self.fail = False

    def __call__(self, frame_bytes):
        if self.fail:
            raise NotConnectedError("serial port not connected")
        self.frames.append(bytes(frame_bytes))


def _decode_frames(frames):
    """Run frames through a FrameParser and return [(type, decoded_payload)]."""
    parser = protocol.FrameParser()
    out = []
    for f in frames:
        for ftype, payload in parser.push(f):
            out.append((ftype, payload))
    return out


RACE_A = {
    "race_id": 42,
    "state": "armed",
    "armed": True,
    "slot_count": 50,
    "toa_ms": 57,
    "guard_ms": 20,
    "frame_epoch_ms": 0,
    "slots": [
        {"node_id": 7, "slot": 0},
        {"node_id": 12, "slot": 1},
        {"node_id": 99, "slot": 2},
    ],
}


@pytest.fixture
def store(tmp_path):
    s = Store(str(tmp_path / "srt.db"))
    yield s
    s.close()


# --------------------------------------------------------------------------- #
# 1. Correct frames, in order, correct bytes                                  #
# --------------------------------------------------------------------------- #

def test_emits_four_set_frames_in_order_with_correct_bytes(store):
    get = MockCloudGet([(200, RACE_A)])
    send = RecordingSend()
    dl = CloudDownlinker(store, get, send, "http://cloud", "key")

    stats = dl.poll_once()
    assert stats["status"] == "applied"
    assert stats["sent"] == 4
    assert len(send.frames) == 4

    decoded = _decode_frames(send.frames)
    types = [t for t, _ in decoded]
    # Contractual order: TIMING, EPOCH, SLOTMAP, ARMED.
    assert types == [
        protocol.T_SET_TIMING,
        protocol.T_SET_EPOCH,
        protocol.T_SET_SLOTMAP,
        protocol.T_SET_ARMED,
    ]

    # Verify byte content by re-encoding the expected frames.
    assert send.frames[0] == protocol.encode_set_timing(50, 57, 20)
    assert send.frames[1] == protocol.encode_set_epoch(0)
    assert send.frames[2] == protocol.encode_set_slotmap(
        [(7, 0), (12, 1), (99, 2)]
    )
    assert send.frames[3] == protocol.encode_set_armed(True)

    # And decode the SLOTMAP/ARMED payloads structurally.
    _, slotmap_payload = decoded[2]
    count = slotmap_payload[0]
    assert count == 3
    import struct
    pairs = []
    off = 1
    for _ in range(count):
        nid, slot = struct.unpack("<HB", slotmap_payload[off:off + 3])
        pairs.append((nid, slot))
        off += 3
    assert pairs == [(7, 0), (12, 1), (99, 2)]

    _, armed_payload = decoded[3]
    assert armed_payload == b"\x01"

    _, timing_payload = decoded[0]
    sc, toa, guard = struct.unpack("<BHH", timing_payload)
    assert (sc, toa, guard) == (50, 57, 20)


# --------------------------------------------------------------------------- #
# 2. Change detection                                                         #
# --------------------------------------------------------------------------- #

def test_unchanged_state_emits_nothing_on_second_poll(store):
    get = MockCloudGet([(200, RACE_A)])   # same response every call
    send = RecordingSend()
    dl = CloudDownlinker(store, get, send, "http://cloud", "key")

    s1 = dl.poll_once()
    assert s1["status"] == "applied"
    assert len(send.frames) == 4

    s2 = dl.poll_once()
    assert s2["status"] == "unchanged"
    assert len(send.frames) == 4  # nothing new emitted


def test_changed_slotmap_re_emits(store):
    race_b = dict(RACE_A)
    race_b["slots"] = [
        {"node_id": 7, "slot": 0},
        {"node_id": 12, "slot": 1},
        {"node_id": 99, "slot": 5},   # slot changed 2 -> 5
    ]
    get = MockCloudGet([(200, RACE_A), (200, race_b)])
    send = RecordingSend()
    dl = CloudDownlinker(store, get, send, "http://cloud", "key")

    dl.poll_once()
    assert len(send.frames) == 4
    s2 = dl.poll_once()
    assert s2["status"] == "applied"
    assert len(send.frames) == 8  # re-emitted all four

    # The new slotmap reflects the changed slot.
    assert send.frames[6] == protocol.encode_set_slotmap(
        [(7, 0), (12, 1), (99, 5)]
    )


def test_force_re_emits_even_when_unchanged(store):
    get = MockCloudGet([(200, RACE_A)])
    send = RecordingSend()
    dl = CloudDownlinker(store, get, send, "http://cloud", "key")

    dl.poll_once()
    assert len(send.frames) == 4
    s2 = dl.poll_once(force=True)
    assert s2["status"] == "applied"
    assert len(send.frames) == 8


# --------------------------------------------------------------------------- #
# 3. ACK handling                                                             #
# --------------------------------------------------------------------------- #

def test_record_ack_applied_and_rejected(store):
    get = MockCloudGet([(200, RACE_A)])
    send = RecordingSend()
    dl = CloudDownlinker(store, get, send, "http://cloud", "key")

    applied = protocol.decode_ack(bytes([protocol.T_SET_ARMED, 0]))
    dl.record_ack(applied)
    h = dl.health()
    armed_name = protocol.TYPE_NAMES[protocol.T_SET_ARMED]
    assert h["last_acks"][armed_name]["result"] == "applied"

    rejected = protocol.decode_ack(bytes([protocol.T_SET_SLOTMAP, 3]))
    dl.record_ack(rejected)
    h = dl.health()
    slotmap_name = protocol.TYPE_NAMES[protocol.T_SET_SLOTMAP]
    assert h["last_acks"][slotmap_name]["code"] == 3
    assert "rejected" in h["last_acks"][slotmap_name]["result"]
    assert "rejected" in (dl.last_error or "").lower()


# --------------------------------------------------------------------------- #
# 4. Serial disconnected -> no crash, retries next poll                       #
# --------------------------------------------------------------------------- #

def test_disconnected_serial_does_not_crash_and_retries(store):
    get = MockCloudGet([(200, RACE_A)])
    send = RecordingSend()
    send.fail = True   # serial down: every send raises NotConnectedError
    dl = CloudDownlinker(store, get, send, "http://cloud", "key")

    s1 = dl.poll_once()
    assert s1["status"] == "error"
    assert "send failed" in (dl.last_error or "")
    assert send.frames == []  # nothing got through

    # Link comes back: next poll must RE-attempt (cache was NOT advanced).
    send.fail = False
    s2 = dl.poll_once()
    assert s2["status"] == "applied"
    assert len(send.frames) == 4


def test_no_active_race_is_idle(store):
    get = MockCloudGet([(204, None)])
    send = RecordingSend()
    dl = CloudDownlinker(store, get, send, "http://cloud", "key")
    s = dl.poll_once()
    assert s["status"] == "idle"
    assert send.frames == []


def test_cache_persists_across_instances(store):
    """A new downlinker on the same store sees the cached fingerprint."""
    get = MockCloudGet([(200, RACE_A)])
    send1 = RecordingSend()
    dl1 = CloudDownlinker(store, get, send1, "http://cloud", "key")
    dl1.poll_once()
    assert len(send1.frames) == 4

    # New instance, same store, same state -> should be unchanged (no re-send).
    get2 = MockCloudGet([(200, RACE_A)])
    send2 = RecordingSend()
    dl2 = CloudDownlinker(store, get2, send2, "http://cloud", "key")
    s = dl2.poll_once()
    assert s["status"] == "unchanged"
    assert send2.frames == []
