"""protocol.py — Sail Race Tracker USB serial wire contract.

Pure-stdlib codec for the framed serial link between the Raspberry Pi gateway
and the base-node ESP32. No serial / pyserial dependency lives here — this
module only knows how to turn bytes into frames and frames into bytes.

Wire framing:

    [MAGIC 0xA5][TYPE u8][LEN u16 LE][PAYLOAD][CRC16 LE]

CRC is CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF, no reflection,
xorout 0x0000; crc16("123456789") == 0x29B1) computed over
TYPE | LEN | PAYLOAD (everything between MAGIC and the CRC).

This matches base_control.py / contracts/SERIAL_PROTOCOL.md /
gateway/src/contract/serial.ts.
"""

import struct

__all__ = [
    "MAGIC",
    "SERIAL_OVERHEAD",
    "SERIAL_MAX_PAYLOAD",
    "T_UPLINK",
    "T_HEALTH",
    "T_LOG",
    "T_ACK",
    "T_NODE_STATS",
    "T_SET_ARMED",
    "T_SET_EPOCH",
    "T_SET_SLOTMAP",
    "T_SET_TIMING",
    "T_PING",
    "TYPE_NAMES",
    "crc16_ccitt_false",
    "encode_frame",
    "encode_set_armed",
    "encode_set_epoch",
    "encode_set_slotmap",
    "encode_set_timing",
    "encode_ping",
    "decode_uplink",
    "decode_health",
    "decode_node_stats",
    "decode_ack",
    "decode_log",
    "decode_payload",
    "FrameParser",
]

# --- Frame types ------------------------------------------------------------

MAGIC = 0xA5
SERIAL_OVERHEAD = 6  # magic + type + len(2) + crc(2)
SERIAL_MAX_PAYLOAD = 2048

# ESP32 -> Pi
T_UPLINK = 0x01
T_HEALTH = 0x02
T_LOG = 0x03
T_ACK = 0x05
T_NODE_STATS = 0x06
# Pi -> ESP32 (control)
T_SET_ARMED = 0x81
T_SET_EPOCH = 0x82
T_SET_SLOTMAP = 0x83
T_SET_TIMING = 0x84
T_PING = 0x85

TYPE_NAMES = {
    T_UPLINK: "UPLINK",
    T_HEALTH: "HEALTH",
    T_LOG: "LOG",
    T_ACK: "ACK",
    T_NODE_STATS: "NODE_STATS",
    T_SET_ARMED: "SET_ARMED",
    T_SET_EPOCH: "SET_EPOCH",
    T_SET_SLOTMAP: "SET_SLOTMAP",
    T_SET_TIMING: "SET_TIMING",
    T_PING: "PING",
}


# --- CRC-16/CCITT-FALSE -----------------------------------------------------

def crc16_ccitt_false(data, start=0, end=None):
    """CRC-16/CCITT-FALSE.

    poly 0x1021, init 0xFFFF, no reflection, xorout 0x0000. check == 0x29B1.
    Computed over data[start:end].
    """
    if end is None:
        end = len(data)
    crc = 0xFFFF
    for i in range(start, end):
        crc ^= data[i] << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc & 0xFFFF


# --- Generic framing --------------------------------------------------------

def encode_frame(frame_type, payload):
    """Build a full wire frame: MAGIC, TYPE, LEN(LE), PAYLOAD, CRC16(LE)."""
    payload = bytes(payload)
    if len(payload) > SERIAL_MAX_PAYLOAD:
        raise ValueError("serial payload too large")
    out = bytearray()
    out.append(MAGIC)
    out.append(frame_type & 0xFF)
    out += struct.pack("<H", len(payload))
    out += payload
    crc = crc16_ccitt_false(out, 1, len(out))  # TYPE..PAYLOAD (skip MAGIC)
    out += struct.pack("<H", crc)
    return bytes(out)


# --- Control-frame encoders (Pi -> ESP32) -----------------------------------

def encode_set_armed(armed):
    """SET_ARMED: single byte, 1 = armed, 0 = disarmed."""
    return encode_frame(T_SET_ARMED, bytes([1 if armed else 0]))


def encode_set_epoch(frame_epoch_ms):
    """SET_EPOCH: u64 LE frame-epoch in ms UTC (0 = self-anchor from GPS)."""
    return encode_frame(
        T_SET_EPOCH, struct.pack("<Q", int(frame_epoch_ms) & 0xFFFFFFFFFFFFFFFF)
    )


def encode_set_timing(slot_count, toa_ms, guard_ms):
    """SET_TIMING: u8 slot_count, u16 toa_ms, u16 guard_ms (all LE)."""
    payload = struct.pack(
        "<BHH", slot_count & 0xFF, toa_ms & 0xFFFF, guard_ms & 0xFFFF
    )
    return encode_frame(T_SET_TIMING, payload)


def encode_set_slotmap(entries):
    """SET_SLOTMAP: u8 count then count x (u16 node_id, u8 slot).

    entries: iterable of (node_id, slot).
    """
    entries = list(entries)
    payload = bytearray([len(entries) & 0xFF])
    for node_id, slot in entries:
        payload += struct.pack("<HB", node_id & 0xFFFF, slot & 0xFF)
    return encode_frame(T_SET_SLOTMAP, payload)


def encode_ping():
    """PING: empty payload; requests a HEALTH frame from the base node."""
    return encode_frame(T_PING, b"")


# --- Telemetry decoders (ESP32 -> Pi) ---------------------------------------

def decode_ack(payload):
    """ACK: u8 ref_type, u8 code (0 = applied)."""
    if len(payload) != 2:
        return {"raw": payload.hex()}
    ref_type, code = payload[0], payload[1]
    return {
        "ref_type": ref_type,
        "ref_name": TYPE_NAMES.get(ref_type, "0x%02x" % ref_type),
        "code": code,
        "result": "applied" if code == 0 else "rejected(%d)" % code,
    }


def decode_health(payload):
    """HEALTH: 40-byte struct "<IIIIHBBBBBBQQ"."""
    if len(payload) != 40:
        return {"raw": payload.hex()}
    (fw_version, uptime_s, packets_rx_total, crc_errors_total,
     pps_chz, gps_fix, pps_locked, sats, slot_count, armed, _reserved,
     frame_epoch_ms, last_beacon_ms) = struct.unpack("<IIIIHBBBBBBQQ", payload)
    fw = "%d.%d.%d" % (
        (fw_version >> 16) & 0xFF, (fw_version >> 8) & 0xFF, fw_version & 0xFF
    )
    return {
        "fw_version": fw,
        "uptime_s": uptime_s,
        "packets_rx_total": packets_rx_total,
        "crc_errors_total": crc_errors_total,
        "pps_hz": pps_chz / 100.0,
        "gps_fix": gps_fix,
        "pps_locked": pps_locked,
        "sats": sats,
        "slot_count": slot_count,
        "armed": armed,
        "frame_epoch_ms": frame_epoch_ms,
        "last_beacon_ms": last_beacon_ms,
    }


def decode_uplink(payload):
    """UPLINK: 38-byte struct "<BHHiiHHHIHBhhQ"."""
    if len(payload) != 38:
        return {"raw": payload.hex()}
    (version, node_id, sequence, lat_e7, lon_e7, speed_cms, course_cdeg,
     battery_mv, gps_time, subsec_ms, flags, rssi_dbm, snr_cdb,
     rx_time_ms) = struct.unpack("<BHHiiHHHIHBhhQ", payload)
    return {
        "version": version,
        "node_id": node_id,
        "sequence": sequence,
        "lat": lat_e7 / 1e7,
        "lon": lon_e7 / 1e7,
        "speed_cms": speed_cms,
        "course_cdeg": course_cdeg,
        "battery_mv": battery_mv,
        "gps_time": gps_time,
        "subsec_ms": subsec_ms,
        "flags": flags,
        "rssi_dbm": rssi_dbm,
        "snr_db": snr_cdb / 100.0,
        "rx_time_ms": rx_time_ms,
        "ts_ms": gps_time * 1000 + subsec_ms,
    }


def decode_node_stats(payload):
    """NODE_STATS: u8 count then count x (u16 node_id, i16 rssi, u32 packets, u64 last_heard)."""
    if len(payload) < 1:
        return {"raw": payload.hex()}
    count = payload[0]
    entries = []
    off = 1
    for _ in range(count):
        if off + 16 > len(payload):
            break
        node_id, last_rssi, packets, last_heard = struct.unpack(
            "<HhIQ", payload[off:off + 16]
        )
        entries.append({
            "node_id": node_id,
            "last_rssi_dbm": last_rssi,
            "packets": packets,
            "last_heard_ms": last_heard,
        })
        off += 16
    return {"count": count, "entries": entries}


def decode_log(payload):
    """LOG: UTF-8 text."""
    try:
        return {"text": payload.decode("utf-8", "replace")}
    except Exception:
        return {"raw": payload.hex()}


def decode_payload(frame_type, payload):
    """Decode a payload by frame type. Unknown types return {"raw": hex}."""
    if frame_type == T_ACK:
        return decode_ack(payload)
    if frame_type == T_HEALTH:
        return decode_health(payload)
    if frame_type == T_UPLINK:
        return decode_uplink(payload)
    if frame_type == T_NODE_STATS:
        return decode_node_stats(payload)
    if frame_type == T_LOG:
        return decode_log(payload)
    return {"raw": payload.hex()}


# --- Robust incremental de-framer -------------------------------------------

class FrameParser:
    """Incremental de-framer.

    Tolerates partial reads and garbage between frames; resyncs on MAGIC.
    ``push(chunk)`` appends bytes and returns a list of ``(type, payload)``
    tuples for every complete, CRC-valid frame found. ``crc_errors`` counts
    discarded false-magic / bad-CRC bytes.
    """

    def __init__(self):
        self.buf = bytearray()
        self.crc_errors = 0

    def push(self, chunk):
        """Append a chunk; return list of (type, payload) for each valid frame."""
        self.buf += chunk
        frames = []
        cursor = 0
        n = len(self.buf)
        while True:
            # 1. resync to MAGIC
            while cursor < n and self.buf[cursor] != MAGIC:
                cursor += 1
            if n - cursor < 4:  # need magic + type + len
                break
            length = self.buf[cursor + 2] | (self.buf[cursor + 3] << 8)
            if length > SERIAL_MAX_PAYLOAD:
                self.crc_errors += 1  # impossible length -> false magic
                cursor += 1
                continue
            total = length + SERIAL_OVERHEAD
            if n - cursor < total:
                break  # need more bytes
            crc_calc = crc16_ccitt_false(self.buf, cursor + 1, cursor + 4 + length)
            crc_rx = (
                self.buf[cursor + 4 + length]
                | (self.buf[cursor + 5 + length] << 8)
            )
            if crc_calc == crc_rx:
                ftype = self.buf[cursor + 1]
                payload = bytes(self.buf[cursor + 4:cursor + 4 + length])
                frames.append((ftype, payload))
                cursor += total
            else:
                self.crc_errors += 1  # bad CRC -> drop leading magic, resync
                cursor += 1
        # keep only the unconsumed tail
        self.buf = self.buf[cursor:] if cursor < n else bytearray()
        return frames
