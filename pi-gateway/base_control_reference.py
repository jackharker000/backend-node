#!/usr/bin/env python3
"""
base_control.py — drive a Sail Race Tracker "base node" ESP32 over USB serial.

Standalone bench/live test tool that bypasses the cloud gateway and talks the
USB serial wire contract directly:

    [MAGIC 0xA5][TYPE u8][LEN u16 LE][PAYLOAD][CRC16 LE]

CRC is CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF, no reflection, xorout 0)
computed over TYPE ‖ LEN ‖ PAYLOAD (everything between MAGIC and the CRC).

This matches contracts/SERIAL_PROTOCOL.md and gateway/src/contract/serial.ts.

Dependencies: pyserial (only needed for actual port I/O; --selftest needs nothing
beyond the stdlib).

Usage examples:
    python3 base_control.py --port /dev/ttyACM0 arm \
        --slot-count 50 --toa 57 --guard 20 --slots 7:0,12:1,99:2
    python3 base_control.py --port /dev/ttyACM0 disarm
    python3 base_control.py --port /dev/ttyACM0 ping
    python3 base_control.py --selftest
"""

import argparse
import struct
import sys
import time

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
    """poly 0x1021, init 0xFFFF, no reflection, xorout 0x0000. check==0x29B1."""
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
    return encode_frame(T_SET_ARMED, bytes([1 if armed else 0]))


def encode_set_epoch(frame_epoch_ms):
    return encode_frame(T_SET_EPOCH, struct.pack("<Q", int(frame_epoch_ms) & 0xFFFFFFFFFFFFFFFF))


def encode_set_timing(slot_count, toa_ms, guard_ms):
    payload = struct.pack("<BHH", slot_count & 0xFF, toa_ms & 0xFFFF, guard_ms & 0xFFFF)
    return encode_frame(T_SET_TIMING, payload)


def encode_set_slotmap(entries):
    """entries: list of (node_id, slot)."""
    payload = bytearray([len(entries) & 0xFF])
    for node_id, slot in entries:
        payload += struct.pack("<HB", node_id & 0xFFFF, slot & 0xFF)
    return encode_frame(T_SET_SLOTMAP, payload)


def encode_ping():
    return encode_frame(T_PING, b"")


# --- Telemetry decoders (ESP32 -> Pi) ---------------------------------------

def decode_ack(payload):
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
    if len(payload) != 40:
        return {"raw": payload.hex()}
    (fw_version, uptime_s, packets_rx_total, crc_errors_total,
     pps_chz, gps_fix, pps_locked, sats, slot_count, armed, _reserved,
     frame_epoch_ms, last_beacon_ms) = struct.unpack("<IIIIHBBBBBBQQ", payload)
    fw = "%d.%d.%d" % ((fw_version >> 16) & 0xFF, (fw_version >> 8) & 0xFF, fw_version & 0xFF)
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
    }


def decode_node_stats(payload):
    if len(payload) < 1:
        return {"raw": payload.hex()}
    count = payload[0]
    entries = []
    off = 1
    for _ in range(count):
        if off + 16 > len(payload):
            break
        node_id, last_rssi, packets, last_heard = struct.unpack("<HhIQ", payload[off:off + 16])
        entries.append({
            "node_id": node_id,
            "last_rssi_dbm": last_rssi,
            "packets": packets,
            "last_heard_ms": last_heard,
        })
        off += 16
    return {"count": count, "entries": entries}


def decode_payload(frame_type, payload):
    if frame_type == T_ACK:
        return decode_ack(payload)
    if frame_type == T_HEALTH:
        return decode_health(payload)
    if frame_type == T_UPLINK:
        return decode_uplink(payload)
    if frame_type == T_NODE_STATS:
        return decode_node_stats(payload)
    if frame_type == T_LOG:
        try:
            return {"text": payload.decode("utf-8", "replace")}
        except Exception:
            return {"raw": payload.hex()}
    return {"raw": payload.hex()}


# --- Robust incremental de-framer -------------------------------------------

class FrameParser:
    """Tolerates partial reads, garbage between frames, resyncs on MAGIC."""

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
            crc_rx = self.buf[cursor + 4 + length] | (self.buf[cursor + 5 + length] << 8)
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


# --- Serial I/O -------------------------------------------------------------

def open_port(port, baud):
    try:
        import serial  # pyserial
    except ImportError:
        sys.exit("ERROR: pyserial not installed. Run: pip install --break-system-packages pyserial")
    return serial.Serial(port, baudrate=baud, timeout=0.1)


def send_frame(ser, frame, label):
    print(">> %-12s %s" % (label, frame.hex()))
    ser.write(frame)
    ser.flush()


def read_responses(ser, duration_s):
    """Read and decode incoming frames for ~duration_s seconds."""
    parser = FrameParser()
    deadline = time.time() + duration_s
    got = 0
    while time.time() < deadline:
        chunk = ser.read(256)
        if chunk:
            for ftype, payload in parser.push(chunk):
                got += 1
                name = TYPE_NAMES.get(ftype, "0x%02x" % ftype)
                decoded = decode_payload(ftype, payload)
                print("<< %-12s %s" % (name, format_decoded(decoded)))
        else:
            time.sleep(0.02)
    if got == 0:
        print("   (no response frames received in %.1fs)" % duration_s)
    if parser.crc_errors:
        print("   (%d CRC/resync errors while reading)" % parser.crc_errors)


def format_decoded(d):
    return ", ".join("%s=%s" % (k, v) for k, v in d.items())


# --- Slot parsing -----------------------------------------------------------

def parse_slots(s):
    """Parse '7:0,12:1,99:2' into [(7,0),(12,1),(99,2)]."""
    entries = []
    if not s:
        return entries
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise argparse.ArgumentTypeError("bad slot '%s' (want node_id:slot)" % part)
        node_s, slot_s = part.split(":", 1)
        entries.append((int(node_s, 0), int(slot_s, 0)))
    return entries


# --- Self test against committed vectors ------------------------------------

VECTORS = {
    # CRC check vectors (crc.json)
    "crc_ascii_123456789": ("crc", b"123456789", 0x29B1),
    "crc_hex_uplink": ("crc",
                       bytes.fromhex("013412cdab803323e7f80a3c6814025046930fc0235e68ee0203"),
                       0xE00E),
    # frame vectors (serial_*.json + SERIAL_PROTOCOL.md inline)
    "serial_set_armed":   ("frame", encode_set_armed(True), "a5810100015d08"),
    "serial_set_epoch":   ("frame", encode_set_epoch(1751000000000), "a582080000a6bbaf970100006604"),
    "serial_set_slotmap": ("frame", encode_set_slotmap([(7, 0), (12, 5), (99, 49)]),
                           "a5830a00030700000c0005630031c01c"),
    "serial_set_timing":  ("frame", encode_set_timing(50, 57, 20), "a5840500323900140031e2"),
}


def selftest():
    ok = True
    print("=== base_control.py selftest (validating against committed vectors) ===")
    for name, spec in VECTORS.items():
        kind = spec[0]
        if kind == "crc":
            _, data, expected = spec
            got = crc16_ccitt_false(data)
            passed = got == expected
            print("[%s] %-22s crc=0x%04X expected=0x%04X" %
                  ("PASS" if passed else "FAIL", name, got, expected))
        else:
            _, frame, expected_hex = spec
            got_hex = frame.hex()
            passed = got_hex == expected_hex.lower()
            print("[%s] %-22s %s%s" %
                  ("PASS" if passed else "FAIL", name, got_hex,
                   "" if passed else "  (expected %s)" % expected_hex))
        ok = ok and passed

    # extra: PING frame is well-formed (a5 85 00 00 <crc>)
    ping = encode_ping()
    ping_ok = ping[:4] == bytes([0xA5, 0x85, 0x00, 0x00]) and len(ping) == 6
    print("[%s] %-22s %s" % ("PASS" if ping_ok else "FAIL", "ping_frame_shape", ping.hex()))
    ok = ok and ping_ok

    print("=== %s ===" % ("ALL VECTORS PASS" if ok else "SELFTEST FAILED"))
    return 0 if ok else 1


# --- CLI --------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        description="Drive a Sail Race Tracker base-node ESP32 over USB serial.")
    p.add_argument("--port", help="serial port, e.g. /dev/ttyACM0")
    p.add_argument("--baud", type=int, default=115200, help="baud rate (default 115200)")
    p.add_argument("--listen", type=float, default=2.0,
                   help="seconds to read responses after sending (default 2.0)")
    p.add_argument("--selftest", action="store_true",
                   help="encode known vectors and assert they match; no port needed")

    sub = p.add_subparsers(dest="cmd")

    pa = sub.add_parser("arm", help="SET_TIMING, SET_EPOCH(0), SET_SLOTMAP, SET_ARMED(1)")
    pa.add_argument("--slot-count", type=int, required=True)
    pa.add_argument("--toa", type=int, required=True, help="time-on-air ms")
    pa.add_argument("--guard", type=int, required=True, help="guard interval ms")
    pa.add_argument("--slots", default="", help="node_id:slot,... e.g. 7:0,12:1,99:2")
    pa.add_argument("--epoch", type=int, default=0,
                    help="epoch ms UTC; 0 = let ESP32 self-anchor from GPS (default 0)")

    sub.add_parser("disarm", help="SET_ARMED(0)")
    sub.add_parser("ping", help="PING (request a HEALTH frame)")

    return p


def main(argv):
    args = build_parser().parse_args(argv)

    if args.selftest:
        return selftest()

    if not args.cmd:
        print("ERROR: no command. Use arm / disarm / ping, or --selftest.", file=sys.stderr)
        return 2
    if not args.port:
        print("ERROR: --port is required for arm/disarm/ping.", file=sys.stderr)
        return 2

    ser = open_port(args.port, args.baud)
    try:
        if args.cmd == "arm":
            slots = parse_slots(args.slots)
            send_frame(ser, encode_set_timing(args.slot_count, args.toa, args.guard), "SET_TIMING")
            send_frame(ser, encode_set_epoch(args.epoch), "SET_EPOCH")
            send_frame(ser, encode_set_slotmap(slots), "SET_SLOTMAP")
            send_frame(ser, encode_set_armed(True), "SET_ARMED")
        elif args.cmd == "disarm":
            send_frame(ser, encode_set_armed(False), "SET_ARMED")
        elif args.cmd == "ping":
            send_frame(ser, encode_ping(), "PING")

        print("-- reading responses for %.1fs --" % args.listen)
        read_responses(ser, args.listen)
    finally:
        ser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
