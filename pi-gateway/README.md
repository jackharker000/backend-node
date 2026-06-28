# srt-gateway — Sail Race Tracker Raspberry Pi gateway

The gateway runs on a Raspberry Pi attached to a Sail Race Tracker **base
node** (an ESP32) over USB serial. It is the bridge between the on-water LoRa
mesh and the cloud:

* **Uplink** — decodes boat-position fixes the base node receives over the air
  and forwards them to the cloud, with a durable local SQLite buffer so nothing
  is lost across network drops.
* **Downlink** — pulls the authoritative race configuration from the cloud and
  programs the base node's TDMA schedule (timing, epoch, slot map, armed state)
  over serial.
* **Health** — exposes a local `GET /health` JSON endpoint aggregating the
  state of every subsystem.

It is deliberately near-stdlib: the only hard runtime dependency is
`pyserial` (live serial I/O). Cloud HTTP uses stdlib `urllib`; TOML config uses
stdlib `tomllib` (3.11+) or the `tomli` backport on older Pythons.

## Architecture

```
                 +-----------------------------------------------+
   base node     |                 srt-gateway                   |     cloud
   (ESP32)       |                                               |
   USB serial    |  serial_io.SerialReader  --on_frame-->        |
   <===========> |     T_UPLINK   -> store.insert_fix            |
                 |     T_HEALTH   -> cache base health           |
                 |     T_ACK      -> downlink.record_ack         |
                 |     T_NODE_STATS/T_LOG -> log/upsert_node     |
                 |                                               |
                 |  store.Store (SQLite WAL, durable buffer)     |
                 |        |                    ^                  |
                 |        v                    |                  |
                 |  uplink.CloudUplinker  --POST /ingest----------+---> /ingest
                 |  downlink.CloudDownlinker <--GET /race/current-+<--- /race/current
                 |        |  (SET_TIMING/EPOCH/SLOTMAP/ARMED)     |
                 |        v                                       |
                 |  serial_io.SerialReader.send  ============>    | (to base)
                 |                                               |
                 |  health.HealthServer  -- GET /health (:8080)  |
                 +-----------------------------------------------+
```

`main.py` (the **supervisor**) wires these together, runs each in its own
background thread with crash isolation, and shuts down cleanly on SIGINT/SIGTERM.

### Modules

| Module          | Responsibility                                                    |
|-----------------|-------------------------------------------------------------------|
| `protocol.py`   | Wire codec: framing, CRC-16/CCITT-FALSE, encoders/decoders, parser |
| `store.py`      | Durable SQLite (WAL) buffer; idempotent fix insert; node/race state |
| `serial_io.py`  | Serial read loop, auto-reconnect, replay, `send()` for downlink    |
| `uplink.py`     | Store-and-forward to cloud `/ingest`; zero-loss, dead-letter poison |
| `downlink.py`   | Pull race state from `/race/current`; program base with SET_* frames |
| `http_client.py`| Stdlib `urllib` POST/GET returning a `requests`-like response       |
| `config.py`     | Defaults <- TOML <- `SRT_GATEWAY_*` env vars                        |
| `health.py`     | Stdlib `http.server` `GET /health` aggregator                      |
| `main.py`       | Supervisor: wiring, threads, crash isolation, CLI                  |

## Install on the Pi

```bash
# 1. System deps
sudo apt-get update && sudo apt-get install -y python3 python3-pip

# 2. Create the service user and give it serial access (dialout)
sudo useradd --system --create-home --home-dir /home/base-node base-node || true
sudo usermod -aG dialout base-node

# 3. Get the code and install the package (pulls pyserial)
sudo -u base-node git clone <repo> /home/base-node/srt-gateway
cd /home/base-node/srt-gateway
sudo pip3 install .          # installs the `srt-gateway` console script + deps
#   (or, without installing: run with PYTHONPATH=/home/base-node/srt-gateway/src)

# 4. Config
sudo mkdir -p /etc/srt-gateway
sudo cp config.toml.example /etc/srt-gateway/config.toml
sudo nano /etc/srt-gateway/config.toml      # set cloud_url, device, db_path

# 5. Install + start the systemd unit
sudo cp systemd/srt-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now srt-gateway
sudo systemctl status srt-gateway
journalctl -u srt-gateway -f                 # follow logs
```

The cloud API key is best injected out-of-band rather than committed to the
config file:

```bash
sudo systemctl edit srt-gateway
# [Service]
# Environment=SRT_GATEWAY_CLOUD_API_KEY=your-secret-key
```

## Configuration

Resolution order (later wins): **defaults → TOML file → `SRT_GATEWAY_*` env**.
See `config.toml.example` for the annotated file.

| Key                   | Env var                          | Default                              |
|-----------------------|----------------------------------|--------------------------------------|
| `serial_device`       | `SRT_GATEWAY_SERIAL_DEVICE`      | `/dev/ttyACM1` (`auto` = auto-detect)|
| `serial_baud`         | `SRT_GATEWAY_SERIAL_BAUD`        | `115200`                             |
| `cloud_url`           | `SRT_GATEWAY_CLOUD_URL`          | `http://localhost:8787`              |
| `cloud_api_key`       | `SRT_GATEWAY_CLOUD_API_KEY`      | `""`                                 |
| `ingest_interval_s`   | `SRT_GATEWAY_INGEST_INTERVAL_S`  | `5`                                  |
| `downlink_interval_s` | `SRT_GATEWAY_DOWNLINK_INTERVAL_S`| `10`                                 |
| `db_path`             | `SRT_GATEWAY_DB_PATH`            | `/home/base-node/srt-gateway/srt.db` |
| `health_port`         | `SRT_GATEWAY_HEALTH_PORT`        | `8080`                               |
| `log_level`           | `SRT_GATEWAY_LOG_LEVEL`          | `INFO`                               |

`serial_device = "auto"` (or empty) auto-detects the first `/dev/ttyACM*` then
`/dev/ttyUSB*`.

## Running

```bash
# Normal (live serial + cloud), reading the installed config:
srt-gateway --config /etc/srt-gateway/config.toml
# equivalently:
python3 -m srt_gateway.main --config /etc/srt-gateway/config.toml

# One ingest+downlink cycle then exit (smoke test against a real cloud):
python3 -m srt_gateway.main --config /etc/srt-gateway/config.toml --once

# Override log level:
python3 -m srt_gateway.main --config ... --log-level DEBUG
```

### Replay demo (no hardware, no cloud)

The whole ingest pipeline can be proven with **no base node and no cloud** by
replaying a captured byte-log of serial traffic through the exact live decode
path:

```bash
# Generate a small capture of UPLINK frames (or use one captured off the wire):
python3 - <<'PY'
import struct, sys; sys.path.insert(0, "src")
from srt_gateway import protocol
frames = bytearray()
for node in (7, 12, 99):
    for seq in (1, 2):
        p = struct.pack("<BHHiiHHHIHBhhQ", 1, node, seq,
                        515000000+node, -1000000+seq, 250, 18000, 3700,
                        1700000000+seq, 0, 0, -90, 750, (1700000000+seq)*1000)
        frames += protocol.encode_frame(protocol.T_UPLINK, p)
open("capture.bin", "wb").write(frames)
PY

# Replay it: frames are parsed and stored, uplink/downlink run one pass.
PYTHONPATH=src SRT_GATEWAY_DB_PATH=/tmp/demo.db SRT_GATEWAY_HEALTH_PORT=0 \
  python3 -m srt_gateway.main --replay capture.bin

# Inspect what landed:
sqlite3 /tmp/demo.db "SELECT node_id, seq, lat, lon FROM fixes;"
```

With no cloud reachable, the uplink reports `retry` (rows stay buffered,
zero loss) and the downlink reports `idle` — exactly the correct behaviour.

## `/health`

`GET http://<pi>:8080/health` returns a JSON snapshot aggregating every
subsystem (computed fresh per request):

```json
{
  "ok": true,
  "uptime_s": 1234.5,
  "ts_ms": 1700000000000,
  "serial":   { "connected": true, "port": "/dev/ttyACM1",
                "frames_total": 9001, "reconnects": 0, "last_error": null },
  "store":    { "total_fixes": 12000, "unsynced": 3, "node_count": 3,
                "nodes": [ { "node_id": 7, "ts_ms": ..., "lat": ..., "lon": ...,
                             "battery_mv": 3700, "rssi": -90 } ] },
  "uplink":   { "pending": 3, "last_sync_ok": 1700000000.0, "last_error": null,
                "dead_letter_count": 0, "total_synced": 11997 },
  "downlink": { "last_poll_status": "applied", "last_error": null,
                "last_downlink_ok": { "...applied race state..." },
                "last_acks": { "SET_ARMED": { "result": "applied", "code": 0 } } },
  "base":     { "fw_version": "1.2.3", "sats": 9, "armed": 1, "...": "..." }
}
```

`ok` stays `true` while the gateway is storing-and-forwarding even if the
serial link or cloud is momentarily down (that is normal, recoverable
operation); it only flips `false` on an internal store error.

## Cloud HTTP contract (Phase 2)

The gateway is the client; the cloud implements these two endpoints. Both
expect `Authorization: Bearer <cloud_api_key>` and `Content-Type:
application/json`.

### `POST /ingest` — boat fixes go up

Body:

```json
{
  "fixes": [
    {
      "id":         12345,        // stable per-fix idempotency key (gateway PK)
      "node_id":    7,
      "seq":        1024,         // u16 sequence (wraps)
      "ts_ms":      1700000000123,// fix time, UTC ms (gps_time*1000+subsec_ms)
      "lat":        51.5074,
      "lon":        -0.1278,
      "sog":        2.5,          // speed over ground, m/s
      "cog":        180.0,        // course over ground, deg
      "battery_mv": 3700,
      "rssi":       -90,          // dBm
      "snr":        7.5,          // dB
      "flags":      0
    }
    // ... up to batch_size fixes
  ]
}
```

**Idempotency:** `id` is the gateway's stable primary key for that fix. The
cloud MUST dedup on `id` so that re-delivering the same fix (after a lost ACK,
a retry, or a gateway crash between "accepted" and "marked synced") is a
no-op. This is what makes the store-and-forward zero-loss **and**
zero-duplicate.

**Response semantics the gateway relies on:**

| Status      | Gateway action                                                         |
|-------------|------------------------------------------------------------------------|
| `2xx`       | Whole batch accepted → mark those fixes synced.                        |
| `429`, `5xx`| Transient → keep fixes un-synced, retry with exponential backoff.      |
| `4xx`       | Malformed/poison → gateway re-POSTs each fix individually; any fix that *still* gets a 4xx is quarantined to a local `dead_letter` table (surfaced as `dead_letter_count` in `/health`) so one bad row never wedges the queue. |

A transport failure (connection refused, DNS, timeout) is treated as
transient (retry). The cloud should return a normal HTTP status for
application errors rather than dropping the connection.

### `GET /race/current` — race config comes down

Response:

```json
{
  "race_id":        42,
  "state":          "armed",     // "idle" | "armed" | "running" | ...
  "armed":          true,        // master go/no-go for the base
  "slot_count":     50,          // TDMA slots per frame
  "toa_ms":         57,          // time-on-air budget per slot (ms)
  "guard_ms":       20,          // guard interval per slot (ms)
  "frame_epoch_ms": 0,           // optional; 0/absent => base self-anchors from GPS
  "slots": [
    { "node_id": 7,  "slot": 0 },
    { "node_id": 12, "slot": 1 },
    { "node_id": 99, "slot": 2 }
  ]
}
```

Return `204 No Content` (or any non-2xx) when there is no active race; the
gateway then does nothing (does not disarm the base on its own).

**How the gateway applies it.** On each `downlink_interval_s` poll the gateway
fetches this document, computes a fingerprint of `(slot_count, toa_ms,
guard_ms, frame_epoch_ms, armed, sorted slots)`, and **only when the
fingerprint changes** (or on first poll after start) translates it into four
serial frames sent to the base **in this exact order**:

1. `SET_TIMING(slot_count, toa_ms, guard_ms)` — define the frame geometry
2. `SET_EPOCH(frame_epoch_ms or 0)` — anchor time (`0` = base self-anchors)
3. `SET_SLOTMAP([(node_id, slot), ...])` — who transmits when
4. `SET_ARMED(armed)` — go/no-go, applied **last**

Arming last guarantees the base knows the geometry, time anchor and slot
assignments before it starts gating transmissions. The base ACKs each frame
asynchronously with a `T_ACK` (reported in `/health` under
`downlink.last_acks`); a rejected ACK is logged loudly. If the serial link is
down when a change needs sending, the gateway logs it and retries on the next
poll (the cached fingerprint is not advanced until all four frames are sent).

## Development / tests

```bash
pip install --break-system-packages pytest pyserial   # if needed
cd srt-gateway
python3 -m pytest -q
```

See `CHANGES.md` for what is covered by automated tests vs. what still needs
real hardware / a live cloud to validate.
