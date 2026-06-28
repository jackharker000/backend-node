# backend-node

Backend services for the **Sail Race Tracker**.

## `pi-gateway/` — Raspberry Pi gateway (`srt-gateway`)

Phase 1: the bridge between the on-boat LoRa hardware and the cloud. Reads decoded
boat positions off the base ESP32 over USB serial, stores them locally in SQLite
(authoritative record), forwards them to the cloud with store-and-forward
(zero-loss / zero-duplicate), and pushes race-control commands from the cloud back
down to the base ESP32 over serial.

- Build/run/install: see `pi-gateway/README.md`.
- Runs with **no hardware** via `python3 -m srt_gateway.main --replay <bytelog>`.
- Full test suite: `cd pi-gateway && python3 -m pytest -q` (69 tests).
- The base↔Pi serial protocol and the cloud HTTP contract are documented in
  `pi-gateway/README.md` (the cloud contract is what Phase 2 / Cloudflare implements).
