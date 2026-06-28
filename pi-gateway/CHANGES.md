# CHANGES / status — what's tested vs. what needs hardware or a live cloud

Honest accounting of confidence in each part of the gateway.

## Fully covered by automated tests (`python3 -m pytest -q`)

All 69 tests pass with no hardware and no network.

* **protocol.py** — CRC check vector, encode/parse round-trips for every frame
  type, resync after garbage, partial/byte-by-byte reads, impossible-length
  handling, known wire vectors, all `decode_*` field mappings.
* **store.py** — idempotent insert, sequence-wrap dedup policy, WAL mode,
  unsynced/mark-synced round-trip, unit conversions (cm/s→m/s, cdeg→deg),
  node-table updates, sync cursor, latest-per-node.
* **serial_io.py** — frame dispatch with decoded payloads, garbage interleaving,
  disconnect→reconnect resume, `connected` flips false immediately on drop,
  a decoder exception not killing the loop, replay-bytelog recovery, replay via
  `SerialReader.run`, `find_base_port` precedence/globs, `send()` connected and
  `NotConnectedError` when disconnected.
* **uplink.py** — happy path, drop-then-recover zero-loss, idempotent duplicate
  delivery, delay+backoff, poison record dead-lettering, transient-not-poison,
  resume-after-outage, high-water across small batches, stable idempotency key,
  JSON body shape.
* **downlink.py** (`tests/test_downlink.py`) —
  * the four `SET_*` frames are emitted **in order** with byte-correct payloads
    (decoded back via `protocol` to verify slot_count / slots / armed / timing);
  * change detection: identical state twice emits nothing on the 2nd poll; a
    changed slot map re-emits; `force=True` always re-emits;
  * change-detection fingerprint **persists across instances** (a restarted
    gateway with the same store + same state does not re-spam the base);
  * ACK handling: applied vs. rejected `T_ACK` recorded + surfaced in health;
  * serial disconnected (`send` raises `NotConnectedError`) does not crash the
    poll, sets `last_error`, and re-attempts on the next poll;
  * no active race (`204`) → `idle`, nothing sent.
* **main.py** (`tests/test_main_smoke.py`) — **full pipeline, no hardware, no
  cloud**: a byte-log of encoded `UPLINK` frames (built with
  `protocol.encode_frame`) is replayed through the real `Gateway`; fixes land
  in a tmp SQLite DB and the in-process `HealthState.snapshot()` (the same data
  `/health` serves) reports `total_fixes`, per-node last-heard, the cached base
  `HEALTH`, unsynced backlog, and uplink/downlink status.

## Manually verified (not in the pytest suite, but run during development)

* `python3 -m srt_gateway.main --replay capture.bin` as a real CLI process:
  parses 6 fixes into the DB, uplink reports `retry` (cloud unreachable, rows
  stay buffered → zero loss), downlink reports `idle`.
* `health.HealthServer` binds a real socket, serves `GET /health` as JSON over
  HTTP, and returns `404` for unknown paths.

## Needs real hardware (a base node on USB serial) to validate

* Live serial open of a real `/dev/ttyACM*` via `pyserial` (the lazy import
  path in `serial_io._open`). Tests use `FakeSerial`; the real `serial.Serial`
  construction is exercised only on the Pi.
* Auto-detect picking the correct device when multiple `/dev/ttyACM*` exist.
* Actual board brown-out / re-enumeration recovery timing (backoff is tested
  with an injected clock; real reconnection latency is not).
* Real `T_ACK` round-trip from the base after a `SET_*` (we test `record_ack`
  with synthesized ACKs; end-to-end serial ACK timing is hardware-dependent).
* Real base `T_HEALTH` / `T_NODE_STATS` cadence and field values.

## Needs a live cloud to validate

* The actual `POST /ingest` and `GET /race/current` HTTP contract against the
  Phase-2 cloud (status codes, idempotency on `id`, auth). The gateway side is
  tested with mock `http_post` / `http_get`; the stdlib `http_client.py`
  `urllib` calls are exercised only against a real endpoint.
* End-to-end downlink: cloud race change → base actually re-programmed →
  observable behaviour change on the water.

## Known simplifications / future work

* ACK handling is fire-and-forget with last-ACK tracking; there is no blocking
  await or automatic re-send of an individual rejected `SET_*` frame (a
  rejection is logged and surfaced in `/health`; the next changed/forced poll
  re-sends the whole state). Full per-frame await was explicitly out of scope.
* `frame_epoch_ms` is passed through as given; the gateway does not compute its
  own epoch (the base self-anchors from GPS when sent `0`).
* `boats`/`races` correlation is minimal (downlink mirrors race `state` only);
  boat↔node resolution is left to the cloud.

## Live bring-up fixes (2026-06-28)

- **store.py: SQLite cross-thread fix.** The serial reader runs in its own
  thread; the Store connection was main-thread-only, causing
  `sqlite3.ProgrammingError: SQLite objects created in a thread can only be
  used in that same thread` on the first decoded uplink — so zero fixes were
  ever persisted on real hardware. Fixed: open the connection with
  `check_same_thread=False` and serialize all access with a `threading.RLock`
  (wrapped `insert_fix`). Proven with a threaded smoke test (50 cross-thread
  inserts, 0 errors).
- **serial_io.py: harden poll_once against re-enumeration faults.** A board
  brown-out / USB re-enumeration can leave pyserial's handle in a half-valid
  state where `read()` raises `TypeError`/`OSError`/`AttributeError` on a
  NoneType fd/size rather than `SerialException`. Now any such read fault is
  treated as a disconnect → clean close → reconnect next cycle, so the reader
  thread never dies. Also coerce `read_size` to a valid int and guard against a
  concurrent `_close()` racing the read.
- **Verified end-to-end on hardware:** boat (node 1) → base (SX1276 TTGO) →
  USB serial → gateway → SQLite. Live fixes persisted; `/health` reports
  serial connected + per-node last-position. Installed + running as the
  `srt-gateway` systemd service (enabled, Restart=always).
