"""Tests for the cloud store-and-forward uplink (srt_gateway.uplink).

These prove the safety-critical guarantee: ZERO LOSS, ZERO DUPLICATES across
network drops, duplicate deliveries and delays. We drive the uplink against a
``MockCloud`` that models the real /ingest as an *idempotent set keyed on fix
id*, and can be configured to drop / delay / duplicate / 4xx specific calls.
"""

import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
)

from srt_gateway.store import Store          # noqa: E402
from srt_gateway.uplink import CloudUplinker  # noqa: E402


# --------------------------------------------------------------------------- #
# Test helpers                                                                #
# --------------------------------------------------------------------------- #

class FakeResponse:
    """Minimal response-like: .status_code and .json()."""

    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class MockCloud:
    """An idempotent /ingest endpoint modelled as a set keyed on fix id.

    Records every fix id it has *durably ingested* in ``self.ingested`` (a set,
    so re-delivery is harmless — the core of idempotency). ``self.received_log``
    keeps the raw per-call id lists so tests can assert a duplicate delivery
    really did reach the server twice yet had no duplicate effect.

    Failure injection — each is keyed by the (1-based) call index:

      drop_calls:   set of call numbers that raise ConnectionError (cloud drop)
      timeout_calls:set of call numbers that raise TimeoutError
      status_calls: {call_number: status_code} to force an HTTP status
      dup_calls:    set of call numbers whose payload is ingested TWICE
                    (models a duplicate delivery / lost-ack-then-retry)
      poison_ids:   set of fix ids that ALWAYS get a 400 when present in a
                    batch (a persistently malformed/poison record). A batch
                    containing any poison id returns 400.
    """

    def __init__(self, *, drop_calls=None, timeout_calls=None,
                 status_calls=None, dup_calls=None, poison_ids=None):
        self.ingested = set()
        self.received_log = []        # list of lists of ids, one per accepted call
        self.call_count = 0
        self.drop_calls = set(drop_calls or ())
        self.timeout_calls = set(timeout_calls or ())
        self.status_calls = dict(status_calls or {})
        self.dup_calls = set(dup_calls or ())
        self.poison_ids = set(poison_ids or ())

    def post(self, url, json_body, headers):
        self.call_count += 1
        n = self.call_count

        # Sanity: auth + endpoint shape.
        assert url.endswith("/ingest"), url
        assert headers.get("Authorization", "").startswith("Bearer "), headers

        # --- injected transport failures (raise BEFORE any ingest) --------- #
        if n in self.drop_calls:
            raise ConnectionError("simulated cloud drop on call %d" % n)
        if n in self.timeout_calls:
            raise TimeoutError("simulated timeout on call %d" % n)

        fixes = json_body["fixes"]
        ids = [f["id"] for f in fixes]

        # --- injected HTTP status (5xx transient) -------------------------- #
        if n in self.status_calls and self.status_calls[n] >= 500:
            # 5xx: nothing ingested.
            return FakeResponse(self.status_calls[n], {"error": "server"})

        # --- poison detection -> 400 (nothing ingested) -------------------- #
        bad = [i for i in ids if i in self.poison_ids]
        if bad:
            return FakeResponse(400, {"error": "bad fix", "bad_ids": bad})

        # --- forced 4xx for a specific call (non-poison path) -------------- #
        if n in self.status_calls and 400 <= self.status_calls[n] < 500:
            return FakeResponse(self.status_calls[n], {"error": "client"})

        # --- happy path: idempotent ingest into the set ------------------- #
        self._ingest(ids)
        if n in self.dup_calls:
            # Model a duplicate delivery: the very same payload lands twice.
            self._ingest(ids)

        high_water = max(self.ingested) if self.ingested else 0
        return FakeResponse(
            200, {"accepted": len(ids), "high_water": high_water}
        )

    def _ingest(self, ids):
        # Idempotent: a set absorbs duplicates with no double effect.
        for i in ids:
            self.ingested.add(i)
        self.received_log.append(list(ids))


def make_uplink(node_id=7, sequence=0, ts_ms=1_700_000_000_000,
                speed_cms=532, course_cdeg=18000, battery_mv=4012,
                rssi_dbm=-95, snr_db=7.5, flags=0, lat=51.5, lon=-1.25,
                rx_time_ms=123456789):
    """Decoded-uplink dict shaped like protocol.decode_uplink output."""
    return {
        "version": 1, "node_id": node_id, "sequence": sequence,
        "lat": lat, "lon": lon, "speed_cms": speed_cms,
        "course_cdeg": course_cdeg, "battery_mv": battery_mv,
        "gps_time": ts_ms // 1000, "subsec_ms": ts_ms % 1000,
        "flags": flags, "rssi_dbm": rssi_dbm, "snr_db": snr_db,
        "rx_time_ms": rx_time_ms, "ts_ms": ts_ms,
    }


@pytest.fixture
def store(tmp_path):
    s = Store(str(tmp_path / "gw.db"))
    yield s
    s.close()


def seed(store, n, node_id=7, base=1_700_000_000_000):
    """Insert n fresh fixes (seqs 0..n-1, 1s apart) -> all un-synced."""
    for i in range(n):
        assert store.insert_fix(
            make_uplink(node_id=node_id, sequence=i, ts_ms=base + i * 1000)
        ) is True


def make_uplinker(store, cloud, **kw):
    return CloudUplinker(
        store, cloud.post, "https://cloud.example/api", "secret-key",
        sleep=lambda s: None, clock=lambda: 1000.0, **kw,
    )


def all_ids(store):
    import sqlite3
    conn = sqlite3.connect(store.db_path)
    conn.row_factory = sqlite3.Row
    ids = [r["id"] for r in conn.execute("SELECT id FROM fixes ORDER BY id")]
    conn.close()
    return ids


# --------------------------------------------------------------------------- #
# 1. Happy path                                                               #
# --------------------------------------------------------------------------- #

def test_happy_path_all_synced(store):
    seed(store, 10)
    cloud = MockCloud()
    up = make_uplinker(store, cloud)

    stats = up.run_once()

    assert stats["status"] == "ok"
    assert stats["synced"] == 10
    assert store.unsynced_count() == 0
    # Server has exactly the 10 fixes, once each.
    assert cloud.ingested == set(all_ids(store))
    assert len(cloud.ingested) == 10
    assert up.health()["total_synced"] == 10
    assert up.health()["pending"] == 0


# --------------------------------------------------------------------------- #
# 2. Cloud DROPS -> zero loss, later run syncs                                #
# --------------------------------------------------------------------------- #

def test_drop_then_recover_zero_loss(store):
    seed(store, 10)
    # First call drops (ConnectionError); also test a 503 path on a 2nd attempt.
    cloud = MockCloud(drop_calls={1}, status_calls={2: 503})
    up = make_uplinker(store, cloud)

    # Attempt 1: connection error -> nothing marked.
    s1 = up.run_once()
    assert s1["status"] == "retry" and s1["retryable"] is True
    assert s1["synced"] == 0
    assert store.unsynced_count() == 10        # ZERO LOSS: still all pending
    assert cloud.ingested == set()             # cloud got nothing

    # Attempt 2: 503 -> still nothing marked.
    s2 = up.run_once()
    assert s2["status"] == "retry"
    assert store.unsynced_count() == 10

    # Attempt 3: cloud healthy -> all sync.
    s3 = up.run_once()
    assert s3["status"] == "ok"
    assert store.unsynced_count() == 0
    assert len(cloud.ingested) == 10


# --------------------------------------------------------------------------- #
# 3. Cloud DUPLICATES the delivery -> idempotent, no double-mark/no loss      #
# --------------------------------------------------------------------------- #

def test_duplicate_delivery_is_idempotent(store):
    seed(store, 10)
    # Call 1 delivers the batch TWICE server-side (lost-ack-then-retry style).
    cloud = MockCloud(dup_calls={1})
    up = make_uplinker(store, cloud)

    stats = up.run_once()
    assert stats["status"] == "ok"

    # Server received the payload twice (proves the duplicate really happened)...
    assert len(cloud.received_log) == 2
    assert cloud.received_log[0] == cloud.received_log[1]
    # ...yet the idempotent set has each fix exactly once.
    assert len(cloud.ingested) == 10
    assert cloud.ingested == set(all_ids(store))

    # And locally every fix is synced exactly once (no double mark, no loss).
    assert store.unsynced_count() == 0
    assert up.health()["total_synced"] == 10

    # Re-running is harmless (nothing left, no extra server effect).
    again = up.run_once()
    assert again["status"] == "idle"
    assert len(cloud.ingested) == 10


# --------------------------------------------------------------------------- #
# 4. Cloud DELAYS then succeeds (timeout, then ok) via backoff loop           #
# --------------------------------------------------------------------------- #

def test_delay_then_success_via_backoff(store):
    seed(store, 8)
    # First call times out, second succeeds.
    cloud = MockCloud(timeout_calls={1})
    sleeps = []
    up = CloudUplinker(
        store, cloud.post, "https://cloud.example/api", "secret-key",
        sleep=lambda s: sleeps.append(s), clock=lambda: 1000.0,
        backoff_base=1.0, backoff_factor=2.0, backoff_max=60.0,
    )

    # Drive the loop: iter1 = timeout(retry+backoff), iter2 = ok,
    # iter3 = idle(stop).
    up.run_forever(interval=0.0, max_iterations=3)

    assert store.unsynced_count() == 0
    assert len(cloud.ingested) == 8
    # We backed off once after the timeout.
    assert sleeps and sleeps[0] == 1.0


# --------------------------------------------------------------------------- #
# 5. Poison record dead-lettered, rest of queue still flows                   #
# --------------------------------------------------------------------------- #

def test_poison_record_dead_lettered(store):
    seed(store, 10)
    ids = all_ids(store)
    poison = ids[3]   # one specific fix is malformed -> persistent 400.
    cloud = MockCloud(poison_ids={poison})
    up = make_uplinker(store, cloud)

    stats = up.run_once()

    # The batch 400'd, was bisected to 1-fix posts, poison isolated.
    assert stats["status"] == "partial"
    assert stats["dead_lettered"] == 1
    assert stats["synced"] == 9

    # Queue is NOT blocked: the 9 good fixes are synced & in the cloud.
    assert store.unsynced_count() == 0          # poison left the live queue too
    assert cloud.ingested == set(ids) - {poison}
    assert poison not in cloud.ingested

    # Poison is quarantined in dead_letter and surfaced in /health.
    assert up.dead_letter_count() == 1
    assert up.health()["dead_letter_count"] == 1

    # Re-running does not resurrect the poison nor re-ingest the good fixes.
    again = up.run_once()
    assert again["status"] == "idle"
    assert cloud.ingested == set(ids) - {poison}


def test_poison_does_not_dead_letter_on_transient(store):
    """A 5xx must NEVER be dead-lettered — it is transient, not poison."""
    seed(store, 5)
    cloud = MockCloud(status_calls={1: 503})
    up = make_uplinker(store, cloud)

    s = up.run_once()
    assert s["status"] == "retry"
    assert up.dead_letter_count() == 0          # nothing quarantined
    assert store.unsynced_count() == 5          # all still pending


# --------------------------------------------------------------------------- #
# 6. Resume after a multi-call outage -> catch up, exactly once               #
# --------------------------------------------------------------------------- #

def test_resume_after_outage(store):
    seed(store, 50)
    # Cloud down for the first 3 run_once calls (drop, drop, 503), then up.
    cloud = MockCloud(drop_calls={1, 2}, status_calls={3: 500})
    up = make_uplinker(store, cloud, batch_size=100)

    # 3 failed attempts: zero loss the whole way.
    for _ in range(3):
        s = up.run_once()
        assert s["status"] == "retry"
        assert store.unsynced_count() == 50
        assert cloud.ingested == set()

    # Cloud recovers: catch up.
    s = up.run_once()
    assert s["status"] == "ok"
    assert s["synced"] == 50
    assert store.unsynced_count() == 0

    # Exactly 50 unique fixes server-side, none lost, none duplicated.
    assert len(cloud.ingested) == 50
    assert cloud.ingested == set(all_ids(store))
    assert up.health()["total_synced"] == 50


def test_resume_small_batches_high_water(store):
    """High-water (synced flag) drives resumable catch-up across batches."""
    seed(store, 50)
    cloud = MockCloud()
    up = make_uplinker(store, cloud, batch_size=10)

    synced_total = 0
    for _ in range(5):
        s = up.run_once()
        assert s["status"] == "ok"
        synced_total += s["synced"]
    assert synced_total == 50
    assert store.unsynced_count() == 0
    assert len(cloud.ingested) == 50
    # Informational high-water cursor advanced to the max id.
    assert store.get_sync_cursor("uplink_hwm") == max(all_ids(store))


# --------------------------------------------------------------------------- #
# 7. Idempotency-key stability                                                #
# --------------------------------------------------------------------------- #

def test_idempotency_key_is_stable_fix_id(store):
    seed(store, 3)
    captured = []

    def capturing_post(url, json_body, headers):
        captured.append([f["id"] for f in json_body["fixes"]])
        return FakeResponse(200, {"accepted": len(json_body["fixes"]),
                                  "high_water": 0})

    up = CloudUplinker(
        store, capturing_post, "https://cloud.example/api", "secret-key",
        sleep=lambda s: None, clock=lambda: 1000.0,
    )

    db_ids = all_ids(store)
    # POST once.
    up.run_once()
    # Force a re-POST of the same rows by clearing synced behind the scenes.
    import sqlite3
    conn = sqlite3.connect(store.db_path)
    conn.execute("UPDATE fixes SET synced=0")
    conn.commit()
    conn.close()
    up.run_once()

    # Both POSTs carried the SAME stable per-fix ids (the DB primary key),
    # so the idempotent cloud dedups the resend.
    assert captured[0] == db_ids
    assert captured[1] == db_ids
    assert captured[0] == captured[1]


def test_fix_json_body_shape(store):
    seed(store, 1)
    captured = {}

    def capturing_post(url, json_body, headers):
        captured["body"] = json_body
        captured["headers"] = headers
        return FakeResponse(200, {"accepted": 1, "high_water": 0})

    up = CloudUplinker(
        store, capturing_post, "https://cloud.example/api", "secret-key",
        sleep=lambda s: None, clock=lambda: 1000.0,
    )
    up.run_once()

    body = captured["body"]
    assert "fixes" in body and len(body["fixes"]) == 1
    fix = body["fixes"][0]
    for k in ("id", "node_id", "seq", "ts_ms", "lat", "lon",
              "sog", "cog", "battery_mv", "rssi", "snr", "flags"):
        assert k in fix, "missing %s in fix body" % k
    assert captured["headers"]["Authorization"] == "Bearer secret-key"
