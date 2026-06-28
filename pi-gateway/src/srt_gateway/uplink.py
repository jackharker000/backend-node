"""uplink.py — cloud store-and-forward uplink for the SRT Pi gateway.

This is the most safety-critical module in the gateway. Its single job is to
take un-synced boat-position fixes out of the durable local SQLite store and
forward them to the cloud ``/ingest`` endpoint, then flip ``synced=1`` only
*after* the cloud has confirmed acceptance. Everything here is designed around
one invariant:

    ZERO LOSS, ZERO DUPLICATES across drops, dupes and delays.

Guarantees and how we get them
------------------------------
* **Local DB never blocks on the network.** Writing fixes is serial_io/store's
  job. Here we only READ un-synced rows and mark them synced *after* a
  confirmed 2xx. A network outage simply leaves rows un-synced; nothing in the
  hot ingest path waits on us.

* **Mark-after-accept (zero loss).** ``mark_synced`` is called *only* after the
  cloud returns a 2xx for that exact set of ids. If the POST raises, times out,
  or returns 5xx, we leave the rows un-synced and return so the caller can
  retry with backoff. A crash between "cloud accepted" and "mark_synced" simply
  re-sends those rows next time — harmless, because /ingest is idempotent.

* **Idempotent / zero duplicates.** Each fix carries its stable primary-key
  ``id`` as the per-fix idempotency key in the JSON body. The cloud /ingest is
  idempotent on that key, so re-sending the same fix is a no-op server-side.
  Combined with the ``synced`` high-water flag (a row is only ever read while
  ``synced=0`` and only marked once a 2xx covers it), every fix is delivered
  and marked exactly once even across retries and duplicate deliveries.

* **Resumable.** ``synced`` *is* the high-water mechanism: ``get_unsynced``
  always returns the oldest un-synced rows. An outage only delays the cloud; on
  reconnect ``run_once`` naturally catches up from wherever it left off. No
  external cursor can drift out of sync with the data, because the data itself
  (the flag) is the cursor. (We also record an informational ``uplink_hwm``
  cursor in ``sync_state`` for observability, but correctness never depends on
  it.)

* **Retry with exponential backoff.** ``run_forever`` retries retryable
  failures (connection error, timeout, 5xx) with exponential backoff capped at
  ``backoff_max``. ``run_once`` stays a pure, synchronous, testable unit — it
  performs exactly one batch attempt and reports what happened; the backoff
  lives in the loop so tests can drive ``run_once`` deterministically.

Dead-letter (poison record) policy
-----------------------------------
A retryable failure (timeout / connection error / 5xx) is a *transient* cloud
problem: we never advance, we just retry the whole batch forever — that is what
guarantees zero loss during an outage.

A ``4xx`` is different: it means the cloud *rejected* the request as malformed
(e.g. 400 Bad Request on a poison/corrupt fix). Retrying a 400 forever would
wedge the entire queue behind one bad row. So on a 4xx for a multi-fix batch we
**isolate the poison** by re-POSTing each fix individually (1-fix batches):

  * fixes the cloud accepts (2xx) are ``mark_synced`` normally;
  * fixes that *individually* still get a 4xx are **quarantined**: their ids are
    recorded in a local ``dead_letter`` table (id + last status + body +
    timestamp) and then ``mark_synced`` so they leave the un-synced queue and
    stop blocking healthy traffic;
  * fixes that individually hit a *retryable* error are left un-synced for the
    next pass (transient, not poison).

Quarantine is deliberately conservative: a single 400 only dead-letters the
specific offending fix, never its batch-mates. Dead-lettered rows are kept (the
store is append-only) and surfaced in /health via ``dead_letter_count`` for
operator follow-up; they are never silently dropped.

The ``dead_letter`` table is created lazily by this module on the store's own
connection. We do NOT touch the shared ``schema.sql`` (it is mirrored to the
cloud and must stay clean); dead-lettering is a gateway-local concern.
"""

import time

__all__ = ["CloudUplinker", "RetryableError"]


# Columns copied verbatim from a fix row into the cloud JSON body. ``id`` is the
# stable per-fix idempotency key the cloud dedups on.
_FIX_JSON_FIELDS = (
    "id", "node_id", "seq", "ts_ms", "lat", "lon",
    "sog", "cog", "battery_mv", "rssi", "snr", "flags",
)


class RetryableError(Exception):
    """Raised internally to signal a transient failure (don't mark synced)."""


def _is_retryable_status(status):
    # 5xx (and 429 Too Many Requests) are transient: retry, never dead-letter.
    return status >= 500 or status == 429


def _is_reject_status(status):
    # 4xx (except 429) is a hard reject: the request is malformed -> poison.
    return 400 <= status < 500 and status != 429


class CloudUplinker:
    """Batch un-synced fixes from a local Store and POST them to the cloud.

    Parameters
    ----------
    store:
        A ``srt_gateway.store.Store`` (or compatible) exposing
        ``get_unsynced(limit)``, ``mark_synced(ids)``, ``unsynced_count()``,
        ``get_sync_cursor`` / ``set_sync_cursor``.
    http_post:
        Dependency-injected callable ``http_post(url, json_body, headers)``
        returning a response-like object with ``.status_code`` and ``.json()``.
        Network failures must surface as raised exceptions (e.g.
        ``ConnectionError``, ``TimeoutError``); HTTP error responses surface as
        a normal return with a 4xx/5xx ``.status_code``.
    cloud_url:
        Base cloud URL; ``/ingest`` is appended.
    api_key:
        Bearer token sent as ``Authorization: Bearer <api_key>``.
    batch_size:
        Max fixes pulled and POSTed per ``run_once`` (default 100).
    backoff_base / backoff_max / backoff_factor:
        Exponential backoff parameters used by ``run_forever``.
    sleep:
        Injectable sleep (defaults to ``time.sleep``) so the loop is testable.
    clock:
        Injectable monotonic-ish wall clock returning seconds (defaults to
        ``time.time``) used only for health timestamps.
    """

    def __init__(self, store, http_post, cloud_url, api_key, *,
                 batch_size=100, backoff_base=1.0, backoff_max=60.0,
                 backoff_factor=2.0, sleep=time.sleep, clock=time.time):
        self.store = store
        self.http_post = http_post
        self.cloud_url = cloud_url.rstrip("/")
        self.api_key = api_key
        self.batch_size = batch_size
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.backoff_factor = backoff_factor
        self._sleep = sleep
        self._clock = clock

        self._ensure_dead_letter_table()

        # Health / observability state.
        self.last_sync_ok = None      # wall-clock seconds of last accepted batch
        self.last_error = None        # str of most recent failure
        self.total_synced = 0         # fixes mark_synced this process lifetime

    # -- dead-letter storage -------------------------------------------------

    def _conn(self):
        # Reuse the store's own sqlite connection (same WAL db). The store keeps
        # it on ``_conn``; fall back gracefully if a test double doesn't.
        return getattr(self.store, "_conn", None)

    def _ensure_dead_letter_table(self):
        conn = self._conn()
        if conn is None:
            return
        conn.execute(
            """CREATE TABLE IF NOT EXISTS dead_letter (
                   id           INTEGER PRIMARY KEY,  -- the poisoned fix id
                   status_code  INTEGER,
                   body         TEXT,
                   quarantined_ms INTEGER
               )"""
        )
        conn.commit()

    def _quarantine(self, fix_id, status_code, body):
        """Record a poison fix in the local dead_letter table (idempotent)."""
        conn = self._conn()
        if conn is None:
            return
        conn.execute(
            """INSERT INTO dead_letter (id, status_code, body, quarantined_ms)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   status_code = excluded.status_code,
                   body        = excluded.body,
                   quarantined_ms = excluded.quarantined_ms""",
            (fix_id, status_code, str(body)[:1000], int(self._clock() * 1000)),
        )
        conn.commit()

    def dead_letter_count(self):
        conn = self._conn()
        if conn is None:
            return 0
        return conn.execute(
            "SELECT COUNT(*) AS c FROM dead_letter"
        ).fetchone()["c"]

    # -- request building ----------------------------------------------------

    @staticmethod
    def _row_to_fix(row):
        """Build the cloud JSON fix dict from a fix row.

        ``id`` is included as the stable per-fix idempotency key so that
        re-POSTing the same fix is a guaranteed server-side no-op.
        """
        return {k: row[k] for k in _FIX_JSON_FIELDS}

    def _headers(self):
        return {
            "Authorization": "Bearer " + self.api_key,
            "Content-Type": "application/json",
        }

    def _post(self, fixes):
        """POST a list of fix dicts. Returns the response-like object.

        Raises ``RetryableError`` if the transport raised (connection/timeout)
        — those are always transient.
        """
        url = self.cloud_url + "/ingest"
        body = {"fixes": fixes}
        try:
            return self.http_post(url, body, self._headers())
        except Exception as exc:  # ConnectionError, TimeoutError, socket errors
            raise RetryableError(repr(exc)) from exc

    # -- single batch attempt (the testable unit) ----------------------------

    def run_once(self):
        """Pull one batch of un-synced fixes, POST, and mark on acceptance.

        Returns a stats dict describing exactly what happened:

            {
              "fetched":    int,   # rows pulled from the store
              "synced":     int,   # rows accepted + marked synced this call
              "dead_lettered": int,# rows quarantined this call
              "retryable":  bool,  # True if a transient failure left work behind
              "status":     str,   # "idle" | "ok" | "partial" | "retry"
              "error":      str|None,
            }

        On a retryable failure (`status="retry"`, `retryable=True`) the caller
        should back off and call again; nothing was marked, so no loss.
        """
        stats = {"fetched": 0, "synced": 0, "dead_lettered": 0,
                 "retryable": False, "status": "idle", "error": None}

        rows = self.store.get_unsynced(self.batch_size)
        stats["fetched"] = len(rows)
        if not rows:
            return stats

        fixes = [self._row_to_fix(r) for r in rows]
        ids = [r["id"] for r in rows]

        try:
            resp = self._post(fixes)
        except RetryableError as exc:
            # Transport-level failure (drop/timeout): mark NOTHING, retry later.
            self.last_error = str(exc)
            stats.update(retryable=True, status="retry", error=str(exc))
            return stats

        status = resp.status_code

        if 200 <= status < 300:
            # Confirmed acceptance -> now (and only now) flip the flag.
            self._accept(ids)
            stats.update(synced=len(ids), status="ok")
            return stats

        if _is_retryable_status(status):
            # 5xx / 429: transient. Don't mark, let the caller back off.
            self.last_error = "HTTP %d" % status
            stats.update(retryable=True, status="retry",
                         error="HTTP %d" % status)
            return stats

        if _is_reject_status(status):
            # 4xx: the batch contains at least one poison record. Isolate it.
            synced, dead, retry_left = self._isolate_poison(rows)
            stats.update(synced=synced, dead_lettered=dead,
                         retryable=retry_left,
                         status="partial",
                         error="HTTP %d (isolated)" % status)
            if synced or dead:
                self.last_error = "HTTP %d (poison isolated)" % status
            return stats

        # Unknown status code (e.g. 1xx/3xx) — treat conservatively as retry.
        self.last_error = "HTTP %d (unexpected)" % status
        stats.update(retryable=True, status="retry",
                     error="HTTP %d (unexpected)" % status)
        return stats

    def _accept(self, ids):
        """Mark ids synced and advance health/observability state."""
        self.store.mark_synced(ids)
        self.total_synced += len(ids)
        self.last_sync_ok = self._clock()
        # Informational high-water cursor (correctness does NOT depend on it).
        try:
            hwm = max(ids)
            cur = self.store.get_sync_cursor("uplink_hwm", default=0)
            if hwm > cur:
                self.store.set_sync_cursor("uplink_hwm", hwm)
        except Exception:
            pass

    def _isolate_poison(self, rows):
        """Re-POST each fix individually to quarantine only the bad one(s).

        Returns (n_synced, n_dead_lettered, retry_left).

        * 2xx individually  -> mark_synced (good fix, batch-mate of a poison).
        * 4xx individually  -> dead-letter + mark_synced (poison, removed from q).
        * retryable individually -> leave un-synced (transient; try next pass).
        """
        n_synced = 0
        n_dead = 0
        retry_left = False

        for row in rows:
            fix = self._row_to_fix(row)
            fid = row["id"]
            try:
                resp = self._post([fix])
            except RetryableError as exc:
                # Transient for this single fix — leave it un-synced.
                self.last_error = str(exc)
                retry_left = True
                continue

            st = resp.status_code
            if 200 <= st < 300:
                self._accept([fid])
                n_synced += 1
            elif _is_retryable_status(st):
                retry_left = True
            elif _is_reject_status(st):
                # Confirmed poison: quarantine then remove from the live queue.
                body = None
                try:
                    body = resp.json()
                except Exception:
                    body = None
                self._quarantine(fid, st, body)
                self.store.mark_synced([fid])  # leaves un-synced queue
                n_dead += 1
            else:
                retry_left = True

        return n_synced, n_dead, retry_left

    # -- driver loop ---------------------------------------------------------

    def run_forever(self, interval=5.0, max_iterations=None):
        """Continuously drain the queue with exponential backoff on failure.

        ``run_once`` remains the unit of work; this just schedules it. On a
        retryable failure we back off exponentially (capped at ``backoff_max``);
        any successful/partial drain resets the backoff and, once the queue is
        empty, we idle for ``interval``.

        ``max_iterations`` bounds the loop for tests; ``None`` runs forever.
        """
        backoff = self.backoff_base
        iterations = 0
        while max_iterations is None or iterations < max_iterations:
            iterations += 1
            stats = self.run_once()

            if stats["status"] == "retry":
                # Transient failure: sleep with exponential backoff, then retry.
                self._sleep(backoff)
                backoff = min(backoff * self.backoff_factor, self.backoff_max)
                continue

            # Made progress (or nothing to do): reset backoff.
            backoff = self.backoff_base

            if stats["status"] == "idle":
                self._sleep(interval)
            elif stats["status"] == "partial" and stats["retryable"]:
                # Some fixes still transiently failing — small backoff.
                self._sleep(backoff)
            # status == "ok": loop straight back to drain the next batch.

    # -- health --------------------------------------------------------------

    def health(self):
        """Snapshot for the gateway /health endpoint."""
        return {
            "pending": self.store.unsynced_count(),
            "last_sync_ok": self.last_sync_ok,
            "last_error": self.last_error,
            "dead_letter_count": self.dead_letter_count(),
            "total_synced": self.total_synced,
        }
