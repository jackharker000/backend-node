-- schema.sql — Sail Race Tracker gateway authoritative local store.
--
-- SHARED CONTRACT: this schema is mirrored to the cloud later, so keep it
-- clean and stable. All statements are idempotent (IF NOT EXISTS) so the
-- gateway can apply this file on every boot without harm.
--
-- `fixes` is append-only: decoded boat position fixes from the serial link
-- land here durably; a separate uplink process forwards un-synced rows to
-- the cloud and flips `synced`.

CREATE TABLE IF NOT EXISTS fixes (
    id          INTEGER PRIMARY KEY,
    node_id     INTEGER NOT NULL,
    boat_id     INTEGER,            -- resolved later from boats; NULL at ingest
    race_id     INTEGER,            -- resolved later; NULL at ingest
    ts_ms       INTEGER NOT NULL,   -- gps_time*1000 + subsec_ms (UTC ms)
    lat         REAL    NOT NULL,
    lon         REAL    NOT NULL,
    sog         REAL,               -- speed over ground, m/s (speed_cms/100)
    cog         REAL,               -- course over ground, deg (course_cdeg/100)
    battery_mv  INTEGER NOT NULL,
    seq         INTEGER,            -- u16 sequence (wraps at 65535); set NULL
                                    -- when a row is retired by a later wrap-cycle
                                    -- so it leaves the UNIQUE(node_id, seq) index
                                    -- (SQLite treats NULLs as distinct). History kept.
    rssi        INTEGER NOT NULL,   -- rssi_dbm
    snr         REAL    NOT NULL,   -- snr_db
    flags       INTEGER NOT NULL,
    synced      INTEGER NOT NULL DEFAULT 0,
    rx_time_ms  INTEGER NOT NULL,   -- base-node receive timestamp (ms)
    UNIQUE (node_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_fixes_race_ts ON fixes (race_id, ts_ms);
CREATE INDEX IF NOT EXISTS idx_fixes_node_ts ON fixes (node_id, ts_ms);

CREATE TABLE IF NOT EXISTS nodes (
    node_id         INTEGER PRIMARY KEY,
    slot            INTEGER,
    last_seen       INTEGER,
    last_battery_mv INTEGER,
    last_rssi       INTEGER,
    fw_version      TEXT
);

CREATE TABLE IF NOT EXISTS boats (
    boat_id INTEGER PRIMARY KEY,
    node_id INTEGER,
    name    TEXT,
    sail_no TEXT
);

CREATE TABLE IF NOT EXISTS races (
    race_id    INTEGER PRIMARY KEY,
    name       TEXT,
    state      TEXT,
    started_ms INTEGER
);

CREATE TABLE IF NOT EXISTS courses (
    course_id INTEGER PRIMARY KEY,
    race_id   INTEGER,
    name      TEXT
);

CREATE TABLE IF NOT EXISTS marks (
    mark_id   INTEGER PRIMARY KEY,
    course_id INTEGER,
    name      TEXT,
    lat       REAL,
    lon       REAL,
    seq       INTEGER
);

-- Uplink high-water cursor and other small key/value bookkeeping.
CREATE TABLE IF NOT EXISTS sync_state (
    key   TEXT PRIMARY KEY,
    value INTEGER
);
