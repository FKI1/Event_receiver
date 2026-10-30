import sqlite3

STATUSES = ("pending", "success", "failed", "skipped")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL UNIQUE,
    type        TEXT NOT NULL,
    payload     TEXT NOT NULL,          -- JSON
    received_at REAL NOT NULL           -- unix time
);

CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_pk        INTEGER NOT NULL UNIQUE REFERENCES events(id),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'success', 'failed', 'skipped')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    -- pending-задача доступна воркеру, когда next_attempt_at <= now.
    -- Используется и как пауза между ретраями, и как "аренда" (lease)
    -- на время обработки.
    next_attempt_at REAL NOT NULL,
    last_error      TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_ready ON tasks (status, next_attempt_at, id);
"""


def connect(path):
    conn = sqlite3.connect(path, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path):
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
