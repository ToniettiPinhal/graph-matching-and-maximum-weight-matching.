from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Any, Sequence, Tuple, Dict


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    invoices_source TEXT,
    transactions_source TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS invoices_raw (
    run_id TEXT NOT NULL,
    invoice_id TEXT NOT NULL,
    invoice_number TEXT,
    issue_date TEXT,
    due_date TEXT,
    counterparty TEXT,
    amount TEXT,
    currency TEXT,
    reference TEXT,
    status TEXT,
    raw_json TEXT,
    PRIMARY KEY (run_id, invoice_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS transactions_raw (
    run_id TEXT NOT NULL,
    txn_id TEXT NOT NULL,
    txn_date TEXT,
    counterparty TEXT,
    amount TEXT,
    currency TEXT,
    direction TEXT,
    reference TEXT,
    description TEXT,
    raw_json TEXT,
    PRIMARY KEY (run_id, txn_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS invoices (
    run_id TEXT NOT NULL,
    invoice_id TEXT NOT NULL,
    invoice_number TEXT,
    issue_date TEXT,
    due_date TEXT,
    counterparty TEXT,
    amount REAL,
    currency TEXT,
    reference TEXT,
    status TEXT,
    counterparty_norm TEXT,
    reference_norm TEXT,
    PRIMARY KEY (run_id, invoice_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS transactions (
    run_id TEXT NOT NULL,
    txn_id TEXT NOT NULL,
    txn_date TEXT,
    counterparty TEXT,
    amount REAL,
    currency TEXT,
    direction TEXT,
    reference TEXT,
    description TEXT,
    counterparty_norm TEXT,
    reference_norm TEXT,
    PRIMARY KEY (run_id, txn_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS match_candidates (
    run_id TEXT NOT NULL,
    invoice_id TEXT NOT NULL,
    txn_id TEXT NOT NULL,
    score REAL NOT NULL,
    reasons TEXT NOT NULL,
    PRIMARY KEY (run_id, invoice_id, txn_id),
    FOREIGN KEY (run_id, invoice_id) REFERENCES invoices(run_id, invoice_id),
    FOREIGN KEY (run_id, txn_id) REFERENCES transactions(run_id, txn_id)
);

CREATE TABLE IF NOT EXISTS matches (
    run_id TEXT NOT NULL,
    invoice_id TEXT NOT NULL,
    txn_id TEXT NOT NULL,
    score REAL NOT NULL,
    reasons TEXT NOT NULL,
    matched_at TEXT NOT NULL,
    PRIMARY KEY (run_id, invoice_id),
    UNIQUE (run_id, txn_id),
    FOREIGN KEY (run_id, invoice_id) REFERENCES invoices(run_id, invoice_id),
    FOREIGN KEY (run_id, txn_id) REFERENCES transactions(run_id, txn_id)
);

CREATE TABLE IF NOT EXISTS exceptions (
    run_id TEXT NOT NULL,
    exc_id TEXT NOT NULL,
    kind TEXT NOT NULL,                -- UNMATCHED_INVOICE / UNMATCHED_TRANSACTION / AMBIGUOUS
    entity_type TEXT NOT NULL,         -- INVOICE / TRANSACTION
    entity_id TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, exc_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_candidates_invoice ON match_candidates(run_id, invoice_id);
CREATE INDEX IF NOT EXISTS idx_candidates_txn ON match_candidates(run_id, txn_id);
CREATE INDEX IF NOT EXISTS idx_invoices_due ON invoices(run_id, due_date);
CREATE INDEX IF NOT EXISTS idx_txn_date ON transactions(run_id, txn_date);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def execmany(conn: sqlite3.Connection, sql: str, rows: Sequence[Sequence[Any]]) -> None:
    conn.executemany(sql, rows)
    conn.commit()


def query_df(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> list[dict]:
    cur = conn.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]
