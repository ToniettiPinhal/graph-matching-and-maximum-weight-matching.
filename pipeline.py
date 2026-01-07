from __future__ import annotations

import argparse
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from .db import connect, init_db, execmany
from .etl import read_csv_any, ingest_invoices, ingest_transactions, normalize_invoices, normalize_transactions, to_rows, df_to_raw_json
from .reconcile_graph import build_candidates, solve_matching
from .scoring import ScoreParams


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to SQLite db, e.g. data/reconcileflow.sqlite")
    ap.add_argument("--invoices", required=True, help="Invoices CSV path")
    ap.add_argument("--transactions", required=True, help="Transactions CSV path")
    ap.add_argument("--notes", default="", help="Notes saved in runs table")
    ap.add_argument("--run", action="store_true", help="If set, also build candidates and solve matching")
    ap.add_argument("--min_score", type=float, default=35.0)
    ap.add_argument("--date_window", type=int, default=5)
    ap.add_argument("--max_candidates", type=int, default=30)
    args = ap.parse_args()

    run_id = uuid.uuid4().hex[:12]
    created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    conn = connect(args.db)
    init_db(conn)

    # register run
    conn.execute(
        "INSERT INTO runs(run_id, created_at, invoices_source, transactions_source, notes) VALUES (?,?,?,?,?)",
        (run_id, created_at, str(args.invoices), str(args.transactions), args.notes),
    )
    conn.commit()

    inv_raw = ingest_invoices(read_csv_any(args.invoices))
    txn_raw = ingest_transactions(read_csv_any(args.transactions))

    inv_raw["raw_json"] = df_to_raw_json(inv_raw)
    txn_raw["raw_json"] = df_to_raw_json(txn_raw)

    # store raw
    execmany(conn,
        """INSERT OR REPLACE INTO invoices_raw(run_id, invoice_id, invoice_number, issue_date, due_date, counterparty, amount, currency, reference, status, raw_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        to_rows(inv_raw.assign(run_id=run_id), ["run_id","invoice_id","invoice_number","issue_date","due_date","counterparty","amount","currency","reference","status","raw_json"])
    )
    execmany(conn,
        """INSERT OR REPLACE INTO transactions_raw(run_id, txn_id, txn_date, counterparty, amount, currency, direction, reference, description, raw_json)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        to_rows(txn_raw.assign(run_id=run_id), ["run_id","txn_id","txn_date","counterparty","amount","currency","direction","reference","description","raw_json"])
    )

    # normalize
    inv = normalize_invoices(inv_raw)
    txn = normalize_transactions(txn_raw)

    execmany(conn,
        """INSERT OR REPLACE INTO invoices(run_id, invoice_id, invoice_number, issue_date, due_date, counterparty, amount, currency, reference, status, counterparty_norm, reference_norm)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        to_rows(inv.assign(run_id=run_id), ["run_id","invoice_id","invoice_number","issue_date","due_date","counterparty","amount","currency","reference","status","counterparty_norm","reference_norm"])
    )
    execmany(conn,
        """INSERT OR REPLACE INTO transactions(run_id, txn_id, txn_date, counterparty, amount, currency, direction, reference, description, counterparty_norm, reference_norm)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        to_rows(txn.assign(run_id=run_id), ["run_id","txn_id","txn_date","counterparty","amount","currency","direction","reference","description","counterparty_norm","reference_norm"])
    )

    if args.run:
        params = ScoreParams(
            min_score_to_keep=args.min_score,
            date_window_days=args.date_window,
            max_candidates_per_invoice=args.max_candidates
        )
        cand = build_candidates(inv, txn, params=params)
        if len(cand):
            execmany(conn,
                """INSERT OR REPLACE INTO match_candidates(run_id, invoice_id, txn_id, score, reasons)
                   VALUES (?,?,?,?,?)""",
                [(run_id, str(r.invoice_id), str(r.txn_id), float(r.score), str(r.reasons)) for r in cand.itertuples(index=False)]
            )

        matches = solve_matching(cand) if len(cand) else pd.DataFrame(columns=["invoice_id","txn_id","score","reasons"])
        matched_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        if len(matches):
            execmany(conn,
                """INSERT OR REPLACE INTO matches(run_id, invoice_id, txn_id, score, reasons, matched_at)
                   VALUES (?,?,?,?,?,?)""",
                [(run_id, str(r.invoice_id), str(r.txn_id), float(r.score), str(r.reasons), matched_at) for r in matches.itertuples(index=False)]
            )

        # exceptions
        inv_ids = set(inv["invoice_id"].astype(str))
        txn_ids = set(txn["txn_id"].astype(str))
        m_inv = set(matches["invoice_id"].astype(str)) if len(matches) else set()
        m_txn = set(matches["txn_id"].astype(str)) if len(matches) else set()

        now = matched_at
        exc_rows = []
        for iid in sorted(inv_ids - m_inv):
            exc_rows.append((run_id, uuid.uuid4().hex[:12], "UNMATCHED_INVOICE", "INVOICE", iid, "No match found", now))
        for tid in sorted(txn_ids - m_txn):
            exc_rows.append((run_id, uuid.uuid4().hex[:12], "UNMATCHED_TRANSACTION", "TRANSACTION", tid, "No match found", now))

        if exc_rows:
            execmany(conn,
                """INSERT OR REPLACE INTO exceptions(run_id, exc_id, kind, entity_type, entity_id, details, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                exc_rows
            )

    print(f"OK. run_id={run_id} db={args.db}")


if __name__ == "__main__":
    main()
