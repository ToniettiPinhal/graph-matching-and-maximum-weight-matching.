from __future__ import annotations

import pandas as pd


def kpis(invoices: pd.DataFrame, txns: pd.DataFrame, matches: pd.DataFrame) -> dict:
    inv_n = len(invoices)
    txn_n = len(txns)
    m_n = len(matches)

    matched_inv = m_n
    unmatched_inv = max(inv_n - matched_inv, 0)
    unmatched_txn = max(txn_n - m_n, 0)  # 1:1 matching

    inv_total = float(pd.to_numeric(invoices["amount"], errors="coerce").fillna(0).sum()) if inv_n else 0.0
    txn_total = float(pd.to_numeric(txns["amount"], errors="coerce").fillna(0).sum()) if txn_n else 0.0

    matched_amount = 0.0
    if m_n:
        # join to invoice amounts
        m2 = matches.merge(invoices[["invoice_id", "amount"]], on="invoice_id", how="left")
        matched_amount = float(pd.to_numeric(m2["amount"], errors="coerce").fillna(0).sum())

    return {
        "invoices": inv_n,
        "transactions": txn_n,
        "matched": m_n,
        "reconciliation_rate": (m_n / inv_n) if inv_n else 0.0,
        "unmatched_invoices": unmatched_inv,
        "unmatched_transactions": unmatched_txn,
        "invoice_total": inv_total,
        "transaction_total": txn_total,
        "matched_amount": matched_amount,
    }
