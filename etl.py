from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple

import pandas as pd

from .normalize import parse_date, parse_amount, norm_text, norm_reference


def read_csv_any(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    df = pd.read_csv(p, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    return df


def ingest_invoices(df: pd.DataFrame) -> pd.DataFrame:
    # expected columns (synthetic generator uses these):
    # invoice_id, invoice_number, issue_date, due_date, counterparty, amount, currency, reference, status
    out = df.copy()
    for col in ["invoice_id", "invoice_number", "issue_date", "due_date", "counterparty", "amount", "currency", "reference", "status"]:
        if col not in out.columns:
            out[col] = ""
    return out


def ingest_transactions(df: pd.DataFrame) -> pd.DataFrame:
    # expected columns:
    # txn_id, txn_date, counterparty, amount, currency, direction, reference, description
    out = df.copy()
    for col in ["txn_id", "txn_date", "counterparty", "amount", "currency", "direction", "reference", "description"]:
        if col not in out.columns:
            out[col] = ""
    return out


def normalize_invoices(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["issue_date"] = out["issue_date"].map(parse_date)
    out["due_date"] = out["due_date"].map(parse_date)
    out["amount"] = out["amount"].map(parse_amount)
    out["counterparty_norm"] = out["counterparty"].map(norm_text)
    out["reference_norm"] = out["reference"].map(norm_reference)
    return out


def normalize_transactions(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["txn_date"] = out["txn_date"].map(parse_date)
    out["amount"] = out["amount"].map(parse_amount)
    out["counterparty_norm"] = out["counterparty"].map(norm_text)
    out["reference_norm"] = out["reference"].map(norm_reference)
    # direction sanity
    if "direction" in out.columns:
        out["direction"] = out["direction"].fillna("IN").map(lambda x: str(x).strip().upper() or "IN")
    else:
        out["direction"] = "IN"
    return out


def to_rows(df: pd.DataFrame, cols: list[str]) -> list[tuple]:
    return [tuple(None if pd.isna(v) else v for v in row) for row in df[cols].itertuples(index=False, name=None)]


def df_to_raw_json(df: pd.DataFrame) -> list[str]:
    # store row-wise json for traceability
    return [json.dumps({k: (None if pd.isna(v) else v) for k, v in row.items()}, ensure_ascii=False) for _, row in df.iterrows()]
