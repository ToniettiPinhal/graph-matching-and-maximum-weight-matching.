from __future__ import annotations

import uuid
from datetime import datetime, date, timedelta
from typing import Dict, Tuple, List

import pandas as pd
import networkx as nx

from .scoring import compute_score, ScoreParams


def _date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


def build_candidates(
    invoices: pd.DataFrame,
    txns: pd.DataFrame,
    params: ScoreParams
) -> pd.DataFrame:
    """Generate candidate edges (invoice_id, txn_id, score, reasons)."""
    inv = invoices.copy()
    tr = txns.copy()

    # Basic filters: only incoming transactions
    if "direction" in tr.columns:
        tr = tr[tr["direction"].fillna("IN").str.upper().isin(["IN", "CREDIT", "RECEIPT"])].copy()

    # Precompute for speed
    tr["txn_date_d"] = tr["txn_date"].map(_date)
    inv["due_date_d"] = inv["due_date"].map(_date)

    edges: List[Tuple[str, str, float, str]] = []

    # Index transactions by (rounded amount cents) buckets to reduce comparisons
    tr = tr.dropna(subset=["amount"]).copy()
    inv = inv.dropna(subset=["amount"]).copy()

    tr["amt_bucket"] = (tr["amount"].astype(float).round(2) * 100).astype(int)
    inv["amt_bucket"] = (inv["amount"].astype(float).round(2) * 100).astype(int)

    bucket_map: Dict[int, pd.DataFrame] = {}
    for b, g in tr.groupby("amt_bucket"):
        bucket_map[int(b)] = g

    for _, irow in inv.iterrows():
        inv_id = str(irow["invoice_id"])
        a = float(irow["amount"])
        due = irow.get("due_date")
        due_d = irow.get("due_date_d")

        # candidate buckets around invoice amount within tolerance
        cents = int(round(a * 100))
        # widen bucket range (handles small rounding differences)
        candidates = []
        for b in range(cents - 3, cents + 4):
            if b in bucket_map:
                candidates.append(bucket_map[b])
        if not candidates:
            continue
        cand = pd.concat(candidates, ignore_index=True)

        # date window filter
        if due_d is not None:
            lo = due_d - timedelta(days=params.date_window_days)
            hi = due_d + timedelta(days=params.date_window_days)
            cand = cand[(cand["txn_date_d"].isna()) | ((cand["txn_date_d"] >= lo) & (cand["txn_date_d"] <= hi))].copy()

        # score each candidate
        scored: List[Tuple[str, float, str]] = []
        for _, trow in cand.iterrows():
            txn_id = str(trow["txn_id"])
            s, reasons = compute_score(
                invoice_amount=a,
                txn_amount=float(trow["amount"]),
                invoice_counterparty=str(irow.get("counterparty", "")),
                txn_counterparty=str(trow.get("counterparty", "")),
                invoice_reference=str(irow.get("reference", "")),
                txn_reference=str(trow.get("reference", "")),
                invoice_due_date=str(due) if due is not None else None,
                txn_date=str(trow.get("txn_date")) if trow.get("txn_date") is not None else None,
            )
            if s >= params.min_score_to_keep:
                scored.append((txn_id, float(s), reasons))

        scored.sort(key=lambda x: x[1], reverse=True)
        scored = scored[: params.max_candidates_per_invoice]

        for txn_id, s, reasons in scored:
            edges.append((inv_id, txn_id, s, reasons))

    return pd.DataFrame(edges, columns=["invoice_id", "txn_id", "score", "reasons"])


def solve_matching(candidates: pd.DataFrame) -> pd.DataFrame:
    """Maximum-weight matching on bipartite graph."""
    G = nx.Graph()
    # bipartite sets labeled by prefix
    for r in candidates.itertuples(index=False):
        u = f"I:{r.invoice_id}"
        v = f"T:{r.txn_id}"
        G.add_edge(u, v, weight=float(r.score), reasons=str(r.reasons))

    matching = nx.algorithms.matching.max_weight_matching(G, maxcardinality=False, weight="weight")

    rows: List[Tuple[str, str, float, str]] = []
    for u, v in matching:
        if u.startswith("I:"):
            inv_node, txn_node = u, v
        else:
            inv_node, txn_node = v, u
        inv_id = inv_node.split(":", 1)[1]
        txn_id = txn_node.split(":", 1)[1]
        data = G.get_edge_data(inv_node, txn_node) or {}
        rows.append((inv_id, txn_id, float(data.get("weight", 0.0)), str(data.get("reasons", ""))))

    out = pd.DataFrame(rows, columns=["invoice_id", "txn_id", "score", "reasons"])
    out.sort_values("score", ascending=False, inplace=True)
    return out


def build_graph_for_invoice(candidates: pd.DataFrame, invoice_id: str) -> nx.Graph:
    sub = candidates[candidates["invoice_id"].astype(str) == str(invoice_id)].copy()
    G = nx.Graph()
    inv_node = f"I:{invoice_id}"
    G.add_node(inv_node, kind="invoice")
    for r in sub.itertuples(index=False):
        txn_node = f"T:{r.txn_id}"
        G.add_node(txn_node, kind="txn")
        G.add_edge(inv_node, txn_node, weight=float(r.score), reasons=str(r.reasons))
    return G
