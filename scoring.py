from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Tuple, List

from rapidfuzz import fuzz

from .normalize import norm_text, norm_reference


@dataclass(frozen=True)
class ScoreParams:
    amount_tolerance: float = 0.01          # relative tolerance (1%)
    date_window_days: int = 5               # abs window for txn_date around due_date
    min_score_to_keep: float = 35.0
    max_candidates_per_invoice: int = 30


def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def compute_score(
    invoice_amount: float,
    txn_amount: float,
    invoice_counterparty: str,
    txn_counterparty: str,
    invoice_reference: str,
    txn_reference: str,
    invoice_due_date: str | None,
    txn_date: str | None,
) -> Tuple[float, str]:
    score = 0.0
    reasons: List[str] = []

    # Amount score
    a = _safe_float(invoice_amount)
    b = _safe_float(txn_amount)
    if a is not None and b is not None:
        if a == 0:
            amt_ok = abs(b) < 1e-9
            rel_err = 0.0 if amt_ok else 1.0
        else:
            rel_err = abs(a - b) / abs(a)
        if rel_err <= 0.001:
            score += 55
            reasons.append("amount:exact")
        elif rel_err <= 0.01:
            score += 42
            reasons.append("amount:close")
        elif rel_err <= 0.03:
            score += 18
            reasons.append("amount:maybe")
        else:
            score -= 15
            reasons.append("amount:far")

    # Reference similarity (high weight if present)
    inv_ref = norm_reference(invoice_reference)
    tx_ref = norm_reference(txn_reference)
    if inv_ref and tx_ref:
        ref_sim = fuzz.token_set_ratio(inv_ref, tx_ref)
        if ref_sim >= 95:
            score += 35
            reasons.append(f"ref:{ref_sim}")
        elif ref_sim >= 85:
            score += 22
            reasons.append(f"ref:{ref_sim}")
        elif ref_sim >= 70:
            score += 10
            reasons.append(f"ref:{ref_sim}")
        else:
            score -= 6
            reasons.append(f"ref:{ref_sim}")

    # Counterparty similarity
    inv_cp = norm_text(invoice_counterparty)
    tx_cp = norm_text(txn_counterparty)
    if inv_cp and tx_cp:
        cp_sim = fuzz.token_set_ratio(inv_cp, tx_cp)
        if cp_sim >= 95:
            score += 18
            reasons.append(f"cp:{cp_sim}")
        elif cp_sim >= 85:
            score += 12
            reasons.append(f"cp:{cp_sim}")
        elif cp_sim >= 70:
            score += 6
            reasons.append(f"cp:{cp_sim}")
        else:
            score -= 4
            reasons.append(f"cp:{cp_sim}")

    # Date proximity
    if invoice_due_date and txn_date:
        try:
            d1 = date.fromisoformat(invoice_due_date)
            d2 = date.fromisoformat(txn_date)
            dd = abs((d2 - d1).days)
            if dd == 0:
                score += 10
                reasons.append("date:0")
            elif dd <= 2:
                score += 8
                reasons.append(f"date:{dd}")
            elif dd <= 5:
                score += 4
                reasons.append(f"date:{dd}")
            else:
                score -= 2
                reasons.append(f"date:{dd}")
        except Exception:
            pass

    # Slight penalty if too many weak signals
    if score < 0:
        score = 0.0

    return score, ";".join(reasons)
