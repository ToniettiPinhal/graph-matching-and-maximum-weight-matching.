from __future__ import annotations

import re
from datetime import datetime
from dateutil import parser


_WS = re.compile(r"\s+")
_NONALNUM = re.compile(r"[^a-z0-9 ]+")


def parse_date(s: str | None) -> str | None:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    try:
        dt = parser.parse(s, dayfirst=False, yearfirst=False)
        return dt.date().isoformat()
    except Exception:
        return None


def parse_amount(x: str | float | int | None) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    # Handle "1.234,56" and "1,234.56"
    s = s.replace(" ", "")
    if "," in s and "." in s:
        # Decide decimal separator based on last occurrence
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def norm_text(s: str | None) -> str:
    if s is None:
        return ""
    s = str(s).lower().strip()
    s = _WS.sub(" ", s)
    s = _NONALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


def norm_reference(s: str | None) -> str:
    t = norm_text(s)
    # remove common filler words
    for w in ["pix", "transfer", "payment", "invoice", "ref", "id", "n", "no"]:
        t = re.sub(rf"\b{w}\b", " ", t)
    t = _WS.sub(" ", t).strip()
    return t
