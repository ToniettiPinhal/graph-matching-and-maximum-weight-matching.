from __future__ import annotations

import argparse
import random
import string
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


COUNTERPARTIES = [
    "Aurora Studio", "Nova Consulting", "Blue Harbor", "Cobalt Media", "Atlas Systems",
    "Saffron Labs", "Orchid Design", "Nimbus Co.", "Redwood Health", "Keystone Works"
]

def _rand_id(prefix: str, n: int = 10) -> str:
    return prefix + "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))

def _typo(s: str) -> str:
    # introduce minor noise
    if len(s) < 4:
        return s
    s = list(s)
    i = random.randrange(0, len(s)-1)
    if random.random() < 0.5:
        s[i], s[i+1] = s[i+1], s[i]
    else:
        s[i] = random.choice(string.ascii_lowercase)
    return "".join(s)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data", help="Output folder")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n_invoices", type=int, default=800)
    ap.add_argument("--n_transactions", type=int, default=920)
    args = ap.parse_args()

    random.seed(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    base = date.today() - timedelta(days=90)

    invoices = []
    for i in range(args.n_invoices):
        cp = random.choice(COUNTERPARTIES)
        inv_id = _rand_id("INV_")
        inv_no = f"{random.randint(10000,99999)}"
        issue = base + timedelta(days=random.randint(0, 80))
        due = issue + timedelta(days=random.choice([7, 14, 21, 30]))
        amount = round(random.uniform(50, 2500), 2)
        ref = f"{inv_no}-{random.randint(100,999)}"
        status = random.choice(["OPEN","OPEN","PAID","OPEN"])
        invoices.append({
            "invoice_id": inv_id,
            "invoice_number": inv_no,
            "issue_date": issue.isoformat(),
            "due_date": due.isoformat(),
            "counterparty": cp if random.random() > 0.07 else _typo(cp),
            "amount": f"{amount:.2f}",
            "currency": "USD",
            "reference": ref if random.random() > 0.10 else "",
            "status": status
        })

    inv_df = pd.DataFrame(invoices)

    # Create transactions: many correspond to invoices; some noise, duplicates, partials
    txns = []

    # Choose invoices to pay
    pay_inv = inv_df.sample(n=min(args.n_transactions, len(inv_df)), random_state=args.seed).reset_index(drop=True)
    for _, row in pay_inv.iterrows():
        if len(txns) >= args.n_transactions:
            break
        cp = row["counterparty"]
        # date near due date
        due = date.fromisoformat(row["due_date"])
        txn_date = due + timedelta(days=random.randint(-2, 3))
        amount = float(row["amount"])

        kind = random.random()
        if kind < 0.08:
            # partial payment
            part = round(amount * random.uniform(0.3, 0.8), 2)
            txns.append({
                "txn_id": _rand_id("TXN_"),
                "txn_date": txn_date.isoformat(),
                "counterparty": cp,
                "amount": f"{part:.2f}",
                "currency": "USD",
                "direction": "IN",
                "reference": row["reference"] if random.random() > 0.25 else "",
                "description": f"Payment for invoice {row['invoice_number']}"
            })
            continue

        if kind < 0.12:
            # duplicate payment (same amount)
            for _k in range(2):
                txns.append({
                    "txn_id": _rand_id("TXN_"),
                    "txn_date": (txn_date + timedelta(days=random.randint(0,1))).isoformat(),
                    "counterparty": cp if random.random() > 0.10 else _typo(cp),
                    "amount": f"{amount:.2f}",
                    "currency": "USD",
                    "direction": "IN",
                    "reference": row["reference"] if random.random() > 0.15 else "",
                    "description": f"Payment inv {row['invoice_number']}"
                })
            continue

        # normal payment with minor amount noise
        noise = random.choice([0.0, 0.0, 0.0, 0.01, -0.01, 0.02, -0.02])
        txns.append({
            "txn_id": _rand_id("TXN_"),
            "txn_date": txn_date.isoformat(),
            "counterparty": cp if random.random() > 0.10 else _typo(cp),
            "amount": f"{(amount + noise):.2f}",
            "currency": "USD",
            "direction": "IN",
            "reference": row["reference"] if random.random() > 0.20 else "",
            "description": f"Payment for {cp}"
        })

    # Add noise transactions unrelated
    while len(txns) < args.n_transactions:
        cp = random.choice(COUNTERPARTIES)
        txn_date = base + timedelta(days=random.randint(0, 85))
        amount = round(random.uniform(10, 3000), 2)
        txns.append({
            "txn_id": _rand_id("TXN_"),
            "txn_date": txn_date.isoformat(),
            "counterparty": cp if random.random() > 0.10 else _typo(cp),
            "amount": f"{amount:.2f}",
            "currency": "USD",
            "direction": "IN",
            "reference": "" if random.random() > 0.4 else f"{random.randint(10000,99999)}-{random.randint(100,999)}",
            "description": "Misc incoming"
        })

    txn_df = pd.DataFrame(txns).head(args.n_transactions)

    inv_path = out / "invoices.csv"
    txn_path = out / "transactions.csv"
    inv_df.to_csv(inv_path, index=False)
    txn_df.to_csv(txn_path, index=False)

    print(f"Wrote {inv_path} ({len(inv_df)})")
    print(f"Wrote {txn_path} ({len(txn_df)})")


if __name__ == "__main__":
    main()
