# ReconcileFlow Pro (Portfolio Toy Project)

**Goal:** A full-stack reconciliation system that ingests invoice + payment transaction CSVs, stores everything in **SQLite**, builds a **bipartite graph** of candidate matches, runs **maximum-weight matching** to produce 1:1 reconciliations, and serves an interactive **Streamlit** app with dashboards, graph exploration, and a SQL console.

This is a *toy project* built for portfolio purposes with **synthetic data** generation and no proprietary datasets.

---

## What you get
- **Python + pandas** ETL (normalize dates/currency/text)
- **SQLite** storage + audit trail
- **Graph matching** using `networkx` (max weight matching on a bipartite graph)
- **Streamlit** UI:
  - Upload CSVs / generate demo data
  - Run reconciliation (graph build + matching)
  - KPI dashboard & exceptions
  - Graph explorer (visualize candidate edges and chosen matches)
  - SQL console (read-only) to query the database

---

## Quickstart

### 1) Create environment
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Generate synthetic demo data (recommended)
```bash
python python/generate_data.py --out data --seed 42 --n_invoices 800 --n_transactions 920
```

This creates:
- `data/invoices.csv`
- `data/transactions.csv`

### 3) Load into SQLite + run reconciliation
```bash
python python/pipeline.py --db data/reconcileflow.sqlite --invoices data/invoices.csv --transactions data/transactions.csv --run
```

### 4) Launch the Streamlit app
```bash
streamlit run streamlit_app/app.py
```

---

## Data model (SQLite)
Tables:
- `invoices_raw`, `transactions_raw` (as ingested)
- `invoices`, `transactions` (normalized)
- `match_candidates` (edges: invoice_id, txn_id, score, reasons)
- `matches` (final 1:1 matches from max-weight matching)
- `exceptions` (unmatched/ambiguous/duplicates)
- `runs` (audit trail for pipeline runs)

---

## Matching approach (graph)
- Build a **bipartite graph**:
  - Left nodes: invoices
  - Right nodes: transactions
  - Edge weight: score computed from amount/date/reference/name similarity
- Solve **maximum-weight matching** to enforce 1:1 assignment.

Why this is cool:
- It is deterministic, auditable, and produces explainable results (edge reasons).

---

## Portfolio notes
You can screenshot:
- Dashboard KPIs
- Exceptions queue
- Graph explorer
- Example SQL queries

Add a short writeup:
- “Designed normalized schema in SQLite”
- “Implemented graph-based matching with maximum-weight matching”
- “Built end-to-end ETL + UI for reconciliation”

---

## License
MIT (see `LICENSE`).
