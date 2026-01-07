#!/usr/bin/env bash
set -euo pipefail

python python/generate_data.py --out data --seed 42 --n_invoices 800 --n_transactions 920
python python/pipeline.py --db data/reconcileflow.sqlite --invoices data/invoices.csv --transactions data/transactions.csv --run --notes "demo run"
streamlit run streamlit_app/app.py
