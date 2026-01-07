import subprocess
from pathlib import Path

import streamlit as st

from streamlit_app.lib import set_db_path, get_db_path, file_uploader_to_path, ensure_data_dir

st.header("Upload or Generate Data")

ensure_data_dir()

col1, col2 = st.columns(2)
with col1:
    st.subheader("Database")
    db_path = st.text_input("SQLite DB path", value=get_db_path())
    set_db_path(db_path)
    st.caption("The app reads/writes this SQLite file.")

with col2:
    st.subheader("Demo generator")
    seed = st.number_input("Seed", min_value=0, value=42, step=1)
    n_inv = st.number_input("# Invoices", min_value=50, value=800, step=50)
    n_txn = st.number_input("# Transactions", min_value=50, value=920, step=50)
    if st.button("Generate synthetic CSVs"):
        cmd = ["python", "python/generate_data.py", "--out", "data", "--seed", str(seed), "--n_invoices", str(n_inv), "--n_transactions", str(n_txn)]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            st.error(res.stderr)
        else:
            st.success("Generated data/invoices.csv and data/transactions.csv")
            st.code(res.stdout)

st.divider()

st.subheader("Upload your own CSVs (optional)")
st.caption("Expected columns are documented in README. Extra columns are accepted and stored in RAW tables.")

up_inv = st.file_uploader("Invoices CSV", type=["csv"], key="inv_csv")
up_txn = st.file_uploader("Transactions CSV", type=["csv"], key="txn_csv")

if up_inv is not None:
    file_uploader_to_path(up_inv, "data/invoices_uploaded.csv")
    st.success("Saved: data/invoices_uploaded.csv")
if up_txn is not None:
    file_uploader_to_path(up_txn, "data/transactions_uploaded.csv")
    st.success("Saved: data/transactions_uploaded.csv")

st.info("Next: go to **Run Reconciliation**.")
