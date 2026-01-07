import subprocess
import streamlit as st

from streamlit_app.lib import get_db_path

st.header("Run Reconciliation")

db_path = get_db_path()

invoices_path = st.text_input("Invoices CSV path", value="data/invoices.csv")
transactions_path = st.text_input("Transactions CSV path", value="data/transactions.csv")

col1, col2, col3 = st.columns(3)
min_score = col1.slider("Min score", 0, 100, 35)
date_window = col2.slider("Date window (days)", 0, 30, 5)
max_candidates = col3.slider("Max candidates per invoice", 5, 200, 30)

notes = st.text_input("Run notes", value="streamlit run")

if st.button("Run pipeline (ingest + normalize + graph matching)"):
    cmd = [
        "python", "python/pipeline.py",
        "--db", db_path,
        "--invoices", invoices_path,
        "--transactions", transactions_path,
        "--run",
        "--min_score", str(min_score),
        "--date_window", str(date_window),
        "--max_candidates", str(max_candidates),
        "--notes", notes
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        st.error(res.stderr)
    else:
        st.success("Pipeline completed")
        st.code(res.stdout)

st.caption("Then go to Dashboard / Graph Explorer.")
