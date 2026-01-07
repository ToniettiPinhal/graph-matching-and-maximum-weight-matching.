    import re
    import streamlit as st

    from streamlit_app.lib import connect, read_df, latest_run_id, get_db_path

    st.header("SQL Console (read-only)")
    st.caption("For safety, only SELECT queries are allowed.")

    db_path = get_db_path()
    conn = connect(db_path)
    run_id = latest_run_id(conn)

    if not run_id:
        st.warning("No runs found yet.")

    default_query = f"""
SELECT
  i.invoice_id,
  i.invoice_number,
  i.counterparty,
  i.amount AS invoice_amount,
  m.txn_id,
  m.score,
  m.matched_at
FROM matches m
JOIN invoices i
  ON i.run_id=m.run_id AND i.invoice_id=m.invoice_id
WHERE m.run_id='{run_id or ''}'
ORDER BY m.score DESC
LIMIT 50;
"""

    q = st.text_area("SQL", value=default_query, height=260)

    if st.button("Run query"):
        if not re.match(r"^\s*select\b", q.strip(), flags=re.IGNORECASE):
            st.error("Only SELECT statements are allowed.")
            st.stop()
        try:
            df = read_df(conn, q)
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(str(e))

    st.divider()
    st.subheader("Helpful queries")
    st.code(f"""
-- Latest runs
SELECT run_id, created_at, notes FROM runs ORDER BY created_at DESC LIMIT 10;

-- Exceptions by kind
SELECT kind, COUNT(*) AS n FROM exceptions WHERE run_id='{run_id or ''}' GROUP BY kind ORDER BY n DESC;

-- Candidate density per invoice
SELECT invoice_id, COUNT(*) AS candidates
FROM match_candidates WHERE run_id='{run_id or ''}'
GROUP BY invoice_id
ORDER BY candidates DESC
LIMIT 20;
""")
