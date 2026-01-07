import streamlit as st
import plotly.express as px

from streamlit_app.lib import connect, read_df, latest_run_id, get_db_path

st.header("Dashboard")

db_path = get_db_path()
conn = connect(db_path)
run_id = latest_run_id(conn)

if not run_id:
    st.warning("No runs found yet. Go to **Run Reconciliation** first.")
    st.stop()

st.caption(f"Showing latest run_id: `{run_id}`")

inv = read_df(conn, "SELECT * FROM invoices WHERE run_id=?", (run_id,))
txn = read_df(conn, "SELECT * FROM transactions WHERE run_id=?", (run_id,))
matches = read_df(conn, "SELECT * FROM matches WHERE run_id=?", (run_id,))
exc = read_df(conn, "SELECT * FROM exceptions WHERE run_id=?", (run_id,))

inv_n = len(inv)
txn_n = len(txn)
m_n = len(matches)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Invoices", inv_n)
c2.metric("Transactions", txn_n)
c3.metric("Matched (1:1)", m_n)
rate = (m_n / inv_n) if inv_n else 0.0
c4.metric("Reconciliation rate", f"{rate*100:.1f}%")

st.divider()

st.subheader("Exceptions")
if exc.empty:
    st.success("No exceptions recorded.")
else:
    by_kind = exc.groupby("kind").size().reset_index(name="count")
    fig = px.bar(by_kind, x="kind", y="count", title="Exceptions by kind")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(exc.sort_values("created_at", ascending=False), use_container_width=True)

st.divider()
st.subheader("Aging (Invoices by due date bucket)")
if not inv.empty and "due_date" in inv.columns:
    inv2 = inv.copy()
    inv2["due_date"] = inv2["due_date"].astype(str)
    inv2 = inv2[inv2["due_date"].str.len() >= 10].copy()
    inv2["due_date"] = inv2["due_date"].str.slice(0, 10)
    inv2["due_date"] = inv2["due_date"].astype("datetime64[ns]")
    today = px.utils.datetime.now()
    inv2["days_to_due"] = (inv2["due_date"] - today).dt.days

    def bucket(x):
        if x < -30:
            return "overdue 31+ days"
        if x < -7:
            return "overdue 8-30 days"
        if x < 0:
            return "overdue 1-7 days"
        if x <= 7:
            return "due in 0-7 days"
        if x <= 30:
            return "due in 8-30 days"
        return "due in 31+ days"

    inv2["bucket"] = inv2["days_to_due"].map(bucket)
    agg = inv2.groupby("bucket").size().reset_index(name="count")
    fig2 = px.bar(agg, x="bucket", y="count", title="Invoice aging buckets")
    st.plotly_chart(fig2, use_container_width=True)
