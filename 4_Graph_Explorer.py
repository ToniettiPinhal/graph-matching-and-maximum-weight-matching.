import streamlit as st
import plotly.graph_objects as go
import networkx as nx

from streamlit_app.lib import connect, read_df, latest_run_id, get_db_path

st.header("Graph Explorer")

db_path = get_db_path()
conn = connect(db_path)
run_id = latest_run_id(conn)

if not run_id:
    st.warning("No runs found yet.")
    st.stop()

st.caption(f"Latest run_id: `{run_id}`")

inv = read_df(conn, "SELECT invoice_id, invoice_number, counterparty, amount, due_date FROM invoices WHERE run_id=? ORDER BY due_date DESC LIMIT 200", (run_id,))
if inv.empty:
    st.warning("No invoices.")
    st.stop()

invoice_id = st.selectbox("Pick an invoice_id", inv["invoice_id"].astype(str).tolist())

cand = read_df(conn, "SELECT * FROM match_candidates WHERE run_id=? AND invoice_id=? ORDER BY score DESC LIMIT 80", (run_id, invoice_id))
if cand.empty:
    st.info("No candidate edges for this invoice.")
    st.stop()

st.subheader("Candidate edges")
st.dataframe(cand, use_container_width=True)

# Build graph
G = nx.Graph()
inv_node = f"I:{invoice_id}"
G.add_node(inv_node, kind="invoice")
for _, r in cand.iterrows():
    txn_node = f"T:{r['txn_id']}"
    G.add_node(txn_node, kind="txn")
    G.add_edge(inv_node, txn_node, weight=float(r["score"]), reasons=str(r["reasons"]))

# Layout
pos = nx.spring_layout(G, seed=7)

# Build plotly traces
edge_x, edge_y = [], []
for u, v in G.edges():
    x0, y0 = pos[u]
    x1, y1 = pos[v]
    edge_x += [x0, x1, None]
    edge_y += [y0, y1, None]

node_x, node_y, node_text = [], [], []
for n in G.nodes():
    x, y = pos[n]
    node_x.append(x)
    node_y.append(y)
    node_text.append(n)

fig = go.Figure()
fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode="lines", hoverinfo="none"))
fig.add_trace(go.Scatter(x=node_x, y=node_y, mode="markers+text", text=node_text, textposition="top center"))
fig.update_layout(height=650, title="Invoice ↔ Transaction candidate graph (bipartite)", showlegend=False)
st.plotly_chart(fig, use_container_width=True)

st.caption("Tip: This is a bipartite graph. Edges are weighted by the matching score.")
