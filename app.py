    import streamlit as st

    st.set_page_config(
        page_title="ReconcileFlow Pro",
        page_icon="✅",
        layout="wide",
    )

    st.title("ReconcileFlow Pro")
    st.caption("Invoice & transaction reconciliation with SQLite + graph matching (maximum-weight matching). ")

    st.markdown(
        """

        Use the sidebar to navigate:
        - **Upload / Generate**: load synthetic data or upload CSVs
        - **Run Reconciliation**: build candidate graph + solve matching
        - **Dashboard**: KPIs and exceptions
        - **Graph Explorer**: inspect match edges for a specific invoice
        - **SQL Console**: read-only queries for exploration

"""
    )

    st.info("Tip: Start with **Upload / Generate** → Generate demo data, then run the pipeline.")
