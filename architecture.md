# Architecture

```text
CSVs (Invoices, Transactions)
  -> Python ETL (pandas)
     -> SQLite (raw + normalized)
        -> Graph candidate builder (bipartite edges)
           -> Maximum-weight matching (networkx)
              -> SQLite (matches + exceptions)
                 -> Streamlit app (dashboard + graph explorer + SQL console)
```

Key ideas:
- Deterministic matching with explainable scores
- Audit trail with `runs` table
- Graph-based constraint enforcement (1:1)
