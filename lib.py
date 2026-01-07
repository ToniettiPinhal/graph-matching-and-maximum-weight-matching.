from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Tuple

import pandas as pd
import streamlit as st

DEFAULT_DB = "data/reconcileflow.sqlite"


def get_db_path() -> str:
    return st.session_state.get("db_path", DEFAULT_DB)


def set_db_path(p: str) -> None:
    st.session_state["db_path"] = p


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def read_df(conn: sqlite3.Connection, sql: str, params=()) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def ensure_data_dir():
    Path("data").mkdir(parents=True, exist_ok=True)


def file_uploader_to_path(uploaded, out_path: str) -> str:
    ensure_data_dir()
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        f.write(uploaded.getbuffer())
    return str(p)


def latest_run_id(conn: sqlite3.Connection) -> str | None:
    df = read_df(conn, "SELECT run_id, created_at FROM runs ORDER BY created_at DESC LIMIT 1")
    if df.empty:
        return None
    return str(df.iloc[0]["run_id"])
