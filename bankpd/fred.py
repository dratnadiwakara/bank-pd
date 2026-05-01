"""
Incremental FRED DGS10 fetcher.

Source: https://api.stlouisfed.org/fred/series/observations?series_id=DGS10
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import duckdb
import pandas as pd
import requests

from . import config
from .db import max_value

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
HTTP_TIMEOUT = 60


def _fetch_dgs10(api_key: str, start_date: str) -> pd.DataFrame:
    params = {
        "series_id": "DGS10",
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
    }
    resp = requests.get(FRED_URL, params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    obs = resp.json().get("observations", [])
    if not obs:
        return pd.DataFrame(columns=["date", "dgs10_pct", "r_decimal"])

    df = pd.DataFrame(obs)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["dgs10_pct"] = pd.to_numeric(df["value"], errors="coerce")
    df["r_decimal"] = df["dgs10_pct"] / 100.0
    df = df.dropna(subset=["date"])
    return df[["date", "dgs10_pct", "r_decimal"]].sort_values("date").reset_index(drop=True)


def fetch_dgs10_incremental(
    conn: duckdb.DuckDBPyConnection,
    api_key: str,
    *,
    full_start: Optional[str] = None,
) -> int:
    """Append-only fetch into fred_dgs10. Returns rows appended."""
    last = max_value(conn, "fred_dgs10", "date")
    if last is not None:
        start = (pd.Timestamp(last) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        start = full_start or config.START_DATE

    today = date.today().strftime("%Y-%m-%d")
    if start > today:
        return 0

    df = _fetch_dgs10(api_key, start)
    if df.empty:
        return 0

    # Strict: only rows strictly after last
    if last is not None:
        df = df[df["date"] > pd.Timestamp(last)]
        if df.empty:
            return 0

    conn.register("_fred_new", df)
    try:
        conn.execute(
            """
            INSERT INTO fred_dgs10 (date, dgs10_pct, r_decimal)
            SELECT CAST(date AS DATE), dgs10_pct, r_decimal FROM _fred_new
            ON CONFLICT (date) DO NOTHING
            """
        )
    finally:
        conn.unregister("_fred_new")

    return int(len(df))
