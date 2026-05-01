"""
Incremental CRSP daily fetch from WRDS PostgreSQL.

Maintains crsp_daily(permco, date, price, ret, retx, shrout, market_cap)
with per-permco watermark.
"""
from __future__ import annotations

from datetime import date
from typing import Iterable, Optional

import duckdb
import pandas as pd
import wrds

from . import config

WRDS_BATCH = 500           # permcos per WRDS query
DEFAULT_FETCH_END = None   # None -> today


def connect_wrds(username: str, password: str) -> "wrds.Connection":
    return wrds.Connection(wrds_username=username, wrds_password=password)


def _per_permco_watermarks(
    conn: duckdb.DuckDBPyConnection, permcos: Iterable[int]
) -> dict[int, Optional[pd.Timestamp]]:
    permcos = list(permcos)
    if not permcos:
        return {}
    df = pd.DataFrame({"permco": permcos})
    conn.register("_pcs", df)
    try:
        rows = conn.execute(
            """
            SELECT p.permco, MAX(d.date) AS last_date
            FROM _pcs p
            LEFT JOIN crsp_daily d USING (permco)
            GROUP BY p.permco
            """
        ).fetchall()
    finally:
        conn.unregister("_pcs")
    return {int(r[0]): r[1] for r in rows}


def _fetch_batch(
    db: "wrds.Connection",
    permcos: list[int],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    permco_str = ",".join(str(int(p)) for p in permcos)
    sql = f"""
        SELECT permco, date,
               ABS(prc) AS price,
               ret,
               retx,
               shrout,
               shrout * ABS(prc) AS market_cap
        FROM crsp.dsf
        WHERE date >= '{start_date}' AND date <= '{end_date}'
          AND permco IN ({permco_str})
        ORDER BY permco, date
    """
    return db.raw_sql(sql)


def fetch_crsp_tickers(
    conn: duckdb.DuckDBPyConnection,
    permcos: Iterable[int],
    db: "wrds.Connection",
) -> int:
    """
    Refresh crsp_ticker_hist for the given permco list. Full replace.

    Pulls common-stock (shrcd in (10, 11)) ticker history from crsp.stocknames,
    keyed on permno, joined to a (permco, permno) bridge from crsp.dsf.
    """
    permcos = sorted({int(p) for p in permcos})
    if not permcos:
        return 0

    permco_str = ",".join(str(p) for p in permcos)
    sql = f"""
        SELECT DISTINCT
               b.permco,
               s.permno,
               s.ticker,
               s.comnam,
               s.shrcd,
               s.namedt,
               s.nameenddt
        FROM crsp.stocknames s
        JOIN (SELECT DISTINCT permco, permno
              FROM crsp.dsf
              WHERE permco IN ({permco_str})) b USING (permno)
        WHERE s.shrcd IN (10, 11)
    """
    df = db.raw_sql(sql)
    if df is None or len(df) == 0:
        return 0

    df = df.dropna(subset=["permco", "permno", "namedt", "nameenddt"]).copy()
    df["permco"] = df["permco"].astype(int)
    df["permno"] = df["permno"].astype(int)
    df["namedt"] = pd.to_datetime(df["namedt"]).dt.date
    df["nameenddt"] = pd.to_datetime(df["nameenddt"]).dt.date
    if "shrcd" in df.columns:
        df["shrcd"] = pd.to_numeric(df["shrcd"], errors="coerce").astype("Int64")

    conn.register("_ticker_new", df)
    try:
        conn.execute(
            """
            DELETE FROM crsp_ticker_hist
            WHERE permco IN (SELECT DISTINCT permco FROM _ticker_new)
            """
        )
        conn.execute(
            """
            INSERT INTO crsp_ticker_hist
                  (permco, permno, ticker, comnam, shrcd, namedt, nameenddt)
            SELECT permco, permno, ticker, comnam, shrcd, namedt, nameenddt
            FROM _ticker_new
            ON CONFLICT (permco, permno, namedt) DO NOTHING
            """
        )
    finally:
        conn.unregister("_ticker_new")
    return int(len(df))


def fetch_crsp_daily_incremental(
    conn: duckdb.DuckDBPyConnection,
    permcos: Iterable[int],
    db: "wrds.Connection",
    *,
    full_start: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    """For each permco: fetch from MAX(date)+1 to end_date. Insert into crsp_daily."""
    permcos = sorted({int(p) for p in permcos})
    if not permcos:
        return 0

    end = end_date or date.today().strftime("%Y-%m-%d")
    full_start = full_start or config.START_DATE

    watermarks = _per_permco_watermarks(conn, permcos)

    # Group permcos by their fetch_from date so each batch shares one start.
    groups: dict[str, list[int]] = {}
    for p in permcos:
        last = watermarks.get(p)
        if last is None:
            since = full_start
        else:
            since = (pd.Timestamp(last) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        if since > end:
            continue
        groups.setdefault(since, []).append(p)

    total = 0
    for since, pcs in groups.items():
        for i in range(0, len(pcs), WRDS_BATCH):
            chunk = pcs[i : i + WRDS_BATCH]
            df = _fetch_batch(db, chunk, since, end)
            if df is None or len(df) == 0:
                continue
            df = df.dropna(subset=["permco", "date"]).copy()
            df["permco"] = df["permco"].astype(int)
            df["date"] = pd.to_datetime(df["date"]).dt.date

            conn.register("_crsp_new", df)
            try:
                conn.execute(
                    """
                    INSERT INTO crsp_daily
                          (permco, date, price, ret, retx, shrout, market_cap)
                    SELECT permco, date, price, ret, retx, shrout, market_cap
                    FROM _crsp_new
                    ON CONFLICT (permco, date) DO NOTHING
                    """
                )
            finally:
                conn.unregister("_crsp_new")
            total += int(len(df))

    return total
