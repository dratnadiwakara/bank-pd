"""
Shared helpers for vendor-overlay imports (Bloomberg, yfinance, …).

All vendor importers (bbg.py, yfdata.py) write to `crsp_daily_overlay`
through `insert_overlay()` and use `compute_retx_for_overlay()` to
synthesise daily returns vs the most recent prior trading day in the
combined CRSP + overlay panel.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Iterable, Optional

import duckdb
import pandas as pd


@dataclass
class ImportResult:
    rows_imported: int = 0
    tickers_unresolved: list[str] = field(default_factory=list)
    permcos_touched: list[int] = field(default_factory=list)
    date_range: Optional[tuple[date, date]] = None
    retx_synthetic_rows: int = 0
    overlap_checks: list[dict] = field(default_factory=list)
    pd_input_rows_after: Optional[int] = None
    pd_input_max_week_after: Optional[date] = None


def as_date(v) -> Optional[date]:
    """Coerce numpy.datetime64 / pd.Timestamp / str / date → datetime.date.
    pd.Timestamp is a subclass of datetime which is a subclass of date,
    so a plain `isinstance(v, date)` keeps Timestamps unchanged — explicit
    conversion is required."""
    if v is None:
        return None
    if isinstance(v, pd.Timestamp):
        return v.date()
    if isinstance(v, date) and not hasattr(v, "hour"):
        return v
    try:
        return pd.to_datetime(v).date()
    except Exception:
        return None


def resolve_tickers_via_db(
    conn: duckdb.DuckDBPyConnection,
    pairs: list[tuple[str, date]],
) -> dict[tuple[str, date], int]:
    """For each (ticker, date), return the permno whose name window covers it.
    Picks most recent `namedt` <= date among rows with that ticker."""
    if not pairs:
        return {}
    df = pd.DataFrame(pairs, columns=["ticker_norm", "date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    conn.register("_overlay_tk", df)
    try:
        sql = """
        WITH ranked AS (
          SELECT q.ticker_norm, q.date, t.permno,
                 ROW_NUMBER() OVER (
                   PARTITION BY q.ticker_norm, q.date
                   ORDER BY t.namedt DESC, t.nameenddt DESC, t.permno ASC
                 ) AS rn
          FROM _overlay_tk q
          JOIN crsp_ticker_hist t
            ON UPPER(t.ticker) = q.ticker_norm
           AND t.namedt <= q.date
        )
        SELECT ticker_norm, date, permno FROM ranked WHERE rn = 1
        """
        rows = conn.execute(sql).fetchall()
    finally:
        conn.unregister("_overlay_tk")
    return {(r[0], r[1]): int(r[2]) for r in rows}


def resolve_permnos_to_permcos(
    conn: duckdb.DuckDBPyConnection,
    permnos: list[int],
) -> dict[int, int]:
    if not permnos:
        return {}
    placeholders = ",".join(str(int(p)) for p in permnos)
    rows = conn.execute(
        f"SELECT DISTINCT permno, permco FROM crsp_ticker_hist "
        f"WHERE permno IN ({placeholders})"
    ).fetchall()
    return {int(r[0]): int(r[1]) for r in rows}


def compute_retx_for_overlay(
    conn: duckdb.DuckDBPyConnection,
    rows: list[dict],
) -> list[dict]:
    """For each new (permco, date) in `rows`, derive `retx` from the prior
    trading day's market cap (CRSP, existing overlay, or the same import
    batch) and flag `retx_synthetic` if the gap exceeds one calendar day.

    `rows`: list of {'permco', 'date', 'market_cap'}. Returns same list
    augmented with 'retx' and 'retx_synthetic'."""
    if not rows:
        return rows
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["permco"] = df["permco"].astype(int)
    conn.register("_overlay_new", df)
    try:
        sql = """
        WITH all_market AS (
          SELECT permco, date, market_cap FROM crsp_daily
          UNION ALL
          SELECT permco, date, market_cap FROM crsp_daily_overlay
          UNION ALL
          SELECT permco, date, market_cap FROM _overlay_new
        ),
        with_prev AS (
          SELECT n.permco, n.date, n.market_cap,
                 (
                   SELECT MAX(am.date) FROM all_market am
                    WHERE am.permco = n.permco AND am.date < n.date
                 ) AS prev_date
          FROM _overlay_new n
        ),
        with_prev_mcap AS (
          SELECT wp.permco, wp.date, wp.market_cap, wp.prev_date,
                 (
                   SELECT am.market_cap FROM all_market am
                    WHERE am.permco = wp.permco AND am.date = wp.prev_date
                    ORDER BY am.market_cap DESC LIMIT 1
                 ) AS prev_mcap
          FROM with_prev wp
        )
        SELECT permco, date, market_cap, prev_date, prev_mcap
        FROM with_prev_mcap
        """
        result = conn.execute(sql).fetchdf()
    finally:
        conn.unregister("_overlay_new")

    out = []
    for _, r in result.iterrows():
        prev_d = as_date(r["prev_date"])
        prev_m = r["prev_mcap"]
        retx = None
        synthetic = None
        if prev_d is not None and pd.notna(prev_m) and float(prev_m) > 0:
            retx = float(r["market_cap"]) / float(prev_m) - 1.0
            this_d = as_date(r["date"])
            gap_days = (this_d - prev_d).days if (this_d and prev_d) else None
            synthetic = bool(gap_days is not None and gap_days > 1)
        out.append({
            "permco": int(r["permco"]),
            "date": as_date(r["date"]),
            "market_cap": float(r["market_cap"]),
            "retx": retx,
            "retx_synthetic": synthetic,
        })
    return out


def insert_overlay(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    *,
    source: str,
) -> int:
    """INSERT-or-REPLACE into crsp_daily_overlay.
    Required df columns: permco, date, market_cap, retx, retx_synthetic,
    provider_id, ticker_raw, loaded_from. Adds `source` server-side."""
    if df.empty:
        return 0
    df = df.copy()
    df["source"] = source
    conn.register("_overlay_insert", df)
    try:
        conn.execute("""
            DELETE FROM crsp_daily_overlay
            WHERE (permco, date) IN (SELECT permco, date FROM _overlay_insert)
        """)
        conn.execute("""
            INSERT INTO crsp_daily_overlay
                  (permco, date, market_cap, retx, retx_synthetic,
                   source, provider_id, ticker_raw, loaded_from)
            SELECT permco, date, market_cap, retx, retx_synthetic,
                   source, provider_id, ticker_raw, loaded_from
            FROM _overlay_insert
        """)
    finally:
        conn.unregister("_overlay_insert")
    return int(len(df))


def prune_overlay(conn: duckdb.DuckDBPyConnection) -> int:
    """Delete overlay rows that are now covered by WRDS crsp_daily."""
    n_before = conn.execute("SELECT COUNT(*) FROM crsp_daily_overlay").fetchone()[0]
    conn.execute("""
        DELETE FROM crsp_daily_overlay
        WHERE (permco, date) IN (SELECT permco, date FROM crsp_daily)
    """)
    n_after = conn.execute("SELECT COUNT(*) FROM crsp_daily_overlay").fetchone()[0]
    return int(n_before) - int(n_after)


def latest_known_date_per_permco(
    conn: duckdb.DuckDBPyConnection,
    permcos: Iterable[int],
) -> dict[int, Optional[date]]:
    """For each permco, MAX(date) across crsp_daily ∪ crsp_daily_overlay."""
    permcos = sorted({int(p) for p in permcos})
    if not permcos:
        return {}
    placeholders = ",".join(str(p) for p in permcos)
    rows = conn.execute(f"""
        WITH all_dates AS (
          SELECT permco, date FROM crsp_daily WHERE permco IN ({placeholders})
          UNION ALL
          SELECT permco, date FROM crsp_daily_overlay WHERE permco IN ({placeholders})
        )
        SELECT permco, MAX(date) FROM all_dates GROUP BY permco
    """).fetchall()
    return {int(r[0]): as_date(r[1]) for r in rows}


def existing_market_caps(
    conn: duckdb.DuckDBPyConnection,
    pairs: list[tuple[int, date]],
) -> dict[tuple[int, date], float]:
    """Look up (permco, date) -> market_cap from crsp_daily_combined.
    Used by the overlap consistency check."""
    if not pairs:
        return {}
    df = pd.DataFrame(pairs, columns=["permco", "date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    conn.register("_overlap_q", df)
    try:
        rows = conn.execute("""
            SELECT q.permco, q.date, c.market_cap
            FROM _overlap_q q
            LEFT JOIN crsp_daily_combined c
              ON c.permco = q.permco AND c.date = q.date
            WHERE c.market_cap IS NOT NULL
        """).fetchall()
    finally:
        conn.unregister("_overlap_q")
    return {(int(r[0]), as_date(r[1])): float(r[2]) for r in rows}
