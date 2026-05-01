"""
Import Bloomberg market-cap snapshots into `crsp_daily_overlay`.

Bloomberg xlsx schema (header row 0):
    ID_BB_UNIQUE | TICKER | DATE | CUR_MKT_CAP_USD

Conventions:
- `CUR_MKT_CAP_USD` is in millions of USD (Bloomberg default), converted
  to thousands here to match CRSP `market_cap` units.
- `TICKER` may carry a Bloomberg suffix ("BAC US Equity"); stripped
  before resolution.
- `TICKER` → permco resolution uses `crsp_ticker_hist` (most recent
  permno where `namedt <= row.DATE`). Override with optional
  `ticker_map` dict.
- `retx` synthesised in the shared overlay module.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from . import _overlay
from ._overlay import ImportResult, prune_overlay  # re-export for back-compat

REQUIRED_COLS = {"ID_BB_UNIQUE", "TICKER", "DATE", "CUR_MKT_CAP_USD"}

_SUFFIX_RE = re.compile(r"\s+[A-Z]{2}\s+EQUITY$", re.IGNORECASE)


def _normalise_ticker(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip().upper()
    s = _SUFFIX_RE.sub("", s).strip()
    s = s.replace("/", ".")
    return s


def import_bloomberg_excel(
    conn: duckdb.DuckDBPyConnection,
    xlsx_path: Path | str,
    *,
    sheet: str | int = 0,
    ticker_map: Optional[dict[str, int]] = None,
    rebuild_pd_input: bool = True,
    ignore_stale: bool = False,
) -> ImportResult:
    """Read xlsx, validate, resolve tickers, compute retx, insert overlay.

    Aborts if crsp_link is stale beyond LINK_STALE_DAYS — ticker→permco
    resolution would silently mis-map. Pass ignore_stale=True to override."""
    from . import freshness, weekly  # local import to avoid cycle

    rep = freshness.check(conn)
    freshness.assert_not_stale(rep, ignore_stale=ignore_stale,
                               check_y9c=False, check_link=True)

    path = Path(xlsx_path)
    if not path.exists():
        raise FileNotFoundError(f"Bloomberg xlsx not found: {path}")

    raw = pd.read_excel(path, sheet_name=sheet)
    raw.columns = [str(c).strip() for c in raw.columns]

    upper_to_actual = {c.upper(): c for c in raw.columns}
    missing = REQUIRED_COLS - set(upper_to_actual.keys())
    if missing:
        raise ValueError(
            f"xlsx is missing required columns: {sorted(missing)}. "
            f"Found: {list(raw.columns)}"
        )

    df = pd.DataFrame({
        "provider_id":  raw[upper_to_actual["ID_BB_UNIQUE"]].astype(str).str.strip(),
        "ticker_raw":   raw[upper_to_actual["TICKER"]].astype(str),
        "date":         pd.to_datetime(raw[upper_to_actual["DATE"]], errors="coerce").dt.date,
        "mcap_usd_mn":  pd.to_numeric(raw[upper_to_actual["CUR_MKT_CAP_USD"]], errors="coerce"),
    })
    n_raw = len(df)
    df = df.dropna(subset=["date", "mcap_usd_mn"])
    df = df[df["mcap_usd_mn"] > 0].copy()
    df["ticker_norm"] = df["ticker_raw"].apply(_normalise_ticker)
    df["market_cap"]  = df["mcap_usd_mn"].astype(float) * 1_000.0

    ticker_map = {k.upper(): int(v) for k, v in (ticker_map or {}).items()}
    df["permco_override"] = df["ticker_norm"].map(ticker_map)

    need_lookup = df[df["permco_override"].isna()]
    pairs = list(need_lookup[["ticker_norm", "date"]].itertuples(index=False, name=None))
    permno_lookup = _overlay.resolve_tickers_via_db(conn, pairs)
    df["permno_resolved"] = [
        permno_lookup.get((tk, dt)) for tk, dt in zip(df["ticker_norm"], df["date"])
    ]
    permnos_unique = sorted({int(p) for p in df["permno_resolved"].dropna()})
    permno_to_permco = _overlay.resolve_permnos_to_permcos(conn, permnos_unique)

    def _to_permco(row):
        if pd.notna(row["permco_override"]):
            return int(row["permco_override"])
        p = row["permno_resolved"]
        if pd.notna(p):
            return permno_to_permco.get(int(p))
        return None

    df["permco"] = df.apply(_to_permco, axis=1)
    unresolved = sorted(set(df.loc[df["permco"].isna(), "ticker_norm"]))
    df = df.dropna(subset=["permco"]).copy()
    df["permco"] = df["permco"].astype(int)

    if df.empty:
        if unresolved:
            print(f"⚠ {len(unresolved)} ticker(s) unresolved: {unresolved}")
        return ImportResult(rows_imported=0, tickers_unresolved=unresolved)

    rows = df[["permco", "date", "market_cap"]].to_dict(orient="records")
    enriched = _overlay.compute_retx_for_overlay(conn, rows)
    enriched_lookup = {
        (int(e["permco"]), _overlay.as_date(e["date"])): e for e in enriched
    }
    df["_key"]           = list(zip(df["permco"].astype(int), df["date"]))
    df["retx"]           = df["_key"].map(lambda k: enriched_lookup.get(k, {}).get("retx"))
    df["retx_synthetic"] = df["_key"].map(lambda k: enriched_lookup.get(k, {}).get("retx_synthetic"))
    df = df.drop(columns=["_key"])

    insert_df = pd.DataFrame({
        "permco":         df["permco"].astype(int),
        "date":           df["date"],
        "market_cap":     df["market_cap"].astype(float),
        "retx":           df["retx"],
        "retx_synthetic": df["retx_synthetic"],
        "provider_id":    df["provider_id"],
        "ticker_raw":     df["ticker_raw"],
        "loaded_from":    str(path),
    })

    n = _overlay.insert_overlay(conn, insert_df, source="bloomberg")
    result = ImportResult(
        rows_imported=int(n),
        tickers_unresolved=unresolved,
        permcos_touched=sorted(set(insert_df["permco"].astype(int))),
        date_range=(insert_df["date"].min(), insert_df["date"].max()),
        retx_synthetic_rows=int(insert_df["retx_synthetic"].fillna(False).sum()),
    )

    if rebuild_pd_input:
        n_pi = weekly.build_pd_input(conn)
        result.pd_input_rows_after = int(n_pi)
        row = conn.execute("SELECT MAX(week_date) FROM pd_input").fetchone()
        if row and row[0]:
            result.pd_input_max_week_after = _overlay.as_date(row[0])

    if unresolved:
        print(f"⚠ {len(unresolved)} ticker(s) unresolved: {unresolved}")
    if n_raw != len(df):
        dropped = n_raw - len(df)
        print(f"ℹ {dropped} row(s) dropped (missing date/mcap or unresolved ticker)")
    return result
