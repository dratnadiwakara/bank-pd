"""
Yahoo Finance overlay: pull daily close × shares-outstanding for a set of
permcos / RSSDs, write into `crsp_daily_overlay` with source='yfinance'.

Workflow:
  1. resolve permcos (rssds → permcos via crsp_link, or default to all
     permcos in crsp_daily_combined whose tail lags > 1 day)
  2. pick latest ticker per permco from crsp_ticker_hist
  3. apply CRSP→YF alias map (BRK.B → BRK-B, etc.)
  4. for each ticker, since = MAX(crsp_daily_combined.date) for that
     permco — gives a one-day overlap with existing data
  5. yfinance pull: history(close) × get_shares_full() → market_cap
  6. **overlap consistency check** for last_known_date row vs existing
     CRSP/overlay; abort the entire import if any ticker exceeds the
     hard threshold (default 10%); soft-warn between 1% and 10%
  7. drop overlap rows; insert remainder via shared overlay module
  8. optional pd_input rebuild
"""
from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Iterable, Optional

import duckdb
import pandas as pd

from . import _overlay
from ._overlay import ImportResult


# Small map of CRSP-style tickers (with `.`) to yfinance-style (with `-`).
CRSP_TO_YF_ALIASES: dict[str, str] = {
    "BRK.A": "BRK-A", "BRK.B": "BRK-B",
    "BF.A": "BF-A", "BF.B": "BF-B",
    "PBR.A": "PBR-A",
    "GOOG.L": "GOOG", "GOOGL": "GOOGL",  # placeholder
}


def _crsp_to_yf(ticker: str) -> str:
    if not ticker:
        return ticker
    t = ticker.strip().upper()
    if t in CRSP_TO_YF_ALIASES:
        return CRSP_TO_YF_ALIASES[t]
    if "." in t:
        return t.replace(".", "-")
    return t


def _ticker_for_permco(
    conn: duckdb.DuckDBPyConnection,
    permco: int,
    as_of: Optional[date] = None,
) -> Optional[str]:
    """Most recent ticker (common stock) for permco, optionally as of a date."""
    if as_of is None:
        rows = conn.execute(
            """
            SELECT ticker FROM crsp_ticker_hist
            WHERE permco = ?
            ORDER BY nameenddt DESC, namedt DESC, ticker ASC
            LIMIT 1
            """,
            [int(permco)],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT ticker FROM crsp_ticker_hist
            WHERE permco = ? AND namedt <= ?
            ORDER BY nameenddt DESC, namedt DESC, ticker ASC
            LIMIT 1
            """,
            [int(permco), as_of],
        ).fetchall()
    return rows[0][0] if rows else None


def _stale_permcos(conn: duckdb.DuckDBPyConnection) -> list[int]:
    today = date.today()
    rows = conn.execute("""
        WITH all_dates AS (
          SELECT permco, date FROM crsp_daily
          UNION ALL
          SELECT permco, date FROM crsp_daily_overlay
        ),
        latest AS (
          SELECT permco, MAX(date) AS d FROM all_dates GROUP BY permco
        )
        SELECT permco, d FROM latest WHERE d < ?
        ORDER BY permco
    """, [today - timedelta(days=1)]).fetchall()
    return [int(r[0]) for r in rows]


def _yf_pull_one(
    yf_module,
    yf_ticker: str,
    since: date,
    until: date,
    *,
    retries: int = 1,
    backoff: float = 2.0,
) -> Optional[pd.DataFrame]:
    """Returns DataFrame with index=date, columns=['close','shares']. None if empty."""
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            t = yf_module.Ticker(yf_ticker)
            close = t.history(
                start=since.isoformat(),
                end=(until + timedelta(days=1)).isoformat(),
                auto_adjust=False,
            )["Close"]
            if close is None or close.empty:
                return None
            try:
                # Pull shares from a buffer earlier than `since` so the
                # ffill on the first close-row never lands on NaN. yfinance's
                # `get_shares_full` returns sparse rows tied to corporate
                # actions, so the buffer matters.
                shares = t.get_shares_full(
                    start=(since - timedelta(days=120)).isoformat(),
                    end=(until + timedelta(days=1)).isoformat(),
                )
            except Exception:
                shares = None
            if shares is None or shares.empty:
                # Fallback to current shares — constant series
                info = getattr(t, "info", {}) or {}
                shrout_now = info.get("sharesOutstanding")
                if not shrout_now:
                    return None
                shares = pd.Series(float(shrout_now), index=close.index)
            else:
                shares = shares.copy()
                shares.index = pd.to_datetime(shares.index).tz_localize(None)
            close = close.copy()
            # tz-localize handles tz-aware indexes; if already naive,
            # tz_localize(None) is a no-op via DatetimeIndex.
            close_idx = pd.to_datetime(close.index)
            if close_idx.tz is not None:
                close_idx = close_idx.tz_localize(None)
            close.index = close_idx
            close = close[~close.index.duplicated(keep="last")]

            sh_idx = pd.to_datetime(shares.index)
            if sh_idx.tz is not None:
                sh_idx = sh_idx.tz_localize(None)
            shares.index = sh_idx
            shares = shares[~shares.index.duplicated(keep="last")]
            shares = shares.sort_index().reindex(close.index, method="ffill")

            df = pd.DataFrame({"close": close.astype(float),
                               "shares": shares.astype(float)})
            df = df.dropna()
            if df.empty:
                return None
            df.index = [pd.Timestamp(d).date() for d in df.index]
            df = df[~df.index.duplicated(keep="last")]
            return df
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff)
            else:
                print(f"  ⚠ yfinance error for {yf_ticker}: {exc}")
    return None


def import_yahoo_finance(
    conn: duckdb.DuckDBPyConnection,
    *,
    rssds: Optional[Iterable[int]] = None,
    permcos: Optional[Iterable[int]] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    rebuild_pd_input: bool = True,
    sleep_between_tickers: float = 0.4,
    skip_overlap_check: bool = False,
    overlap_tolerance: float = 0.01,
    ticker_map: Optional[dict[str, str]] = None,
    ignore_stale: bool = False,
) -> ImportResult:
    """See module docstring. `ticker_map` is CRSP→yfinance overrides
    (e.g. {'BRK.B': 'BRK-B'}); merged on top of CRSP_TO_YF_ALIASES.

    Aborts if crsp_link is stale beyond LINK_STALE_DAYS — ticker→permco
    resolution would silently mis-map. Pass ignore_stale=True to override."""
    from . import freshness, linker, weekly

    rep = freshness.check(conn)
    freshness.assert_not_stale(rep, ignore_stale=ignore_stale,
                               check_y9c=False, check_link=True)

    try:
        import yfinance as yf
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "yfinance not installed. Install with: pip install yfinance"
        ) from exc

    # Resolve target permcos
    if permcos:
        target = sorted({int(p) for p in permcos})
    elif rssds:
        target = linker.permcos_for_rssds(conn, sorted({int(r) for r in rssds}))
    else:
        target = _stale_permcos(conn)
    if not target:
        print("No permcos to fetch. Run `update-inputs` first or pass --rssd / --permco.")
        return ImportResult()

    # Per-permco last known date in combined source
    last_known = _overlay.latest_known_date_per_permco(conn, target)

    # Bounds
    today = date.today()
    user_since = pd.Timestamp(since).date() if since else None
    user_until = pd.Timestamp(until).date() if until else today

    # Map CRSP ticker → YF ticker, allow user overrides
    overrides = {k.upper(): str(v).upper() for k, v in (ticker_map or {}).items()}

    print(f"yfinance pull: {len(target)} permco(s)")
    pull_results: dict[int, pd.DataFrame] = {}
    ticker_used: dict[int, str] = {}
    tickers_unresolved: list[str] = []

    for permco in target:
        crsp_ticker = _ticker_for_permco(conn, permco)
        if not crsp_ticker:
            tickers_unresolved.append(f"permco {permco} (no ticker history)")
            continue
        yf_ticker = overrides.get(crsp_ticker.upper(), _crsp_to_yf(crsp_ticker))

        permco_since = last_known.get(permco)
        if user_since is not None:
            permco_since = (user_since if permco_since is None
                            else min(permco_since, user_since))
        if permco_since is None:
            permco_since = pd.Timestamp("2000-01-01").date()

        if permco_since > user_until:
            continue

        df = _yf_pull_one(yf, yf_ticker, permco_since, user_until)
        if df is None or df.empty:
            tickers_unresolved.append(f"{crsp_ticker} ({yf_ticker}, permco {permco})")
            time.sleep(sleep_between_tickers)
            continue
        df["market_cap"] = (df["close"] * df["shares"]) / 1_000.0   # raw → thousands
        pull_results[permco] = df
        ticker_used[permco] = yf_ticker
        print(f"  permco {permco}  {crsp_ticker} → {yf_ticker}  "
              f"{permco_since}..{user_until}  rows={len(df)}")
        time.sleep(sleep_between_tickers)

    if not pull_results:
        if tickers_unresolved:
            print(f"⚠ All target permcos failed yfinance: {tickers_unresolved}")
        return ImportResult(tickers_unresolved=tickers_unresolved)

    # Overlap consistency check
    overlap_pairs = []
    for permco, df in pull_results.items():
        last_d = last_known.get(permco)
        if last_d is None or last_d not in df.index:
            continue
        overlap_pairs.append((permco, last_d))
    crsp_mcaps = _overlay.existing_market_caps(conn, overlap_pairs)

    overlap_checks: list[dict] = []
    failed_tickers: list[dict] = []
    warned_tickers: list[dict] = []
    hard_threshold = overlap_tolerance * 10.0   # default 0.10

    for permco, last_d in overlap_pairs:
        crsp_mcap = crsp_mcaps.get((permco, last_d))
        if crsp_mcap is None or crsp_mcap <= 0:
            continue
        yf_mcap = float(pull_results[permco].loc[last_d, "market_cap"])
        pct = (yf_mcap / crsp_mcap) - 1.0
        verdict = "OK"
        if abs(pct) > hard_threshold:
            verdict = "FAIL"
            failed_tickers.append({
                "permco": permco, "ticker": ticker_used[permco],
                "date": last_d, "crsp": crsp_mcap, "yf": yf_mcap, "pct": pct,
            })
        elif abs(pct) > overlap_tolerance:
            verdict = "WARN"
            warned_tickers.append({
                "permco": permco, "ticker": ticker_used[permco],
                "date": last_d, "crsp": crsp_mcap, "yf": yf_mcap, "pct": pct,
            })
        overlap_checks.append({
            "permco": permco, "ticker": ticker_used[permco],
            "date": last_d, "crsp_mcap": crsp_mcap, "yf_mcap": yf_mcap,
            "pct_diff": pct, "verdict": verdict,
        })

    if overlap_checks:
        print("Overlap consistency check:")
        for c in overlap_checks:
            print(f"  permco {c['permco']:>6} {c['ticker']:<8} {c['date']}  "
                  f"CRSP {c['crsp_mcap']:>16,.0f}  yf {c['yf_mcap']:>16,.0f}  "
                  f"Δ {c['pct_diff']:+.2%}  {c['verdict']}")

    if failed_tickers and not skip_overlap_check:
        msg = [f"ABORT: overlap check failed for {len(failed_tickers)} ticker(s)"
               f" (threshold {hard_threshold:.0%}):"]
        for f in failed_tickers:
            msg.append(f"  permco {f['permco']} ({f['ticker']})  {f['date']}  "
                       f"CRSP {f['crsp']:,.0f}  yf {f['yf']:,.0f}  "
                       f"Δ {f['pct']:+.2%}")
        msg.append("No rows inserted. Investigate ticker mapping or pass "
                   "--skip-overlap-check.")
        raise SystemExit("\n".join(msg))

    # Build insert batch — drop overlap rows
    parts = []
    for permco, df in pull_results.items():
        last_d = last_known.get(permco)
        sub = df.reset_index().rename(columns={"index": "date"})
        if last_d is not None:
            sub = sub[sub["date"] != last_d]
        if sub.empty:
            continue
        sub["permco"] = int(permco)
        sub["provider_id"] = ticker_used[permco]
        sub["ticker_raw"]  = ticker_used[permco]
        sub["loaded_from"] = "yfinance"
        parts.append(sub[["permco", "date", "market_cap", "provider_id",
                          "ticker_raw", "loaded_from"]])
    if not parts:
        print("Nothing new to insert (overlap-only batch).")
        return ImportResult(
            tickers_unresolved=tickers_unresolved,
            overlap_checks=overlap_checks,
        )
    batch = pd.concat(parts, ignore_index=True)

    rows = batch[["permco", "date", "market_cap"]].to_dict(orient="records")
    enriched = _overlay.compute_retx_for_overlay(conn, rows)
    enriched_lookup = {
        (int(e["permco"]), _overlay.as_date(e["date"])): e for e in enriched
    }
    batch["_key"]           = list(zip(batch["permco"].astype(int), batch["date"]))
    batch["retx"]           = batch["_key"].map(lambda k: enriched_lookup.get(k, {}).get("retx"))
    batch["retx_synthetic"] = batch["_key"].map(lambda k: enriched_lookup.get(k, {}).get("retx_synthetic"))
    batch = batch.drop(columns=["_key"])

    n = _overlay.insert_overlay(conn, batch, source="yfinance")

    result = ImportResult(
        rows_imported=int(n),
        tickers_unresolved=tickers_unresolved,
        permcos_touched=sorted(set(batch["permco"].astype(int))),
        date_range=(batch["date"].min(), batch["date"].max()),
        retx_synthetic_rows=int(batch["retx_synthetic"].fillna(False).sum()),
        overlap_checks=overlap_checks,
    )

    if rebuild_pd_input:
        n_pi = weekly.build_pd_input(conn)
        result.pd_input_rows_after = int(n_pi)
        row = conn.execute("SELECT MAX(week_date) FROM pd_input").fetchone()
        if row and row[0]:
            result.pd_input_max_week_after = _overlay.as_date(row[0])

    if warned_tickers:
        print(f"⚠ {len(warned_tickers)} ticker(s) within soft-warn band "
              f"(>{overlap_tolerance:.0%}). Inspect overlap_checks.")
    return result
