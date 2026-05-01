"""
Orchestrator for the weekly NP/Merton PD pipeline.

`run_weekly(scope=...)` runs all stages end-to-end with sane logging.
"""
from __future__ import annotations

import time
from typing import Iterable, Literal, Optional

import duckdb
import pandas as pd

from . import config, crsp, fred, freshness, linker, weekly, compute
from .db import get_connection, init_schema


Scope = Literal["boa", "all"]


def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _resolve_permcos(
    conn: duckdb.DuckDBPyConnection,
    scope: Scope,
    explicit: Optional[Iterable[int]] = None,
) -> list[int]:
    if explicit:
        return sorted({int(p) for p in explicit})
    if scope == "boa":
        ids = linker.boa_ids(conn)
        _log(f"BoA resolved -> permco={ids['permco']} rssd={ids['rssd']} ({ids['name']})")
        return [ids["permco"]]
    return linker.link_permcos(conn, confirmed_only=True)


def run_weekly(
    *,
    scope: Scope = "boa",
    permcos: Optional[Iterable[int]] = None,
    skip_fred: bool = False,
    skip_crsp: bool = False,
    ignore_stale: bool = False,
) -> None:
    """End-to-end weekly run."""
    secrets = config.load_secrets()

    conn = get_connection(read_only=False)
    try:
        _log("Stage 1/9 init_schema")
        init_schema(conn)

        # Pre-flight freshness check
        rep = freshness.check(conn)
        for line in freshness.format_report(rep).splitlines():
            _log(line)
        if rep.y9c_stale and not ignore_stale:
            raise SystemExit(
                "ABORT: Y-9C is stale beyond threshold "
                f"({config.Y9C_STALE_DAYS} days). Refresh sibling repo "
                "empirical-data-construction or pass --ignore-stale to override."
            )

        _log("Stage 2/9 refresh_link_table")
        n_link = linker.refresh_link_table(conn)
        _log(f"  crsp_link rows: {n_link:,}")

        target_permcos = _resolve_permcos(conn, scope, permcos)
        _log(f"  target permcos: {len(target_permcos):,}")

        if skip_fred:
            _log("Stage 3/9 fetch_dgs10_incremental SKIPPED")
        else:
            _log("Stage 3/9 fetch_dgs10_incremental")
            n_fred = fred.fetch_dgs10_incremental(conn, secrets.fred_api_key)
            _log(f"  fred_dgs10 +{n_fred:,} rows")

        if skip_crsp:
            _log("Stage 4/9 fetch_crsp_daily_incremental SKIPPED")
            _log("Stage 5/9 fetch_crsp_tickers SKIPPED")
        else:
            _log("Stage 4/9 fetch_crsp_daily_incremental")
            db_wrds = crsp.connect_wrds(secrets.wrds_username, secrets.wrds_password)
            try:
                n_crsp = crsp.fetch_crsp_daily_incremental(conn, target_permcos, db_wrds)
                _log(f"  crsp_daily +{n_crsp:,} rows")

                _log("Stage 5/9 fetch_crsp_tickers")
                # Refresh tickers for every permco that has daily data, so
                # pd_input has tickers regardless of --scope.
                rows = conn.execute(
                    "SELECT DISTINCT permco FROM crsp_daily"
                ).fetchall()
                all_permcos_in_daily = [int(r[0]) for r in rows]
                n_tick = crsp.fetch_crsp_tickers(conn, all_permcos_in_daily, db_wrds)
                _log(f"  crsp_ticker_hist rows: {n_tick:,}")
            finally:
                db_wrds.close()

        _log("Stage 6/9 build_fred_weekly")
        n_fw = weekly.build_fred_weekly(conn)
        _log(f"  fred_weekly rows: {n_fw:,}")

        _log("Stage 7/9 build_pd_input")
        n_pi = weekly.build_pd_input(conn)
        _log(f"  pd_input rows: {n_pi:,}")

        _log("Stage 8/9 assemble_inputs")
        # Incremental: only week_dates not yet in pd_panel for these permcos.
        max_existing = None
        if target_permcos:
            ph = ",".join(str(p) for p in target_permcos)
            row = conn.execute(
                f"SELECT MAX(week_date) FROM pd_panel WHERE permco IN ({ph})"
            ).fetchone()
            max_existing = row[0] if row else None
        week_min = None
        if max_existing is not None:
            week_min = (pd.Timestamp(max_existing) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        inp = compute.assemble_inputs(
            conn, permco_filter=target_permcos, week_date_min=week_min
        )
        _log(f"  assembled rows: {len(inp):,}")

        if inp.empty:
            _log("  nothing to compute — exit")
            return

        _log("Stage 9/9 run_compute + upsert_pd_panel")
        results = compute.run_compute(inp)
        _log(f"  computed rows: {len(results):,}")
        n_up = compute.upsert_pd_panel(conn, results)
        _log(f"  pd_panel upserted: {n_up:,}")

        # Sanity summary
        row = conn.execute(
            "SELECT COUNT(*), MIN(week_date), MAX(week_date) FROM pd_panel"
        ).fetchone()
        _log(f"pd_panel total={row[0]:,}  range={row[1]}..{row[2]}")
    finally:
        conn.close()
