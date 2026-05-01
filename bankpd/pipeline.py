"""
Orchestrator for the four named pipeline tasks:

  - update_inputs  : refresh FRED/CRSP/tickers + rebuild pd_input
                     (all link permcos)
  - compute_weekly : compute PDs for all banks up to last Friday (strict)
  - compute_range  : flexible compute with --since/--until/--rssd filters

Each is exposed via a CLI subcommand in `bankpd/cli.py`.
"""
from __future__ import annotations

import time
from datetime import date
from typing import Iterable, Optional

import duckdb
import pandas as pd

from . import config, crsp, diagnostics, fred, freshness, linker, weekly, compute
from .db import get_connection, init_schema


def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Task 1: update_inputs ────────────────────────────────────────────────────


def update_inputs(
    *,
    ignore_stale: bool = False,
    rssds: Optional[Iterable[int]] = None,
    top_n: Optional[int] = None,
) -> None:
    """
    Refresh FRED + CRSP daily + ticker history, then rebuild pd_input.
    Aborts on stale Y-9C unless ignore_stale=True. Prints freshness +
    coverage diagnostics at the end.

    Permco scope:
      - default: all confirmed link permcos (~1,450 banks).
      - `rssds`: restrict CRSP fetch to permcos linked to these RSSDs.
      - `top_n`: top N RSSDs by latest Y-9C `assets` (mutually exclusive
        with `rssds`).

    pd_input is always rebuilt over **every permco present in
    crsp_daily** regardless of fetch scope, so previously-cached banks
    stay in pd_input across partial-scope runs.
    """
    if rssds and top_n:
        raise ValueError("Pass either rssds or top_n, not both.")

    secrets = config.load_secrets()
    conn = get_connection(read_only=False)
    try:
        _log("Stage 1/8 init_schema")
        init_schema(conn)

        _log("Stage 2/8 freshness pre-flight")
        rep = freshness.check(conn)
        for line in freshness.format_report(rep).splitlines():
            _log(line)
        # Y-9C check fires before link refresh — sibling repo authoritative.
        # Link check runs only after Stage 3 (post-refresh) since
        # update-inputs is the command that *refreshes* the link.
        freshness.assert_not_stale(rep, ignore_stale=ignore_stale,
                                   check_y9c=True, check_link=False)

        _log("Stage 3/8 refresh_link_table")
        n_link = linker.refresh_link_table(conn)
        _log(f"  crsp_link rows: {n_link:,}")
        # Re-check link staleness now that local mirror is refreshed.
        rep = freshness.check(conn)
        freshness.assert_not_stale(rep, ignore_stale=ignore_stale,
                                   check_y9c=False, check_link=True)

        # Resolve fetch scope.
        if rssds:
            rssd_list = sorted({int(r) for r in rssds})
            target_permcos = linker.permcos_for_rssds(conn, rssd_list)
            _log(f"  scope: --rssd ({len(rssd_list)} RSSDs → "
                 f"{len(target_permcos)} permcos)")
        elif top_n:
            rssd_list = linker.top_n_rssds_by_assets(conn, int(top_n))
            target_permcos = linker.permcos_for_rssds(conn, rssd_list)
            _log(f"  scope: --top-n {top_n} → RSSDs={rssd_list} "
                 f"({len(target_permcos)} permcos)")
        else:
            target_permcos = linker.link_permcos(conn, confirmed_only=True)
            _log(f"  scope: all confirmed link permcos ({len(target_permcos):,})")

        _log("Stage 4/8 fetch_dgs10_incremental")
        n_fred = fred.fetch_dgs10_incremental(conn, secrets.fred_api_key)
        _log(f"  fred_dgs10 +{n_fred:,} rows")

        _log("Stage 5/8 fetch_crsp_daily_incremental")
        db_wrds = crsp.connect_wrds(secrets.wrds_username, secrets.wrds_password)
        try:
            n_crsp = crsp.fetch_crsp_daily_incremental(conn, target_permcos, db_wrds)
            _log(f"  crsp_daily +{n_crsp:,} rows")

            _log("Stage 6/8 fetch_crsp_tickers")
            rows = conn.execute(
                "SELECT DISTINCT permco FROM crsp_daily"
            ).fetchall()
            all_permcos_in_daily = [int(r[0]) for r in rows]
            n_tick = crsp.fetch_crsp_tickers(conn, all_permcos_in_daily, db_wrds)
            _log(f"  crsp_ticker_hist rows: {n_tick:,}")
        finally:
            db_wrds.close()

        _log("Stage 7/8 build_fred_weekly")
        n_fw = weekly.build_fred_weekly(conn)
        _log(f"  fred_weekly rows: {n_fw:,}")

        _log("Stage 8/8 build_pd_input")
        n_pi = weekly.build_pd_input(conn)
        _log(f"  pd_input rows: {n_pi:,}")

        _log("--- Coverage ---")
        cov = diagnostics.coverage(conn)
        for line in diagnostics.format_coverage(cov).splitlines():
            _log(line)
    finally:
        conn.close()


# ── Tasks 2, 3, 4: compute paths ─────────────────────────────────────────────


def _run_compute_for_inputs(
    conn: duckdb.DuckDBPyConnection,
    inp: pd.DataFrame,
    *,
    max_workers: Optional[int] = None,
) -> int:
    if inp.empty:
        _log("  nothing to compute — exit")
        return 0
    results = compute.run_compute(inp, max_workers=max_workers)
    _log(f"  computed rows: {len(results):,}")
    n_up = compute.upsert_pd_panel(conn, results)
    _log(f"  pd_panel upserted: {n_up:,}")
    row = conn.execute(
        "SELECT COUNT(*), MIN(week_date), MAX(week_date) FROM pd_panel"
    ).fetchone()
    _log(f"  pd_panel total={row[0]:,}  range={row[1]}..{row[2]}")
    return n_up


def compute_weekly(
    *,
    ignore_stale: bool = False,
    max_workers: Optional[int] = None,
) -> None:
    """
    Task 2: compute PDs for all banks up to **last Friday**, strict.

    Aborts if Y-9C stale, crsp_link stale, or if pd_input has no
    compute-eligible rows for last Friday. Override with
    `ignore_stale=True`.
    """
    conn = get_connection(read_only=False)
    try:
        init_schema(conn)

        rep = freshness.check(conn)
        for line in freshness.format_report(rep).splitlines():
            _log(line)
        freshness.assert_not_stale(rep, ignore_stale=ignore_stale)

        last_fri = diagnostics.last_friday()
        _log(f"Target: compute up to last Friday {last_fri.isoformat()}")

        cov = diagnostics.coverage(conn)
        for line in diagnostics.format_coverage(cov).splitlines():
            _log(line)

        if cov.last_friday_eligible_banks == 0:
            raise SystemExit(
                f"ABORT: pd_input has no compute-eligible rows for last Friday "
                f"({last_fri.isoformat()}). Run `bankpd update-inputs` first."
            )

        inp = compute.assemble_inputs(
            conn,
            week_date_max=last_fri.isoformat(),
            exclude_existing=True,
        )
        _log(f"  assembled rows (≤ {last_fri}): {len(inp):,}")
        _run_compute_for_inputs(conn, inp, max_workers=max_workers)
    finally:
        conn.close()


def compute_range(
    *,
    since: Optional[str] = None,
    until: Optional[str] = None,
    rssds: Optional[Iterable[int]] = None,
    recompute: bool = False,
    ignore_stale: bool = False,
    max_workers: Optional[int] = None,
) -> None:
    """
    Tasks 3 & 4: compute PDs with flexible filters.

    Aborts on stale crsp_link / Y-9C unless `ignore_stale=True`.
    since/until: ISO date strings, inclusive. None = no bound.
    rssds: list of RSSDs to filter to (or None for all).
    recompute: if True, recompute existing pd_panel rows; else skip them.
    """
    conn = get_connection(read_only=False)
    try:
        init_schema(conn)
        rep = freshness.check(conn)
        freshness.assert_not_stale(rep, ignore_stale=ignore_stale)
        rssd_list = sorted({int(r) for r in rssds}) if rssds else None
        scope_descr = (
            f"rssd={','.join(str(r) for r in rssd_list)}" if rssd_list else "all banks"
        )
        _log(
            f"compute_range scope: {scope_descr}  "
            f"since={since or '(min)'}  until={until or '(max)'}  "
            f"recompute={recompute}"
        )
        inp = compute.assemble_inputs(
            conn,
            rssd_filter=rssd_list,
            week_date_min=since,
            week_date_max=until,
            exclude_existing=not recompute,
        )
        _log(f"  assembled rows: {len(inp):,}")
        _run_compute_for_inputs(conn, inp, max_workers=max_workers)
    finally:
        conn.close()


def inputs_status() -> None:
    """Read-only diagnostic: prints freshness + coverage. No fetch, no compute."""
    conn = get_connection(read_only=False)
    try:
        init_schema(conn)
        rep = freshness.check(conn)
        print(freshness.format_report(rep))
        print()
        cov = diagnostics.coverage(conn)
        print(diagnostics.format_coverage(cov))
    finally:
        conn.close()
