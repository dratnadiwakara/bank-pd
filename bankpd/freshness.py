"""
Data-freshness pre-flight: report lag of FRED, CRSP, Y-9C, link, pd_input
sources before running the compute pipeline.

`run-all` calls `check()` early and aborts when y9c_stale=True (unless
--ignore-stale).  `bankpd freshness` calls `check()` standalone (always
exits 0).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import duckdb

from . import config
from .db import attach_external, detach


@dataclass
class FreshnessReport:
    today: date
    fred_max_date: Optional[date] = None
    fred_lag_days: Optional[int] = None
    crsp_max_date: Optional[date] = None
    crsp_lag_days_to_today: Optional[int] = None
    y9c_max_quarter_end: Optional[date] = None
    y9c_age_days: Optional[int] = None
    y9c_stale: bool = False
    link_max_quarter_end: Optional[date] = None
    pd_input_max_week: Optional[date] = None
    pd_input_total_rows: int = 0
    pd_input_stale_y9c_rows: int = 0
    pd_input_stale_crsp_rows: int = 0
    warnings: list[str] = field(default_factory=list)


def _lag(latest: Optional[date], today: date) -> Optional[int]:
    if latest is None:
        return None
    return (today - latest).days


def _to_date(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v))
    except Exception:
        return None


def check(conn: duckdb.DuckDBPyConnection) -> FreshnessReport:
    today = date.today()
    rep = FreshnessReport(today=today)

    rep.fred_max_date = _to_date(
        conn.execute("SELECT MAX(date) FROM fred_dgs10").fetchone()[0]
    )
    rep.fred_lag_days = _lag(rep.fred_max_date, today)
    if rep.fred_max_date is None:
        rep.warnings.append("FRED DGS10 table is empty.")
    elif rep.fred_lag_days is not None and rep.fred_lag_days > 7:
        rep.warnings.append(
            f"FRED DGS10 lags {rep.fred_lag_days} days behind today; "
            "run-all will refresh on stage 3."
        )

    rep.crsp_max_date = _to_date(
        conn.execute("SELECT MAX(date) FROM crsp_daily").fetchone()[0]
    )
    rep.crsp_lag_days_to_today = _lag(rep.crsp_max_date, today)
    if rep.crsp_max_date is None:
        rep.warnings.append(
            "CRSP daily table is empty — run --scope first to populate."
        )
    elif rep.crsp_lag_days_to_today is not None and rep.crsp_lag_days_to_today > config.CRSP_STALE_DAYS:
        rep.warnings.append(
            f"CRSP daily lags {rep.crsp_lag_days_to_today} days behind today "
            f"(threshold {config.CRSP_STALE_DAYS}); recent Fridays may have NULL "
            "market data in pd_input. Pipeline will continue."
        )

    rep.link_max_quarter_end = _to_date(
        conn.execute("SELECT MAX(quarter_end) FROM crsp_link").fetchone()[0]
    )

    attach_external(conn, "ext_y9c", config.y9c_db_path())
    try:
        rep.y9c_max_quarter_end = _to_date(
            conn.execute("SELECT MAX(date) FROM ext_y9c.bs_panel_y9c").fetchone()[0]
        )
    finally:
        detach(conn, "ext_y9c")

    if rep.y9c_max_quarter_end is not None:
        rep.y9c_age_days = (today - rep.y9c_max_quarter_end).days
        rep.y9c_stale = rep.y9c_age_days > config.Y9C_STALE_DAYS
        if rep.y9c_stale:
            rep.warnings.append(
                f"Y-9C is {rep.y9c_age_days} days past its latest quarter-end "
                f"({rep.y9c_max_quarter_end.isoformat()}); threshold "
                f"{config.Y9C_STALE_DAYS}. Refresh sibling repo "
                "empirical-data-construction before computing PDs."
            )
    else:
        rep.warnings.append(
            "Y-9C external panel is unreachable or empty."
        )
        rep.y9c_stale = True

    row = conn.execute(
        "SELECT MAX(week_date), COUNT(*), "
        "       SUM(CASE WHEN y9c_stale THEN 1 ELSE 0 END), "
        "       SUM(CASE WHEN crsp_stale THEN 1 ELSE 0 END) "
        "FROM pd_input"
    ).fetchone()
    rep.pd_input_max_week = _to_date(row[0])
    rep.pd_input_total_rows = int(row[1] or 0)
    rep.pd_input_stale_y9c_rows = int(row[2] or 0)
    rep.pd_input_stale_crsp_rows = int(row[3] or 0)

    return rep


def _icon(ok: bool) -> str:
    return "OK " if ok else "!! "


def format_report(r: FreshnessReport) -> str:
    out: list[str] = []
    out.append("=== Data freshness ===")
    out.append(f"Today: {r.today.isoformat()}")

    if r.fred_max_date is not None:
        ok = r.fred_lag_days is not None and r.fred_lag_days <= 7
        out.append(
            f"FRED DGS10:        latest {r.fred_max_date.isoformat()} "
            f"(lag {r.fred_lag_days:>4} day{'s' if r.fred_lag_days != 1 else ''})  {_icon(ok)}"
        )
    else:
        out.append("FRED DGS10:        (empty)  !!")

    if r.crsp_max_date is not None:
        ok = (
            r.crsp_lag_days_to_today is not None
            and r.crsp_lag_days_to_today <= config.CRSP_STALE_DAYS
        )
        out.append(
            f"CRSP daily:        latest {r.crsp_max_date.isoformat()} "
            f"(lag {r.crsp_lag_days_to_today:>4} day{'s' if r.crsp_lag_days_to_today != 1 else ''})  "
            f"{_icon(ok)}{'' if ok else 'stale'}"
        )
    else:
        out.append("CRSP daily:        (empty)  !!")

    if r.y9c_max_quarter_end is not None:
        out.append(
            f"Y-9C panel:        latest q-end {r.y9c_max_quarter_end.isoformat()} "
            f"(age {r.y9c_age_days:>4} days)  "
            f"{_icon(not r.y9c_stale)}{'' if not r.y9c_stale else 'STALE'}"
        )
    else:
        out.append("Y-9C panel:        (unreachable)  !!")

    if r.link_max_quarter_end is not None:
        out.append(f"crsp_link:         latest q-end {r.link_max_quarter_end.isoformat()}")

    if r.pd_input_max_week is not None:
        out.append(
            f"pd_input:          latest week {r.pd_input_max_week.isoformat()}  "
            f"({r.pd_input_total_rows:,} rows; "
            f"{r.pd_input_stale_y9c_rows:,} stale Y-9C, "
            f"{r.pd_input_stale_crsp_rows:,} stale CRSP)"
        )
    else:
        out.append("pd_input:          (empty)")

    if r.warnings:
        out.append("")
        out.append(f"!! {len(r.warnings)} warning(s):")
        for w in r.warnings:
            out.append(f"  - {w}")

    return "\n".join(out)
