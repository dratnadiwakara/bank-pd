"""
Coverage diagnostic for the output panel — distinct from `freshness`
(which reports input-source lag).

Used by `bankpd update-inputs` and `bankpd inputs-status` to answer:
"if I ran compute right now, what would I get?"
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

import duckdb

from . import config
from .db import attach_external, detach


def last_friday(today: Optional[date] = None) -> date:
    """Most recent date <= today with weekday Friday (Mon=0..Fri=4)."""
    today = today or date.today()
    return today - timedelta(days=(today.weekday() - 4) % 7)


@dataclass
class CoverageReport:
    today: date
    last_friday: date

    pd_input_rows: int = 0
    pd_input_min_week: Optional[date] = None
    pd_input_max_week: Optional[date] = None
    pd_input_distinct_permcos: int = 0
    pd_input_eligible_rows: int = 0
    pd_input_stale_y9c_rows: int = 0
    pd_input_stale_crsp_rows: int = 0
    pd_input_no_y9c_rows: int = 0       # rows with NULL total_liab (no Y-9C ever for that rssd)

    permcos_in_link: int = 0
    permcos_no_crsp_ever: int = 0       # link permcos with zero crsp_daily rows

    overlay_rows: int = 0
    overlay_active_in_view: int = 0     # overlay rows surfaced (no WRDS shadow)
    overlay_max_date: Optional[date] = None
    overlay_by_source: dict = field(default_factory=dict)   # {'bloomberg': n, 'yfinance': n, ...}
    pd_input_by_source: dict = field(default_factory=dict)  # {'crsp': n, 'bloomberg': n, ...}

    pd_panel_rows: int = 0
    pd_panel_min_week: Optional[date] = None
    pd_panel_max_week: Optional[date] = None
    pd_panel_to_compute: int = 0        # eligible pd_input rows not yet in pd_panel

    last_friday_eligible_banks: int = 0
    last_friday_already_computed: int = 0
    last_friday_to_compute: int = 0

    notes: list[str] = field(default_factory=list)


def _to_date(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v))
    except Exception:
        return None


def coverage(conn: duckdb.DuckDBPyConnection) -> CoverageReport:
    today = date.today()
    rep = CoverageReport(today=today, last_friday=last_friday(today))
    last_fri_iso = rep.last_friday.isoformat()

    # pd_input shape
    row = conn.execute(
        """
        SELECT COUNT(*),
               MIN(week_date),
               MAX(week_date),
               COUNT(DISTINCT permco)
        FROM pd_input
        """
    ).fetchone()
    rep.pd_input_rows = int(row[0] or 0)
    rep.pd_input_min_week = _to_date(row[1])
    rep.pd_input_max_week = _to_date(row[2])
    rep.pd_input_distinct_permcos = int(row[3] or 0)

    # eligible / stale counts
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN sE IS NOT NULL AND market_cap IS NOT NULL AND market_cap > 0
                    AND total_liab IS NOT NULL AND total_liab > 0
                    AND r IS NOT NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN y9c_stale THEN 1 ELSE 0 END),
          SUM(CASE WHEN crsp_stale THEN 1 ELSE 0 END),
          SUM(CASE WHEN total_liab IS NULL THEN 1 ELSE 0 END)
        FROM pd_input
        """
    ).fetchone()
    rep.pd_input_eligible_rows = int(row[0] or 0)
    rep.pd_input_stale_y9c_rows = int(row[1] or 0)
    rep.pd_input_stale_crsp_rows = int(row[2] or 0)
    rep.pd_input_no_y9c_rows = int(row[3] or 0)

    # link / crsp coverage
    row = conn.execute("SELECT COUNT(DISTINCT permco) FROM crsp_link").fetchone()
    rep.permcos_in_link = int(row[0] or 0)
    row = conn.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT permco FROM crsp_link
          EXCEPT
          SELECT DISTINCT permco FROM crsp_daily
        )
        """
    ).fetchone()
    rep.permcos_no_crsp_ever = int(row[0] or 0)

    # Bloomberg overlay
    row = conn.execute(
        "SELECT COUNT(*), MAX(date) FROM crsp_daily_overlay"
    ).fetchone()
    rep.overlay_rows = int(row[0] or 0)
    rep.overlay_max_date = _to_date(row[1])
    row = conn.execute(
        """
        SELECT COUNT(*) FROM crsp_daily_overlay o
        WHERE NOT EXISTS (
          SELECT 1 FROM crsp_daily c
          WHERE c.permco = o.permco AND c.date = o.date
        )
        """
    ).fetchone()
    rep.overlay_active_in_view = int(row[0] or 0)
    rep.overlay_by_source = {
        str(r[0]): int(r[1])
        for r in conn.execute(
            "SELECT source, COUNT(*) FROM crsp_daily_overlay GROUP BY source"
        ).fetchall()
    }
    rep.pd_input_by_source = {
        str(r[0]): int(r[1])
        for r in conn.execute(
            "SELECT data_source, COUNT(*) FROM pd_input GROUP BY data_source"
        ).fetchall()
    }

    # pd_panel shape
    row = conn.execute(
        "SELECT COUNT(*), MIN(week_date), MAX(week_date) FROM pd_panel"
    ).fetchone()
    rep.pd_panel_rows = int(row[0] or 0)
    rep.pd_panel_min_week = _to_date(row[1])
    rep.pd_panel_max_week = _to_date(row[2])

    # to-compute count: eligible pd_input rows missing from pd_panel
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM pd_input pi
        WHERE pi.sE IS NOT NULL AND pi.market_cap IS NOT NULL AND pi.market_cap > 0
          AND pi.total_liab IS NOT NULL AND pi.total_liab > 0
          AND pi.r IS NOT NULL
          AND (pi.week_date, pi.permco) NOT IN
              (SELECT week_date, permco FROM pd_panel)
        """
    ).fetchone()
    rep.pd_panel_to_compute = int(row[0] or 0)

    # last-Friday-specific
    row = conn.execute(
        """
        SELECT
          COUNT(*) FILTER (
            WHERE sE IS NOT NULL AND market_cap IS NOT NULL AND market_cap > 0
              AND total_liab IS NOT NULL AND total_liab > 0
              AND r IS NOT NULL),
          COUNT(*) FILTER (
            WHERE sE IS NOT NULL AND market_cap IS NOT NULL AND market_cap > 0
              AND total_liab IS NOT NULL AND total_liab > 0
              AND r IS NOT NULL
              AND (week_date, permco) IN (SELECT week_date, permco FROM pd_panel))
        FROM pd_input
        WHERE week_date = ?
        """,
        [last_fri_iso],
    ).fetchone()
    rep.last_friday_eligible_banks = int(row[0] or 0)
    rep.last_friday_already_computed = int(row[1] or 0)
    rep.last_friday_to_compute = (
        rep.last_friday_eligible_banks - rep.last_friday_already_computed
    )

    # notes
    if rep.pd_input_max_week is not None and rep.pd_input_max_week < rep.last_friday:
        rep.notes.append(
            f"pd_input.max(week_date) = {rep.pd_input_max_week} < last Friday "
            f"({rep.last_friday}). Run `update-inputs` to extend."
        )
    if rep.last_friday_eligible_banks == 0:
        rep.notes.append(
            f"No banks have compute-eligible rows for last Friday ({rep.last_friday}). "
            "Either CRSP daily lags, Y-9C lags, or pd_input wasn't built. "
            "Run `update-inputs`."
        )
    if rep.permcos_no_crsp_ever > 0:
        rep.notes.append(
            f"{rep.permcos_no_crsp_ever:,} link permcos have zero CRSP daily rows. "
            "Run `update-inputs` to fetch them (or expect their PDs to never compute)."
        )

    return rep


def format_coverage(r: CoverageReport) -> str:
    out: list[str] = []
    out.append("=== Coverage ===")
    out.append(f"Today: {r.today.isoformat()}    Last Friday: {r.last_friday.isoformat()}")
    if r.pd_input_rows == 0:
        out.append("pd_input: empty")
    else:
        out.append(
            f"pd_input: {r.pd_input_rows:,} rows  "
            f"({r.pd_input_min_week} .. {r.pd_input_max_week})  "
            f"{r.pd_input_distinct_permcos:,} permcos"
        )
        out.append(
            f"  eligible: {r.pd_input_eligible_rows:,}    "
            f"stale Y-9C: {r.pd_input_stale_y9c_rows:,}    "
            f"stale CRSP: {r.pd_input_stale_crsp_rows:,}    "
            f"no Y-9C: {r.pd_input_no_y9c_rows:,}"
        )

    out.append(
        f"crsp_link: {r.permcos_in_link:,} permcos    "
        f"missing CRSP daily: {r.permcos_no_crsp_ever:,}"
    )

    if r.overlay_rows > 0:
        breakdown = " + ".join(
            f"{n:,} {src}" for src, n in sorted(r.overlay_by_source.items())
        )
        out.append(
            f"crsp_daily_overlay: {r.overlay_rows:,} rows  "
            f"({breakdown}; {r.overlay_active_in_view:,} active in view, "
            f"latest {r.overlay_max_date})"
        )
    if r.pd_input_by_source:
        ptot = " + ".join(
            f"{n:,} {src}" for src, n in sorted(r.pd_input_by_source.items())
        )
        out.append(f"pd_input by source: {ptot}")

    if r.pd_panel_rows == 0:
        out.append("pd_panel: empty")
    else:
        out.append(
            f"pd_panel: {r.pd_panel_rows:,} rows  "
            f"({r.pd_panel_min_week} .. {r.pd_panel_max_week})"
        )
    out.append(f"to-compute (eligible & missing from pd_panel): {r.pd_panel_to_compute:,}")

    out.append(
        f"Last Friday {r.last_friday.isoformat()}:  "
        f"{r.last_friday_eligible_banks:,} eligible banks,  "
        f"{r.last_friday_already_computed:,} already computed,  "
        f"{r.last_friday_to_compute:,} to compute"
    )

    if r.notes:
        out.append("")
        out.append(f"Notes ({len(r.notes)}):")
        for n in r.notes:
            out.append(f"  - {n}")

    return "\n".join(out)
