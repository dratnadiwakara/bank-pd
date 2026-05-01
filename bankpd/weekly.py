"""
Resample CRSP daily → Friday-anchored weekly with backward-looking 252-day vol.
Resample FRED DGS10 daily → Friday-anchored weekly (last available rate ≤ Friday).
Build the consolidated `pd_input` table — the compute-ready join of
CRSP weekly market data + FRED + crsp_link (rssd) + ticker history + Y-9C.

The volatility window is computed on the daily grid (252 trading days backward
from each daily date) and then sampled at each Friday's date_eff.
"""
from __future__ import annotations

from typing import Optional

import duckdb

from . import config
from .db import attach_external, detach


def _generate_friday_calendar_sql(start_date: str, end_date: str) -> str:
    return f"""
        SELECT week_date FROM (
          SELECT
            range AS week_date
          FROM range(
            DATE '{start_date}',
            DATE '{end_date}' + INTERVAL 1 DAY,
            INTERVAL 1 DAY
          )
        )
        WHERE EXTRACT(dow FROM week_date) = 5  -- Friday
    """


def build_fred_weekly(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    start = start_date or config.START_DATE
    if end_date is None:
        row = conn.execute("SELECT MAX(date) FROM fred_dgs10").fetchone()
        if row is None or row[0] is None:
            return 0
        end = row[0].strftime("%Y-%m-%d") if hasattr(row[0], "strftime") else str(row[0])
    else:
        end = end_date

    sql = f"""
    WITH fridays AS (
      {_generate_friday_calendar_sql(start, end)}
    )
    SELECT f.week_date, d.r_decimal
    FROM fridays f
    ASOF LEFT JOIN (
      SELECT date, r_decimal
      FROM fred_dgs10
      WHERE r_decimal IS NOT NULL
    ) d
      ON f.week_date >= d.date
    WHERE d.date IS NOT NULL
    """

    conn.execute("DELETE FROM fred_weekly")
    conn.execute("INSERT INTO fred_weekly (week_date, r_decimal) " + sql)
    n = conn.execute("SELECT COUNT(*) FROM fred_weekly").fetchone()[0]
    return int(n)


def build_pd_input(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    """
    Rebuild pd_input as the consolidated input panel.

    Anchored to **every Friday** between the earliest crsp_daily date for
    each permco and `end_date` (default: today). When CRSP lags more than
    config.CRSP_STALE_DAYS behind a Friday, market data is NULL'd out and
    crsp_stale=TRUE — the row still exists. Y-9C is forward-filled via
    ASOF; staleness flagged when y9c_age_days > config.Y9C_STALE_DAYS.

    Only hard filter: rssd IS NOT NULL (no point storing weeks where the
    bank can't be linked to a regulatory ID).
    """
    from datetime import date as _date
    start = start_date or config.START_DATE
    end = end_date or _date.today().strftime("%Y-%m-%d")

    attach_external(conn, "ext_y9c", config.y9c_db_path())
    try:
        sql = f"""
        WITH daily_with_vol AS (
          SELECT
            permco,
            date,
            price,
            market_cap,
            STDDEV_SAMP(retx) OVER (
              PARTITION BY permco
              ORDER BY date
              ROWS BETWEEN {config.VOL_WINDOW - 1} PRECEDING AND CURRENT ROW
            ) * sqrt(252) AS sE_raw,
            COUNT(retx) OVER (
              PARTITION BY permco
              ORDER BY date
              ROWS BETWEEN {config.VOL_WINDOW - 1} PRECEDING AND CURRENT ROW
            ) AS n_obs_252
          FROM crsp_daily
        ),
        permco_bounds AS (
          SELECT permco, MIN(date) AS first_date FROM crsp_daily GROUP BY permco
        ),
        fridays AS (
          {_generate_friday_calendar_sql(start, end)}
        ),
        permco_fridays AS (
          SELECT pb.permco, f.week_date
          FROM permco_bounds pb
          JOIN fridays f ON f.week_date >= pb.first_date
        ),
        crsp_resampled AS (
          SELECT pf.permco,
                 pf.week_date,
                 d.date AS date_eff,
                 d.price,
                 d.market_cap,
                 CASE WHEN d.n_obs_252 >= {config.VOL_MIN_PERIODS} THEN d.sE_raw END AS sE,
                 d.n_obs_252
          FROM permco_fridays pf
          ASOF LEFT JOIN daily_with_vol d
            ON d.permco = pf.permco AND pf.week_date >= d.date
        ),
        with_link AS (
          SELECT cr.*, cl.rssd
          FROM crsp_resampled cr
          ASOF LEFT JOIN crsp_link cl
            ON cl.permco = cr.permco AND cr.week_date >= cl.quarter_end
        ),
        ticker_candidates AS (
          SELECT wl.permco, wl.week_date, t.permno, t.ticker, t.namedt, t.nameenddt
          FROM with_link wl
          JOIN crsp_ticker_hist t
            ON t.permco = wl.permco
           AND t.namedt    <= wl.week_date
           AND t.nameenddt >= wl.week_date
        ),
        ticker_ranked AS (
          SELECT
            permco, week_date, permno, ticker,
            ROW_NUMBER() OVER (
              PARTITION BY permco, week_date
              ORDER BY nameenddt DESC, namedt DESC, ticker ASC
            ) AS rn
          FROM ticker_candidates
        ),
        with_ticker AS (
          SELECT wl.*, tr.permno, tr.ticker
          FROM with_link wl
          LEFT JOIN ticker_ranked tr
            ON tr.permco = wl.permco
           AND tr.week_date = wl.week_date
           AND tr.rn = 1
        ),
        with_y9c AS (
          SELECT
            wt.*,
            yp.date         AS y9c_quarter_end,
            CAST(yp.total_liab AS DOUBLE) AS total_liab,
            CAST(yp.assets    AS DOUBLE) AS assets,
            CAST(yp.equity    AS DOUBLE) AS equity
          FROM with_ticker wt
          ASOF LEFT JOIN ext_y9c.bs_panel_y9c yp
            ON yp.id_rssd = wt.rssd AND wt.week_date >= yp.date
        ),
        joined AS (
          SELECT wy.*, fw.r_decimal AS r
          FROM with_y9c wy
          LEFT JOIN fred_weekly fw USING (week_date)
        ),
        flagged AS (
          SELECT
            *,
            CASE WHEN date_eff IS NOT NULL
                 THEN date_diff('day', date_eff, week_date)
            END AS crsp_lag_days,
            CASE WHEN y9c_quarter_end IS NOT NULL
                 THEN date_diff('day', y9c_quarter_end, week_date)
            END AS y9c_age_days
          FROM joined
        )
        SELECT
          permco,
          week_date,
          CASE WHEN crsp_lag_days IS NULL OR crsp_lag_days > {config.CRSP_STALE_DAYS}
               THEN NULL ELSE date_eff END AS date_eff,
          rssd,
          ticker,
          permno,
          CASE WHEN crsp_lag_days IS NULL OR crsp_lag_days > {config.CRSP_STALE_DAYS}
               THEN NULL ELSE market_cap END AS market_cap,
          CASE WHEN crsp_lag_days IS NULL OR crsp_lag_days > {config.CRSP_STALE_DAYS}
               THEN NULL ELSE price END AS price,
          CASE WHEN crsp_lag_days IS NULL OR crsp_lag_days > {config.CRSP_STALE_DAYS}
               THEN NULL ELSE sE END AS sE,
          CASE WHEN crsp_lag_days IS NULL OR crsp_lag_days > {config.CRSP_STALE_DAYS}
               THEN NULL ELSE n_obs_252 END AS n_obs_252,
          r,
          y9c_quarter_end,
          total_liab,
          assets,
          equity,
          CASE
            WHEN total_liab IS NULL OR total_liab <= 0 THEN NULL
            WHEN crsp_lag_days IS NULL OR crsp_lag_days > {config.CRSP_STALE_DAYS} THEN NULL
            ELSE market_cap / total_liab
          END AS E_scaled,
          EXTRACT(year  FROM week_date)::INTEGER AS year,
          EXTRACT(month FROM week_date)::INTEGER AS month,
          y9c_age_days,
          (y9c_age_days IS NOT NULL AND y9c_age_days > {config.Y9C_STALE_DAYS}) AS y9c_stale,
          crsp_lag_days,
          (crsp_lag_days IS NULL OR crsp_lag_days > {config.CRSP_STALE_DAYS}) AS crsp_stale
        FROM flagged
        WHERE rssd IS NOT NULL
        """

        conn.execute("DELETE FROM pd_input")
        conn.execute(
            """
            INSERT INTO pd_input (
              permco, week_date, date_eff, rssd, ticker, permno,
              market_cap, price, sE, n_obs_252, r,
              y9c_quarter_end, total_liab, assets, equity, E_scaled,
              year, month,
              y9c_age_days, y9c_stale, crsp_lag_days, crsp_stale
            )
            """ + sql
        )
        n = conn.execute("SELECT COUNT(*) FROM pd_input").fetchone()[0]
        return int(n)
    finally:
        detach(conn, "ext_y9c")
