"""
DuckDB helpers and schema DDL for the bank-pd local store.

Patterned after empirical-data-construction/utils/duckdb_utils.py.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

import duckdb

from . import config


SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS fred_dgs10 (
  date DATE PRIMARY KEY,
  dgs10_pct DOUBLE,
  r_decimal DOUBLE
);

CREATE TABLE IF NOT EXISTS fred_weekly (
  week_date DATE PRIMARY KEY,
  r_decimal DOUBLE
);

CREATE TABLE IF NOT EXISTS crsp_daily (
  permco INTEGER NOT NULL,
  date   DATE    NOT NULL,
  price  DOUBLE,
  ret    DOUBLE,
  retx   DOUBLE,
  shrout DOUBLE,
  market_cap DOUBLE,
  PRIMARY KEY (permco, date)
);
CREATE INDEX IF NOT EXISTS ix_crsp_daily_date ON crsp_daily(date);

CREATE TABLE IF NOT EXISTS crsp_link (
  permco INTEGER NOT NULL,
  rssd   INTEGER NOT NULL,
  quarter_end DATE NOT NULL,
  name TEXT,
  confirmed BOOLEAN,
  PRIMARY KEY (permco, rssd, quarter_end)
);

CREATE TABLE IF NOT EXISTS crsp_ticker_hist (
  permco    INTEGER NOT NULL,
  permno    INTEGER NOT NULL,
  ticker    TEXT,
  comnam    TEXT,
  shrcd     INTEGER,
  namedt    DATE NOT NULL,
  nameenddt DATE NOT NULL,
  PRIMARY KEY (permco, permno, namedt)
);
CREATE INDEX IF NOT EXISTS ix_ticker_permco ON crsp_ticker_hist(permco);

CREATE TABLE IF NOT EXISTS pd_input (
  permco    INTEGER NOT NULL,
  week_date DATE    NOT NULL,
  date_eff  DATE,
  rssd      INTEGER,
  ticker    TEXT,
  permno    INTEGER,
  market_cap DOUBLE,
  price     DOUBLE,
  sE        DOUBLE,
  n_obs_252 INTEGER,
  r         DOUBLE,
  y9c_quarter_end DATE,
  total_liab DOUBLE,
  assets    DOUBLE,
  equity    DOUBLE,
  E_scaled  DOUBLE,
  year      INTEGER,
  month     INTEGER,
  y9c_age_days  INTEGER,
  y9c_stale     BOOLEAN,
  crsp_lag_days INTEGER,
  crsp_stale    BOOLEAN,
  built_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (permco, week_date)
);
CREATE INDEX IF NOT EXISTS ix_pd_input_rssd ON pd_input(rssd, week_date);
CREATE INDEX IF NOT EXISTS ix_pd_input_week ON pd_input(week_date);

CREATE TABLE IF NOT EXISTS pd_panel (
  week_date DATE NOT NULL,
  permco    INTEGER NOT NULL,
  rssd      INTEGER,
  total_liab DOUBLE,
  market_cap_raw DOUBLE,
  E_scaled DOUBLE,
  sE DOUBLE,
  r  DOUBLE,
  L DOUBLE, B DOUBLE, mdef DOUBLE, fs DOUBLE, bookF DOUBLE,
  merton_PD DOUBLE,
  np_PD DOUBLE,
  L_fallback_used TINYINT,
  fs_fallback_used TINYINT,
  B_fallback_used TINYINT,
  bookF_fallback_used TINYINT,
  mdef_fallback_used TINYINT,
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (week_date, permco)
);
CREATE INDEX IF NOT EXISTS ix_pd_panel_rssd ON pd_panel(rssd, week_date);
"""


def _apply_pragmas(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(f"PRAGMA threads={config.DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{config.DUCKDB_MEMORY_LIMIT}'")


def get_connection(
    db_path: Optional[Path] = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    p = Path(db_path) if db_path else config.data_db_path()
    conn = duckdb.connect(str(p), read_only=read_only)
    _apply_pragmas(conn)
    return conn


@contextmanager
def transactional_connection(
    db_path: Optional[Path] = None,
) -> Iterator[duckdb.DuckDBPyConnection]:
    conn = get_connection(db_path, read_only=False)
    try:
        conn.execute("BEGIN TRANSACTION")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def attach_external(
    conn: duckdb.DuckDBPyConnection,
    alias: str,
    db_path: Path,
) -> None:
    if not Path(db_path).exists():
        raise FileNotFoundError(f"External DuckDB not found: {db_path}")
    conn.execute(f"ATTACH '{db_path}' AS {alias} (READ_ONLY)")


def detach(conn: duckdb.DuckDBPyConnection, alias: str) -> None:
    try:
        conn.execute(f"DETACH {alias}")
    except duckdb.Error:
        pass


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(SCHEMA_DDL)


def max_value(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    col: str,
    where: Optional[str] = None,
):
    sql = f"SELECT MAX({col}) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    row = conn.execute(sql).fetchone()
    return row[0] if row else None
