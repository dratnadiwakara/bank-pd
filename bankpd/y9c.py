"""
Read-only access to external Y-9C panel.

Materialises a view over the attached external DuckDB so other modules can ASOF-join.
"""
from __future__ import annotations

from typing import Iterable, Optional

import duckdb
import pandas as pd

from . import config
from .db import attach_external, detach


def attach_y9c(conn: duckdb.DuckDBPyConnection, alias: str = "ext_y9c") -> None:
    attach_external(conn, alias, config.y9c_db_path())


def detach_y9c(conn: duckdb.DuckDBPyConnection, alias: str = "ext_y9c") -> None:
    detach(conn, alias)


def y9c_panel_df(
    conn: duckdb.DuckDBPyConnection,
    rssd_list: Optional[Iterable[int]] = None,
) -> pd.DataFrame:
    """Pull (rssd, quarter_end, total_liab, assets, equity) into pandas."""
    attach_y9c(conn)
    try:
        if rssd_list is not None:
            ids = sorted({int(r) for r in rssd_list})
            if not ids:
                return pd.DataFrame(
                    columns=["rssd", "quarter_end", "total_liab", "assets", "equity"]
                )
            placeholders = ",".join(str(i) for i in ids)
            where = f"WHERE id_rssd IN ({placeholders})"
        else:
            where = ""
        sql = f"""
            SELECT
              CAST(id_rssd AS INTEGER) AS rssd,
              CAST(date    AS DATE)    AS quarter_end,
              CAST(total_liab AS DOUBLE) AS total_liab,
              CAST(assets   AS DOUBLE) AS assets,
              CAST(equity   AS DOUBLE) AS equity
            FROM ext_y9c.bs_panel_y9c
            {where}
            ORDER BY rssd, quarter_end
        """
        return conn.execute(sql).fetchdf()
    finally:
        detach_y9c(conn)
