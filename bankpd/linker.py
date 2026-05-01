"""
Mirror permco↔RSSD link from external DuckDB into local crsp_link table.
"""
from __future__ import annotations

from typing import Optional

import duckdb
import pandas as pd

from . import config
from .db import attach_external, detach


def refresh_link_table(conn: duckdb.DuckDBPyConnection) -> int:
    """Full refresh of local crsp_link from the external view."""
    attach_external(conn, "ext_link", config.link_db_path())
    try:
        conn.execute("DELETE FROM crsp_link")
        conn.execute(
            """
            INSERT INTO crsp_link (permco, rssd, quarter_end, name, confirmed)
            SELECT
              CAST(permco AS INTEGER),
              CAST(bhc_rssd AS INTEGER),
              CAST(quarter_end AS DATE),
              name,
              CAST(confirmed AS BOOLEAN)
            FROM ext_link.crsp_frb_link
            WHERE permco IS NOT NULL AND bhc_rssd IS NOT NULL
            """
        )
        n = conn.execute("SELECT COUNT(*) FROM crsp_link").fetchone()[0]
        return int(n)
    finally:
        detach(conn, "ext_link")


def find_bank(
    conn: duckdb.DuckDBPyConnection,
    name_pattern: str,
    *,
    confirmed_only: bool = True,
) -> pd.DataFrame:
    """Search crsp_link by name. Returns one row per (permco, rssd) with date span."""
    where = "name ILIKE ?"
    params = [name_pattern]
    if confirmed_only:
        where += " AND confirmed"
    sql = f"""
        SELECT permco, rssd, name,
               MIN(quarter_end) AS first_qe,
               MAX(quarter_end) AS last_qe,
               COUNT(*) AS n_quarters
        FROM crsp_link
        WHERE {where}
        GROUP BY permco, rssd, name
        ORDER BY n_quarters DESC, last_qe DESC
    """
    return conn.execute(sql, params).fetchdf()


def boa_ids(conn: duckdb.DuckDBPyConnection) -> dict:
    """Return Bank of America Corp top-tier BHC permco/RSSD."""
    df = find_bank(conn, "%bank of america corp%")
    if df.empty:
        df = find_bank(conn, "%bank of america%")
    if df.empty:
        raise RuntimeError("Bank of America not found in crsp_link")
    top = df.iloc[0]
    return {
        "permco": int(top["permco"]),
        "rssd": int(top["rssd"]),
        "name": str(top["name"]),
    }


def link_permcos(
    conn: duckdb.DuckDBPyConnection,
    *,
    confirmed_only: bool = True,
) -> list[int]:
    """All permcos in the link table (optionally filtered to confirmed rows)."""
    where = "WHERE confirmed" if confirmed_only else ""
    rows = conn.execute(
        f"SELECT DISTINCT permco FROM crsp_link {where} ORDER BY permco"
    ).fetchall()
    return [int(r[0]) for r in rows]


def permcos_for_rssds(
    conn: duckdb.DuckDBPyConnection,
    rssds: list[int],
) -> list[int]:
    """Resolve a list of RSSDs to their permcos via crsp_link."""
    if not rssds:
        return []
    placeholders = ",".join(str(int(r)) for r in rssds)
    rows = conn.execute(
        f"SELECT DISTINCT permco FROM crsp_link WHERE rssd IN ({placeholders}) ORDER BY permco"
    ).fetchall()
    return [int(r[0]) for r in rows]


def top_n_rssds_by_assets(
    conn: duckdb.DuckDBPyConnection,
    n: int,
) -> list[int]:
    """
    Return the top-N RSSDs by total assets, using each RSSD's most recent
    Y-9C quarter (so large banks aren't missed when the very latest quarter
    is sparsely populated).

    Filters to RSSDs that appear in `crsp_link` (i.e., listed banks with a
    CRSP permco).
    """
    from . import config
    from .db import attach_external, detach
    attach_external(conn, "ext_y9c", config.y9c_db_path())
    try:
        sql = """
        WITH latest_per_bank AS (
          SELECT id_rssd, MAX(date) AS d
          FROM ext_y9c.bs_panel_y9c
          WHERE assets IS NOT NULL
          GROUP BY id_rssd
        ),
        ranked AS (
          SELECT y.id_rssd, y.assets, y.date
          FROM ext_y9c.bs_panel_y9c y
          JOIN latest_per_bank l
            ON y.id_rssd = l.id_rssd AND y.date = l.d
          WHERE y.id_rssd IN (SELECT DISTINCT rssd FROM crsp_link WHERE confirmed)
        )
        SELECT id_rssd
        FROM ranked
        ORDER BY assets DESC
        LIMIT ?
        """
        rows = conn.execute(sql, [int(n)]).fetchall()
        return [int(r[0]) for r in rows]
    finally:
        detach(conn, "ext_y9c")
