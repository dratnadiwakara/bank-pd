"""
Light smoke tests — focused on import + DDL only.
The full BoA end-to-end run is exercised via CLI: `python -m bankpd.cli run-all --scope boa`.
"""
import os
import tempfile
from pathlib import Path

import duckdb

from bankpd import config
from bankpd.db import get_connection, init_schema


def test_imports():
    import bankpd  # noqa: F401
    import bankpd.compute  # noqa: F401
    import bankpd.compute_merton_dtd  # noqa: F401
    import bankpd.merton_pd_from_paper  # noqa: F401
    import bankpd.weekly  # noqa: F401
    import bankpd.pipeline  # noqa: F401
    import bankpd.cli  # noqa: F401


def test_init_schema_in_temp_db():
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "tmp.duckdb"
        conn = duckdb.connect(str(db_path))
        try:
            from bankpd.db import init_schema
            init_schema(conn)
            tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
            assert {"fred_dgs10", "fred_weekly", "crsp_daily",
                    "crsp_link", "crsp_ticker_hist",
                    "pd_input", "pd_panel"}.issubset(tables)
        finally:
            conn.close()


def test_value_surface_present():
    assert config.value_surface_path().exists(), \
        f"ValueSurface.mat missing at {config.value_surface_path()}"


def test_secrets_loadable():
    s = config.load_secrets()
    assert s.fred_api_key
    assert s.wrds_username
    assert s.wrds_password
