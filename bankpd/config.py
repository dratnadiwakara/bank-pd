"""
Central configuration for the bank-pd weekly PD pipeline.

Constants follow Nagel-Purnanandam (2019) defaults from version2.
Paths default to absolute Windows locations; override with env vars.
Secrets (FRED key, WRDS creds) come from a YAML at C:\\key-variables\\key-variables.yaml.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

# ── Repo layout ──────────────────────────────────────────────────────────────

BANK_PD_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = BANK_PD_ROOT / "data"
INPUTS_DIR = BANK_PD_ROOT / "inputs"

# ── External data (sibling repo) ─────────────────────────────────────────────

EMPIRICAL_ROOT = Path(
    os.getenv("FIN_DATA_ROOT", r"C:\empirical-data-construction")
)

# ── Secrets ──────────────────────────────────────────────────────────────────

SECRETS_PATH = Path(
    os.getenv("BANK_PD_SECRETS", r"C:\key-variables\key-variables.yaml")
)

# ── Compute constants (preserved from version2) ──────────────────────────────

VOL_VALUE = 0.2
T_PD = 5.0
GAMMA_PD = 0.002

# ── Pipeline constants ───────────────────────────────────────────────────────

START_DATE = "2000-01-01"
WEEK_DAY = "FRI"               # ISO weekday anchor
VOL_WINDOW = 252               # trading days
VOL_MIN_PERIODS = 126

# ── Data-freshness thresholds ────────────────────────────────────────────────

Y9C_STALE_DAYS = int(os.getenv("BANK_PD_Y9C_STALE_DAYS", "45"))
CRSP_STALE_DAYS = int(os.getenv("BANK_PD_CRSP_STALE_DAYS", "7"))

# ── DuckDB tuning ────────────────────────────────────────────────────────────

DUCKDB_THREADS = 4
DUCKDB_MEMORY_LIMIT = "6GB"


# ── Path helpers ─────────────────────────────────────────────────────────────


def data_db_path() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / "bank_pd.duckdb"


def value_surface_path() -> Path:
    return INPUTS_DIR / "ValueSurface.mat"


def y9c_db_path() -> Path:
    return EMPIRICAL_ROOT / "y9c" / "y9c.duckdb"


def link_db_path() -> Path:
    return EMPIRICAL_ROOT / "permco-rssd-link" / "permco-rssd-link.duckdb"


# ── Secrets ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Secrets:
    fred_api_key: str
    wrds_username: str
    wrds_password: str


def load_secrets(path: Optional[Path] = None) -> Secrets:
    p = Path(path) if path else SECRETS_PATH
    if not p.exists():
        raise FileNotFoundError(f"Secrets file not found: {p}")
    with p.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    try:
        return Secrets(
            fred_api_key=str(cfg["api_keys"]["fred"]).strip(),
            wrds_username=str(cfg["wrds"]["wrds_username"]).strip(),
            wrds_password=str(cfg["wrds"]["wrds_password"]).strip(),
        )
    except KeyError as exc:
        raise KeyError(f"Missing required key in {p}: {exc}") from exc
