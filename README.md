# bank-pd

Weekly **Nagel–Purnanandam (NP, 2019) modified-Merton Probability of Default** and **classic Merton PD** for listed US banks. Friday-anchored panel, ~2000-01-07 onward, refreshed incrementally.

## What it answers

- **Cross-section today**: the NP PD for every bank at the most recent Friday.
- **Time series for one bank**: weekly (or monthly-resampled) NP PD + Merton PD for any bank by RSSD or PERMCO. Example: Bank of America Corp.

## Data sources

| Source | Where | Maintained by |
|---|---|---|
| Y-9C quarterly bank financials | `C:\empirical-data-construction\y9c\y9c.duckdb` | sibling repo `empirical-data-construction` |
| PERMCO ↔ RSSD link | `C:\empirical-data-construction\permco-rssd-link\permco-rssd-link.duckdb` | sibling repo |
| NP value surface | `inputs/ValueSurface.mat` | one-time copy from Nagel–Purnanandam paper code |
| FRED DGS10 (10y CMT yield) | API → local `fred_dgs10` table | this repo |
| CRSP daily prices, returns, market cap | WRDS `crsp.dsf` → local `crsp_daily` table | this repo |
| Secrets (FRED key, WRDS creds) | `C:\key-variables\key-variables.yaml` | local file |

Local store: `data/bank_pd.duckdb`.

## Install

```
python -m pip install -e .
```

Required Python packages: `duckdb`, `pandas`, `numpy`, `scipy`, `wrds`, `requests`, `pyyaml`. See `pyproject.toml`.

## One-time setup

1. Confirm `inputs/ValueSurface.mat` exists (copied during scaffold).
2. Confirm `C:\key-variables\key-variables.yaml` has:
   ```yaml
   api_keys:
     fred: "..."
   wrds:
     wrds_username: "..."
     wrds_password: "..."
   ```
3. Confirm `C:\empirical-data-construction\y9c\y9c.duckdb` and `permco-rssd-link.duckdb` exist (refresh via the sibling repo if not).

## Run

The pipeline exposes four task-aligned commands.

### Task 1: update inputs (all banks, up to last Friday)

```
python -m bankpd.cli update-inputs
```

Fetches FRED DGS10, CRSP daily, and ticker history for **all link permcos**.
Rebuilds `pd_input`. Aborts if Y-9C is stale beyond 45 days (override with
`--ignore-stale`). Prints freshness + coverage diagnostics at the end —
including the count of banks with compute-eligible rows for last Friday.

### Task 2: weekly compute (strict)

```
python -m bankpd.cli compute-weekly
```

Computes PDs for **all banks up to last Friday**. Strict: aborts if
`pd_input` has no compute-eligible rows for last Friday (run
`update-inputs` first), or if Y-9C is stale (override with
`--ignore-stale`).

### Task 3: backfill from a date

```
python -m bankpd.cli compute --since 2024-01-01
```

Computes PDs for all banks from `--since` (inclusive) onward. If
`--since` is omitted, uses the earliest week in `pd_input`. No staleness
gate — fills in whatever `pd_input` rows are eligible and missing from
`pd_panel`.

### Task 4: compute for specific banks

```
python -m bankpd.cli compute --rssd 1073757,1027004
```

Same as Task 3 but restricted to listed RSSDs. Combine with `--since` /
`--until` for windowing. Pass `--recompute` to overwrite existing
`pd_panel` rows.

### Diagnostics

```
python -m bankpd.cli inputs-status   # freshness + coverage, read-only
python -m bankpd.cli freshness       # input-source lag only
python -m bankpd.cli show-boa        # resolve BoA permco/RSSD
```

`inputs-status` is the right starting point: it tells you whether to run
`update-inputs`, what `compute-weekly` would do, and how many
`(week_date, permco)` pairs are pending compute.

### Bloomberg overlay (fill stale CRSP weeks)

```
python -m bankpd.cli import-bloomberg path/to/bbg.xlsx [--ticker-map BAC=3151]
python -m bankpd.cli prune-overlay
```

Bloomberg xlsx schema (4 cols, header row 0):
`ID_BB_UNIQUE | TICKER | DATE | CUR_MKT_CAP_USD` (USD millions).

Overlay rows go into `crsp_daily_overlay`; `pd_input` reads through view
`crsp_daily_combined` (WRDS wins, overlay fills gaps). Source recorded
in `pd_input.data_source` and `pd_input.provider_id`.

### Yahoo Finance overlay (free alternative)

```
python -m bankpd.cli import-yfinance --rssd 1073757,1039502
python -m bankpd.cli import-yfinance               # all stale permcos
```

Pulls `close × shares_outstanding` from yfinance and writes overlay rows
with `source='yfinance'`. No API key. Per-permco `since` defaults to
last day in `crsp_daily_combined` for that permco → **one-day overlap**
with existing data is used as a consistency check. Mismatch > 10% on
any ticker aborts the whole import (signals broken ticker mapping).

## Query examples (DuckDB CLI)

```sql
-- Cross-section: NP PD for all banks at the latest available Friday
WITH latest AS (SELECT MAX(week_date) AS d FROM pd_panel)
SELECT p.rssd, l.name, p.np_PD, p.merton_PD, p.market_cap_raw
FROM pd_panel p, latest
LEFT JOIN crsp_link l USING (permco)
WHERE p.week_date = latest.d
ORDER BY p.np_PD DESC;

-- BoA weekly time series
SELECT week_date, np_PD, merton_PD
FROM pd_panel
WHERE rssd = 1073757
ORDER BY week_date;

-- BoA monthly resample (last week of month)
SELECT date_trunc('month', week_date) AS month,
       last(np_PD ORDER BY week_date) AS np_PD,
       last(merton_PD ORDER BY week_date) AS merton_PD
FROM pd_panel WHERE rssd = 1073757
GROUP BY 1 ORDER BY 1;

-- BoA inputs over time (from pd_input)
SELECT week_date, ticker, total_liab, market_cap, sE, r, E_scaled
FROM pd_input WHERE permco = 3151 ORDER BY week_date DESC LIMIT 20;

-- Latest pd_input cross-section for inspection
SELECT permco, ticker, rssd, week_date, market_cap, total_liab, sE, r
FROM pd_input
WHERE week_date = (SELECT MAX(week_date) FROM pd_input)
ORDER BY market_cap DESC LIMIT 50;
```

## Pipeline stages

`update-inputs`:

1. `init_schema`
2. `freshness.check` (aborts on stale Y-9C)
3. `refresh_link_table` – mirror external link DB to `crsp_link`
4. `fetch_dgs10_incremental` – append new FRED DGS10 days
5. `fetch_crsp_daily_incremental` – append new CRSP days per permco (WRDS), for all link permcos
6. `fetch_crsp_tickers` – refresh ticker history (`crsp.stocknames`, common stock) for every permco present in `crsp_daily`
7. `build_fred_weekly` – Friday spot DGS10
8. `build_pd_input` – consolidated input panel: 252-day backward rolling vol on the daily grid → pick last trading day per Friday → ASOF-join `crsp_link`, Y-9C panel, `fred_weekly`, ticker history. Anchored on every Friday between each permco's first CRSP date and today; market data NULL'd when CRSP lag > 7 days.

`compute-weekly` / `compute`:

9. `assemble_inputs` – read compute-ready rows from `pd_input` (filtered by `--since` / `--until` / `--rssd`; skips `(week_date, permco)` pairs already in `pd_panel` unless `--recompute`)
10. `run_compute` + `upsert_pd_panel` – run NP value-surface + classic Merton kernels, INSERT OR REPLACE keyed `(week_date, permco)`

## Conventions

- Weekly anchor: **Friday close**. If Friday is a holiday, `date_eff` is the previous trading day; the `week_date` anchor remains the Friday.
- Volatility: **252 trading days of daily `retx`** ending at `date_eff` (annualized by √252). Min-periods 126.
- Y-9C ASOF: each week uses the **most recent quarter-end ≤ week_date** for `total_liab`.
- `E` passed to the value surface = `market_cap / total_liab` (preserves version2 NP calibration).
- Coverage: banks with no Y-9C `total_liab` for that quarter are dropped at compute (mainly sub-$3B banks post-2018). Their rows still exist in `pd_input` with NULL `total_liab` for visibility.
- Staleness flags in `pd_input`:
  - `y9c_stale = TRUE` when `week_date - y9c_quarter_end > 45 days`
  - `crsp_stale = TRUE` when `week_date - date_eff > 7 days` (or no CRSP data ≤ Friday). Stale CRSP rows have NULL `market_cap`, `price`, `sE`, `n_obs_252`, `E_scaled`.

## Calibration constants (from version2 / NP paper)

`vol_value=0.2`, `T_pd=5.0`, `gamma_pd=0.002`.

## Files

- `bankpd/` package — pipeline modules
- `inputs/ValueSurface.mat` — NP precomputed lookup
- `data/bank_pd.duckdb` — output store
- `notebooks/` — exploratory + smoke-test notebooks
- `tests/` — smoke tests
- `NOTES.md` — chronological dev log + lessons learned
- `docs/architecture.md` — design rationale + data flow
