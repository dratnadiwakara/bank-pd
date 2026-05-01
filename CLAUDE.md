# bank-pd — agent context

## What this project is

Weekly **Nagel–Purnanandam (NP, 2019) modified-Merton Probability of Default**
plus **classic Merton PD** for listed US banks. Friday-anchored panel,
2000-01-07 onward, refreshed incrementally.

Two end-state queries:

- Cross-section: NP PD for every bank at the latest available Friday
- Time series: NP PD + Merton PD for one bank by RSSD or PERMCO (e.g.,
  Bank of America), weekly with optional monthly resample

Replaces a quarterly version2 pipeline at
`C:\Users\dimut\OneDrive\github\_delete\np-dtd\version2`. The compute kernels
(`compute_merton_dtd.py`, `merton_pd_from_paper.py`) and `ValueSurface.mat`
lookup grid are reused verbatim from version2; everything around them is
new and uses local DuckDB.

## Python venv

**Always use this venv. Do not create `.venv` in the project folder.**

```
C:\envs\bank-pd-venv\Scripts\python.exe
```

Python 3.14.4 on Windows. Created via `python -m venv C:\envs\bank-pd-venv`.

Run any command with the explicit interpreter path and `PYTHONPATH=.` so the
`bankpd` package is importable from the repo root:

```bash
PYTHONPATH=. "C:/envs/bank-pd-venv/Scripts/python.exe" -m bankpd.cli freshness
PYTHONPATH=. "C:/envs/bank-pd-venv/Scripts/python.exe" -m bankpd.cli run-all --scope boa
PYTHONPATH=. "C:/envs/bank-pd-venv/Scripts/python.exe" -m pytest tests/test_smoke.py -x
```

To recreate the venv from scratch (e.g., on another machine):

```bash
python -m venv C:\envs\bank-pd-venv
"C:\envs\bank-pd-venv\Scripts\python.exe" -m pip install -r requirements.txt
"C:\envs\bank-pd-venv\Scripts\python.exe" -m pip install --no-deps wrds
```

`wrds` requires `--no-deps` because its `setup.cfg` pins
`pandas<2.3,>=2.2`. Functionally compatible with pandas 3.x.

## External data sources (read-only)

| Source | Path | Refreshed by |
|---|---|---|
| Y-9C quarterly bank financials | `C:\empirical-data-construction\y9c\y9c.duckdb` view `bs_panel_y9c` | sibling repo `empirical-data-construction` |
| PERMCO ↔ RSSD link | `C:\empirical-data-construction\permco-rssd-link\permco-rssd-link.duckdb` view `crsp_frb_link` | sibling repo |
| ValueSurface.mat | `inputs/ValueSurface.mat` | one-time copy from NP paper code |
| FRED DGS10 | API → local `fred_dgs10` | this repo |
| CRSP daily, ticker history | WRDS `crsp.dsf`, `crsp.stocknames` → local | this repo |
| Secrets | `C:\key-variables\key-variables.yaml` | local file |

## Local store

`data/bank_pd.duckdb` — single DuckDB file with:

- `fred_dgs10`, `fred_weekly` — risk-free rate
- `crsp_daily`, `crsp_ticker_hist`, `crsp_link` — equity data + identifiers
- `pd_input` — consolidated weekly input panel (one fat ASOF join, all
  sources together; staleness-flagged)
- `pd_panel` — output: `np_PD`, `merton_PD` keyed `(week_date, permco)`

## Pipeline stages

1. `init_schema`
2. `freshness.check` — abort if Y-9C stale beyond 45 days (override with
   `--ignore-stale`)
3. `refresh_link_table`
4. `fetch_dgs10_incremental`
5. `fetch_crsp_daily_incremental`
6. `fetch_crsp_tickers`
7. `build_fred_weekly`
8. `build_pd_input`
9. `assemble_inputs` + `run_compute` + `upsert_pd_panel`

(Stages 1–9 in the log; `freshness.check` runs between 1 and 2.)

## Conventions

- Weekly anchor: **Friday close**. Holiday Friday → `date_eff` = last
  trading day ≤ Friday; `week_date` stays Friday.
- Volatility: 252 trading days of daily `retx` ending at `date_eff`,
  annualised by √252. Min-periods 126.
- Y-9C ASOF: each week uses the most recent quarter-end ≤ `week_date` for
  `total_liab`, `assets`, `equity`. `y9c_age_days > 45` flagged
  `y9c_stale=TRUE` (system-level Y-9C stale aborts pipeline).
- CRSP staleness: `crsp_lag_days = week_date - date_eff`. > 7 days →
  `crsp_stale=TRUE` and market data NULL'd in `pd_input`.
- `E_scaled = market_cap / total_liab` — preserves NP value-surface
  calibration.
- Hard filter in `pd_input`: `rssd IS NOT NULL`. Compute path additionally
  requires `sE`, `market_cap`, `total_liab`, `r` non-null.
- Coverage: banks with no Y-9C `total_liab` for that quarter excluded at
  compute (mostly sub-$3B post-2018).

## Calibration constants (from NP paper)

`vol_value=0.2`, `T_pd=5.0`, `gamma_pd=0.002`. Override-able via env vars
`BANK_PD_Y9C_STALE_DAYS` (default 45) and `BANK_PD_CRSP_STALE_DAYS`
(default 7).

## Reusable helpers

- `compute_merton_dtd.compute_merton_dtd(input_csv_path, value_surface_path, ...)`
  — Delaunay interp on the 4-D NP value surface. Reads CSV; writes one
  DataFrame.
- `merton_pd_from_paper.merton_pd_from_paper(E, r, sE, T, gamma)` — classic
  Merton via fsolve on `(V0, sigma_v0)`.

## Performance note

Stage 9 compute is dominated by `Delaunay` build on scipy 1.17 + Python
3.14 — ~130 s per rate slice × 21 slices, 4 waves with ~6 cores → ~13–18
min wall on a 12-core box, regardless of input row count. Documented in
`NOTES.md`.

## Files of interest

- `bankpd/` — package source
- `notebooks/boa_verify.py` — summary stats + plot for BoA `pd_panel`
- `notebooks/compare_to_authors.py` — compare NP/Merton PD vs
  `BankDefaultProb_NP.csv` from the NP paper authors
- `NOTES.md` — chronological dev log + lessons learned (newest on top)
- `docs/architecture.md` — data-flow diagram + design rationale
