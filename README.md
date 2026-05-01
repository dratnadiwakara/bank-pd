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

### Single-bank smoke test (Bank of America)

```
python -m bankpd.cli run-all --scope boa
```

Pulls only BoA's CRSP daily, computes ~1,250 weekly observations.

### All listed banks (full panel)

```
python -m bankpd.cli run-all --scope all
```

First run is heavy (CRSP daily for ~5,000 permcos). Subsequent runs are incremental — only new Fridays get fetched and computed.

### Resolve BoA ids without computing

```
python -m bankpd.cli show-boa
```

### Pre-flight freshness report

```
python -m bankpd.cli freshness
```

Prints lag of every input source. Always exits 0. `run-all` runs the same
check internally and **aborts** when Y-9C is stale beyond threshold
(default 45 days past latest quarter end). Pass `--ignore-stale` to
override (e.g., for backfills). CRSP staleness (default 7 days past
today) is non-fatal — affected Fridays end up with NULL market data in
`pd_input` and are skipped at compute.

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

1. `init_schema` – ensure local DDL exists
2. `refresh_link_table` – mirror external link DB to `crsp_link`
3. `fetch_dgs10_incremental` – append new FRED DGS10 days
4. `fetch_crsp_daily_incremental` – append new CRSP days per permco (WRDS)
5. `fetch_crsp_tickers` – refresh ticker history (`crsp.stocknames`, common stock) for every permco present in `crsp_daily`
6. `build_fred_weekly` – Friday spot DGS10
7. `build_pd_input` – consolidated input panel: 252-day backward rolling vol on the daily grid → pick last trading day per Friday → ASOF-join crsp_link, Y-9C panel, fred_weekly, ticker history. Filtered to compute-ready rows (sE finite, total_liab>0, etc.)
8. `assemble_inputs` – read compute-ready rows from `pd_input` (incremental for new weeks)
9. `run_compute` + `upsert_pd_panel` – run NP value-surface + classic Merton kernels, INSERT OR REPLACE keyed `(week_date, permco)`

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
