# bank-pd architecture

## Overview

```
                ┌─────────────────────────────────────┐
                │  External read-only DuckDBs         │
                │  (sibling repo empirical-data-...)  │
                │                                     │
                │   y9c.duckdb / bs_panel_y9c         │
                │   permco-rssd-link.duckdb /         │
                │     crsp_frb_link                   │
                └────────────┬────────────────────────┘
                             │ ATTACH READ_ONLY
                             ▼
   ┌────────────┐    ┌──────────────────────────────┐
   │  FRED API  │──▶ │  fred_dgs10  (daily)         │
   │  (DGS10)   │    └──────────────────────────────┘
   └────────────┘
   ┌──────────────────┐  ┌──────────────────────────┐
   │ WRDS crsp.dsf    │─▶│  crsp_daily              │
   │ (per permco day) │  └──────────────────────────┘
   └──────────────────┘
   ┌──────────────────┐  ┌──────────────────────────┐
   │ WRDS             │─▶│  crsp_ticker_hist        │
   │ crsp.stocknames  │  │ (permco, permno, ticker, │
   │ + crsp.dsf bridge│  │  namedt, nameenddt)      │
   └──────────────────┘  └──────────────────────────┘
                                 ▼
                       ┌────────────────────────────┐
                       │ build_fred_weekly          │
                       │ build_pd_input             │
                       │ (one fat ASOF join)        │
                       └────────────────────────────┘
                                 ▼
                       ┌────────────────────────────┐
                       │ pd_input                   │
                       │ (permco, week_date) PK     │
                       │ rssd, ticker, market_cap,  │
                       │ sE, r, total_liab,         │
                       │ assets, equity, E_scaled   │
                       └────────────────────────────┘
                                 ▼
                       ┌────────────────────────────┐
                       │ compute_merton_dtd         │
                       │ + merton_pd_from_paper     │
                       │ (ValueSurface.mat lookup)  │
                       └────────────────────────────┘
                                 ▼
                       ┌────────────────────────────┐
                       │ pd_panel                    │
                       │ (week_date, permco) PK      │
                       └────────────────────────────┘
```

## Why DuckDB

- Zero-config local store, columnar, fast window functions.
- Native `ASOF JOIN` — exactly the join semantics needed for forward-filling
  quarterly Y-9C onto each Friday.
- Free `ATTACH … READ_ONLY` against external DuckDBs maintained by the sibling
  repo means we never copy or duplicate Y-9C data.
- Single `.duckdb` file is easy to back up.

## Why weekly Friday anchor (not ISO last-trading-day)

The user wants a stable cadence aligned with how risk dashboards are typically
read (week ending Friday). Holidays are handled by `date_eff` (last trading day
≤ Friday) so the volatility lookback is always a real trading-day window.

## Why 252-trading-day daily vol

Matches version2 + the Nagel-Purnanandam paper. Computed on the daily grid so
the same realised vol estimate is sampled at any anchor cadence (Friday this
project, quarter-end the previous version). Migrating between cadences does not
change `sE`.

## Why drop missing `total_liab`

The NP value surface is calibrated against `E = market_cap / total_liab`. If
`total_liab` is missing, dividing by zero or substituting a different scaling
breaks the surface lookup. Cleaner to exclude.

## Why two PD names (`mdef` and `np_PD`)

`mdef` is the historical output column name from the version2 kernel and the
NP paper. `np_PD` is the semantically obvious name end-users will reach for.
Storing both costs nothing and keeps SQL ergonomic.

## Incremental refresh

| Table | Strategy |
|---|---|
| `fred_dgs10` | Append where `date > MAX(date)` |
| `crsp_daily` | Per-permco watermark, append where `date > MAX(date)` for that permco |
| `crsp_weekly` | DELETE+INSERT all rows (cheap; ~250 k rows/permco × n_permcos still tractable). Vol depends on 252-day lookback so any partial rebuild needs the full daily history available — easier to just rebuild. |
| `fred_weekly` | DELETE+INSERT all rows |
| `pd_panel` | INSERT OR REPLACE keyed `(week_date, permco)`. `assemble_inputs` filters rows newer than `MAX(week_date)` per permco to avoid recomputing unchanged history |

## Compute kernel reuse

`compute_merton_dtd.compute_merton_dtd` is unchanged from version2:

1. Load `ValueSurface.mat` (xLt, xBt, xEt, xmdef, xsigEt, xsig, xfs, xF, xr).
2. For each row, find the cell in `(E, sE, vol=0.2)` space via Delaunay
   triangulation; weight across rate slices triangularly by `r`.
3. Fall back to nearest-neighbour (cKDTree) when Delaunay support is absent.
4. Per-row classic Merton via `merton_pd_from_paper.merton_pd_from_paper`
   (fsolve on `(V0, sigma_v0)` from `(E, r, sE, T=5, gamma=0.002)`).

The value surface load is the dominant cost (~30 MB scipy.io.loadmat). Batch
all compute rows into a single `compute_merton_dtd` call to amortise this.

## Module dependency graph

```
config  ◀──── db
   ▲          ▲
   │          │
   └──── linker
   └──── fred
   └──── crsp
   └──── y9c
                    ▲
                    │
                  weekly
                    ▲
                    │
                  compute  ◀── compute_merton_dtd ◀── merton_pd_from_paper
                    ▲
                    │
                pipeline
                    ▲
                    │
                   cli
```
