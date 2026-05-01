# bank-pd development notes

Chronological log of design decisions, gotchas, and lessons learned.
Newest entries on top.

---

## 2026-04-30 (evening) — relax `pd_input` filters + freshness check

Old `build_pd_input` dropped any week missing `sE`, `market_cap`, `total_liab`,
or `r`. That meant Fridays where WRDS hadn't yet caught up disappeared from
`pd_input` entirely — easy to mis-read as "no bank that week".

Refactored:

- **Anchor**: every Friday between a permco's first `crsp_daily` date and
  today (was: capped at MAX(crsp_daily.date)). Trailing weeks now exist
  with mostly-NULL market data.
- **CRSP staleness**: `crsp_lag_days = week_date - date_eff`. When > 7
  days (config `BANK_PD_CRSP_STALE_DAYS`), NULL out `market_cap`, `price`,
  `sE`, `n_obs_252`, and `E_scaled`. Set `crsp_stale = TRUE`. Compute
  step's strict filter drops these rows automatically.
- **Y-9C staleness**: `y9c_age_days = week_date - y9c_quarter_end`. When
  > 45 days (config `BANK_PD_Y9C_STALE_DAYS`), set `y9c_stale = TRUE`.
  Forward-fill stays in place — the bank still has total_liab, just
  slightly aged.
- **Hard filter**: only `rssd IS NOT NULL` left. Everything else is in
  the row, even if NULL.

Added 4 columns to `pd_input`: `y9c_age_days`, `y9c_stale`,
`crsp_lag_days`, `crsp_stale`.

New module `bankpd/freshness.py`:
- `check(conn) -> FreshnessReport` — queries every source for its lag.
- `format_report(r)` — pretty-print summary block.

New CLI subcommand `bankpd freshness`. Pipeline `run-all` runs the
freshness check between init_schema and refresh_link_table; **hard-fails
with SystemExit(1)** when Y-9C is stale beyond threshold (use
`--ignore-stale` to override). CRSP staleness is non-fatal — pipeline
continues, compute simply skips weeks with NULL market data.

Compute path (`compute.assemble_inputs`) now applies the strict kernel-
input filter (`sE NOT NULL`, `market_cap > 0`, `total_liab > 0`,
`r NOT NULL`); `pd_panel` coverage is unchanged.

---

## 2026-04-30 (later) — refactor: `crsp_weekly` → `pd_input`

`crsp_weekly` was a thin Friday-anchored CRSP table. `compute.assemble_inputs`
did the ASOF joins to `crsp_link`, Y-9C, and `fred_weekly` at compute time.
Replaced both with a single fat `pd_input` table that materialises the full
compute-ready panel — identifiers (`permco`, `rssd`, `ticker`, `permno`),
market data (`market_cap`, `price`, `sE`, `n_obs_252`, `r`), Y-9C balance
sheet (`total_liab`, `assets`, `equity`, `y9c_quarter_end`), and the
pre-computed `E_scaled = market_cap / total_liab`.

Why: cleaner compute step (the kernel reads one table), reproducibility
(`pd_input` snapshots exactly what fed `pd_panel`), and ad-hoc cross-section
queries don't need to re-do the join.

New table `crsp_ticker_hist`: permno-level ticker history from
`crsp.stocknames` (filtered `shrcd ∈ {10, 11}` for common stock), bridged
to `permco` via `crsp.dsf`. Refreshed full each pipeline run.

Ticker selection in `pd_input`: ASOF on `namedt ≤ week_date ≤ nameenddt`,
tie-break by most recent `nameenddt`, then most recent `namedt`, then
alphabetic ticker. The plan called for tie-break by largest `market_cap` at
`date_eff` but our `crsp_daily` is already deduplicated to one row per
`(permco, date)` (no permno-level market caps stored), so we use the
namedt-based tie-break instead. For typical big banks (one common-stock
permno active at any time) the tie-break never fires.

`pd_input` is built for **every permco in `crsp_daily`**, regardless of
`--scope`. `--scope` still controls (a) which permcos get fresh CRSP data
and ticker refresh, (b) which permcos get `pd_panel` computed.

Stage list grew from 8 to 9: added `fetch_crsp_tickers` between
`fetch_crsp_daily_incremental` and `build_fred_weekly`; replaced
`build_crsp_weekly` with `build_pd_input`.

Migration path: drop the old `crsp_weekly` table; the new `init_schema`
creates `pd_input` and `crsp_ticker_hist`. No data is lost — `pd_input` is
fully derivable from `crsp_daily` + the external read-only DBs.

---

## 2026-04-30 — Initial scaffold

### Origin

This project replaces the older quarterly pipeline at
`C:\Users\dimut\OneDrive\github\_delete\np-dtd\version2`. That pipeline:
- Used CSV-versioned delta files under `inputs/` and `outputs/`.
- Fetched Y-9C via a placeholder ODBC `fetch_y9c_data`.
- Used WRDS for CRSP and FRED API for DGS10.
- Operated quarterly with 252-day rolling daily vol summarised to quarter-end.

The compute kernels (`compute_merton_dtd.py`, `merton_pd_from_paper.py`) and the
`ValueSurface.mat` lookup grid are reused **verbatim**. Everything around them
is rewritten to produce a Friday-anchored weekly panel using local DuckDB and
the sibling `empirical-data-construction` data repo.

### Design choices (locked in by user)

1. **Friday weekly anchor.** Holidays → `date_eff = last trading day ≤ Friday`,
   `week_date` stays Friday.
2. **Drop rows with missing `total_liab`.** Both NP and Merton PDs require it
   under version2 conventions (NP value surface is calibrated for
   `E = market_cap / total_liab`).
3. **Weekly spot DGS10** for `r` (not quarterly mean).
4. **Secrets at `C:\key-variables\key-variables.yaml`**.

### Data layout

- Local DuckDB: `data/bank_pd.duckdb` with tables
  `fred_dgs10`, `fred_weekly`, `crsp_daily`, `crsp_weekly`, `crsp_link`, `pd_panel`.
- External read-only: `y9c.duckdb` (view `bs_panel_y9c`), `permco-rssd-link.duckdb`
  (view `crsp_frb_link`). ATTACH read-only inside compute step; never copied
  locally so the sibling repo remains the source of truth.

### Gotchas surfaced during scaffolding

- **`compute_merton_dtd.py`** used `from merton_pd_from_paper import …` (top-level
  import). Changed to relative `from .merton_pd_from_paper import …` so the
  package import works under `python -m bankpd.cli`.
- **Kernel reads CSV, not DataFrame.** `compute_merton_dtd` accepts only
  `input_csv_path`. We write a temp CSV per call (`compute.run_compute`) — keep
  one batched call per run rather than per row to amortise the 30-MB ValueSurface
  load.
- **`E` rescale**: version2 silently divides `market_cap / total_liab` *before*
  the value-surface lookup. The NP value surface in `ValueSurface.mat` was
  calibrated against this rescale. Preserve it. If you ever forget, NP PDs blow up
  by orders of magnitude.
- **Volatility window.** 252 trading days of *daily* returns with min-periods 126.
  Even though we anchor weekly, the vol is **never** computed from weekly returns.
  Sample the daily-grid sE at each Friday's `date_eff`.
- **Y-9C $3B threshold (post-2018)** drops smaller BHCs from `bs_panel_y9c`.
  Those rows fall through the ASOF join and are excluded by design.
- **WRDS vs FRED lag.** WRDS CRSP is typically T+1. Don't run the pipeline with
  the expectation of same-day Friday data — wait one trading day past Friday.
- **DuckDB ASOF JOIN syntax.** Equality conditions go in `ON … = …`; the
  inequality goes last (e.g., `cw.week_date >= yp.date`). DuckDB picks the
  matching row that maximises the inequality.
- **`compute_merton_dtd` `preserve_columns`** is case-insensitive on lookup but
  preserves the column under the *exact name* you pass in. We pass lowercase
  names (`rssd, week_date, ...`) to keep DuckDB upserts simple.

### Performance: Delaunay build dominates compute

`compute_merton_dtd._run_from_value_surface_fast_parallel` builds one Delaunay
triangulation per rate slice (21 slices) over `321 × 3 × 41 = 39,483` 3-D points.
On scipy 1.17 / Python 3.14 each `Delaunay(points)` call takes **~130 s**. With
`os.cpu_count() - 1` worker threads the 21 builds run in parallel waves, so the
end-to-end build phase is ~5 min on a 12-core box.

For the BoA single-bank smoke test (1,279 rows): query phase (`find_simplex` +
`cKDTree.query`) is fast, ~3.5 s per slice. Total wall time ≈ build + query ≈
**5–8 min**. Acceptable for one-off; do not panic at the long Stage 8.

For all-banks scaling (~5,000 permcos × ~1,200 weeks = ~6 M rows), the query
phase dominates. Estimated wall time on the same box: 4–8 hours. Run over a
weekend the first time. Ideas if this needs to be faster later:
- Replace Delaunay with `scipy.interpolate.RegularGridInterpolator` — the value
  surface IS a regular 4-D grid, so triangulation is overkill.
- Drop NaN/duplicate points before constructing Delaunay (current code keeps
  them).
- Vectorise the per-row classic Merton fsolve.

### Open items

- **Verify BoA permco/RSSD** at first run.
  ✅ Resolved (smoke test): RSSD = **1073757**, PERMCO = **3151** (NOT 20436 as
  the plan guessed — the link table is authoritative).
- **Full-scope first run** will pull CRSP daily for ~5,000 permcos. Estimate
  several hours; consider running over a weekend.
- **NP PD vs `mdef` naming.** The compute kernel returns `mdef`; `pd_panel` stores
  it twice — as `mdef` (kernel output) and `np_PD` (semantic name) — so query SQL
  reads naturally either way.

### Smoke-test outcome (2026-04-30)

`python -m bankpd.cli run-all --scope boa`:

- 1,279 weekly rows, 2000-06-30 → 2024-12-27.
- np_PD: mean 0.27, std 0.14, range [0, 0.74] (~80 rows are NaN/0 where the
  value-surface Delaunay has no support — design choice: NN fallback is *not*
  applied to mdef, only to L/B/fs/bookF).
- merton_PD: mean 0.21, std 0.26, range [7.5e-5, 0.99].
- Sep 2008-Mar 2009: NP PD mean 0.54 (max 0.67), Merton PD mean 0.88 (max 0.99).
  Crisis spike present and large in both series.
- Fallback flag rates ~6.5% (within plan budget of <5–10%).
- 2008, 2011 (Euro crisis), 2020 (COVID) spikes visible in the plot at
  `notebooks/boa_pd_timeseries.png`. Looks realistic.

Wall clock: ~13 min on 12-core box for 1,279 rows. Delaunay build dominates;
this scales sub-linearly with row count, so all-banks should be a small
multiple of this for the same value-surface load.

### Comparison vs authors' published series

Authors' file: `_delete/np-dtd/matlab/BankDefaultProb_NP.csv` — quarterly
(Jan/Apr/Jul/Oct first-of-month), columns `permco, year, month, Modified_PD,
Merton_PD`. 120 BoA observations (1987-Q1 .. 2016-Q4).

ASOF match: each author observation aligned to the first Friday on or after
the author date. 60 NP PD pairs and 65 Merton PD pairs in our overlap window.

| series | corr(author, ours) | mean(diff) | median(diff) | mean abs diff | max abs diff |
|---|---|---|---|---|---|
| NP PD | 0.8948 | +0.0158 | +0.0202 | 0.0416 | 0.3825 |
| Merton PD | 0.9895 | +0.0465 | +0.0004 | 0.0616 | 0.2916 |

Reading:
- Pre-2008 (~2000-2007): our series matches authors' essentially exactly
  (mean abs diff ~0.005-0.01).
- 2008 crisis: our Merton PD pegs at the 0.99 cap while authors top out at
  ~0.88 — same direction, our peak slightly higher. Drives most of the
  Merton mean abs diff.
- 2008+ NP PD: ours runs systematically ~0.02 higher than authors. Likely
  attributable to Y-9C source differences (sibling repo's `bs_panel_y9c`
  vs whatever the authors processed in 2018) and/or date offset (authors
  observe at quarter-end, our weekly Friday sample lands a few days later).
- Visible NaN/zero gaps in our NP PD at 2008-12 and 2010-09 (rows where the
  Delaunay value-surface had no support and we deliberately do not apply
  NN fallback to `mdef`).

Verdict: **passes sanity gate**. NP PD correlation 0.89, Merton 0.99. Series
shape, crisis timing and magnitudes all align with authors. Compare script:
`notebooks/compare_to_authors.py`. Plot: `notebooks/boa_compare_authors.png`.

### Diagnosis: 83 BoA rows had NaN np_PD

Failing rows: mainly Oct 2008 → Mar 2010 (the GFC), plus 2006-06, 2006-07,
2015-02, 2019-06, 2022-03. All have `L_fallback_used=fs_fallback_used=
B_fallback_used=bookF_fallback_used=1` — i.e., the value-surface Delaunay had
no support for these `(E, sE, vol=0.2)` query points.

Root cause:
- Surface support: `xEt ∈ [0.0011, 5.42]`, `xsigEt ∈ [0.0011, 0.806]`,
  `xsig ∈ {0.15, 0.20, 0.25}`, `xr ∈ {0.000, 0.005, …, 0.100}`.
- BoA's crisis weeks have `sE ∈ [0.75, 1.53]` — outside the surface's
  `sigEt ≤ 0.806` boundary.
- Authors' MATLAB `scatteredInterpolant(Et, sigEt, sig, mdef)` defaults to
  `Method='linear'` AND `ExtrapolationMethod='linear'`, so the authors get
  smoothly extrapolated values outside the convex hull and never produce
  NaN.
- Our Python implementation in `compute_merton_dtd._interp_rate_slice`
  applied a 1-NN cKDTree fallback for L/B/fs/bookF but **deliberately left
  `mdef` as NaN** outside the hull (per a comment in version2:
  *"mdef: never apply NN fallback"*).

### Fix #1 (2026-04-30 evening) — out-of-hull extrapolation for `mdef`

In `bankpd/compute_merton_dtd.py`:
1. Inside `_interp_rate_slice`, switch the cKDTree fallback from k=1
   single-NN to k=4 inverse-distance-weighted NN, applied to **all five**
   surface outputs including `mdef`. k=4 IDW is a closer approximation to
   MATLAB's linear extrapolation than single-NN.
2. In `_run_from_value_surface_fast_parallel`, apply the nearest-rate
   fallback to `mdef` (using the new `mdefr_nn` table) before clipping to
   `[0, 1]`.
3. Added `mdef_fallback_used` to the schema and `pd_panel`.

Why k=4 IDW instead of literal MATLAB-linear extrapolation: replicating
`scatteredInterpolant`'s linear extrapolation in Python would require either
`scipy.interpolate.LinearNDInterpolator` plus a manual extrapolator (none
out-of-the-box in scipy) or a slow RBF. k=4 IDW gives ~the same answer near
the hull boundary (where most of our extrapolation lives) and reuses the
existing cKDTree infrastructure.

### Fix #2 (same evening) — triangular rate weights at exact grid points

After fix #1 the 83 NaN crisis-week rows were filled, but inspection turned
up another 19 BoA weeks where `np_PD ≈ 1e-17` (effectively zero) for inputs
clearly inside the value surface (e.g., 2005-04-08, 2017-03-17, 2019-04-05,
2021-12-24). Inputs were normal (E ~ 0.13, sE ~ 0.25, r ~ 0.025-0.045).

Root cause: `_triangular_rate_weights` used strict inequalities
`w[dr < 0] = 1+dr; w[dr > 0] = 1-dr` and explicitly zeroed `dr <= -1` and
`dr >= 1`. When the input rate `r` lands exactly on a grid step (the FRED
DGS10 quarterly mean does, several times: 0.045, 0.025, 0.015, etc.), `dr`
at the matching slice is exactly 0 — neither branch fires, so the weight at
the correct slice stays 0. The two adjacent slices have `dr = ±1` which
also got zeroed by the explicit `<= -1` / `>= 1` clamps. Net result: all
weights for that row are 0, mdef is zero-weighted, rate fallback should
have kicked in but didn't because tiny FP drift kept `support` slightly
above zero rather than exactly zero. Output: a weighted-sum of essentially
nothing → ~1e-17.

Authors' MATLAB has the same code shape but their quarterly-averaged DGS10
values rarely land exactly on a 0.005 step. Our Friday-spot DGS10 values do
land on grid steps occasionally because the Federal Reserve publishes the
yield to 2 decimals (e.g., 4.50%).

Fix: replaced the triangular form with explicit linear interpolation across
rate slices using `np.floor` + fractional position. Weights at any exact
grid match are exactly `[..., 0, 1, 0, ...]`. For `r` outside `[minr, maxr]`,
clamp to the boundary slice (NN at grid edge). The previous "weights all
zero → NaN → nearest-rate fallback" path was redundant given clamping;
removed implicit reliance on it. The `_nearest_rate_fallback` helper
remains unused but is kept for now (low cost, useful if the rate grid
changes).

### What lives where (file → role)

- `bankpd/config.py` – paths, constants, secret loader (YAML)
- `bankpd/db.py` – DuckDB helpers + schema DDL
- `bankpd/linker.py` – mirror external link DB → local `crsp_link`; bank lookup
- `bankpd/fred.py` – append-only DGS10 fetch
- `bankpd/crsp.py` – per-permco watermark CRSP fetch via WRDS
- `bankpd/y9c.py` – read-only ATTACH + panel loader
- `bankpd/weekly.py` – daily→Friday resample + 252-day rolling vol (DuckDB SQL)
- `bankpd/compute.py` – assemble inputs, call kernels, upsert `pd_panel`
- `bankpd/compute_merton_dtd.py` – reused kernel (NP value surface)
- `bankpd/merton_pd_from_paper.py` – reused kernel (classic Merton via fsolve)
- `bankpd/pipeline.py` – orchestrator
- `bankpd/cli.py` – argparse entry point
