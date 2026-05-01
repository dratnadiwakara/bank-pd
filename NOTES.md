# bank-pd development notes

Chronological log of design decisions, gotchas, and lessons learned.
Newest entries on top.

---

## 2026-05-01 (evening) — crsp_link staleness gate (1-year hard threshold)

`crsp_link` (PERMCO ↔ BHC RSSD mapping) silently forward-fills via the
ASOF join in `build_pd_input`. If the local mirror is more than ~year
old, ticker → permco → rssd resolution can lock onto the wrong BHC after
M&A / re-orgs, and Y-9C balance sheets get joined to the wrong bank.
Added a hard staleness gate:

- New config: `LINK_STALE_DAYS = 365` (env override
  `BANK_PD_LINK_STALE_DAYS`).
- `freshness.FreshnessReport` gains `link_age_days`, `link_stale`.
  Format-report shows OK / STALE alongside Y-9C.
- New helper `freshness.assert_not_stale(rep, ignore_stale, check_y9c,
  check_link)` — single point that any pipeline / import command calls
  after `freshness.check()` to abort cleanly with `SystemExit(1)`.
- All compute / import commands now gate on it:
  - `update-inputs`: Y-9C check before refresh (sibling-repo source);
    link check **after** refresh (because the command's job is to
    refresh the mirror — only abort if even a fresh mirror is stale,
    i.e., NY Fed CSV in sibling repo is itself stale).
  - `compute-weekly`, `compute`, `import-yfinance`, `import-bloomberg`:
    abort if link stale (no refresh available; tells user to run
    `update-inputs` first or refresh sibling repo).
- All commands gain `--ignore-stale` flag for operator override
  (backfills, reproductions of historical states).
- `inputs-status` and `freshness` themselves never abort — they're
  diagnostic.

Why `import-yfinance` / `import-bloomberg` need the link check: ticker
→ permno → permco → rssd resolution all flows through `crsp_link` (and
`crsp_ticker_hist`, which is itself driven by the link permco list at
fetch time). A stale link silently mis-routes Bloomberg / yfinance rows
to the wrong bank.

Y-9C check is intentionally NOT enforced on the import commands —
overlay imports are a market-data path, Y-9C balance-sheet staleness
doesn't directly corrupt them. (compute commands still check Y-9C.)

---

## 2026-05-01 (later afternoon) — Yahoo Finance overlay + multi-source generalisation

WRDS lags >1 year; Bloomberg overlay requires a paid terminal. Added free
**yfinance** path for stale-CRSP fill.

**Schema change**: generalised `crsp_daily_overlay` from bloomberg-only:
- Renamed `bbg_unique_id` → `provider_id`
- Added `source TEXT` column (`'bloomberg' | 'yfinance' | …`)
- Same rename on `pd_input` (`bbg_unique_id` → `provider_id`)
- View `crsp_daily_combined` now passes `source` from the overlay row
  directly instead of hard-coding `'bloomberg'`.

**Refactor**: extracted shared overlay helpers into `bankpd/_overlay.py`
(`compute_retx_for_overlay`, `resolve_tickers_via_db`,
`resolve_permnos_to_permcos`, `as_date`, `insert_overlay`,
`prune_overlay`, `latest_known_date_per_permco`, `existing_market_caps`).
Both `bbg.py` and `yfdata.py` use them. Each importer now does only its
vendor-specific bits (xlsx parsing for bbg; yfinance API call + alias
map for yf).

**`bankpd/yfdata.py`**:
- Resolves rssd → permco → ticker via `crsp_ticker_hist`
- Applies `CRSP_TO_YF_ALIASES` (BRK.B → BRK-B, etc.); operator can extend
  via `--ticker-map CRSP=YF`
- For each permco, `since` defaults to the per-permco
  `MAX(crsp_daily_combined.date)` so the import has a **one-day overlap**
  with existing data
- Pulls `Ticker.history(close)` × `Ticker.get_shares_full()`,
  market_cap = (close × shares) / 1000 (raw USD → thousands)
- `auto_adjust=False` to keep raw close × shares semantics matching CRSP

**Overlap consistency check** (per user requirement):
- For each permco's first yfinance row on `last_known_date`, compare to
  existing `crsp_daily_combined.market_cap`
- ≤ 1% (default) — OK
- 1–10% — soft WARN, continues
- > 10% — **ABORT entire import** (no rows for any ticker). Signals a
  fundamental ticker / permco / split issue. Override with
  `--skip-overlap-check`. Exit code 1 so cron / CI sees failure.

CLI: `bankpd import-yfinance [--rssd … | --permco …] [--since DATE]
[--until DATE] [--ticker-map CRSP=YF] [--overlap-tolerance 0.01]
[--skip-overlap-check] [--no-rebuild]`.

**Diagnostics** (`inputs-status`) now shows per-source breakdown:
`crsp_daily_overlay: N rows (4 bloomberg + 35 yfinance; 39 active in
view, latest 2026-05-01)` and
`pd_input by source: 1304 crsp + 70 bloomberg + 35 yfinance`.

**Workflow**:
```
1. bankpd update-inputs --top-n 8        # fetch WRDS, build pd_input
2. bankpd import-yfinance                # fill stale tail (auto-detects)
3. bankpd compute-weekly                  # compute PDs through last Friday
```

If WRDS lags > 7 days, step 2 closes the gap.

---

## 2026-05-01 (afternoon) — Bloomberg market-cap overlay

WRDS CRSP daily lags 1+ week. To compute PDs for the latest Friday before
WRDS catches up, accept a Bloomberg market-cap snapshot from the user.

Bloomberg xlsx schema (4 cols, header row 0):
```
ID_BB_UNIQUE | TICKER | DATE | CUR_MKT_CAP_USD
```

`CUR_MKT_CAP_USD` is millions of USD (Bloomberg default); converted to
thousands at import to match CRSP `market_cap` units.

**New table** `crsp_daily_overlay` (keyed `(permco, date)`) — keeps
provenance (`bbg_unique_id`, `ticker_raw`, `loaded_from`) without
polluting the WRDS-shaped `crsp_daily` table.

**New view** `crsp_daily_combined` — WRDS rows preferred, overlay rows
fill gaps. `build_pd_input` now reads from this view. Once WRDS catches
up to a date the overlay covered, the overlay row is automatically hidden
by the view (no manual reconciliation needed; `bankpd prune-overlay`
exists for housekeeping).

**`pd_input` schema** gained two provenance columns: `data_source`
(`'crsp'` or `'bloomberg'`) and `bbg_unique_id`. Carry forward through
the existing `daily_with_vol` → `crsp_resampled` ASOF chain.

**Ticker → permco resolution** (in `bbg.import_bloomberg_excel`):
1. Operator-supplied `--ticker-map BAC=3151` overrides
2. Else: `crsp_ticker_hist` lookup with most recent `namedt ≤ row.date`
3. Then `permno → permco` via the same table
4. Unresolved tickers warn + skip (don't fail entire import)

Bloomberg ticker suffix (`"BAC US Equity"`) stripped via regex before
matching. `/` in share-class tickers normalised to `.` to match CRSP
convention.

**`retx` synthesis**: at import, for each new (permco, date), look up
the prior trading day's market_cap in `crsp_daily ∪ existing overlay ∪
the import batch`. Compute `mcap[t] / mcap[t-1] - 1`. If gap > 1
calendar day, set `retx_synthetic=TRUE`. The boundary row (first
overlay day after a multi-day WRDS gap) computes a multi-day return; one
row's contribution to the 252-day vol window biases sE upward by ~tens
of bps. Acceptable for the operator's stated use case (last few stale
weeks).

**Bias from weekly Bloomberg snapshots**: if Bloomberg gives weekly
(not daily) market caps, each week's "1-day" return is really a 7-day
return. With 4 such rows in a 252-day window, sE drift is ~10 bps in
typical regimes. For higher precision, pull daily Bloomberg.

**CLI**:
- `bankpd import-bloomberg PATH.xlsx [--sheet 0] [--ticker-map BAC=3151] [--no-rebuild]`
- `bankpd prune-overlay`

**`inputs-status`** now reports overlay coverage:
```
crsp_daily_overlay: 4 rows  (4 active in view, latest 2025-01-24;
                             pd_input rows from bloomberg: 70)
```

**Workflow**:
```
1. bankpd inputs-status
2. # in Bloomberg terminal, export xlsx with the 4 columns above
3. bankpd import-bloomberg ~/bbg.xlsx
4. bankpd compute-weekly       # or compute --rssd ...
```

**Gotcha during implementation**: pandas Timestamp passes
`isinstance(t, datetime.date)` (Timestamp inherits from datetime which
inherits from date). The lookup-key normalisation for retx-merge
required explicit `.date()` conversion via `pd.Timestamp(v).date()`.

---

## 2026-05-01 — split pipeline into four named tasks

`run-all --scope` was confusing; split CLI into four explicit task-aligned
commands so the operator can pick the right thing without remembering flag
combinations:

| Command | Purpose |
|---|---|
| `update-inputs` | Refresh FRED + CRSP + tickers for **all** link permcos, rebuild `pd_input`. Aborts on stale Y-9C. Prints freshness + coverage at the end. |
| `compute-weekly` | Strict: compute PDs for all banks up to **last Friday**. Aborts if pd_input lacks eligible rows for last Friday or Y-9C is stale. |
| `compute --since/--until/--rssd [--recompute]` | Flexible compute. No staleness gate. Default scope = all banks present in `pd_input`. |
| `inputs-status` | Read-only freshness + coverage diagnostic. No fetch, no compute. |

`run-all` removed.

New module `bankpd/diagnostics.py` produces `CoverageReport` (output-side
state — distinct from `freshness.FreshnessReport` which reports input
lag). Reports:
- `pd_input` rows, week range, distinct permcos
- eligible / stale-Y9C / stale-CRSP / no-Y9C row counts
- link permcos missing CRSP daily entirely
- `pd_panel` rows + missing-from-pd_panel count
- last-Friday-specific: eligible banks, already-computed, to-compute

`compute.assemble_inputs` generalised to take `rssd_filter`,
`week_date_max`, `exclude_existing` (in addition to existing
`permco_filter`, `week_date_min`).

Migration:
- `run-all --scope all` → `update-inputs` then `compute-weekly`
- `run-all --scope boa` → `compute --rssd 1073757`

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
