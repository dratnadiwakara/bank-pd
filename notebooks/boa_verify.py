"""
BoA smoke-test verification.

Run after `python -m bankpd.cli run-all --scope boa`. Loads pd_panel for BoA,
prints summary stats, and saves a PNG of NP PD vs Merton PD over time.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from bankpd import config

BOA_RSSD = 1073757
OUT_PNG = Path(__file__).resolve().parent / "boa_pd_timeseries.png"


def main() -> None:
    conn = duckdb.connect(str(config.data_db_path()), read_only=True)
    try:
        df = conn.execute(
            """
            SELECT week_date, np_PD, merton_PD, sE, r,
                   market_cap_raw, total_liab,
                   L_fallback_used, fs_fallback_used,
                   B_fallback_used, bookF_fallback_used,
                   mdef_fallback_used
            FROM pd_panel
            WHERE rssd = ?
            ORDER BY week_date
            """,
            [BOA_RSSD],
        ).fetchdf()
    finally:
        conn.close()

    if df.empty:
        print("pd_panel is empty for BoA — run the pipeline first.")
        return

    df["week_date"] = pd.to_datetime(df["week_date"])

    print(f"Rows: {len(df):,}")
    print(f"Date range: {df['week_date'].min().date()}..{df['week_date'].max().date()}")
    print()
    print("PD summary:")
    print(df[["np_PD", "merton_PD"]].describe().round(6))
    print()

    fb_cols = ["L_fallback_used", "fs_fallback_used",
               "B_fallback_used", "bookF_fallback_used",
               "mdef_fallback_used"]
    fb_rates = df[fb_cols].mean().round(4)
    print("Fallback-flag mean (share of rows using NN fallback):")
    print(fb_rates)
    print()

    # 2008-Q4 spike
    crisis = df[(df["week_date"] >= "2008-09-01") & (df["week_date"] <= "2009-03-31")]
    if not crisis.empty:
        print(f"Sep 2008-Mar 2009 NP PD: max={crisis['np_PD'].max():.4f}  "
              f"mean={crisis['np_PD'].mean():.4f}")
        print(f"Sep 2008-Mar 2009 Merton PD: max={crisis['merton_PD'].max():.4f}  "
              f"mean={crisis['merton_PD'].mean():.4f}")
    print()

    try:
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(df["week_date"], df["np_PD"], label="NP PD", lw=1.0)
        ax.plot(df["week_date"], df["merton_PD"], label="Merton PD", lw=1.0, alpha=0.7)
        ax.set_title("Bank of America Corp — Weekly PD (RSSD 1073757)")
        ax.set_xlabel("week_date (Friday)")
        ax.set_ylabel("Probability of default (5-year horizon)")
        ax.legend()
        ax.grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(OUT_PNG, dpi=120)
        print(f"Saved plot: {OUT_PNG}")
    except ImportError:
        print("matplotlib not installed — install with: pip install matplotlib")


if __name__ == "__main__":
    main()
