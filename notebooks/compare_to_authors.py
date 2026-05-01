"""
Compare bank-pd weekly NP PD + Merton PD against the authors' published
quarterly series in BankDefaultProb_NP.csv (Nagel-Purnanandam 2019).

Author CSV columns: permco, year, month, Modified_PD (NP), Merton_PD.
Author cadence: quarterly, observation = start of Jan/Apr/Jul/Oct.

Match each (permco, author_date) to the bank-pd weekly row with the closest
week_date >= author_date (i.e. first Friday on/after the author observation).
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from bankpd import config

AUTHOR_CSV = Path(
    r"C:\Users\dimut\OneDrive\github\_delete\np-dtd\matlab\BankDefaultProb_NP.csv"
)
BOA_PERMCO = 3151
OUT_PNG = Path(__file__).resolve().parent / "boa_compare_authors.png"


def main() -> None:
    auth = pd.read_csv(AUTHOR_CSV)
    auth = auth[auth["permco"] == BOA_PERMCO].copy()
    auth["author_date"] = pd.to_datetime(
        dict(year=auth["year"], month=auth["month"], day=1)
    )
    auth = auth.sort_values("author_date").reset_index(drop=True)
    print(f"Author rows for permco {BOA_PERMCO}: {len(auth):,}")
    print(f"  range: {auth['author_date'].min().date()}..{auth['author_date'].max().date()}")

    conn = duckdb.connect(str(config.data_db_path()), read_only=True)
    try:
        ours = conn.execute(
            """
            SELECT week_date, np_PD, merton_PD
            FROM pd_panel
            WHERE permco = ?
            ORDER BY week_date
            """,
            [BOA_PERMCO],
        ).fetchdf()
    finally:
        conn.close()
    ours["week_date"] = pd.to_datetime(ours["week_date"])
    print(f"Our rows  for permco {BOA_PERMCO}: {len(ours):,}")
    print(f"  range: {ours['week_date'].min().date()}..{ours['week_date'].max().date()}")

    # ASOF: for each author_date, pick the first ours.week_date >= author_date
    auth_sorted = auth.sort_values("author_date")
    ours_sorted = ours.sort_values("week_date")
    merged = pd.merge_asof(
        auth_sorted,
        ours_sorted,
        left_on="author_date",
        right_on="week_date",
        direction="forward",   # first ours.week_date >= author.author_date
        tolerance=pd.Timedelta(days=10),
    )
    merged = merged.dropna(subset=["week_date"])
    print(f"Matched rows (<=10 days forward): {len(merged):,}")

    # Drop NaN PD rows for stats
    np_match = merged.dropna(subset=["Modified_PD", "np_PD"])
    me_match = merged.dropna(subset=["Merton_PD", "merton_PD"])

    print()
    print("--- NP PD (author Modified_PD vs our np_PD) ---")
    print(f"  n = {len(np_match):,}")
    diff_np = np_match["np_PD"] - np_match["Modified_PD"]
    print(f"  mean(diff)         = {diff_np.mean():+.4f}")
    print(f"  median(diff)       = {diff_np.median():+.4f}")
    print(f"  std(diff)          = {diff_np.std():.4f}")
    print(f"  mean abs diff      = {diff_np.abs().mean():.4f}")
    print(f"  max abs diff       = {diff_np.abs().max():.4f}")
    print(f"  corr(author, ours) = {np_match[['Modified_PD','np_PD']].corr().iloc[0,1]:.4f}")

    print()
    print("--- Merton PD (author Merton_PD vs our merton_PD) ---")
    print(f"  n = {len(me_match):,}")
    diff_me = me_match["merton_PD"] - me_match["Merton_PD"]
    print(f"  mean(diff)         = {diff_me.mean():+.4f}")
    print(f"  median(diff)       = {diff_me.median():+.4f}")
    print(f"  std(diff)          = {diff_me.std():.4f}")
    print(f"  mean abs diff      = {diff_me.abs().mean():.4f}")
    print(f"  max abs diff       = {diff_me.abs().max():.4f}")
    print(f"  corr(author, ours) = {me_match[['Merton_PD','merton_PD']].corr().iloc[0,1]:.4f}")

    print()
    print("--- Sample alignment (head 8 / tail 8) ---")
    show = merged[["author_date", "week_date", "Modified_PD", "np_PD",
                   "Merton_PD", "merton_PD"]].copy()
    print(show.head(8).to_string(index=False))
    print("...")
    print(show.tail(8).to_string(index=False))

    try:
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

        ax = axes[0]
        ax.plot(merged["author_date"], merged["Modified_PD"],
                "o-", label="Authors NP", markersize=4)
        ax.plot(merged["week_date"], merged["np_PD"],
                "x-", label="Ours np_PD", markersize=4, alpha=0.7)
        ax.set_title(f"BoA (permco {BOA_PERMCO}) — NP PD: authors vs bank-pd")
        ax.set_ylabel("NP PD"); ax.legend(); ax.grid(alpha=0.3)

        ax = axes[1]
        ax.plot(merged["author_date"], merged["Merton_PD"],
                "o-", label="Authors Merton", markersize=4)
        ax.plot(merged["week_date"], merged["merton_PD"],
                "x-", label="Ours merton_PD", markersize=4, alpha=0.7)
        ax.set_xlabel("date"); ax.set_ylabel("Merton PD")
        ax.legend(); ax.grid(alpha=0.3)

        fig.tight_layout()
        fig.savefig(OUT_PNG, dpi=120)
        print(f"\nSaved plot: {OUT_PNG}")
    except ImportError:
        print("(matplotlib missing)")


if __name__ == "__main__":
    main()
