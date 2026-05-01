"""
Thin command-line entry point for the bank-pd pipeline.

Usage:
    python -m bankpd.cli run-all --scope boa
    python -m bankpd.cli run-all --scope all
    python -m bankpd.cli run-all --permcos 20436
    python -m bankpd.cli show-boa
"""
from __future__ import annotations

import argparse
import sys

from . import freshness, linker
from .db import get_connection, init_schema
from .pipeline import run_weekly


def _cmd_run_all(args: argparse.Namespace) -> int:
    permcos = None
    if args.permcos:
        permcos = [int(x) for x in args.permcos.split(",") if x.strip()]
    run_weekly(
        scope=args.scope,
        permcos=permcos,
        skip_fred=args.skip_fred,
        skip_crsp=args.skip_crsp,
        ignore_stale=args.ignore_stale,
    )
    return 0


def _cmd_freshness(_args: argparse.Namespace) -> int:
    conn = get_connection(read_only=False)
    try:
        init_schema(conn)
        rep = freshness.check(conn)
        print(freshness.format_report(rep))
    finally:
        conn.close()
    return 0


def _cmd_show_boa(_args: argparse.Namespace) -> int:
    conn = get_connection(read_only=False)
    try:
        init_schema(conn)
        n = linker.refresh_link_table(conn)
        print(f"crsp_link rows: {n:,}")
        df = linker.find_bank(conn, "%bank of america%")
        print(df.head(10).to_string(index=False))
        ids = linker.boa_ids(conn)
        print(f"\nResolved BoA: {ids}")
    finally:
        conn.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bankpd")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("run-all", help="run the weekly pipeline end-to-end")
    pr.add_argument("--scope", choices=["boa", "all"], default="boa")
    pr.add_argument("--permcos", type=str, default=None,
                    help="comma-separated explicit permco list (overrides --scope)")
    pr.add_argument("--skip-fred", action="store_true")
    pr.add_argument("--skip-crsp", action="store_true")
    pr.add_argument("--ignore-stale", action="store_true",
                    help="proceed even when Y-9C is stale beyond threshold")
    pr.set_defaults(func=_cmd_run_all)

    pf = sub.add_parser("freshness",
                        help="report data-freshness for FRED/CRSP/Y-9C/pd_input (always exits 0)")
    pf.set_defaults(func=_cmd_freshness)

    ps = sub.add_parser("show-boa", help="resolve and print BoA ids from crsp_link")
    ps.set_defaults(func=_cmd_show_boa)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
