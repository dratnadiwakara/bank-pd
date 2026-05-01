"""
Command-line entry point for the bank-pd pipeline.

Usage:
    bankpd update-inputs [--ignore-stale]
        Refresh FRED + CRSP daily + ticker history for all link permcos,
        rebuild pd_input, print coverage diagnostics.

    bankpd compute-weekly [--ignore-stale] [--max-workers N]
        Strict: compute PDs for all banks up to last Friday. Aborts if
        pd_input has no eligible rows for last Friday.

    bankpd compute [--since DATE] [--until DATE] [--rssd ID1,ID2,...]
                   [--recompute] [--max-workers N]
        Flexible compute. Defaults: all banks, full pd_input range,
        skip already-computed pairs.

    bankpd freshness
        Print FRED/CRSP/Y-9C/pd_input lag report.

    bankpd inputs-status
        Print freshness + coverage. Read-only — no fetch, no compute.

    bankpd show-boa
        Resolve and print BoA permco/RSSD from crsp_link.
"""
from __future__ import annotations

import argparse
import sys

from . import bbg, freshness, linker, yfdata
from .db import get_connection, init_schema
from .pipeline import (
    compute_range,
    compute_weekly,
    inputs_status,
    update_inputs,
)


def _parse_rssd_list(s: str | None) -> list[int] | None:
    if not s:
        return None
    out: list[int] = []
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        out.append(int(tok))
    return out or None


def _cmd_update_inputs(args: argparse.Namespace) -> int:
    update_inputs(
        ignore_stale=args.ignore_stale,
        rssds=_parse_rssd_list(args.rssd),
        top_n=args.top_n,
    )
    return 0


def _cmd_compute_weekly(args: argparse.Namespace) -> int:
    compute_weekly(ignore_stale=args.ignore_stale, max_workers=args.max_workers)
    return 0


def _cmd_compute(args: argparse.Namespace) -> int:
    compute_range(
        since=args.since,
        until=args.until,
        rssds=_parse_rssd_list(args.rssd),
        recompute=args.recompute,
        ignore_stale=args.ignore_stale,
        max_workers=args.max_workers,
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


def _cmd_inputs_status(_args: argparse.Namespace) -> int:
    inputs_status()
    return 0


def _parse_ticker_map(s: str | None) -> dict[str, int] | None:
    if not s:
        return None
    out: dict[str, int] = {}
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        if "=" not in tok:
            raise SystemExit(f"--ticker-map entry malformed: {tok!r} (expected TICKER=PERMCO)")
        k, v = tok.split("=", 1)
        out[k.strip().upper()] = int(v.strip())
    return out or None


def _cmd_import_bloomberg(args: argparse.Namespace) -> int:
    sheet_arg: str | int = args.sheet
    if isinstance(sheet_arg, str) and sheet_arg.isdigit():
        sheet_arg = int(sheet_arg)
    conn = get_connection(read_only=False)
    try:
        init_schema(conn)
        result = bbg.import_bloomberg_excel(
            conn,
            args.path,
            sheet=sheet_arg,
            ticker_map=_parse_ticker_map(args.ticker_map),
            rebuild_pd_input=not args.no_rebuild,
            ignore_stale=args.ignore_stale,
        )
        print(f"Imported: {result.rows_imported:,} rows from {args.path}")
        if result.permcos_touched:
            print(f"  Permcos touched: {len(result.permcos_touched)} "
                  f"({', '.join(str(p) for p in result.permcos_touched[:10])}"
                  f"{'…' if len(result.permcos_touched) > 10 else ''})")
        if result.date_range:
            print(f"  Date range: {result.date_range[0]} .. {result.date_range[1]}")
        print(f"  Synthetic retx rows: {result.retx_synthetic_rows}")
        if result.tickers_unresolved:
            print(f"  Unresolved tickers ({len(result.tickers_unresolved)}): "
                  f"{result.tickers_unresolved}")
        if result.pd_input_rows_after is not None:
            print(f"After rebuild: pd_input has {result.pd_input_rows_after:,} rows; "
                  f"max week {result.pd_input_max_week_after}.")
    finally:
        conn.close()
    return 0


def _cmd_prune_overlay(_args: argparse.Namespace) -> int:
    conn = get_connection(read_only=False)
    try:
        init_schema(conn)
        n = bbg.prune_overlay(conn)
        print(f"Pruned {n:,} overlay rows now covered by WRDS crsp_daily.")
    finally:
        conn.close()
    return 0


def _parse_str_map(s: str | None) -> dict[str, str] | None:
    if not s:
        return None
    out: dict[str, str] = {}
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        if "=" not in tok:
            raise SystemExit(f"--ticker-map entry malformed: {tok!r} (expected CRSP=YF)")
        k, v = tok.split("=", 1)
        out[k.strip().upper()] = v.strip().upper()
    return out or None


def _cmd_import_yfinance(args: argparse.Namespace) -> int:
    rssds   = _parse_rssd_list(args.rssd)
    permcos = _parse_rssd_list(args.permco)
    conn = get_connection(read_only=False)
    try:
        init_schema(conn)
        result = yfdata.import_yahoo_finance(
            conn,
            rssds=rssds,
            permcos=permcos,
            since=args.since,
            until=args.until,
            ticker_map=_parse_str_map(args.ticker_map),
            skip_overlap_check=args.skip_overlap_check,
            overlap_tolerance=args.overlap_tolerance,
            rebuild_pd_input=not args.no_rebuild,
            ignore_stale=args.ignore_stale,
        )
        print(f"Imported: {result.rows_imported:,} rows (yfinance)")
        if result.permcos_touched:
            print(f"  Permcos touched: {len(result.permcos_touched)} "
                  f"({', '.join(str(p) for p in result.permcos_touched[:10])}"
                  f"{'…' if len(result.permcos_touched) > 10 else ''})")
        if result.date_range:
            print(f"  Date range: {result.date_range[0]} .. {result.date_range[1]}")
        print(f"  Synthetic retx rows: {result.retx_synthetic_rows}")
        if result.tickers_unresolved:
            print(f"  Unresolved tickers ({len(result.tickers_unresolved)}): "
                  f"{result.tickers_unresolved}")
        if result.pd_input_rows_after is not None:
            print(f"After rebuild: pd_input has {result.pd_input_rows_after:,} rows; "
                  f"max week {result.pd_input_max_week_after}.")
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

    pu = sub.add_parser(
        "update-inputs",
        help="refresh FRED+CRSP+tickers and rebuild pd_input",
    )
    pu.add_argument("--ignore-stale", action="store_true",
                    help="proceed even if Y-9C is stale beyond threshold")
    pu.add_argument("--rssd", type=str, default=None,
                    help="comma-separated RSSD list — fetch only these "
                         "banks (default: all confirmed link permcos)")
    pu.add_argument("--top-n", type=int, default=None,
                    help="fetch only top N RSSDs by latest Y-9C assets "
                         "(mutually exclusive with --rssd)")
    pu.set_defaults(func=_cmd_update_inputs)

    pw = sub.add_parser(
        "compute-weekly",
        help="strict: compute PDs for all banks up to last Friday",
    )
    pw.add_argument("--ignore-stale", action="store_true")
    pw.add_argument("--max-workers", type=int, default=None,
                    help="thread pool size for compute kernel")
    pw.set_defaults(func=_cmd_compute_weekly)

    pc = sub.add_parser(
        "compute",
        help="flexible compute: filter by --since/--until/--rssd",
    )
    pc.add_argument("--since", type=str, default=None,
                    help="ISO date (inclusive lower bound on week_date)")
    pc.add_argument("--until", type=str, default=None,
                    help="ISO date (inclusive upper bound on week_date)")
    pc.add_argument("--rssd", type=str, default=None,
                    help="comma-separated RSSD list to restrict to")
    pc.add_argument("--recompute", action="store_true",
                    help="recompute (week_date, permco) pairs already in pd_panel")
    pc.add_argument("--ignore-stale", action="store_true",
                    help="proceed even if Y-9C / crsp_link is stale beyond threshold")
    pc.add_argument("--max-workers", type=int, default=None)
    pc.set_defaults(func=_cmd_compute)

    pf = sub.add_parser(
        "freshness",
        help="report data-source lag (FRED/CRSP/Y-9C/pd_input). Always exits 0.",
    )
    pf.set_defaults(func=_cmd_freshness)

    pi = sub.add_parser(
        "inputs-status",
        help="read-only freshness + coverage diagnostic",
    )
    pi.set_defaults(func=_cmd_inputs_status)

    pb = sub.add_parser(
        "import-bloomberg",
        help="import Bloomberg market-cap xlsx into crsp_daily_overlay",
    )
    pb.add_argument("path", type=str, help="path to Bloomberg .xlsx file")
    pb.add_argument("--sheet", type=str, default="0",
                    help="sheet name or 0-based index (default: 0)")
    pb.add_argument("--ticker-map", type=str, default=None,
                    help="comma-separated TICKER=PERMCO overrides")
    pb.add_argument("--no-rebuild", action="store_true",
                    help="skip pd_input rebuild after import")
    pb.add_argument("--ignore-stale", action="store_true",
                    help="proceed even if crsp_link is stale beyond threshold")
    pb.set_defaults(func=_cmd_import_bloomberg)

    pp = sub.add_parser(
        "prune-overlay",
        help="delete overlay rows now covered by WRDS crsp_daily",
    )
    pp.set_defaults(func=_cmd_prune_overlay)

    py = sub.add_parser(
        "import-yfinance",
        help="pull free Yahoo Finance market caps into crsp_daily_overlay",
    )
    py.add_argument("--rssd",   type=str, default=None,
                    help="comma-separated RSSDs to fetch")
    py.add_argument("--permco", type=str, default=None,
                    help="comma-separated permcos to fetch")
    py.add_argument("--since",  type=str, default=None,
                    help="ISO start date (default: per-permco MAX(crsp_daily_combined.date))")
    py.add_argument("--until",  type=str, default=None,
                    help="ISO end date (default: today)")
    py.add_argument("--ticker-map", type=str, default=None,
                    help="comma-separated CRSP=YF overrides, e.g. BRK.B=BRK-B")
    py.add_argument("--overlap-tolerance", type=float, default=0.01,
                    help="soft-warn threshold for overlap-day mismatch (default 1%%)")
    py.add_argument("--skip-overlap-check", action="store_true",
                    help="bypass overlap consistency validation")
    py.add_argument("--no-rebuild", action="store_true")
    py.add_argument("--ignore-stale", action="store_true",
                    help="proceed even if crsp_link is stale beyond threshold")
    py.set_defaults(func=_cmd_import_yfinance)

    ps = sub.add_parser("show-boa", help="resolve BoA ids from crsp_link")
    ps.set_defaults(func=_cmd_show_boa)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
