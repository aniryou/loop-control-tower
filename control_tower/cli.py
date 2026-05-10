"""``python -m control_tower stats <log>`` — read a log, print operational stats.

The CLI is the first user-visible interface to the typed reader and cycle
reconstructor. Output is plain text in five fixed-column tables (one per
aggregator) for grep/awk friendliness; ``--json`` dumps the same five
aggregator outputs as one JSON object for downstream tools.

A missing log file is exit 2 with a one-line stderr message; an empty file
prints empty tables and exits 0.

Stdlib only.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path

from control_tower.cycles import Cycle, reconstruct
from control_tower.events import default_event_log_path, read_file
from control_tower.stats import (
    DispatchCounts,
    LockStats,
    RoleStats,
    at_cap_stats,
    cycle_summary,
    dispatch_stats,
    hard_failure_stats,
    lock_stats,
)

_PROG = "control_tower"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog=_PROG)
    sub = parser.add_subparsers(dest="cmd", required=True)

    stats_p = sub.add_parser(
        "stats", help="aggregate stats from a static event log"
    )
    stats_p.add_argument(
        "path",
        nargs="?",
        default=None,
        help=(
            "path to the NDJSON event log "
            "(default: $LOOP_EVENT_LOG or /tmp/loop-events-<session>.jsonl)"
        ),
    )
    stats_p.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="emit one JSON object instead of plain-text tables",
    )

    watch_p = sub.add_parser(
        "watch", help="open a live TUI dashboard tailing the event log"
    )
    watch_p.add_argument(
        "path",
        nargs="?",
        default=None,
        help=(
            "path to the NDJSON event log "
            "(default: $LOOP_EVENT_LOG or /tmp/loop-events-<session>.jsonl)"
        ),
    )
    watch_p.add_argument(
        "--from-start",
        dest="from_start",
        action="store_true",
        help="replay the existing file before tailing (default: tail from end)",
    )
    watch_p.add_argument(
        "--poll-interval-s",
        dest="poll_interval_s",
        type=float,
        default=0.1,
        help="seconds between tail polls (default: 0.1)",
    )

    args = parser.parse_args(argv)
    if args.cmd == "stats":
        return _cmd_stats(args)
    if args.cmd == "watch":
        return _cmd_watch(args)
    parser.error(f"unknown command: {args.cmd}")
    return 2  # unreachable — parser.error raises SystemExit


def _cmd_watch(args: argparse.Namespace) -> int:
    """Lazy-imports textual so ``stats`` doesn't pay the import cost."""
    path = Path(args.path) if args.path else default_event_log_path()
    from control_tower.watch import run_watch

    return run_watch(
        path,
        from_start=args.from_start,
        poll_interval_s=args.poll_interval_s,
    )


def _cmd_stats(args: argparse.Namespace) -> int:
    path = Path(args.path) if args.path else default_event_log_path()
    if not path.exists():
        print(f"error: log file not found: {path}", file=sys.stderr)
        return 2

    cycles: list[Cycle] = list(reconstruct(read_file(path)))
    cs = cycle_summary(cycles)
    ls = lock_stats(cycles)
    ds = dispatch_stats(cycles)
    ac = at_cap_stats(cycles)
    hf = hard_failure_stats(cycles)

    if args.as_json:
        payload: dict[str, object] = {
            "cycle_summary": {k: dataclasses.asdict(v) for k, v in cs.items()},
            "lock_stats": {k: dataclasses.asdict(v) for k, v in ls.items()},
            "dispatch_stats": {k: dataclasses.asdict(v) for k, v in ds.items()},
            "at_cap_stats": ac,
            "hard_failure_stats": hf,
        }
        json.dump(payload, sys.stdout, sort_keys=True)
        sys.stdout.write("\n")
        return 0

    _print_cycle_summary(cs)
    print()
    _print_lock_stats(ls)
    print()
    _print_dispatch_stats(ds)
    print()
    _print_at_cap_stats(ac)
    print()
    _print_hard_failures(hf)
    return 0


def _fmt_dur(d: float | None) -> str:
    return "—" if d is None else f"{d:.1f}"


def _fmt_rate(r: float | None) -> str:
    return "—" if r is None else f"{r * 100:.1f}%"


def _print_cycle_summary(cs: dict[str, RoleStats]) -> None:
    rows = sorted(cs.items())
    headers = ["role", "total", "ok", "skip", "fail", "open", "median_s", "p95_s"]
    table = [headers] + [
        [
            role,
            str(s.total),
            str(s.ok),
            str(s.skip),
            str(s.fail),
            str(s.open),
            _fmt_dur(s.median_duration_s),
            _fmt_dur(s.p95_duration_s),
        ]
        for role, s in rows
    ]
    _render_columns(table)


def _print_lock_stats(ls: dict[str, LockStats]) -> None:
    print("lock-race rate")
    rows = sorted(ls.items())
    headers = ["role", "acquired", "lost", "rate"]
    table = [headers] + [
        [role, str(s.acquired), str(s.lost), _fmt_rate(s.rate)]
        for role, s in rows
    ]
    _render_columns(table)


def _pr_sort_key(key: str) -> tuple[int, str]:
    """Sort '#41' numerically; non-conforming keys sort after, alphabetically."""
    if key.startswith("#"):
        try:
            return (0, f"{int(key[1:]):012d}")
        except ValueError:
            pass
    return (1, key)


def _print_dispatch_stats(ds: dict[str, DispatchCounts]) -> None:
    print("dispatch fires by pr")
    rows = sorted(ds.items(), key=lambda kv: _pr_sort_key(kv[0]))
    headers = ["pr", "fired", "skipped"]
    table = [headers] + [
        [pr, str(c.fired), str(c.skipped)] for pr, c in rows
    ]
    _render_columns(table)


def _print_at_cap_stats(ac: dict[str, int]) -> None:
    print("at-cap events by kind")
    rows = sorted(ac.items())
    headers = ["kind", "count"]
    table = [headers] + [[k, str(v)] for k, v in rows]
    _render_columns(table)


def _print_hard_failures(hf: dict[str, int]) -> None:
    print("hard failures")
    rows = sorted(hf.items())
    headers = ["role", "count"]
    table = [headers] + [[k, str(v)] for k, v in rows]
    _render_columns(table)


def _render_columns(table: list[list[str]]) -> None:
    """Print ``table`` (header row + data rows) with 2-space-padded columns.

    First column left-aligned (textual key), remaining columns right-aligned
    (numeric). An empty data section still prints the header row so the
    reader sees the schema.
    """
    if not table:
        return
    n_cols = len(table[0])
    widths = [max(len(row[i]) for row in table) for i in range(n_cols)]
    for row in table:
        cells: list[str] = []
        for i, cell in enumerate(row):
            cells.append(cell.ljust(widths[i]) if i == 0 else cell.rjust(widths[i]))
        print("  ".join(cells))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
