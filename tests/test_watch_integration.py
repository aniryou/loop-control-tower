"""Integration tests for control_tower.watch — file-driven (worker thread).

These tests write to an actual log file and let the WatchApp's tail worker
pick events up, exercising the full producer→queue→drain→widget pipeline.
The unit tests in test_watch.py cover state-machine semantics directly via
``feed_event``; this file pins the I/O path.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path
from typing import Any


def _ev(
    event: str,
    *,
    cycle_id: str,
    role: str = "dev-1",
    ts: str = "2026-05-10T10:00:00+00:00",
    **extra: Any,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "ts": ts,
        "session": "s",
        "repo": "o/r",
        "role": role,
        "event": event,
        "schema_version": 1,
        "cycle_id": cycle_id,
    }
    out.update(extra)
    return out


def _append(path: Path, ev: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(ev) + "\n")
        fh.flush()


async def _wait_until(predicate, timeout_s: float, step_s: float = 0.05) -> bool:
    """Poll-with-timeout helper that yields control to the textual loop."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(step_s)
    return False


def test_watch_tails_appended_events(tmp_path: Path) -> None:
    """Background writer appends 5 events; ticker shows all 5 in order."""
    from control_tower.watch import EventTicker, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")  # exists, empty — tail seeks to end

    raw_events = [
        _ev("cycle_start", cycle_id=f"c{i}") for i in range(5)
    ]

    async def _go() -> None:
        app = WatchApp(log, from_start=False, poll_interval_s=0.05)
        async with app.run_test():
            # Background writer drips events in over ~0.5s
            def _writer() -> None:
                for raw in raw_events:
                    _append(log, raw)
                    time.sleep(0.05)

            t = threading.Thread(target=_writer, daemon=True)
            t.start()

            ticker = app.query_one("#ticker", EventTicker)
            ok = await _wait_until(lambda: len(ticker.events) == 5, timeout_s=5.0)
            assert ok, f"expected 5 events, got {len(ticker.events)}"
            t.join(timeout=1.0)

            # newest first; the deque's first item is the last raw event written
            seen_ids = [ev.extra["cycle_id"] for ev in ticker.events]
            assert seen_ids == ["c4", "c3", "c2", "c1", "c0"]

    asyncio.run(_go())


def test_watch_from_start_replays_existing_file(tmp_path: Path) -> None:
    """--from-start replays the historical file before tailing new appends."""
    from control_tower.watch import EventTicker, WatchApp

    log = tmp_path / "events.jsonl"
    pre_existing = [_ev("cycle_start", cycle_id=f"old{i}") for i in range(3)]
    with log.open("w", encoding="utf-8") as fh:
        for raw in pre_existing:
            fh.write(json.dumps(raw) + "\n")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            ticker = app.query_one("#ticker", EventTicker)
            ok = await _wait_until(
                lambda: len(ticker.events) == 3, timeout_s=5.0
            )
            assert ok, f"expected 3 replayed events, got {len(ticker.events)}"
            seen_ids = sorted(ev.extra["cycle_id"] for ev in ticker.events)
            assert seen_ids == ["old0", "old1", "old2"]

            # Now append more — tailing continues
            _append(log, _ev("cycle_start", cycle_id="new0"))
            ok = await _wait_until(
                lambda: any(
                    e.extra["cycle_id"] == "new0" for e in ticker.events
                ),
                timeout_s=5.0,
            )
            assert ok

    asyncio.run(_go())


def test_watch_missing_file_then_appears(tmp_path: Path) -> None:
    """File missing at start; created mid-run; events surface."""
    from control_tower.watch import EventTicker, WatchApp

    log = tmp_path / "later.jsonl"
    assert not log.exists()

    async def _go() -> None:
        app = WatchApp(log, from_start=False, poll_interval_s=0.05)
        async with app.run_test():
            ticker = app.query_one("#ticker", EventTicker)
            assert len(ticker.events) == 0

            # Create the file and append. tail_file always treats files that
            # appeared mid-flight as "every byte is new", so this is read.
            _append(log, _ev("cycle_start", cycle_id="late0"))

            ok = await _wait_until(
                lambda: any(
                    e.extra["cycle_id"] == "late0" for e in ticker.events
                ),
                timeout_s=5.0,
            )
            assert ok, "appended event after file appeared was not picked up"

    asyncio.run(_go())


def test_watch_counter_pane_matches_stats_cli_output(tmp_path: Path) -> None:
    """Counter pane data == python -m control_tower stats <same-file> output."""
    from control_tower.cli import main as cli_main
    from control_tower.watch import CountersPane, WatchApp

    log = tmp_path / "events.jsonl"
    raw_events = []
    for i, dur in enumerate([10.0, 20.0, 30.0]):
        raw_events += [
            _ev("cycle_start", cycle_id=f"a{i}"),
            _ev("cycle_end", cycle_id=f"a{i}", exit_code=0, duration_s=dur),
        ]
    raw_events += [
        _ev("cycle_start", cycle_id="f1"),
        _ev("cycle_end", cycle_id="f1", exit_code=2, duration_s=40.0),
    ]
    raw_events += [
        _ev("cycle_start", cycle_id="s1"),
        _ev("cycle_skip", cycle_id="s1"),
    ]
    with log.open("w", encoding="utf-8") as fh:
        for raw in raw_events:
            fh.write(json.dumps(raw) + "\n")

    async def _go() -> dict[str, Any]:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            ok = await _wait_until(
                lambda: len(app._events) == len(raw_events), timeout_s=5.0
            )
            assert ok, (
                f"expected {len(raw_events)} events, got {len(app._events)}"
            )
            app._refresh_counters()
            pane = app.query_one("#counters", CountersPane)
            return {
                "cs": pane._cs,
                "ls": pane._ls,
                "ds": pane._ds,
                "ac": pane._ac,
                "hf": pane._hf,
            }

    pane_data = asyncio.run(_go())

    # Run the CLI and capture its --json payload over the same file
    import io
    import contextlib

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        rc = cli_main(["stats", "--json", str(log)])
    assert rc == 0
    cli_payload = json.loads(stdout.getvalue())

    # Compare the cycle_summary scalar fields (RoleStats dataclasses serialize
    # to dicts via the CLI's dataclasses.asdict).
    pane_cs_serialized = {
        role: {
            "total": s.total,
            "ok": s.ok,
            "skip": s.skip,
            "fail": s.fail,
            "open": s.open,
            "median_duration_s": s.median_duration_s,
            "p95_duration_s": s.p95_duration_s,
        }
        for role, s in pane_data["cs"].items()
    }
    assert pane_cs_serialized == cli_payload["cycle_summary"]

    pane_ls_serialized = {
        role: {"acquired": s.acquired, "lost": s.lost, "rate": s.rate}
        for role, s in pane_data["ls"].items()
    }
    assert pane_ls_serialized == cli_payload["lock_stats"]

    pane_ds_serialized = {
        pr: {"fired": c.fired, "skipped": c.skipped}
        for pr, c in pane_data["ds"].items()
    }
    assert pane_ds_serialized == cli_payload["dispatch_stats"]

    assert pane_data["ac"] == cli_payload["at_cap_stats"]
    assert pane_data["hf"] == cli_payload["hard_failure_stats"]
