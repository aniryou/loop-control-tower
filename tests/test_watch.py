"""Tests for control_tower.watch — the live TUI dashboard.

Exercises ``WatchApp`` headlessly via textual's ``App.run_test()``. Async
test bodies are wrapped with ``asyncio.run`` so this file does not need
``pytest-asyncio``. Tests assert against the app's internal state
(``_role_states``, ticker deque, stored events) rather than rendered text
to stay robust across textual versions.
"""

from __future__ import annotations

import asyncio
import json
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
        "schema_version": 2,
        "cycle_id": cycle_id,
    }
    out.update(extra)
    return out


def _parsed(d: dict[str, Any]):
    from control_tower.events import parse_event

    return parse_event(json.dumps(d))


def test_watch_role_card_idle_to_running_to_ok(tmp_path: Path) -> None:
    """A cycle_start opens the role's card; cycle_end exit_code=0 closes it as 'ok'."""
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            assert app._role_states == {}

            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1")))
            assert "dev-1" in app._role_states
            assert app._role_states["dev-1"].current_cycle_id == "c1"
            assert app._role_states["dev-1"].last_outcome is None

            app.feed_event(
                _parsed(_ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=10.0))
            )
            assert app._role_states["dev-1"].current_cycle_id is None
            assert app._role_states["dev-1"].last_outcome == "ok"

    asyncio.run(_go())


def test_watch_role_card_records_skip_outcome(tmp_path: Path) -> None:
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1")))
            app.feed_event(_parsed(_ev("cycle_skip", cycle_id="c1")))
            assert app._role_states["dev-1"].last_outcome == "skip"
            assert app._role_states["dev-1"].current_cycle_id is None

    asyncio.run(_go())


def test_watch_role_card_records_fail_outcome(tmp_path: Path) -> None:
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1")))
            app.feed_event(
                _parsed(_ev("cycle_end", cycle_id="c1", exit_code=2, duration_s=5.0))
            )
            assert app._role_states["dev-1"].last_outcome == "fail"

    asyncio.run(_go())


def test_watch_role_card_tracks_llm_run(tmp_path: Path) -> None:
    """llm_started populates current_run_mode; llm_exited clears it."""
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1")))
            app.feed_event(
                _parsed(_ev("llm_started", cycle_id="c1", run_id="r1", mode="claim"))
            )
            assert app._role_states["dev-1"].current_run_id == "r1"
            assert app._role_states["dev-1"].current_run_mode == "claim"

            app.feed_event(
                _parsed(_ev("llm_exited", cycle_id="c1", run_id="r1", exit_code=0))
            )
            assert app._role_states["dev-1"].current_run_id is None
            assert app._role_states["dev-1"].current_run_mode is None

    asyncio.run(_go())


def test_watch_ticker_caps_at_50_evicting_oldest(tmp_path: Path) -> None:
    """The 51st event evicts the oldest; the deque holds exactly 50 entries."""
    from control_tower.watch import TICKER_MAX, EventTicker, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    assert TICKER_MAX == 50  # contract pin

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            for i in range(51):
                app.feed_event(_parsed(_ev("cycle_start", cycle_id=f"c{i}")))
            ticker = app.query_one("#ticker", EventTicker)
            assert len(ticker.events) == 50
            # newest at index 0; oldest pushed (i=0) should be evicted
            cycle_ids = [ev.extra["cycle_id"] for ev in ticker.events]
            assert "c0" not in cycle_ids
            assert "c1" in cycle_ids
            assert "c50" in cycle_ids
            assert cycle_ids[0] == "c50"

    asyncio.run(_go())


def test_watch_counters_match_cycle_summary(tmp_path: Path) -> None:
    """Counters pane data equals cycle_summary(...) over the same events."""
    from control_tower.cycles import reconstruct
    from control_tower.stats import (
        at_cap_stats,
        cycle_summary,
        dispatch_stats,
        hard_failure_stats,
        lock_stats,
    )
    from control_tower.watch import CountersPane, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

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

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            for raw in raw_events:
                app.feed_event(_parsed(raw))
            app._refresh_counters()
            pane = app.query_one("#counters", CountersPane)
            cycles = list(reconstruct(iter(app._events)))
            assert pane._cs == cycle_summary(cycles)
            assert pane._ls == lock_stats(cycles)
            assert pane._ds == dispatch_stats(cycles)
            assert pane._ac == at_cap_stats(cycles)
            assert pane._hf == hard_failure_stats(cycles)
            assert pane._cs["dev-1"].total == 5
            assert pane._cs["dev-1"].ok == 3

    asyncio.run(_go())


def test_watch_q_quits_app(tmp_path: Path) -> None:
    """Pressing 'q' shuts the app down cleanly."""
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test() as pilot:
            assert app.is_running
            await pilot.press("q")
            await pilot.pause(0.05)
        assert not app.is_running

    asyncio.run(_go())


def test_watch_worker_thread_exits_on_shutdown(tmp_path: Path) -> None:
    """The tail worker thread terminates after app shutdown."""
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    captured: dict[str, bool | None] = {"alive_during": None, "alive_after": None}

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            await asyncio.sleep(0.05)
            assert app._worker is not None
            captured["alive_during"] = app._worker.is_alive()
        # Outside the context the app has unmounted; allow worker to drain
        await asyncio.sleep(0.2)
        captured["alive_after"] = app._worker.is_alive()

    asyncio.run(_go())
    assert captured["alive_during"] is True
    assert captured["alive_after"] is False


def test_watch_clear_ticker_action(tmp_path: Path) -> None:
    from control_tower.watch import EventTicker, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test() as pilot:
            for i in range(3):
                app.feed_event(_parsed(_ev("cycle_start", cycle_id=f"c{i}")))
            ticker = app.query_one("#ticker", EventTicker)
            assert len(ticker.events) == 3
            await pilot.press("c")
            await pilot.pause(0.05)
            assert len(ticker.events) == 0

    asyncio.run(_go())


def test_watch_pause_action_toggles_state(tmp_path: Path) -> None:
    from control_tower.watch import EventTicker, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test() as pilot:
            ticker = app.query_one("#ticker", EventTicker)
            assert ticker.paused is False
            await pilot.press("p")
            await pilot.pause(0.05)
            assert ticker.paused is True
            await pilot.press("p")
            await pilot.pause(0.05)
            assert ticker.paused is False

    asyncio.run(_go())


def test_watch_parse_errors_are_skipped(tmp_path: Path) -> None:
    """ParseError items pass through feed_event without disturbing state."""
    from control_tower.events import ParseError
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            app.feed_event(ParseError(raw="bogus", reason="invalid_json"))
            assert app._role_states == {}
            assert app._events == []

    asyncio.run(_go())


def test_watch_two_roles_get_independent_cards(tmp_path: Path) -> None:
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="d1", role="dev-1")))
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="r1", role="reviewer-1")))
            assert set(app._role_states) == {"dev-1", "reviewer-1"}
            assert app._role_states["dev-1"].current_cycle_id == "d1"
            assert app._role_states["reviewer-1"].current_cycle_id == "r1"

    asyncio.run(_go())
