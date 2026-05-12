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


# --- view selection (loop-control-tower-1pl) -----------------------------


def test_view_ticker_composes_only_the_ticker(tmp_path: Path) -> None:
    """--view=ticker yields the ticker; not StatusRow / CountersPane / heartbeat."""
    from textual.css.query import NoMatches

    from control_tower.watch import (
        CountersPane,
        EventTicker,
        FailureAlertStrip,
        HeartbeatStrip,
        StatusRow,
        WatchApp,
    )

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="ticker")
        async with app.run_test():
            assert app.query_one("#ticker", EventTicker) is not None
            for sel, cls in [
                ("#status", StatusRow),
                ("#counters", CountersPane),
                ("#heartbeat", HeartbeatStrip),
                ("#failures", FailureAlertStrip),
            ]:
                try:
                    app.query_one(sel, cls)
                except NoMatches:
                    continue
                else:
                    raise AssertionError(f"{sel} should not exist in ticker view")

    asyncio.run(_go())


def test_view_stats_composes_only_the_counters(tmp_path: Path) -> None:
    from textual.css.query import NoMatches

    from control_tower.watch import CountersPane, EventTicker, StatusRow, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="stats")
        async with app.run_test():
            assert app.query_one("#counters", CountersPane) is not None
            for sel, cls in [("#status", StatusRow), ("#ticker", EventTicker)]:
                try:
                    app.query_one(sel, cls)
                except NoMatches:
                    continue
                else:
                    raise AssertionError(f"{sel} should not exist in stats view")

    asyncio.run(_go())


def test_view_pulse_composes_status_and_pulse_widgets(tmp_path: Path) -> None:
    """--view=pulse yields HeartbeatStrip + StatusRow + FailureAlertStrip — not ticker / counters."""
    from textual.css.query import NoMatches

    from control_tower.watch import (
        CountersPane,
        EventTicker,
        FailureAlertStrip,
        HeartbeatStrip,
        StatusRow,
        WatchApp,
    )

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")
        async with app.run_test():
            assert app.query_one("#heartbeat", HeartbeatStrip) is not None
            assert app.query_one("#status", StatusRow) is not None
            assert app.query_one("#failures", FailureAlertStrip) is not None
            for sel, cls in [("#ticker", EventTicker), ("#counters", CountersPane)]:
                try:
                    app.query_one(sel, cls)
                except NoMatches:
                    continue
                else:
                    raise AssertionError(f"{sel} should not exist in pulse view")

    asyncio.run(_go())


def test_view_all_default_preserves_legacy_widgets(tmp_path: Path) -> None:
    """No --view (default 'all') still composes the legacy three regions."""
    from control_tower.watch import CountersPane, EventTicker, StatusRow, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05)
        async with app.run_test():
            assert app.query_one("#status", StatusRow) is not None
            assert app.query_one("#ticker", EventTicker) is not None
            assert app.query_one("#counters", CountersPane) is not None

    asyncio.run(_go())


def test_view_invalid_value_raises(tmp_path: Path) -> None:
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    try:
        WatchApp(log, view="banana")
    except ValueError:
        return
    raise AssertionError("invalid view should raise ValueError")


# --- heartbeat + parse-error counter (loop-control-tower-1pl) ----------


def test_pulse_heartbeat_starts_in_waiting_state(tmp_path: Path) -> None:
    from control_tower.watch import HeartbeatStrip, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")
        async with app.run_test():
            hb = app.query_one("#heartbeat", HeartbeatStrip)
            assert hb.last_event_ts is None
            assert hb.parse_error_count == 0

    asyncio.run(_go())


def test_pulse_heartbeat_records_session_and_repo_on_first_event(tmp_path: Path) -> None:
    from control_tower.watch import HeartbeatStrip, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")
        async with app.run_test():
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1")))
            hb = app.query_one("#heartbeat", HeartbeatStrip)
            assert hb.last_event_ts is not None
            assert hb.session == "s"
            assert hb.repo == "o/r"

    asyncio.run(_go())


def test_pulse_parse_errors_increment_visible_counter(tmp_path: Path) -> None:
    from control_tower.events import ParseError
    from control_tower.watch import HeartbeatStrip, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")
        async with app.run_test():
            app.feed_event(ParseError(raw="bogus", reason="invalid_json"))
            app.feed_event(ParseError(raw="bogus2", reason="invalid_json"))
            hb = app.query_one("#heartbeat", HeartbeatStrip)
            assert hb.parse_error_count == 2
            assert app._parse_error_count == 2

    asyncio.run(_go())


def test_pulse_heartbeat_aggregates_update_on_refresh(tmp_path: Path) -> None:
    """_refresh_counters feeds role / cycle / cost totals into the heartbeat strip."""
    from control_tower.watch import HeartbeatStrip, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")
        async with app.run_test():
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1", role="dev-1")))
            app.feed_event(
                _parsed(
                    _ev(
                        "llm_exited",
                        cycle_id="c1",
                        role="dev-1",
                        run_id="r1",
                        total_cost_usd=0.42,
                        input_tokens=100,
                        output_tokens=20,
                        num_turns=2,
                    )
                )
            )
            app.feed_event(
                _parsed(_ev("cycle_end", cycle_id="c1", role="dev-1", exit_code=0, duration_s=1.0))
            )
            app._refresh_counters()
            hb = app.query_one("#heartbeat", HeartbeatStrip)
            assert hb.role_count == 1
            assert hb.cycle_count == 1
            assert abs(hb.total_cost_usd - 0.42) < 1e-9

    asyncio.run(_go())


# --- failure alert strip (loop-control-tower-1pl) ----------------------


def test_pulse_failure_strip_hidden_until_hard_failure(tmp_path: Path) -> None:
    from control_tower.watch import FailureAlertStrip, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")
        async with app.run_test():
            fs = app.query_one("#failures", FailureAlertStrip)
            assert fs.failure_count == 0
            assert "-active" not in fs.classes

            # cycle_end with non-zero exit_code is NOT a hard_failure
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1")))
            app.feed_event(
                _parsed(_ev("cycle_end", cycle_id="c1", exit_code=2, duration_s=1.0))
            )
            assert fs.failure_count == 0
            assert "-active" not in fs.classes

    asyncio.run(_go())


def test_pulse_failure_strip_activates_on_hard_failure(tmp_path: Path) -> None:
    from control_tower.watch import FailureAlertStrip, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")
        async with app.run_test():
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1")))
            app.feed_event(
                _parsed(
                    _ev(
                        "hard_failure",
                        cycle_id="c1",
                        role="dev-1",
                        reason="max-turns",
                    )
                )
            )
            fs = app.query_one("#failures", FailureAlertStrip)
            assert fs.failure_count == 1
            assert fs.last_role == "dev-1"
            assert fs.last_reason == "max-turns"
            assert "-active" in fs.classes

            # subsequent failures increment + replace 'last'
            app.feed_event(
                _parsed(
                    _ev(
                        "hard_failure",
                        cycle_id="c1",
                        role="reviewer-1",
                        reason="api-error",
                    )
                )
            )
            assert fs.failure_count == 2
            assert fs.last_role == "reviewer-1"
            assert fs.last_reason == "api-error"

    asyncio.run(_go())


def test_pulse_failure_strip_falls_back_to_unknown_when_reason_missing(
    tmp_path: Path,
) -> None:
    """v1 events without a reason field still surface in the alert strip."""
    from control_tower.watch import FailureAlertStrip, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")
        async with app.run_test():
            app.feed_event(
                _parsed(_ev("hard_failure", cycle_id="c1", role="dev-1"))
            )
            fs = app.query_one("#failures", FailureAlertStrip)
            assert fs.last_reason == "unknown"

    asyncio.run(_go())


def test_view_ticker_still_processes_events(tmp_path: Path) -> None:
    """ticker view widgets receive feed_event without crashing on missing #status/#heartbeat."""
    from control_tower.watch import EventTicker, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="ticker")
        async with app.run_test():
            for i in range(3):
                app.feed_event(_parsed(_ev("cycle_start", cycle_id=f"c{i}")))
            ticker = app.query_one("#ticker", EventTicker)
            assert len(ticker.events) == 3
            # role state still updates internally (it's a cheap dict)
            assert "dev-1" in app._role_states

    asyncio.run(_go())


def test_view_stats_refresh_counters_does_not_crash_without_other_widgets(
    tmp_path: Path,
) -> None:
    from control_tower.watch import CountersPane, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="stats")
        async with app.run_test():
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1")))
            app.feed_event(
                _parsed(_ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=1.0))
            )
            app._refresh_counters()
            pane = app.query_one("#counters", CountersPane)
            assert pane._cs["dev-1"].total == 1
            assert pane._cs["dev-1"].ok == 1

    asyncio.run(_go())


# --- drill-down views (loop-control-tower-255) -------------------------


def test_is_failure_event_classifies_the_three_failure_classes() -> None:
    from control_tower.watch import _is_failure_event

    def _e(**kw: Any):
        return _parsed(_ev(**kw))  # type: ignore[arg-type]

    assert _is_failure_event(_e(event="hard_failure", cycle_id="c1"))
    assert _is_failure_event(_e(event="lock_race_lost", cycle_id="c1"))
    assert _is_failure_event(_e(event="cycle_end", cycle_id="c1", exit_code=2))
    # NOT failures:
    assert not _is_failure_event(_e(event="cycle_end", cycle_id="c1", exit_code=0))
    assert not _is_failure_event(_e(event="cycle_skip", cycle_id="c1"))
    assert not _is_failure_event(_e(event="cycle_start", cycle_id="c1"))
    assert not _is_failure_event(_e(event="llm_started", cycle_id="c1"))


def test_view_cycle_requires_filter_cycle_id(tmp_path: Path) -> None:
    """WatchApp(view=cycle) without a cycle_id is a misconfiguration."""
    from control_tower.watch import WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    try:
        WatchApp(log, view="cycle")
    except ValueError:
        return
    raise AssertionError("view=cycle without filter_cycle_id should raise")


def test_view_cycle_filters_to_one_cycle_id_chronologically(tmp_path: Path) -> None:
    """Events for other cycles are dropped; matching events arrive oldest-first."""
    from control_tower.watch import EventTicker, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(
            log,
            from_start=True,
            poll_interval_s=0.05,
            view="cycle",
            filter_cycle_id="c1",
        )
        async with app.run_test():
            # interleave c1, c2 events
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c1")))
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="c2")))
            app.feed_event(_parsed(_ev("llm_started", cycle_id="c1", run_id="r1")))
            app.feed_event(_parsed(_ev("llm_started", cycle_id="c2", run_id="r2")))
            app.feed_event(
                _parsed(_ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=1.0))
            )

            ticker = app.query_one("#ticker", EventTicker)
            assert ticker.oldest_first is True
            # only c1's three events made it in
            assert len(ticker.events) == 3
            events_in_order = [ev.event for ev in ticker.events]
            assert events_in_order == ["cycle_start", "llm_started", "cycle_end"]

    asyncio.run(_go())


def test_view_failures_admits_only_failure_family_events(tmp_path: Path) -> None:
    from control_tower.watch import EventTicker, WatchApp

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="failures")
        async with app.run_test():
            # success events: should NOT appear
            app.feed_event(_parsed(_ev("cycle_start", cycle_id="ok1")))
            app.feed_event(
                _parsed(_ev("cycle_end", cycle_id="ok1", exit_code=0, duration_s=1.0))
            )
            app.feed_event(_parsed(_ev("cycle_skip", cycle_id="skip1")))
            app.feed_event(_parsed(_ev("llm_started", cycle_id="ok1", run_id="r")))
            # failure events: SHOULD appear
            app.feed_event(
                _parsed(_ev("hard_failure", cycle_id="hf1", reason="max-turns"))
            )
            app.feed_event(
                _parsed(_ev("cycle_end", cycle_id="f1", exit_code=2, duration_s=1.0))
            )
            app.feed_event(_parsed(_ev("lock_race_lost", cycle_id="lr1")))

            ticker = app.query_one("#ticker", EventTicker)
            assert ticker.oldest_first is False  # newest-first for live failures
            assert len(ticker.events) == 3
            assert {ev.event for ev in ticker.events} == {
                "hard_failure",
                "cycle_end",
                "lock_race_lost",
            }
            # newest-first: the lock_race_lost we pushed last is at index 0
            assert ticker.events[0].event == "lock_race_lost"

    asyncio.run(_go())


def test_in_tmux_detects_TMUX_env_var(monkeypatch: Any) -> None:
    from control_tower.watch import _in_tmux

    monkeypatch.delenv("TMUX", raising=False)
    assert _in_tmux() is False
    monkeypatch.setenv("TMUX", "/tmp/tmux-1000/default,1234,0")
    assert _in_tmux() is True


def test_spawn_tmux_popup_invokes_tmux_display_popup(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The helper shells out to ``tmux display-popup -E -- <argv>``."""
    import control_tower.watch as watch_mod

    captured: dict[str, Any] = {}

    class _FakePopen:
        def __init__(self, cmd: list[str], **kwargs: Any) -> None:
            captured["cmd"] = cmd
            captured["kwargs"] = kwargs

    monkeypatch.setattr(watch_mod.subprocess, "Popen", _FakePopen)

    watch_mod._spawn_tmux_popup(
        ["python", "-m", "control_tower", "watch", "--view=failures"],
        title="failures",
    )

    cmd = captured["cmd"]
    assert cmd[:3] == ["tmux", "display-popup", "-E"]
    assert "-T" in cmd and cmd[cmd.index("-T") + 1] == "failures"
    assert "--" in cmd
    tail = cmd[cmd.index("--") + 1 :]
    assert tail == ["python", "-m", "control_tower", "watch", "--view=failures"]
    assert captured["kwargs"].get("start_new_session") is True


def test_action_open_failures_outside_tmux_notifies_user(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Pressing 'f' outside tmux shows a fallback message, no subprocess."""
    import control_tower.watch as watch_mod
    from control_tower.watch import WatchApp

    monkeypatch.delenv("TMUX", raising=False)

    calls: list[list[str]] = []

    def _fail_popen(*args: Any, **kwargs: Any) -> None:
        calls.append(args[0])
        raise AssertionError("Popen should not be invoked outside tmux")

    monkeypatch.setattr(watch_mod.subprocess, "Popen", _fail_popen)

    log = tmp_path / "events.jsonl"
    log.write_text("")

    notifications: list[str] = []

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")

        def _capture(msg: str, **kw: Any) -> None:
            notifications.append(msg)

        async with app.run_test() as pilot:
            app.notify = _capture  # type: ignore[method-assign]
            await pilot.press("f")
            await pilot.pause(0.05)

    asyncio.run(_go())
    assert calls == []
    assert any("tmux" in n.lower() for n in notifications)


def test_action_open_failures_inside_tmux_spawns_subprocess(
    tmp_path: Path, monkeypatch: Any
) -> None:
    import control_tower.watch as watch_mod
    from control_tower.watch import WatchApp

    monkeypatch.setenv("TMUX", "/tmp/tmux-1000/default,1234,0")

    captured: list[list[str]] = []

    class _FakePopen:
        def __init__(self, cmd: list[str], **kwargs: Any) -> None:
            captured.append(cmd)

    monkeypatch.setattr(watch_mod.subprocess, "Popen", _FakePopen)

    log = tmp_path / "events.jsonl"
    log.write_text("")

    async def _go() -> None:
        app = WatchApp(log, from_start=True, poll_interval_s=0.05, view="pulse")
        async with app.run_test() as pilot:
            await pilot.press("f")
            await pilot.pause(0.05)

    asyncio.run(_go())
    assert len(captured) == 1
    cmd = captured[0]
    assert cmd[0] == "tmux"
    tail = cmd[cmd.index("--") + 1 :]
    assert "--view=failures" in tail
    assert "--from-start" in tail
    assert str(log) in tail


def test_ticker_oldest_first_orders_appends_correctly() -> None:
    """oldest_first=True appends rather than appendleft."""
    from control_tower.events import Event
    from control_tower.watch import EventTicker

    t = EventTicker(oldest_first=True)
    for i in range(3):
        t.push(
            Event(
                ts=f"2026-05-10T10:00:0{i}+00:00",
                session="s",
                repo="o/r",
                role="dev-1",
                event="cycle_start",
                schema_version=2,
                extra={"cycle_id": f"c{i}"},
            )
        )
    assert [e.extra["cycle_id"] for e in t.events] == ["c0", "c1", "c2"]
