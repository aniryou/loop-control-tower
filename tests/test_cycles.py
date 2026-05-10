"""Tests for control_tower.cycles: pair raw Events into typed Cycles / LLMRuns.

These tests pin the substrate that downstream stats and the live dashboard read.
The reconstructor is fed Events directly (not raw NDJSON) — see
test_events.py for the parser contract.
"""

from __future__ import annotations

from typing import Any

import pytest

from control_tower.events import Event, ParseError


def _ev(
    event: str,
    *,
    cycle_id: str | None = None,
    role: str = "dev-1",
    session: str = "s",
    repo: str = "o/r",
    ts: str = "2026-05-10T10:00:00+00:00",
    **extra: Any,
) -> Event:
    payload: dict[str, Any] = {}
    if cycle_id is not None:
        payload["cycle_id"] = cycle_id
    payload.update(extra)
    return Event(
        ts=ts,
        session=session,
        repo=repo,
        role=role,
        event=event,
        schema_version=1,
        extra=payload,
    )


# --- imports / re-exports -------------------------------------------------


def test_reexports_resolve() -> None:
    from control_tower import Cycle, LLMRun, reconstruct  # noqa: F401


def test_module_imports() -> None:
    from control_tower.cycles import Cycle, LLMRun, reconstruct  # noqa: F401


# --- empty / trivial ------------------------------------------------------


def test_reconstruct_empty_yields_nothing() -> None:
    from control_tower.cycles import reconstruct

    assert list(reconstruct([])) == []


# --- canonical Mode-1 cycle ----------------------------------------------


def test_canonical_mode1_cycle_ok() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1", mode="1"),
        _ev("eligibility", cycle_id="c1"),
        _ev("lock_acquired", cycle_id="c1"),
        _ev(
            "llm_started",
            cycle_id="c1",
            run_id="R1",
            mode="1",
            issue=42,
        ),
        _ev(
            "llm_exited",
            cycle_id="c1",
            run_id="R1",
            exit_code=0,
            duration_s=12.0,
        ),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=15.0),
    ]
    cycles = list(reconstruct(events))
    assert len(cycles) == 1
    cycle = cycles[0]
    assert cycle.cycle_id == "c1"
    assert cycle.role == "dev-1"
    assert cycle.session == "s"
    assert cycle.repo == "o/r"
    assert cycle.outcome == "ok"
    assert cycle.duration_s == 15.0
    assert cycle.closed is not None and cycle.closed.event == "cycle_end"
    assert cycle.started.event == "cycle_start"
    assert len(cycle.events) == 6
    assert len(cycle.llm_runs) == 1
    run = cycle.llm_runs[0]
    assert run.run_id == "R1"
    assert run.mode == "1"
    assert run.target == 42
    assert run.exit_code == 0
    assert run.duration_s == 12.0
    assert run.exited is not None
    assert run.started.event == "llm_started"


def test_cycle_end_with_exit_code_2_is_fail() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("cycle_end", cycle_id="c1", exit_code=2, duration_s=3.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.outcome == "fail"
    assert cycle.duration_s == 3.0


def test_cycle_skip_closer_is_skip() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("cycle_skip", cycle_id="c1", duration_s=0.5),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.outcome == "skip"
    assert cycle.duration_s == 0.5
    assert cycle.closed is not None and cycle.closed.event == "cycle_skip"


def test_cycle_skip_without_duration_is_none() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("cycle_skip", cycle_id="c1"),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.outcome == "skip"
    assert cycle.duration_s is None


# --- isolation between concurrent cycles ---------------------------------


def test_two_interleaved_cycles_no_event_leakage() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="a", role="dev-1"),
        _ev("cycle_start", cycle_id="b", role="dev-2"),
        _ev("eligibility", cycle_id="a"),
        _ev("eligibility", cycle_id="b"),
        _ev("cycle_end", cycle_id="b", exit_code=0, duration_s=2.0),
        _ev("cycle_end", cycle_id="a", exit_code=0, duration_s=4.0),
    ]
    cycles = list(reconstruct(events))
    assert len(cycles) == 2
    by_id = {c.cycle_id: c for c in cycles}
    assert by_id["a"].role == "dev-1"
    assert by_id["b"].role == "dev-2"
    assert by_id["a"].duration_s == 4.0
    assert by_id["b"].duration_s == 2.0
    # b closed first, so it should be yielded first
    assert [c.cycle_id for c in cycles] == ["b", "a"]
    # No event leakage across cycles
    assert all(
        ev.extra.get("cycle_id") == c.cycle_id
        for c in cycles
        for ev in c.events
    )


def test_role_is_taken_from_cycle_start_per_cycle() -> None:
    from control_tower.cycles import reconstruct

    events: list[Event | ParseError] = []
    for i in range(3):
        cid = f"d{i}"
        events.append(_ev("cycle_start", cycle_id=cid, role="dev-1"))
        events.append(_ev("cycle_end", cycle_id=cid, exit_code=0, duration_s=1.0))
    for i in range(2):
        cid = f"f{i}"
        events.append(
            _ev("cycle_start", cycle_id=cid, role="dispatch:followup")
        )
        events.append(
            _ev("cycle_skip", cycle_id=cid)
        )
    cycles = list(reconstruct(events))
    assert len(cycles) == 5
    roles = sorted(c.role for c in cycles)
    assert roles == ["dev-1", "dev-1", "dev-1", "dispatch:followup", "dispatch:followup"]


# --- LLM run pairing ------------------------------------------------------


def test_llm_started_without_exited_within_cycle() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("llm_started", cycle_id="c1", run_id="R1", mode="1", pr=7),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=5.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert len(cycle.llm_runs) == 1
    run = cycle.llm_runs[0]
    assert run.exited is None
    assert run.exit_code is None
    assert run.duration_s is None
    assert run.target == 7
    assert run.mode == "1"


def test_two_llm_runs_in_one_cycle_in_start_order() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("llm_started", cycle_id="c1", run_id="A", mode="1"),
        _ev("llm_exited", cycle_id="c1", run_id="A", exit_code=0, duration_s=1.0),
        _ev("llm_started", cycle_id="c1", run_id="B", mode="2"),
        _ev("llm_exited", cycle_id="c1", run_id="B", exit_code=1, duration_s=2.0),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=4.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert [r.run_id for r in cycle.llm_runs] == ["A", "B"]
    assert cycle.llm_runs[0].exit_code == 0
    assert cycle.llm_runs[1].exit_code == 1


def test_orphan_llm_exited_dropped() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("llm_exited", cycle_id="c1", run_id="ghost", exit_code=0, duration_s=1.0),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=2.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.llm_runs == ()


# --- defensive / robustness ----------------------------------------------


def test_duplicate_cycle_start_keeps_first() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1", role="first"),
        _ev("cycle_start", cycle_id="c1", role="second"),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=1.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.role == "first"


def test_events_after_cycle_end_dropped() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=1.0),
        _ev("eligibility", cycle_id="c1"),  # post-close, should drop
        _ev("cycle_end", cycle_id="c1", exit_code=2, duration_s=99.0),  # ignored
    ]
    cycles = list(reconstruct(events))
    assert len(cycles) == 1
    cycle = cycles[0]
    assert cycle.outcome == "ok"
    assert cycle.duration_s == 1.0
    assert len(cycle.events) == 2


def test_parse_errors_silently_skipped() -> None:
    from control_tower.cycles import reconstruct

    events: list[Event | ParseError] = [
        _ev("cycle_start", cycle_id="c1"),
        ParseError(raw="bad", reason="invalid_json"),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=1.0),
    ]
    cycles = list(reconstruct(events))
    assert len(cycles) == 1
    assert cycles[0].outcome == "ok"


def test_event_without_cycle_id_dropped() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("standalone"),  # no cycle_id
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=1.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert len(cycle.events) == 2  # standalone dropped


def test_open_cycle_yielded_at_stream_end() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("eligibility", cycle_id="c1"),
        _ev("llm_started", cycle_id="c1", run_id="R1"),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.outcome == "open"
    assert cycle.closed is None
    assert cycle.duration_s is None
    assert len(cycle.events) == 3
    # The dangling LLM run should be present with no exit info.
    assert len(cycle.llm_runs) == 1
    assert cycle.llm_runs[0].exited is None


def test_open_cycles_yielded_in_start_order() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("cycle_start", cycle_id="c2"),
        _ev("cycle_start", cycle_id="c3"),
    ]
    cycles = list(reconstruct(events))
    assert [c.cycle_id for c in cycles] == ["c1", "c2", "c3"]
    assert all(c.outcome == "open" for c in cycles)


# --- Mode-3 (triage) flow penned in cycle.events ------------------------


def test_mode3_triage_events_attached() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="m3", role="dispatch:conflicts"),
        _ev("triage_result", cycle_id="m3", verdict="tractable"),
        _ev("llm_started", cycle_id="m3", run_id="R1", mode="3", pr=99),
        _ev("llm_exited", cycle_id="m3", run_id="R1", exit_code=0, duration_s=8.0),
        _ev("cycle_end", cycle_id="m3", exit_code=0, duration_s=10.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.role == "dispatch:conflicts"
    assert any(ev.event == "triage_result" for ev in cycle.events)
    assert cycle.llm_runs[0].mode == "3"
    assert cycle.llm_runs[0].target == 99


def test_llm_started_target_prefers_pr_over_issue() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("llm_started", cycle_id="c1", run_id="R1", pr=12, issue=34),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=1.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.llm_runs[0].target == 12


def test_llm_started_falls_back_to_issue_when_no_pr() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("llm_started", cycle_id="c1", run_id="R1", issue=34),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=1.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.llm_runs[0].target == 34


def test_cycle_end_without_duration_is_none() -> None:
    from control_tower.cycles import reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("cycle_end", cycle_id="c1", exit_code=0),
    ]
    (cycle,) = list(reconstruct(events))
    assert cycle.outcome == "ok"
    assert cycle.duration_s is None


# --- frozen / hashable contract -----------------------------------------


def test_cycle_and_llm_run_are_frozen() -> None:
    from control_tower.cycles import Cycle, LLMRun, reconstruct

    events = [
        _ev("cycle_start", cycle_id="c1"),
        _ev("llm_started", cycle_id="c1", run_id="R1"),
        _ev("llm_exited", cycle_id="c1", run_id="R1", exit_code=0, duration_s=1.0),
        _ev("cycle_end", cycle_id="c1", exit_code=0, duration_s=2.0),
    ]
    (cycle,) = list(reconstruct(events))
    assert isinstance(cycle, Cycle)
    assert isinstance(cycle.llm_runs[0], LLMRun)
    with pytest.raises((AttributeError, Exception)):
        cycle.outcome = "fail"  # type: ignore[misc]
