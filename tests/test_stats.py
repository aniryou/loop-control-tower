"""Tests for control_tower.stats: pure aggregator functions over Iterable[Cycle].

These pin per-role/per-pr/per-kind counters and percentiles consumed by the
CLI (and, later, the live dashboard). Stats functions never touch the
filesystem — every test here fabricates Cycles via reconstruct() over
hand-built Events.
"""

from __future__ import annotations

import math
from typing import Any

from control_tower.cycles import reconstruct
from control_tower.events import Event


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


def _ok_cycle(cid: str, role: str, dur: float) -> list[Event]:
    return [
        _ev("cycle_start", cycle_id=cid, role=role),
        _ev("cycle_end", cycle_id=cid, role=role, exit_code=0, duration_s=dur),
    ]


def _fail_cycle(cid: str, role: str, dur: float) -> list[Event]:
    return [
        _ev("cycle_start", cycle_id=cid, role=role),
        _ev("cycle_end", cycle_id=cid, role=role, exit_code=2, duration_s=dur),
    ]


def _skip_cycle(cid: str, role: str, dur: float | None = None) -> list[Event]:
    closer_extra: dict[str, Any] = {}
    if dur is not None:
        closer_extra["duration_s"] = dur
    return [
        _ev("cycle_start", cycle_id=cid, role=role),
        _ev("cycle_skip", cycle_id=cid, role=role, **closer_extra),
    ]


# --- imports / re-exports -------------------------------------------------


def test_module_imports() -> None:
    from control_tower.stats import (  # noqa: F401
        DispatchCounts,
        LockStats,
        RoleStats,
        at_cap_stats,
        cycle_summary,
        dispatch_stats,
        hard_failure_stats,
        lock_stats,
    )


# --- cycle_summary --------------------------------------------------------


def test_cycle_summary_basic_outcome_counts() -> None:
    """3 ok + 1 fail + 1 skip for dev-1 → counts; median over 4 closed (skip excluded)."""
    from control_tower.stats import cycle_summary

    events: list[Event] = []
    for i, dur in enumerate([10.0, 20.0, 30.0]):
        events += _ok_cycle(f"a{i}", "dev-1", dur)
    events += _fail_cycle("f1", "dev-1", 40.0)
    events += _skip_cycle("s1", "dev-1", 99.0)
    cycles = list(reconstruct(events))
    s = cycle_summary(cycles)["dev-1"]
    assert s.total == 5
    assert s.ok == 3
    assert s.fail == 1
    assert s.skip == 1
    assert s.open == 0
    # closed = ok + fail = [10, 20, 30, 40]; the 99s skip must NOT pull median.
    assert s.median_duration_s == 25.0


def test_cycle_summary_skip_excluded_from_percentiles() -> None:
    from control_tower.stats import cycle_summary

    events: list[Event] = _ok_cycle("a", "dev-1", 10.0) + _skip_cycle("s", "dev-1", 99.0)
    cycles = list(reconstruct(events))
    s = cycle_summary(cycles)["dev-1"]
    assert s.median_duration_s == 10.0


def test_cycle_summary_no_closed_cycles_durations_none() -> None:
    """A role with only a skip and an open has median/p95 of None, not 0.0."""
    from control_tower.stats import cycle_summary

    events: list[Event] = _skip_cycle("s1", "dev-1", 5.0) + [
        _ev("cycle_start", cycle_id="o1", role="dev-1"),
    ]
    cycles = list(reconstruct(events))
    s = cycle_summary(cycles)["dev-1"]
    assert s.skip == 1
    assert s.open == 1
    assert s.median_duration_s is None
    assert s.p95_duration_s is None


def test_cycle_summary_empty_input() -> None:
    from control_tower.stats import cycle_summary

    assert cycle_summary([]) == {}


def test_cycle_summary_p95_pinned_value() -> None:
    """p95 over [10, 20, ..., 100] must be 95.5 (statistics.quantiles inclusive)."""
    from control_tower.stats import cycle_summary

    events: list[Event] = []
    for i, dur in enumerate([10, 20, 30, 40, 50, 60, 70, 80, 90, 100]):
        events += _ok_cycle(f"c{i}", "r", float(dur))
    cycles = list(reconstruct(events))
    s = cycle_summary(cycles)["r"]
    assert s.median_duration_s == 55.0
    assert s.p95_duration_s == 95.5


def test_cycle_summary_single_closed_cycle_has_median_and_p95() -> None:
    """One closed cycle: median == p95 == that value (statistics.quantiles requires n>=2)."""
    from control_tower.stats import cycle_summary

    cycles = list(reconstruct(_ok_cycle("c1", "dev-1", 7.0)))
    s = cycle_summary(cycles)["dev-1"]
    assert s.median_duration_s == 7.0
    assert s.p95_duration_s == 7.0


def test_cycle_summary_open_cycles_counted() -> None:
    from control_tower.stats import cycle_summary

    events = [_ev("cycle_start", cycle_id="o1", role="dev-1")]
    cycles = list(reconstruct(events))
    s = cycle_summary(cycles)["dev-1"]
    assert s.open == 1
    assert s.total == 1


def test_cycle_summary_two_roles_keyed_independently() -> None:
    from control_tower.stats import cycle_summary

    events = (
        _ok_cycle("a", "dev-1", 1.0)
        + _ok_cycle("b", "dev-2", 2.0)
        + _ok_cycle("c", "dev-2", 4.0)
    )
    cycles = list(reconstruct(events))
    summary = cycle_summary(cycles)
    assert summary["dev-1"].total == 1
    assert summary["dev-2"].total == 2
    assert summary["dev-2"].median_duration_s == 3.0


def test_cycle_summary_closed_cycle_without_duration_excluded_from_percentiles() -> None:
    """A closed cycle with duration_s=None counts in totals but not percentiles."""
    from control_tower.stats import cycle_summary

    events = [
        _ev("cycle_start", cycle_id="c1", role="dev-1"),
        _ev("cycle_end", cycle_id="c1", role="dev-1", exit_code=0),  # no duration_s
    ] + _ok_cycle("c2", "dev-1", 10.0)
    cycles = list(reconstruct(events))
    s = cycle_summary(cycles)["dev-1"]
    assert s.total == 2
    assert s.ok == 2
    assert s.median_duration_s == 10.0


# --- lock_stats -----------------------------------------------------------


def test_lock_stats_acquired_and_lost() -> None:
    """32 lock_acquired + 4 lock_race_lost for dev-1 → rate ≈ 0.111."""
    from control_tower.stats import lock_stats

    events: list[Event] = []
    for i in range(32):
        cid = f"a{i}"
        events += [
            _ev("cycle_start", cycle_id=cid, role="dev-1"),
            _ev("lock_acquired", cycle_id=cid, role="dev-1"),
            _ev("cycle_end", cycle_id=cid, role="dev-1", exit_code=0, duration_s=1.0),
        ]
    for i in range(4):
        cid = f"l{i}"
        events += [
            _ev("cycle_start", cycle_id=cid, role="dev-1"),
            _ev("lock_race_lost", cycle_id=cid, role="dev-1"),
            _ev("cycle_skip", cycle_id=cid, role="dev-1"),
        ]
    cycles = list(reconstruct(events))
    s = lock_stats(cycles)["dev-1"]
    assert s.acquired == 32
    assert s.lost == 4
    assert s.rate is not None
    assert math.isclose(s.rate, 4 / 36, rel_tol=1e-9)


def test_lock_stats_role_absent_when_no_lock_events() -> None:
    """A role that never emits lock_* events is not in the dict — '0 of 0' is undefined."""
    from control_tower.stats import lock_stats

    cycles = list(reconstruct(_ok_cycle("c1", "dev-1", 1.0)))
    assert "dev-1" not in lock_stats(cycles)


def test_lock_stats_only_acquired_no_lost() -> None:
    """All wins, no races: rate=0.0 (denom positive)."""
    from control_tower.stats import lock_stats

    events = [
        _ev("cycle_start", cycle_id="c1", role="dev-2"),
        _ev("lock_acquired", cycle_id="c1", role="dev-2"),
        _ev("cycle_end", cycle_id="c1", role="dev-2", exit_code=0, duration_s=1.0),
    ]
    cycles = list(reconstruct(events))
    s = lock_stats(cycles)["dev-2"]
    assert s.acquired == 1
    assert s.lost == 0
    assert s.rate == 0.0


# --- dispatch_stats -------------------------------------------------------


def test_dispatch_stats_keyed_by_pr() -> None:
    from control_tower.stats import dispatch_stats

    events: list[Event] = []
    for i in range(3):
        cid = f"f41-{i}"
        events += [
            _ev("cycle_start", cycle_id=cid, role="dispatch:followup"),
            _ev("dispatch_fired", cycle_id=cid, role="dispatch:followup", pr=41),
            _ev("cycle_end", cycle_id=cid, role="dispatch:followup", exit_code=0, duration_s=1.0),
        ]
    for i in range(2):
        cid = f"s41-{i}"
        events += [
            _ev("cycle_start", cycle_id=cid, role="dispatch:followup"),
            _ev("dispatch_skip", cycle_id=cid, role="dispatch:followup", pr=41),
            _ev("cycle_skip", cycle_id=cid, role="dispatch:followup"),
        ]
    cycles = list(reconstruct(events))
    s = dispatch_stats(cycles)["#41"]
    assert s.fired == 3
    assert s.skipped == 2


def test_dispatch_stats_ignores_events_without_pr() -> None:
    from control_tower.stats import dispatch_stats

    events = [
        _ev("cycle_start", cycle_id="c1", role="dispatch:followup"),
        _ev("dispatch_fired", cycle_id="c1", role="dispatch:followup"),  # no pr
        _ev("cycle_end", cycle_id="c1", role="dispatch:followup", exit_code=0, duration_s=1.0),
    ]
    cycles = list(reconstruct(events))
    assert dispatch_stats(cycles) == {}


def test_dispatch_stats_two_prs_keyed_separately() -> None:
    from control_tower.stats import dispatch_stats

    events = [
        _ev("cycle_start", cycle_id="c1", role="dispatch:followup"),
        _ev("dispatch_fired", cycle_id="c1", role="dispatch:followup", pr=41),
        _ev("dispatch_fired", cycle_id="c1", role="dispatch:followup", pr=44),
        _ev("dispatch_skip", cycle_id="c1", role="dispatch:followup", pr=44),
        _ev("cycle_end", cycle_id="c1", role="dispatch:followup", exit_code=0, duration_s=1.0),
    ]
    cycles = list(reconstruct(events))
    out = dispatch_stats(cycles)
    assert out["#41"].fired == 1
    assert out["#41"].skipped == 0
    assert out["#44"].fired == 1
    assert out["#44"].skipped == 1


# --- at_cap_stats ---------------------------------------------------------


def test_at_cap_stats_keyed_by_kind() -> None:
    from control_tower.stats import at_cap_stats

    events = [
        _ev("cycle_start", cycle_id="c1", role="dispatch:followup"),
        _ev("at_cap", cycle_id="c1", role="dispatch:followup", kind="followup"),
        _ev("at_cap", cycle_id="c1", role="dispatch:followup", kind="followup"),
        _ev("at_cap", cycle_id="c1", role="dispatch:followup", kind="conflicts"),
        _ev("cycle_skip", cycle_id="c1", role="dispatch:followup"),
    ]
    cycles = list(reconstruct(events))
    out = at_cap_stats(cycles)
    assert out["followup"] == 2
    assert out["conflicts"] == 1


def test_at_cap_stats_ignores_events_without_kind() -> None:
    from control_tower.stats import at_cap_stats

    events = [
        _ev("cycle_start", cycle_id="c1", role="dispatch:followup"),
        _ev("at_cap", cycle_id="c1", role="dispatch:followup"),  # no kind
        _ev("cycle_skip", cycle_id="c1", role="dispatch:followup"),
    ]
    cycles = list(reconstruct(events))
    assert at_cap_stats(cycles) == {}


# --- hard_failure_stats ---------------------------------------------------


def test_hard_failure_stats_keyed_by_role() -> None:
    from control_tower.stats import hard_failure_stats

    events = [
        _ev("cycle_start", cycle_id="c1", role="dev-1"),
        _ev("hard_failure", cycle_id="c1", role="dev-1"),
        _ev("cycle_end", cycle_id="c1", role="dev-1", exit_code=2, duration_s=1.0),
        _ev("cycle_start", cycle_id="c2", role="dev-2"),
        _ev("hard_failure", cycle_id="c2", role="dev-2"),
        _ev("hard_failure", cycle_id="c2", role="dev-2"),
        _ev("cycle_end", cycle_id="c2", role="dev-2", exit_code=2, duration_s=1.0),
    ]
    cycles = list(reconstruct(events))
    out = hard_failure_stats(cycles)
    assert out["dev-1"] == 1
    assert out["dev-2"] == 2


def test_hard_failure_stats_empty() -> None:
    from control_tower.stats import hard_failure_stats

    cycles = list(reconstruct(_ok_cycle("c1", "dev-1", 1.0)))
    assert hard_failure_stats(cycles) == {}
