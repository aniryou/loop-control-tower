"""Group raw events into typed cycles for downstream stats and UI.

Loop's reader (``control_tower.events``) yields a flat stream of ``Event``
records. Every monitoring question ("median Mode-3 cycle duration", "is
reviewer-1 idle right now?") is naturally phrased over *cycles*, not events.
This module owns that pairing in one place: events with the same
``cycle_id`` are folded into one :class:`Cycle`; ``llm_started`` /
``llm_exited`` pairs (matched by ``run_id``) become :class:`LLMRun`.

Schema contract: see ``aniryou/loop`` ``docs/event-schema.md``. ``cycle_id``
is on every per-cycle event; ``run_id`` is on ``llm_started`` /
``llm_exited``. A cycle opens on ``cycle_start`` and closes on the next
``cycle_end`` or ``cycle_skip`` for the same ``cycle_id``.

Stdlib only.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Literal

from control_tower.events import Event, ParseError

CycleOutcome = Literal["ok", "skip", "fail", "open"]


@dataclass(frozen=True)
class LLMRun:
    """One ``llm_started`` ... ``llm_exited`` pair within a cycle."""

    run_id: str
    mode: str | None
    target: object | None
    started: Event
    exited: Event | None
    exit_code: int | None
    duration_s: float | None


@dataclass(frozen=True)
class Cycle:
    """All events sharing one ``cycle_id``, folded into a typed record."""

    cycle_id: str
    role: str
    session: str
    repo: str
    started: Event
    closed: Event | None
    outcome: CycleOutcome
    duration_s: float | None
    llm_runs: tuple[LLMRun, ...]
    events: tuple[Event, ...]


@dataclass
class _OpenCycle:
    cycle_id: str
    started: Event
    events: list[Event] = field(default_factory=list)
    open_runs: dict[str, Event] = field(default_factory=dict)
    finished_runs: list[LLMRun] = field(default_factory=list)
    run_order: list[str] = field(default_factory=list)


def _target_of(started: Event) -> object | None:
    extra = started.extra
    if "pr" in extra:
        return extra["pr"]
    if "issue" in extra:
        return extra["issue"]
    return None


def _finalize_open_runs(state: _OpenCycle) -> tuple[LLMRun, ...]:
    """Append still-open LLM runs (started but not exited) in start-order."""
    finished_by_id = {r.run_id: r for r in state.finished_runs}
    out: list[LLMRun] = []
    for run_id in state.run_order:
        if run_id in finished_by_id:
            out.append(finished_by_id[run_id])
        elif run_id in state.open_runs:
            started = state.open_runs[run_id]
            out.append(
                LLMRun(
                    run_id=run_id,
                    mode=started.extra.get("mode"),
                    target=_target_of(started),
                    started=started,
                    exited=None,
                    exit_code=None,
                    duration_s=None,
                )
            )
    return tuple(out)


def _close_cycle(
    state: _OpenCycle,
    closer: Event | None,
    outcome: CycleOutcome,
    duration_s: float | None,
) -> Cycle:
    return Cycle(
        cycle_id=state.cycle_id,
        role=state.started.role,
        session=state.started.session,
        repo=state.started.repo,
        started=state.started,
        closed=closer,
        outcome=outcome,
        duration_s=duration_s,
        llm_runs=_finalize_open_runs(state),
        events=tuple(state.events),
    )


def reconstruct(events: Iterable[Event | ParseError]) -> Iterator[Cycle]:
    """Group an event stream into typed :class:`Cycle` records.

    Yields a ``Cycle`` once on its closing ``cycle_end`` / ``cycle_skip``.
    At end of stream, any still-open cycles are yielded with
    ``outcome="open"`` in ``cycle_start`` order — this matters for
    ``tail_file``-driven consumers that hit ``stop`` mid-cycle.

    ``ParseError`` inputs are skipped silently. Events with no ``cycle_id``
    are dropped (forward-compat). Events arriving after a cycle's closer
    are dropped defensively.
    """
    open_cycles: dict[str, _OpenCycle] = {}
    closed_ids: set[str] = set()

    for item in events:
        if isinstance(item, ParseError):
            continue

        cycle_id = item.extra.get("cycle_id")
        if not isinstance(cycle_id, str):
            continue

        if item.event == "cycle_start":
            if cycle_id in open_cycles or cycle_id in closed_ids:
                continue
            state = _OpenCycle(cycle_id=cycle_id, started=item)
            state.events.append(item)
            open_cycles[cycle_id] = state
            continue

        state = open_cycles.get(cycle_id)
        if state is None:
            # Either no cycle_start was seen, or the cycle is already
            # closed — drop defensively.
            continue

        state.events.append(item)

        if item.event == "llm_started":
            run_id = item.extra.get("run_id")
            if isinstance(run_id, str) and run_id not in state.open_runs:
                state.open_runs[run_id] = item
                state.run_order.append(run_id)
            continue

        if item.event == "llm_exited":
            run_id = item.extra.get("run_id")
            started = state.open_runs.pop(run_id, None) if isinstance(run_id, str) else None
            if started is None:
                # Orphan exit — drop without crashing.
                continue
            exit_code = item.extra.get("exit_code")
            duration = item.extra.get("duration_s")
            state.finished_runs.append(
                LLMRun(
                    run_id=run_id,  # type: ignore[arg-type]
                    mode=started.extra.get("mode"),
                    target=_target_of(started),
                    started=started,
                    exited=item,
                    exit_code=exit_code if isinstance(exit_code, int) else None,
                    duration_s=float(duration)
                    if isinstance(duration, (int, float))
                    else None,
                )
            )
            continue

        if item.event in ("cycle_end", "cycle_skip"):
            duration = item.extra.get("duration_s")
            duration_s = (
                float(duration) if isinstance(duration, (int, float)) else None
            )
            if item.event == "cycle_skip":
                outcome: CycleOutcome = "skip"
            else:
                exit_code = item.extra.get("exit_code")
                outcome = "ok" if exit_code == 0 else "fail"
            yield _close_cycle(state, item, outcome, duration_s)
            del open_cycles[cycle_id]
            closed_ids.add(cycle_id)

    # Stream end: yield any still-open cycles in start-order.
    for state in open_cycles.values():
        yield _close_cycle(state, closer=None, outcome="open", duration_s=None)
