"""Pure aggregator functions over Iterable[Cycle].

These are the functions behind ``python -m control_tower stats`` (see
``control_tower.cli``) and the future live dashboard. Everything here is
pure — no I/O, no globals, no logging — so tests can fabricate Cycles and
assert exact numbers.

Schema contract: see ``aniryou/loop`` ``docs/event-schema.md``. Counters
read fields off ``Cycle`` and ``Cycle.events``:

- per-role outcome counts and durations from ``Cycle.outcome`` / ``Cycle.duration_s``
- ``lock_acquired`` / ``lock_race_lost`` from ``Cycle.events`` for the lock-race rate
- ``dispatch_fired`` / ``dispatch_skip`` from ``Cycle.events`` keyed by ``pr``
- ``at_cap`` from ``Cycle.events`` keyed by ``kind``
- ``hard_failure`` from ``Cycle.events`` keyed by ``(role, reason)``;
  events without the v2 ``reason`` field fall back to ``"unknown"``
- ``llm_exited`` cost / token totals from ``Cycle.events`` keyed by role

Stdlib only.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from control_tower.cycles import Cycle

HARD_FAILURE_REASON_FALLBACK = "unknown"


@dataclass(frozen=True)
class RoleStats:
    """Per-role cycle counts and duration percentiles.

    ``median_duration_s`` and ``p95_duration_s`` are computed over **closed**
    cycles only — outcomes ``"ok"`` and ``"fail"``. Both are ``None`` (not
    ``0.0``) when no closed cycle has a recorded duration.
    """

    total: int
    ok: int
    skip: int
    fail: int
    open: int
    median_duration_s: float | None
    p95_duration_s: float | None


@dataclass(frozen=True)
class LockStats:
    """Per-role lock-race counters. ``rate`` is ``lost / (acquired + lost)``."""

    acquired: int
    lost: int
    rate: float | None


@dataclass(frozen=True)
class DispatchCounts:
    """Per-PR dispatcher counters."""

    fired: int
    skipped: int


def cycle_summary(cycles: Iterable[Cycle]) -> dict[str, RoleStats]:
    """Aggregate cycles by role into outcome counts and duration percentiles."""
    grouped: dict[str, list[Cycle]] = defaultdict(list)
    for c in cycles:
        grouped[c.role].append(c)

    out: dict[str, RoleStats] = {}
    for role, role_cycles in grouped.items():
        ok = sum(1 for c in role_cycles if c.outcome == "ok")
        fail = sum(1 for c in role_cycles if c.outcome == "fail")
        skip = sum(1 for c in role_cycles if c.outcome == "skip")
        open_ = sum(1 for c in role_cycles if c.outcome == "open")
        durations = sorted(
            c.duration_s
            for c in role_cycles
            if c.outcome in ("ok", "fail") and c.duration_s is not None
        )
        median_s, p95_s = _median_and_p95(durations)
        out[role] = RoleStats(
            total=len(role_cycles),
            ok=ok,
            skip=skip,
            fail=fail,
            open=open_,
            median_duration_s=median_s,
            p95_duration_s=p95_s,
        )
    return out


def _median_and_p95(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    if len(values) == 1:
        # statistics.quantiles requires n>=2; single-point median == p95 == that value.
        return values[0], values[0]
    median_s = statistics.median(values)
    p95_s = statistics.quantiles(values, n=100, method="inclusive")[94]
    return median_s, p95_s


def lock_stats(cycles: Iterable[Cycle]) -> dict[str, LockStats]:
    """Per-role ``lock_acquired`` and ``lock_race_lost`` counts plus loss rate.

    A role that never appears in either event is absent from the result —
    "0 of 0" has no meaningful rate. ``rate`` is defensively ``None`` when
    ``acquired + lost == 0`` (unreachable in practice given the inclusion
    rule, but pinned by the contract).
    """
    acquired: dict[str, int] = defaultdict(int)
    lost: dict[str, int] = defaultdict(int)
    for c in cycles:
        for ev in c.events:
            if ev.event == "lock_acquired":
                acquired[ev.role] += 1
            elif ev.event == "lock_race_lost":
                lost[ev.role] += 1

    out: dict[str, LockStats] = {}
    for role in set(acquired) | set(lost):
        a = acquired[role]
        lo = lost[role]
        denom = a + lo
        rate = (lo / denom) if denom > 0 else None
        out[role] = LockStats(acquired=a, lost=lo, rate=rate)
    return out


def dispatch_stats(cycles: Iterable[Cycle]) -> dict[str, DispatchCounts]:
    """Per-PR ``dispatch_fired`` / ``dispatch_skip`` counts.

    Keys are the canonical PR string ``"#<n>"``. Events without a ``pr``
    extra field are ignored (forward-compat: dispatcher events tied to
    things other than PRs surface elsewhere).
    """
    fired: dict[str, int] = defaultdict(int)
    skipped: dict[str, int] = defaultdict(int)
    for c in cycles:
        for ev in c.events:
            if ev.event not in ("dispatch_fired", "dispatch_skip"):
                continue
            pr = ev.extra.get("pr")
            if pr is None:
                continue
            key = f"#{pr}"
            if ev.event == "dispatch_fired":
                fired[key] += 1
            else:
                skipped[key] += 1

    out: dict[str, DispatchCounts] = {}
    for key in set(fired) | set(skipped):
        out[key] = DispatchCounts(fired=fired[key], skipped=skipped[key])
    return out


def at_cap_stats(cycles: Iterable[Cycle]) -> dict[str, int]:
    """Count of ``at_cap`` events keyed by their ``kind`` extra (string)."""
    out: dict[str, int] = defaultdict(int)
    for c in cycles:
        for ev in c.events:
            if ev.event != "at_cap":
                continue
            kind = ev.extra.get("kind")
            if not isinstance(kind, str):
                continue
            out[kind] += 1
    return dict(out)


def hard_failure_stats(cycles: Iterable[Cycle]) -> dict[str, dict[str, int]]:
    """Count of ``hard_failure`` events keyed by ``(role, reason)``.

    ``reason`` is the closed enum from schema v2 (``api-error``, ``max-turns``,
    ``pipeline-crash``, ``mode2-give-up``, ``mode3-give-up``, ``no-result-line``,
    ``unknown``). Events that arrive without a string ``reason`` extra fall back
    to :data:`HARD_FAILURE_REASON_FALLBACK` so legacy v1 data and forward-compat
    surprises are still counted (rather than silently dropped).
    """
    out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for c in cycles:
        for ev in c.events:
            if ev.event != "hard_failure":
                continue
            reason = ev.extra.get("reason")
            key = reason if isinstance(reason, str) else HARD_FAILURE_REASON_FALLBACK
            out[ev.role][key] += 1
    return {role: dict(reasons) for role, reasons in out.items()}


@dataclass(frozen=True)
class LLMCostStats:
    """Per-role totals from ``llm_exited`` events.

    Sums the four schema-v2 required fields (``total_cost_usd``,
    ``input_tokens``, ``output_tokens``, ``num_turns``) plus a ``runs``
    count of contributing ``llm_exited`` events. A field that is missing
    or non-numeric on a given event contributes 0 — the v2 contract
    guarantees presence, but the aggregator stays defensive.
    """

    runs: int
    total_cost_usd: float
    input_tokens: int
    output_tokens: int
    num_turns: int


def llm_cost_stats(cycles: Iterable[Cycle]) -> dict[str, LLMCostStats]:
    """Sum ``llm_exited`` cost / token fields per emitting role.

    Roles that never emitted ``llm_exited`` are absent from the result.
    """
    runs: dict[str, int] = defaultdict(int)
    cost: dict[str, float] = defaultdict(float)
    in_tok: dict[str, int] = defaultdict(int)
    out_tok: dict[str, int] = defaultdict(int)
    turns: dict[str, int] = defaultdict(int)

    for c in cycles:
        for ev in c.events:
            if ev.event != "llm_exited":
                continue
            runs[ev.role] += 1
            cost[ev.role] += _as_float(ev.extra.get("total_cost_usd"))
            in_tok[ev.role] += _as_int(ev.extra.get("input_tokens"))
            out_tok[ev.role] += _as_int(ev.extra.get("output_tokens"))
            turns[ev.role] += _as_int(ev.extra.get("num_turns"))

    return {
        role: LLMCostStats(
            runs=runs[role],
            total_cost_usd=cost[role],
            input_tokens=in_tok[role],
            output_tokens=out_tok[role],
            num_turns=turns[role],
        )
        for role in runs
    }


def _as_float(value: object) -> float:
    if isinstance(value, bool):  # bool is an int subclass
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0
