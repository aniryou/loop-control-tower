"""Live TUI dashboard for loop's NDJSON event stream.

``python -m control_tower watch [path]`` opens a three-region textual UI
that follows the event log produced by `aniryou/loop`:

- top: per-role status cards (current cycle, elapsed, current LLM run, last outcome);
- middle: rolling 50-event ticker, newest at top, coloured by family;
- bottom: the six aggregate counters that ``python -m control_tower stats``
  prints, recomputed once per second over cycles seen in this session.

A worker thread runs :func:`control_tower.events.tail_file` and queues parsed
events; the textual main loop drains the queue on a configurable interval and
updates widgets. Counter recomputation calls into :mod:`control_tower.stats`
directly, so the dashboard's numbers and ``stats``'s numbers agree byte-for-byte
over the same input — the integration test in ``tests/test_watch_integration.py``
asserts this.

Keys: ``q`` quit, ``c`` clear ticker, ``p`` pause auto-scroll, ``?`` help.
``Ctrl-C`` is honoured via textual's default SIGINT handling.
"""

from __future__ import annotations

import queue
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Footer, Header, Static

from control_tower.cycles import Cycle, reconstruct
from control_tower.events import Event, ParseError, tail_file
from control_tower.stats import (
    DispatchCounts,
    LLMCostStats,
    LockStats,
    RoleStats,
    at_cap_stats,
    cycle_summary,
    dispatch_stats,
    hard_failure_stats,
    llm_cost_stats,
    lock_stats,
)

TICKER_MAX = 50
COUNTER_REFRESH_S = 1.0


@dataclass
class RoleState:
    """Live, mutable state for one role's status card.

    Updated by ``WatchApp._update_role_state`` from each event seen for the
    role. ``current_cycle_id`` is set on ``cycle_start`` and cleared on the
    matching ``cycle_end`` / ``cycle_skip``; ``last_outcome`` retains the
    most recent terminal outcome so an idle card still shows what it did.
    """

    role: str
    current_cycle_id: str | None = None
    cycle_started_ts: float | None = None
    current_run_id: str | None = None
    current_run_mode: str | None = None
    run_started_ts: float | None = None
    last_outcome: str | None = None
    last_outcome_ts: str | None = None


def _iso_to_epoch(ts: str) -> float | None:
    try:
        return datetime.fromisoformat(ts).timestamp()
    except ValueError:
        return None


_TICKER_SKIP_KEYS = ("cycle_id", "run_id")


def _format_kvs(extra: dict[str, Any]) -> str:
    """Render the event's extra fields as ``k=v`` pairs for the ticker line.

    ``cycle_id`` and ``run_id`` are dropped — they are structural, not
    interesting in a one-line summary.
    """
    parts: list[str] = []
    for k, v in extra.items():
        if k in _TICKER_SKIP_KEYS:
            continue
        parts.append(f"{k}={v}")
    return " ".join(parts)


def _color_for_event(name: str) -> str:
    if name == "lock_race_lost":
        return "yellow"
    if name == "hard_failure":
        return "red"
    if name == "dispatch_fired":
        return "cyan"
    return "white"


def _short_ts(ts: str) -> str:
    """``2026-05-10T10:11:12+00:00`` → ``10:11:12``."""
    if len(ts) >= 19 and ts[10] == "T":
        return ts[11:19]
    return ts


class StatusRow(Static):
    """Top region — one card per role seen on the stream."""

    DEFAULT_CSS = "StatusRow { height: auto; padding: 0 1; }"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.roles: dict[str, RoleState] = {}

    def update_role(self, state: RoleState) -> None:
        self.roles[state.role] = state
        self.refresh_view()

    def refresh_view(self) -> None:
        if not self.roles:
            self.update("(no events yet)")
            return
        now = time.time()
        cards: list[str] = []
        for role in sorted(self.roles):
            s = self.roles[role]
            if s.current_cycle_id is not None:
                elapsed = (
                    now - s.cycle_started_ts
                    if s.cycle_started_ts is not None
                    else 0.0
                )
                cycle_part = f"cycle {s.current_cycle_id} ({elapsed:.0f}s)"
            else:
                cycle_part = "idle"

            run_part = ""
            if s.current_run_id is not None:
                mode = s.current_run_mode or "?"
                relapsed = (
                    now - s.run_started_ts
                    if s.run_started_ts is not None
                    else 0.0
                )
                run_part = f" llm={mode} ({relapsed:.0f}s)"

            outcome_part = ""
            if s.last_outcome is not None:
                ts_short = (
                    _short_ts(s.last_outcome_ts)
                    if s.last_outcome_ts
                    else ""
                )
                outcome_part = f"  last={s.last_outcome}@{ts_short}"

            cards.append(f"[bold]{role}[/bold]: {cycle_part}{run_part}{outcome_part}")
        self.update("\n".join(cards))


class EventTicker(Static):
    """Middle region — rolling ``TICKER_MAX``-event window, newest at top."""

    DEFAULT_CSS = "EventTicker { height: 1fr; padding: 0 1; }"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.events: deque[Event] = deque(maxlen=TICKER_MAX)
        self.paused: bool = False
        self._waiting_for: Path | None = None

    def set_waiting(self, path: Path) -> None:
        self._waiting_for = path
        self.refresh_view()

    def push(self, ev: Event) -> None:
        self._waiting_for = None
        self.events.appendleft(ev)
        if not self.paused:
            self.refresh_view()

    def clear(self) -> None:
        self.events.clear()
        self.refresh_view()

    def toggle_pause(self) -> None:
        self.paused = not self.paused
        self.refresh_view()

    def refresh_view(self) -> None:
        if not self.events:
            if self._waiting_for is not None:
                self.update(f"waiting for {self._waiting_for}")
            else:
                self.update("(no events)")
            return
        lines = []
        for ev in self.events:
            color = _color_for_event(ev.event)
            ts_short = _short_ts(ev.ts)
            kvs = _format_kvs(dict(ev.extra))
            line = f"{ts_short}  {ev.role:10s}  {ev.event:18s}  {kvs}".rstrip()
            lines.append(f"[{color}]{line}[/{color}]")
        if self.paused:
            lines.append("[dim](paused — press p to resume)[/dim]")
        self.update("\n".join(lines))


class CountersPane(Static):
    """Bottom region — six aggregate counters from :mod:`control_tower.stats`."""

    DEFAULT_CSS = "CountersPane { height: auto; padding: 0 1; }"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._cs: dict[str, RoleStats] = {}
        self._ls: dict[str, LockStats] = {}
        self._ds: dict[str, DispatchCounts] = {}
        self._ac: dict[str, int] = {}
        self._hf: dict[str, dict[str, int]] = {}
        self._lc: dict[str, LLMCostStats] = {}

    def update_counts(self, cycles: list[Cycle]) -> None:
        self._cs = cycle_summary(cycles)
        self._ls = lock_stats(cycles)
        self._ds = dispatch_stats(cycles)
        self._ac = at_cap_stats(cycles)
        self._hf = hard_failure_stats(cycles)
        self._lc = llm_cost_stats(cycles)
        self.refresh_view()

    def refresh_view(self) -> None:
        lines: list[str] = ["[bold]cycle summary[/bold]"]
        for role in sorted(self._cs):
            s = self._cs[role]
            med = "—" if s.median_duration_s is None else f"{s.median_duration_s:.1f}"
            p95 = "—" if s.p95_duration_s is None else f"{s.p95_duration_s:.1f}"
            lines.append(
                f"  {role}  total={s.total} ok={s.ok} skip={s.skip} "
                f"fail={s.fail} open={s.open}  median_s={med} p95_s={p95}"
            )

        lines.append("[bold]lock-race rate[/bold]")
        for role in sorted(self._ls):
            ls = self._ls[role]
            rate = "—" if ls.rate is None else f"{ls.rate * 100:.1f}%"
            lines.append(f"  {role}  acquired={ls.acquired} lost={ls.lost}  rate={rate}")

        lines.append("[bold]dispatch fires by pr[/bold]")
        for pr in sorted(self._ds):
            dc = self._ds[pr]
            lines.append(f"  {pr}  fired={dc.fired} skipped={dc.skipped}")

        lines.append("[bold]at-cap events by kind[/bold]")
        for kind in sorted(self._ac):
            lines.append(f"  {kind}  count={self._ac[kind]}")

        lines.append("[bold]hard failures[/bold]")
        for role in sorted(self._hf):
            for reason in sorted(self._hf[role]):
                lines.append(
                    f"  {role}  reason={reason} count={self._hf[role][reason]}"
                )

        lines.append("[bold]llm cost by role[/bold]")
        for role in sorted(self._lc):
            s = self._lc[role]
            lines.append(
                f"  {role}  runs={s.runs} total_usd={s.total_cost_usd:.4f} "
                f"in_tok={s.input_tokens} out_tok={s.output_tokens} "
                f"turns={s.num_turns}"
            )

        self.update("\n".join(lines))


class WatchApp(App):
    """Three-region live TUI for the loop event stream."""

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("c", "clear_ticker", "Clear"),
        Binding("p", "toggle_pause", "Pause"),
        Binding("question_mark", "help", "Help"),
    ]

    def __init__(
        self,
        log_path: Path,
        *,
        from_start: bool = False,
        poll_interval_s: float = 0.1,
    ) -> None:
        super().__init__()
        self.log_path = log_path
        self.from_start = from_start
        self.poll_interval_s = poll_interval_s
        self._queue: queue.Queue[Event | ParseError] = queue.Queue()
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None
        self._role_states: dict[str, RoleState] = {}
        self._events: list[Event] = []

    def compose(self) -> ComposeResult:
        yield Header()
        yield StatusRow(id="status")
        yield EventTicker(id="ticker")
        yield CountersPane(id="counters")
        yield Footer()

    def on_mount(self) -> None:
        ticker = self.query_one("#ticker", EventTicker)
        if not self.log_path.exists():
            ticker.set_waiting(self.log_path)
        else:
            ticker.refresh_view()
        self.query_one("#status", StatusRow).refresh_view()
        self.query_one("#counters", CountersPane).refresh_view()

        self._worker = threading.Thread(
            target=self._tail_worker, daemon=True, name="watch-tail"
        )
        self._worker.start()

        self.set_interval(self.poll_interval_s, self._drain_queue)
        self.set_interval(COUNTER_REFRESH_S, self._refresh_counters)
        self.set_interval(1.0, self._tick_status_row)

    def _tail_worker(self) -> None:
        try:
            for item in tail_file(
                self.log_path,
                from_start=self.from_start,
                poll_interval_s=self.poll_interval_s,
                stop=self._stop_event,
            ):
                if self._stop_event.is_set():
                    break
                self._queue.put(item)
        except Exception:
            # Worker errors must not crash the UI; the ticker just stops
            # receiving events and the user sees the last good state.
            pass

    def _drain_queue(self) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return
            self.feed_event(item)

    def feed_event(self, item: Event | ParseError) -> None:
        """Process one event. Public so tests can drive the app directly."""
        if isinstance(item, ParseError):
            return
        self._events.append(item)
        self._update_role_state(item)
        self.query_one("#ticker", EventTicker).push(item)

    def _update_role_state(self, ev: Event) -> None:
        s = self._role_states.setdefault(ev.role, RoleState(role=ev.role))
        ts = _iso_to_epoch(ev.ts)
        cycle_id = ev.extra.get("cycle_id")
        if not isinstance(cycle_id, str):
            return

        if ev.event == "cycle_start":
            s.current_cycle_id = cycle_id
            s.cycle_started_ts = ts
            s.current_run_id = None
            s.current_run_mode = None
            s.run_started_ts = None
        elif ev.event == "llm_started":
            run_id = ev.extra.get("run_id")
            if isinstance(run_id, str):
                s.current_run_id = run_id
                mode = ev.extra.get("mode")
                s.current_run_mode = mode if isinstance(mode, str) else None
                s.run_started_ts = ts
        elif ev.event == "llm_exited":
            s.current_run_id = None
            s.current_run_mode = None
            s.run_started_ts = None
        elif ev.event == "cycle_end":
            ec = ev.extra.get("exit_code")
            s.last_outcome = "ok" if ec == 0 else "fail"
            s.last_outcome_ts = ev.ts
            s.current_cycle_id = None
            s.cycle_started_ts = None
            s.current_run_id = None
            s.current_run_mode = None
            s.run_started_ts = None
        elif ev.event == "cycle_skip":
            s.last_outcome = "skip"
            s.last_outcome_ts = ev.ts
            s.current_cycle_id = None
            s.cycle_started_ts = None
            s.current_run_id = None
            s.current_run_mode = None
            s.run_started_ts = None

        self.query_one("#status", StatusRow).update_role(s)

    def _refresh_counters(self) -> None:
        cycles = list(reconstruct(iter(self._events)))
        self.query_one("#counters", CountersPane).update_counts(cycles)

    def _tick_status_row(self) -> None:
        # Re-render so elapsed seconds advance for live cycles even when no
        # new events arrive.
        self.query_one("#status", StatusRow).refresh_view()

    def action_clear_ticker(self) -> None:
        self.query_one("#ticker", EventTicker).clear()

    def action_toggle_pause(self) -> None:
        self.query_one("#ticker", EventTicker).toggle_pause()

    def action_help(self) -> None:
        self.notify(
            "q quit · c clear ticker · p pause/resume · ? help",
            timeout=5,
        )

    async def on_unmount(self) -> None:
        self._stop_event.set()
        if self._worker is not None:
            self._worker.join(timeout=1.0)


def run_watch(
    path: Path, *, from_start: bool = False, poll_interval_s: float = 0.1
) -> int:
    """Open the watch UI on ``path``. Returns the app's exit code (0 on quit)."""
    app = WatchApp(path, from_start=from_start, poll_interval_s=poll_interval_s)
    app.run()
    return 0
