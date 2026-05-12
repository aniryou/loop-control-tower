"""Tests for control_tower.events: NDJSON event log parser, reader, follower.

The producer side (loop's runners/lib/event_log.sh) writes one JSON line per
event to ${LOOP_EVENT_LOG:-/tmp/loop-events-${SESSION:-default}.jsonl}; this
suite pins the consumer's contract — parse, read, follow — so later modules
(cycle stats, dispatch queries, the live dashboard) inherit a stable surface.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from control_tower.events import (
    SUPPORTED_SCHEMA_VERSION,
    Event,
    ParseError,
    default_event_log_path,
    parse_event,
    read_file,
    tail_file,
)


def _valid_fields(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "ts": "2026-05-10T10:00:00+00:00",
        "session": "s",
        "repo": "o/r",
        "role": "dev-1",
        "event": "cycle_start",
        "schema_version": 2,
    }
    base.update(overrides)
    return base


# --- parse_event ----------------------------------------------------------


def test_supported_schema_version_constant() -> None:
    assert SUPPORTED_SCHEMA_VERSION == 2


def test_parse_event_minimal_valid() -> None:
    line = (
        '{"ts":"2026-05-10T10:00:00+00:00","session":"s","repo":"o/r",'
        '"role":"dev-1","event":"cycle_start","schema_version":2,'
        '"cycle_id":"123-1"}'
    )
    result = parse_event(line)
    assert isinstance(result, Event)
    assert result.event == "cycle_start"
    assert result.schema_version == 2
    assert result.session == "s"
    assert result.repo == "o/r"
    assert result.role == "dev-1"
    assert result.ts == "2026-05-10T10:00:00+00:00"
    assert result.extra == {"cycle_id": "123-1"}


def test_parse_event_cycle_end_with_extras() -> None:
    line = json.dumps(
        _valid_fields(
            event="cycle_end",
            cycle_id="123-1",
            exit_code=0,
            duration_s=12.5,
            dispatched=True,
        )
    )
    result = parse_event(line)
    assert isinstance(result, Event)
    assert result.event == "cycle_end"
    assert result.extra == {
        "cycle_id": "123-1",
        "exit_code": 0,
        "duration_s": 12.5,
        "dispatched": True,
    }


def test_parse_event_strips_trailing_newline() -> None:
    line = json.dumps(_valid_fields()) + "\n"
    result = parse_event(line)
    assert isinstance(result, Event)


@pytest.mark.parametrize(
    "missing", ["ts", "session", "repo", "role", "event", "schema_version"]
)
def test_parse_event_missing_required_field(missing: str) -> None:
    fields = _valid_fields()
    fields.pop(missing)
    result = parse_event(json.dumps(fields))
    assert isinstance(result, ParseError)
    assert result.reason == "missing_required_field"
    assert result.detail == missing


def test_parse_event_invalid_json() -> None:
    result = parse_event("not json")
    assert isinstance(result, ParseError)
    assert result.reason == "invalid_json"


def test_parse_event_empty_string() -> None:
    result = parse_event("")
    assert isinstance(result, ParseError)
    assert result.reason == "invalid_json"


def test_parse_event_whitespace_only() -> None:
    result = parse_event("  \n")
    assert isinstance(result, ParseError)
    assert result.reason == "invalid_json"


def test_parse_event_json_array_is_invalid() -> None:
    # A JSON array parses but isn't an event object — treat as invalid_json.
    result = parse_event("[1,2,3]")
    assert isinstance(result, ParseError)
    assert result.reason == "invalid_json"


def test_parse_event_unsupported_schema_version() -> None:
    line = json.dumps(_valid_fields(schema_version=99))
    result = parse_event(line)
    assert isinstance(result, ParseError)
    assert result.reason == "unsupported_schema_version"
    assert result.detail == "99"


def test_parse_event_wrong_type_schema_version_string() -> None:
    line = json.dumps(_valid_fields(schema_version="2"))
    result = parse_event(line)
    assert isinstance(result, ParseError)
    assert result.reason == "wrong_type"


def test_parse_event_wrong_type_event_int() -> None:
    line = json.dumps(_valid_fields(event=42))
    result = parse_event(line)
    assert isinstance(result, ParseError)
    assert result.reason == "wrong_type"


def test_parse_event_unknown_event_name_is_forward_compat() -> None:
    line = json.dumps(
        _valid_fields(event="future_event_v9", new_field=42)
    )
    result = parse_event(line)
    assert isinstance(result, Event)
    assert result.event == "future_event_v9"
    assert result.extra == {"new_field": 42}


def test_parse_event_extras_preserved() -> None:
    line = json.dumps(
        _valid_fields(extra_str="hello", extra_list=[1, 2], extra_obj={"k": "v"})
    )
    result = parse_event(line)
    assert isinstance(result, Event)
    assert result.extra == {
        "extra_str": "hello",
        "extra_list": [1, 2],
        "extra_obj": {"k": "v"},
    }


# --- read_file ------------------------------------------------------------


def test_read_file_missing_yields_empty(tmp_path: Path) -> None:
    results = list(read_file(tmp_path / "does_not_exist.jsonl"))
    assert results == []


def test_read_file_three_lines_in_order(tmp_path: Path) -> None:
    p = tmp_path / "events.jsonl"
    lines = [
        json.dumps(_valid_fields(event=f"e{i}")) for i in range(3)
    ]
    p.write_text("\n".join(lines) + "\n")
    results = list(read_file(p))
    assert len(results) == 3
    assert all(isinstance(r, Event) for r in results)
    assert [r.event for r in results if isinstance(r, Event)] == [
        "e0",
        "e1",
        "e2",
    ]


def test_read_file_mixed_valid_and_malformed(tmp_path: Path) -> None:
    p = tmp_path / "events.jsonl"
    valid = json.dumps(_valid_fields(event="ok"))
    p.write_text(f"{valid}\nnot json\n{valid}\n{valid}\n{valid}\n")
    results = list(read_file(p))
    assert len(results) == 5
    assert isinstance(results[1], ParseError)
    assert results[1].reason == "invalid_json"
    assert all(isinstance(results[i], Event) for i in (0, 2, 3, 4))


def test_read_file_partial_trailing_line_yields_parse_error(
    tmp_path: Path,
) -> None:
    p = tmp_path / "events.jsonl"
    valid = json.dumps(_valid_fields(event="ok"))
    p.write_text(f'{valid}\n{{"ts":')
    results = list(read_file(p))
    assert len(results) == 2
    assert isinstance(results[0], Event)
    assert isinstance(results[1], ParseError)


# --- tail_file ------------------------------------------------------------


def _consume_in_thread(
    p: Path,
    *,
    from_start: bool = False,
    poll_interval_s: float = 0.01,
) -> tuple[threading.Thread, threading.Event, list[Event | ParseError]]:
    stop = threading.Event()
    received: list[Event | ParseError] = []

    def consumer() -> None:
        for ev in tail_file(
            p, from_start=from_start, poll_interval_s=poll_interval_s, stop=stop
        ):
            received.append(ev)

    t = threading.Thread(target=consumer, daemon=True)
    t.start()
    return t, stop, received


def _wait_until(
    predicate: object, timeout_s: float = 2.0, poll_s: float = 0.005
) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():  # type: ignore[operator]
            return True
        time.sleep(poll_s)
    return False


def test_tail_file_yields_appended_lines(tmp_path: Path) -> None:
    p = tmp_path / "events.jsonl"
    p.write_text("")

    t, stop, received = _consume_in_thread(p)
    time.sleep(0.05)

    line_a = json.dumps(_valid_fields(event="a"))
    line_b = json.dumps(_valid_fields(event="b"))
    with p.open("a") as f:
        f.write(line_a + "\n")
        f.flush()
    with p.open("a") as f:
        f.write(line_b + "\n")
        f.flush()

    assert _wait_until(lambda: len(received) >= 2)
    stop.set()
    t.join(timeout=1.0)

    assert not t.is_alive()
    assert len(received) == 2
    assert isinstance(received[0], Event) and received[0].event == "a"
    assert isinstance(received[1], Event) and received[1].event == "b"


def test_tail_file_from_start_replays_then_follows(tmp_path: Path) -> None:
    p = tmp_path / "events.jsonl"
    pre = json.dumps(_valid_fields(event="pre"))
    p.write_text(pre + "\n")

    t, stop, received = _consume_in_thread(p, from_start=True)
    time.sleep(0.05)

    new1 = json.dumps(_valid_fields(event="new1"))
    new2 = json.dumps(_valid_fields(event="new2"))
    with p.open("a") as f:
        f.write(new1 + "\n")
        f.write(new2 + "\n")
        f.flush()

    assert _wait_until(lambda: len(received) >= 3)
    stop.set()
    t.join(timeout=1.0)

    assert [r.event for r in received if isinstance(r, Event)] == [
        "pre",
        "new1",
        "new2",
    ]


def test_tail_file_default_skips_existing_history(tmp_path: Path) -> None:
    p = tmp_path / "events.jsonl"
    pre = json.dumps(_valid_fields(event="pre"))
    p.write_text(pre + "\n")

    t, stop, received = _consume_in_thread(p, from_start=False)
    time.sleep(0.1)

    new1 = json.dumps(_valid_fields(event="after"))
    with p.open("a") as f:
        f.write(new1 + "\n")
        f.flush()

    assert _wait_until(lambda: len(received) >= 1)
    stop.set()
    t.join(timeout=1.0)

    assert [r.event for r in received if isinstance(r, Event)] == ["after"]


def test_tail_file_file_created_mid_flight(tmp_path: Path) -> None:
    p = tmp_path / "delayed.jsonl"

    t, stop, received = _consume_in_thread(p)
    time.sleep(0.05)
    assert not p.exists()

    line = json.dumps(_valid_fields(event="later"))
    p.write_text(line + "\n")

    assert _wait_until(lambda: len(received) >= 1)
    stop.set()
    t.join(timeout=1.0)

    assert len(received) == 1
    assert isinstance(received[0], Event)
    assert received[0].event == "later"


def test_tail_file_handles_partial_line(tmp_path: Path) -> None:
    p = tmp_path / "events.jsonl"
    p.write_text("")

    t, stop, received = _consume_in_thread(p)
    time.sleep(0.05)

    line = json.dumps(_valid_fields(event="split"))
    halfway = len(line) // 2
    with p.open("a") as f:
        f.write(line[:halfway])
        f.flush()

    # Hold for a few poll cycles to ensure the consumer doesn't yield a
    # ParseError on the partial buffer.
    time.sleep(0.05)
    assert received == []

    with p.open("a") as f:
        f.write(line[halfway:] + "\n")
        f.flush()

    assert _wait_until(lambda: len(received) >= 1)
    stop.set()
    t.join(timeout=1.0)

    assert len(received) == 1
    assert isinstance(received[0], Event)
    assert received[0].event == "split"


def test_tail_file_stop_ends_iterator_quickly(tmp_path: Path) -> None:
    p = tmp_path / "events.jsonl"
    p.write_text("")

    poll = 0.05
    t, stop, _received = _consume_in_thread(p, poll_interval_s=poll)
    time.sleep(0.1)
    assert t.is_alive()

    stop.set()
    # Spec: stop ends the iterator within poll_interval_s * 2; allow a margin.
    t.join(timeout=poll * 4 + 0.5)
    assert not t.is_alive()


def test_tail_file_stop_ends_wait_for_missing_file(tmp_path: Path) -> None:
    # File never appears; stop must still wake the polling loop.
    p = tmp_path / "never.jsonl"
    poll = 0.05
    t, stop, _received = _consume_in_thread(p, poll_interval_s=poll)
    time.sleep(0.1)
    assert t.is_alive()
    stop.set()
    t.join(timeout=poll * 4 + 0.5)
    assert not t.is_alive()


# --- default_event_log_path ----------------------------------------------


def test_default_event_log_path_loop_event_log_wins(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    custom = tmp_path / "custom.jsonl"
    monkeypatch.setenv("LOOP_EVENT_LOG", str(custom))
    monkeypatch.setenv("SESSION", "ignored")
    assert default_event_log_path() == custom


def test_default_event_log_path_session_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOP_EVENT_LOG", raising=False)
    monkeypatch.setenv("SESSION", "foo")
    assert default_event_log_path() == Path("/tmp/loop-events-foo.jsonl")


def test_default_event_log_path_neither_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOP_EVENT_LOG", raising=False)
    monkeypatch.delenv("SESSION", raising=False)
    assert default_event_log_path() == Path("/tmp/loop-events-default.jsonl")


def test_default_event_log_path_explicit_session_arg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOP_EVENT_LOG", raising=False)
    monkeypatch.setenv("SESSION", "from-env")
    # Explicit arg beats env SESSION but still loses to LOOP_EVENT_LOG.
    assert default_event_log_path("explicit") == Path(
        "/tmp/loop-events-explicit.jsonl"
    )


def test_default_event_log_path_explicit_session_loses_to_loop_event_log(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    custom = tmp_path / "win.jsonl"
    monkeypatch.setenv("LOOP_EVENT_LOG", str(custom))
    assert default_event_log_path("explicit") == custom
