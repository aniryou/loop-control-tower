"""Typed reader for loop's NDJSON event log.

Loop's framework (aniryou/loop) appends one JSON event per line to
``${LOOP_EVENT_LOG:-/tmp/loop-events-${SESSION:-default}.jsonl}``. This module
turns those lines into ``Event`` records (or ``ParseError``s for malformed
input), and offers a ``tail -f``-style follower so downstream code can stream
events as they're produced.

Schema contract: ``aniryou/loop`` ``docs/event-schema.md``. The required
fields are ``ts``, ``session``, ``repo``, ``role``, ``event``, and
``schema_version``. Anything else flows into ``Event.extra`` so this module
doesn't need a release every time loop adds a new event field.

Stdlib only — no third-party deps.
"""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SUPPORTED_SCHEMA_VERSION = 2

_REQUIRED_FIELDS: tuple[str, ...] = (
    "ts",
    "session",
    "repo",
    "role",
    "event",
    "schema_version",
)

_STRING_FIELDS: tuple[str, ...] = ("ts", "session", "repo", "role", "event")


@dataclass(frozen=True)
class Event:
    """One parsed event log entry."""

    ts: str
    session: str
    repo: str
    role: str
    event: str
    schema_version: int
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ParseError:
    """A line that could not be parsed into an :class:`Event`."""

    raw: str
    reason: str
    detail: str | None = None


def parse_event(line: str) -> Event | ParseError:
    """Parse one NDJSON line into an :class:`Event` or :class:`ParseError`.

    Trailing newlines are stripped. Empty / whitespace-only lines are
    reported as ``invalid_json`` rather than silently dropped — callers that
    want to skip blanks can filter on ``isinstance(result, ParseError)``.
    Unknown event names are forward-compatible (returned as ``Event``).
    """
    stripped = line.rstrip("\r\n")
    if not stripped.strip():
        return ParseError(raw=line, reason="invalid_json", detail=None)

    try:
        data = json.loads(stripped)
    except json.JSONDecodeError as exc:
        return ParseError(raw=line, reason="invalid_json", detail=str(exc))

    if not isinstance(data, dict):
        return ParseError(
            raw=line,
            reason="invalid_json",
            detail="top-level value is not an object",
        )

    for required in _REQUIRED_FIELDS:
        if required not in data:
            return ParseError(
                raw=line, reason="missing_required_field", detail=required
            )

    for field_name in _STRING_FIELDS:
        if not isinstance(data[field_name], str):
            return ParseError(
                raw=line,
                reason="wrong_type",
                detail=f"{field_name} must be string",
            )

    schema_version = data["schema_version"]
    # bool is an int subclass; reject booleans masquerading as ints.
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        return ParseError(
            raw=line,
            reason="wrong_type",
            detail="schema_version must be int",
        )

    if schema_version != SUPPORTED_SCHEMA_VERSION:
        return ParseError(
            raw=line,
            reason="unsupported_schema_version",
            detail=str(schema_version),
        )

    extra = {k: v for k, v in data.items() if k not in _REQUIRED_FIELDS}
    return Event(
        ts=data["ts"],
        session=data["session"],
        repo=data["repo"],
        role=data["role"],
        event=data["event"],
        schema_version=schema_version,
        extra=extra,
    )


def read_file(path: Path) -> Iterator[Event | ParseError]:
    """Yield events from a static file from byte 0.

    Yields nothing (raises nothing) if the file does not exist. A trailing
    partial line — no final newline — is parsed and yielded too, which means
    a malformed tail surfaces as a :class:`ParseError`. Callers that want
    follow-the-tail semantics should use :func:`tail_file`.
    """
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for raw_line in fh:
            yield parse_event(raw_line)


def tail_file(
    path: Path,
    *,
    from_start: bool = False,
    poll_interval_s: float = 0.1,
    stop: threading.Event | None = None,
) -> Iterator[Event | ParseError]:
    """Follow ``path`` like ``tail -f``, yielding events as they arrive.

    Parameters
    ----------
    path
        File to follow. May not yet exist — the iterator polls until it does.
    from_start
        If ``True``, replay the existing file contents before following new
        appends. Defaults to ``False`` (start at end of file).
    poll_interval_s
        How long to sleep between reads at EOF / between existence checks.
        The spec is: appended lines surface within ``poll_interval_s * 2``;
        ``stop.set()`` ends the iterator within ``poll_interval_s * 2``.
    stop
        Optional event used to terminate the loop cleanly. If ``None``, a
        fresh internal event is created (which the caller cannot trigger,
        meaning the loop runs until the iterator is garbage-collected; pass
        an explicit event for any non-toy use).

    Partial lines (writes that span multiple flushes) are buffered until a
    newline arrives — the spec rules out yielding two ``ParseError``s for
    one logical event split across two writes.
    """
    if stop is None:
        stop = threading.Event()

    file_existed_at_start = path.exists()
    while not path.exists():
        if stop.wait(poll_interval_s):
            return

    with path.open("r", encoding="utf-8") as fh:
        # If the file existed when we started, the user explicitly opted out
        # of replay (from_start=False) — seek past the historical content.
        # If the file appeared mid-flight, every byte in it is "new" relative
        # to when we started watching, so always read from byte 0.
        if not from_start and file_existed_at_start:
            fh.seek(0, os.SEEK_END)

        buffer = ""
        while not stop.is_set():
            chunk = fh.read()
            if chunk:
                buffer += chunk
                while "\n" in buffer:
                    line, _, buffer = buffer.partition("\n")
                    yield parse_event(line)
                continue
            if stop.wait(poll_interval_s):
                return


def default_event_log_path(session: str | None = None) -> Path:
    """Resolve the event-log path the producer would write to.

    Mirrors loop's ``runners/lib/event_log.sh`` resolution:
    ``$LOOP_EVENT_LOG`` wins if set; otherwise
    ``/tmp/loop-events-{session or $SESSION or "default"}.jsonl``.
    """
    override = os.environ.get("LOOP_EVENT_LOG")
    if override:
        return Path(override)
    name = session if session is not None else os.environ.get("SESSION", "default")
    return Path(f"/tmp/loop-events-{name}.jsonl")
