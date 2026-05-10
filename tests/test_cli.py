"""Tests for control_tower.cli: stats subcommand reads a static log and prints
five sections (or one JSON object). Default-path resolution honours
LOOP_EVENT_LOG. Missing files are a clean exit-2.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


def _write_log(path: Path, lines: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for ev in lines:
            fh.write(json.dumps(ev) + "\n")


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


def _five_cycle_log() -> list[dict[str, Any]]:
    """5 cycles for dev-1: 3 ok + 1 fail + 1 skip."""
    out: list[dict[str, Any]] = []
    for i, dur in enumerate([10.0, 20.0, 30.0]):
        cid = f"a{i}"
        out += [
            _ev("cycle_start", cycle_id=cid),
            _ev("cycle_end", cycle_id=cid, exit_code=0, duration_s=dur),
        ]
    out += [
        _ev("cycle_start", cycle_id="f1"),
        _ev("cycle_end", cycle_id="f1", exit_code=2, duration_s=40.0),
    ]
    out += [
        _ev("cycle_start", cycle_id="s1"),
        _ev("cycle_skip", cycle_id="s1"),
    ]
    return out


def test_stats_prints_five_sections_in_fixed_order(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from control_tower.cli import main

    log = tmp_path / "events.jsonl"
    _write_log(log, _five_cycle_log())

    rc = main(["stats", str(log)])
    assert rc == 0
    out = capsys.readouterr().out

    positions = [
        out.find("role"),  # cycle_summary header (first occurrence)
        out.find("lock-race rate"),
        out.find("dispatch fires by pr"),
        out.find("at-cap events by kind"),
        out.find("hard failures"),
    ]
    assert all(p >= 0 for p in positions), f"missing section in output:\n{out}"
    assert positions == sorted(positions), f"sections out of order: {positions}\n{out}"


def test_stats_dev_1_row_has_correct_counts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from control_tower.cli import main

    log = tmp_path / "events.jsonl"
    _write_log(log, _five_cycle_log())
    main(["stats", str(log)])
    out = capsys.readouterr().out

    dev_line = next(line for line in out.splitlines() if line.startswith("dev-1"))
    cols = dev_line.split()
    # role total ok skip fail open median p95
    assert cols[0] == "dev-1"
    assert cols[1] == "5"
    assert cols[2] == "3"  # ok
    assert cols[3] == "1"  # skip
    assert cols[4] == "1"  # fail
    assert cols[5] == "0"  # open


def test_stats_empty_log_exits_zero_with_section_headers(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from control_tower.cli import main

    log = tmp_path / "empty.jsonl"
    log.write_text("")

    rc = main(["stats", str(log)])
    assert rc == 0
    out = capsys.readouterr().out
    # Section preambles still render so the user sees the schema.
    for label in (
        "role",
        "lock-race rate",
        "dispatch fires by pr",
        "at-cap events by kind",
        "hard failures",
    ):
        assert label in out


def test_stats_missing_file_exits_two_with_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from control_tower.cli import main

    missing = tmp_path / "nope.jsonl"
    rc = main(["stats", str(missing)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "log file not found" in err
    assert str(missing) in err


def test_stats_json_outputs_one_object_with_five_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from control_tower.cli import main

    log = tmp_path / "events.jsonl"
    _write_log(log, _five_cycle_log())

    rc = main(["stats", "--json", str(log)])
    assert rc == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert set(payload.keys()) == {
        "cycle_summary",
        "lock_stats",
        "dispatch_stats",
        "at_cap_stats",
        "hard_failure_stats",
    }
    assert payload["cycle_summary"]["dev-1"]["total"] == 5
    assert payload["cycle_summary"]["dev-1"]["ok"] == 3
    assert payload["cycle_summary"]["dev-1"]["fail"] == 1
    assert payload["cycle_summary"]["dev-1"]["skip"] == 1


def test_stats_default_path_resolves_via_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from control_tower.cli import main

    log = tmp_path / "events.jsonl"
    _write_log(log, _five_cycle_log())
    monkeypatch.setenv("LOOP_EVENT_LOG", str(log))
    rc = main(["stats"])  # no positional path
    assert rc == 0
    out = capsys.readouterr().out
    assert "dev-1" in out


def test_module_main_importable_without_executing() -> None:
    """Importing control_tower.__main__ must NOT execute main() (guard with __name__)."""
    import control_tower.__main__  # noqa: F401
