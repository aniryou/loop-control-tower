"""Validate .loop/loop.config carries the keys loop's framework expects.

The framework shell-sources this file, so we parse it as KEY="value" pairs
(comments and blank lines ignored). Missing or empty required keys would
cause `st dev` / `st review` to fail at runtime — catching that in CI is
cheaper than catching it during a multi-agent run.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

CONFIG_PATH = Path(__file__).resolve().parent.parent / ".loop" / "loop.config"

REQUIRED_KEYS = [
    "REPO_OWNER",
    "REPO_NAME",
    "DEFAULT_BRANCH",
    "BRANCH_PREFIX",
    "WORKTREE_BASE",
    "LOCK_DIR",
    "TEST_CMD",
    "SEVERITY_LABEL_HIGH",
    "SEVERITY_LABEL_MEDIUM",
    "SEVERITY_LABEL_LOW",
    "AGENT_PICKUP_LABEL",
    "BLOCKED_HUMAN_LABEL",
]

ASSIGN_RE = re.compile(r'^([A-Z_][A-Z0-9_]*)=(.*)$')


def _parse_config(text: str) -> dict[str, str]:
    """Parse flat KEY="value" lines. Strips matching surrounding quotes."""
    result: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = ASSIGN_RE.match(line)
        if not m:
            continue
        key, value = m.group(1), m.group(2).strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        result[key] = value
    return result


@pytest.fixture(scope="module")
def config() -> dict[str, str]:
    assert CONFIG_PATH.exists(), f"missing {CONFIG_PATH}"
    return _parse_config(CONFIG_PATH.read_text())


@pytest.mark.parametrize("key", REQUIRED_KEYS)
def test_required_key_present_and_nonempty(config: dict[str, str], key: str) -> None:
    assert key in config, f"{key} missing from .loop/loop.config"
    assert config[key] != "", f"{key} is empty in .loop/loop.config"


def test_branch_prefix_is_dev_agent(config: dict[str, str]) -> None:
    # The reviewer orchestrator filters PRs by this prefix; renaming it
    # silently would cause every dev-agent PR to be skipped.
    assert config["BRANCH_PREFIX"] == "dev-agent"


def test_worktree_base_outside_repo(config: dict[str, str]) -> None:
    repo_root = str(CONFIG_PATH.resolve().parent.parent)
    base = config["WORKTREE_BASE"]
    # Substitute the two interpolations the framework resolves at runtime.
    resolved = base.replace("${REPO_OWNER}", config["REPO_OWNER"]).replace(
        "${REPO_NAME}", config["REPO_NAME"]
    )
    assert not resolved.startswith(repo_root), (
        f"WORKTREE_BASE {resolved!r} must live outside the repo "
        f"({repo_root}) to avoid git tracking worktrees"
    )
