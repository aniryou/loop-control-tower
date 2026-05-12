#!/usr/bin/env bash
# Open a 3-pane tmux dashboard for the loop event stream:
#
#   ┌─────────────┬──────────┐
#   │   pulse     │  stats   │
#   ├─────────────┴──────────┤
#   │         ticker         │
#   └────────────────────────┘
#
# Each pane is an independent `python -m control_tower watch --view=…` process
# tailing the same NDJSON log. There is no cross-pane state — drill-down
# popups (issue loop-control-tower-255) will fill that role.
#
# Usage:
#   scripts/dashboard.sh [path-to-event-log]
#
# Default log path mirrors control_tower.events.default_event_log_path():
# $LOOP_EVENT_LOG, else /tmp/loop-events-${SESSION:-default}.jsonl.
#
# Re-running with an existing $CTT_SESSION tmux session just re-attaches.

set -euo pipefail

LOG="${1:-${LOOP_EVENT_LOG:-/tmp/loop-events-${SESSION:-default}.jsonl}}"
SESSION_NAME="${CTT_SESSION:-ctt}"

if [[ -n "${TMUX:-}" ]]; then
  echo "error: already inside a tmux session; run from a plain shell" >&2
  exit 2
fi

if ! command -v tmux >/dev/null 2>&1; then
  echo "error: tmux is required but not found on PATH" >&2
  exit 2
fi

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "attaching to existing tmux session: $SESSION_NAME" >&2
  exec tmux attach -t "$SESSION_NAME"
fi

PY="${PYTHON:-python}"
WATCH_CMD="$PY -m control_tower watch"

tmux new-session -d -s "$SESSION_NAME" \
  "$WATCH_CMD --view=pulse '$LOG'"
tmux split-window -t "$SESSION_NAME":0 -v -p 60 \
  "$WATCH_CMD --view=ticker '$LOG'"
tmux select-pane -t "$SESSION_NAME":0.0
tmux split-window -t "$SESSION_NAME":0 -h -p 40 \
  "$WATCH_CMD --view=stats '$LOG'"
tmux select-pane -t "$SESSION_NAME":0.0
exec tmux attach -t "$SESSION_NAME"
