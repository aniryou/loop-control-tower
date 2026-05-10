# loop-control-tower

## Overview

`loop-control-tower` is a small CLI/TUI that reads the NDJSON event log
emitted by the [Loop](https://github.com/aniryou/loop) multi-agent
dev-loop framework and surfaces operational stats — cycle counts and
durations, lock-race rates, dispatch fires per PR, at-cap events, and
hard failures.

It is a *consumer* of Loop, not part of the framework. Loop produces the
event log; this tool reads it. Use it from anywhere on a host where the
log file is readable.

## Install

Requires Python 3.12 (the version exercised in CI). Earlier 3.x
versions may work but are untested.

```bash
pip install -r requirements.txt
```

The only runtime dependency is [Textual](https://textual.textualize.io/)
(used by `watch`); `stats` is standard-library only.

## Usage

Both subcommands accept an optional path to an NDJSON event log. If
omitted, the path falls back to `$LOOP_EVENT_LOG` if set, otherwise
`/tmp/loop-events-<session>.jsonl` where `<session>` is `$SESSION` or
`default`. This mirrors Loop's own `runners/lib/event_log.sh`
resolution, so most users can omit the argument entirely.

### `stats`

Aggregate a static event log into five fixed-column tables.

```bash
python -m control_tower stats                       # use default path
python -m control_tower stats path/to/log.jsonl     # explicit path
python -m control_tower stats --json                # one JSON object instead of tables
```

Plain-text output looks like this (rows truncated for brevity):

```
role        total  ok  skip  fail  open  median_s  p95_s
developer       4   3     0     1     0      62.1  104.7
reviewer        3   3     0     0     0      11.4   18.0

lock-race rate
role       acquired  lost   rate
developer         4     1  20.0%

dispatch fires by pr
pr    fired  skipped
#7        2        0

at-cap events by kind
kind        count
developer       1

hard failures
role  count
```

`--json` emits the same five aggregator outputs as a single JSON object
on stdout, suitable for piping to `jq` or downstream tools.

### `watch`

Open a live Textual TUI dashboard that tails the event log and refreshes
the same five tables in place.

```bash
python -m control_tower watch                       # tail from end of file
python -m control_tower watch --from-start          # replay existing file first
python -m control_tower watch --poll-interval-s 0.5 # slower poll (default: 0.1)
```

Press `q` or `Ctrl-C` to exit.
