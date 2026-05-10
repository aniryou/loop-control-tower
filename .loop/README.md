# Loop — multi-agent dev-loop config for this repo

This directory carries the per-repo configuration for the [Loop](https://github.com/aniryou/loop)
multi-agent dev-loop framework. The framework itself lives in `$LOOP_HOME` (typically
`~/code/loop/`); this directory holds only `loop.config`, which the framework
substitutes into prompt templates at invocation time.

## Edit loop.config

Set `REPO_OWNER`, `REPO_NAME`, severity labels, branch prefix, and any
project-specific overrides. Defaults are documented inline.

## Run the agents

From anywhere inside this repo:

    st dev          # scan issues, claim one, drive a PR
    st review <PR>  # review a specific open dev-agent PR
    st loop start   # run the full multi-agent fleet in tmux
    st help         # full command list
