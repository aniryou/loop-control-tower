# loop-control-tower

<!-- BEGIN LOOP INTEGRATION v:1 hash:809f495b -->
## Loop integration (multi-agent dev-loop framework)

This repo is a [Loop](https://github.com/aniryou/loop) consumer. The framework
code itself lives outside this repo (typically `~/code/loop/`); only the
per-repo configuration in `.loop/loop.config` lives here. Loop drives a
multi-agent dev-loop — a developer agent claims open issues and opens PRs,
a reviewer agent reviews them, and a tmux orchestrator runs both in parallel.

### Commands

Run from anywhere inside this repo:

- `st dev` — scan issues, claim one, drive a PR end-to-end.
- `st dev follow-up <PR>` — address reviewer-agent feedback on an existing PR.
- `st dev resolve <PR>` — auto-resolve a triage-gated merge conflict.
- `st review <PR>` — review a specific open dev-agent PR.
- `st loop start` — run the full multi-agent fleet under tmux.
- `st help` — full command list.

### Coordinating with the dev agent

The dev agent owns branches under `dev-agent/*`. Do **not** push,
rebase, or amend commits on those branches by hand while a run is in flight —
the dev agent will force-push during conflict resolution and will overwrite
unsynchronised local work.

Per-issue claim locks live under `/tmp/dev-agent/your-github-org-or-user-your-repo-name/locks`. Treat that directory as
agent-owned: do not delete or edit lock dirs there. Stale locks
(older than `6` hours) are recovered automatically.

### Tunables

`.loop/loop.config` carries every per-repo override: severity labels, branch
prefix, dispatch caps, retry counts, the project test command, etc. Defaults
are documented inline. Edit values there, not in the framework checkout.
<!-- END LOOP INTEGRATION -->

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
