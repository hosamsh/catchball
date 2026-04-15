# catchball
Mix Claude, Codex, and Copilot instead of betting everything on one coding agent.

Run catchball over your tasklist. One agent implements, another reviews.

```bash
uv run catchball --worker claude --reviewer codex

uv run catchball --worker copilot --reviewer claude --worker-model gpt-5.4 --reviewer-model opus

```

## How it works
Tasks live by default in `./tasks`. Name them in execution order, like:

- `010-setup.md`
- `020-build.md`
- `030-tests.md`

catchball stays strictly sequential inside one task list. The worker does the task. The reviewer checks it. If the reviewer leaves no active review file, the task passes. If the reviewer raises issues, the task goes back for fixes. Each run gets its own logs and artifacts.

## Install prerequisites

- Python 3.11+: https://www.python.org/downloads/
- `uv`: https://docs.astral.sh/uv/getting-started/installation/
- At least one coding agent cli on your `PATH`
- Claude Code: https://docs.anthropic.com/en/docs/claude-code/overview
- OpenAI Codex: https://developers.openai.com/codex/
- GitHub Copilot: https://docs.github.com/en/copilot

## FAQ

### Can I point it at another task folder?

Yep.

```bash
uv run catchball --worker claude --reviewer codex --tasks ./packages/api/tasks
```

### Can I start from task 020 or 030?

Yep. Use `--from`.

```bash
uv run catchball --worker claude --reviewer codex --from 020-build.md
```

### Can I provide custom instruction files for the agents?

Yep.

```bash
uv run catchball --worker claude --reviewer codex --worker-instructions ./WORKER.md --reviewer-instructions ./REVIEWER.md
```

If `WORKER.md` and `REVIEWER.md` already exist at repo root, catchball picks them up automatically.

### How does the review round work?

Worker runs first. Reviewer runs after that. If the reviewer creates no active `.review` file, the task passes. If it creates a non-empty one under `reviews/`, the next worker round becomes a fix round. Older issue sets get archived as `.review.done.`.

### How can I change the number of review rounds?

Use `--review-passes`. Default is `3`.

```bash
uv run catchball --worker claude --reviewer codex --review-passes 5
```

### Can I set the effort level?

Yep, if the selected tools support it.

```bash
uv run catchball --worker claude --reviewer codex --worker-effort high --reviewer-effort medium
```

### What does `--allow-dirty-worktree` do?

By default catchball wants a clean git worktree before it starts. This flag skips that check and lets you run against uncommitted changes.

### What are these lock files?

`*.md.lock` means a task is currently claimed by a catchball run. A fresh lock stops the run at that task. Stale lock are cleared after the timeout window.

### Can I run multiple ranges in parallel if there are no dependencies?

That is on you.

If task `010` and task `050` really do not depend on each other, split them into separate task folders or run from different starting points in separate terminals. catchball itself stays strictly sequential inside one task list.

### Where do the logs go now?

Inside the run folder:

- `<run-id>.log`
- `worker-output/`
- `reviewer-output/`
- `reviews/`

### Can I keep rerunning the same task list?

Yep. `.done` files are the skip marker. For a real fresh regeneration, remove the `*.md.done` sidecars first.

### Can I keep one exact output folder instead of getting a fresh timestamped one every time?

Yep.

```bash
uv run catchball --worker claude --reviewer codex --state-dir ./tmp/catchball-run
```

### Can I use the legacy bash runner?

Advise not. `legacy/` is there for historical reference. Use the Python runner.
