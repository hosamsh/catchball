#!/usr/bin/env bash
set -euo pipefail

WORKER=""
REVIEWER=""
MAX_RETRIES=2
TASKS_DIR="./tasks"
RESULTS_DIR="./results"
REVIEWS_DIR="./reviews"
PROJECT_CONTEXT=""

invoke_claude() {
  claude -p "$1" 2>/dev/null
}

invoke_codex() {
  codex exec --quiet "$1" 2>/dev/null
}

invoke_copilot() {
  gh copilot suggest -t shell "$1" 2>/dev/null
}

invoke() {
  local tool="$1"
  local prompt="$2"

  case "$tool" in
    claude)  invoke_claude "$prompt" ;;
    codex)   invoke_codex "$prompt" ;;
    copilot) invoke_copilot "$prompt" ;;
    *)
      printf '%s\n' "ERROR: Unknown tool '$tool'. Supported: claude, codex, copilot" >&2
      exit 1
      ;;
  esac
}

build_worker_prompt() {
  local task_content="$1"
  local attempt="$2"
  local feedback="$3"

  cat <<PROMPT
Implement this task in the current repository.

## Task
${task_content}

## Rules
- Make the requested changes.
- When you finish, give a short summary of what changed, which files changed, and any concerns.
- Be concise. No preamble.
PROMPT

  if [ "$attempt" -gt 1 ] && [ -n "$feedback" ]; then
    cat <<RETRY

## Previous review feedback
${feedback}

Fix those issues.
RETRY
  fi
}

build_reviewer_prompt() {
  local task_content="$1"
  local worker_output="$2"

  cat <<PROMPT
Review this implementation against the task.

## Original task
${task_content}

## Worker summary
${worker_output}

## Your job
1. Check the repository files to verify the work.
2. Decide whether the task requirements were met.
3. Respond with EXACTLY this format. The first line must be the verdict:

PASS
<one-line summary>

OR

FAIL
<specific issues to fix>
PROMPT
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --worker)    WORKER="$2";      shift 2 ;;
    --reviewer)  REVIEWER="$2";    shift 2 ;;
    --retries)   MAX_RETRIES="$2"; shift 2 ;;
    --tasks)     TASKS_DIR="$2";   shift 2 ;;
    --help|-h)
      printf '%s\n' "Usage: ./run.sh --worker <tool> --reviewer <tool> [--retries N] [--tasks dir]"
      printf '%s\n' "Tools: claude, codex, copilot"
      exit 0
      ;;
    *) printf '%s\n' "Unknown option: $1"; exit 1 ;;
  esac
done

if [ -z "$WORKER" ] || [ -z "$REVIEWER" ]; then
  printf '%s\n' "ERROR: Both --worker and --reviewer are required."
  printf '%s\n' "Usage: ./run.sh --worker claude --reviewer codex"
  exit 1
fi

mkdir -p "$RESULTS_DIR" "$REVIEWS_DIR"

TASKS=($(find "$TASKS_DIR" -name '*.md' -type f | sort))

if [ ${#TASKS[@]} -eq 0 ]; then
  printf '%s\n' "No .md files found in $TASKS_DIR/"
  exit 1
fi

printf '%s\n' "╔══════════════════════════════════════════════════════════╗"
printf '%s\n' "║  agent-loop                                             ║"
printf '%s\n' "╠══════════════════════════════════════════════════════════╣"
printf "║  Worker:    %-43s ║\n" "$WORKER"
printf "║  Reviewer:  %-43s ║\n" "$REVIEWER"
printf "║  Tasks:     %-43s ║\n" "${#TASKS[@]} found"
printf "║  Retries:   %-43s ║\n" "$MAX_RETRIES per task"
printf '%s\n' "╚══════════════════════════════════════════════════════════╝"
printf '\n'

PASSED=0
FAILED=0
SKIPPED=0

for task_file in "${TASKS[@]}"; do
  task_name=$(basename "$task_file" .md)
  task_content=$(cat "$task_file")

  printf '%s\n' "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  printf '%s\n' "▶ Task: $task_name"
  printf '%s\n' "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  attempt=1
  feedback=""
  task_passed=false

  while [ "$attempt" -le $((MAX_RETRIES + 1)) ]; do
    printf '\n'
    printf '%s\n' "  ┌─ Attempt $attempt/$((MAX_RETRIES + 1))"
    printf '%s\n' "  │"
    printf '%s\n' "  ├─ 🔨 Sending to $WORKER..."
    worker_prompt=$(build_worker_prompt "$task_content" "$attempt" "$feedback")

    worker_output=$(invoke "$WORKER" "$worker_prompt") || {
      printf '%s\n' "  ├─ ❌ $WORKER did not run"
      attempt=$((attempt + 1))
      continue
    }
    printf '%s\n' "$worker_output" > "$RESULTS_DIR/${task_name}.attempt-${attempt}.md"
    printf '%s\n' "  ├─ 📝 Saved output to $RESULTS_DIR/${task_name}.attempt-${attempt}.md"
    printf '%s\n' "  ├─ 🔍 Sending to $REVIEWER..."
    reviewer_prompt=$(build_reviewer_prompt "$task_content" "$worker_output")

    review_output=$(invoke "$REVIEWER" "$reviewer_prompt") || {
      printf '%s\n' "  ├─ ❌ $REVIEWER did not run"
      attempt=$((attempt + 1))
      continue
    }
    printf '%s\n' "$review_output" > "$REVIEWS_DIR/${task_name}.attempt-${attempt}.md"
    verdict=$(printf '%s\n' "$review_output" | grep -m1 -E '^(PASS|FAIL)' || printf '%s\n' "UNCLEAR")

    if [[ "$verdict" == PASS* ]]; then
      printf '%s\n' "  ├─ ✅ PASSED"
      task_passed=true
      break
    elif [[ "$verdict" == FAIL* ]]; then
      feedback=$(printf '%s\n' "$review_output" | tail -n +2)
      printf '%s\n' "  ├─ ❌ FAILED"
      printf '%s\n' "  ├─ 💬 Feedback: $(printf '%s\n' "$feedback" | head -3)"

      if [ "$attempt" -le "$MAX_RETRIES" ]; then
        printf '%s\n' "  ├─ 🔄 Retrying..."
      fi
    else
      printf '%s\n' "  ├─ ⚠️  Unclear verdict, treating it as FAIL"
      feedback="The review result was unclear. Re-check the implementation."
    fi

    printf '%s\n' "  │"
    attempt=$((attempt + 1))
  done

  if $task_passed; then
    printf '%s\n' "  └─ ✅ $task_name: PASSED"
    PASSED=$((PASSED + 1))
  else
    printf '%s\n' "  └─ ❌ $task_name: FAILED after $MAX_RETRIES retries (needs manual review)"
    FAILED=$((FAILED + 1))
  fi

  printf '\n'
done

printf '\n'
printf '%s\n' "╔══════════════════════════════════════════════════════════╗"
printf '%s\n' "║  Summary                                                ║"
printf '%s\n' "╠══════════════════════════════════════════════════════════╣"
printf "║  ✅ Passed:  %-42s ║\n" "$PASSED"
printf "║  ❌ Failed:  %-42s ║\n" "$FAILED"
printf "║  Total:     %-43s ║\n" "${#TASKS[@]}"
printf '%s\n' "╠══════════════════════════════════════════════════════════╣"
printf '%s\n' "║  Results:  $RESULTS_DIR/                                ║"
printf '%s\n' "║  Reviews:  $REVIEWS_DIR/                                ║"
printf '%s\n' "╚══════════════════════════════════════════════════════════╝"

if [ "$FAILED" -gt 0 ]; then
  printf '\n'
  printf '%s\n' "Tasks needing manual review:"
  for task_file in "${TASKS[@]}"; do
    task_name=$(basename "$task_file" .md)
    last_review="$REVIEWS_DIR/${task_name}.attempt-$((MAX_RETRIES + 1)).md"
    if [ -f "$last_review" ]; then
      first_line=$(head -1 "$last_review")
      if [[ "$first_line" != PASS* ]]; then
        printf '%s\n' "  • $task_name — see $last_review"
      fi
    fi
  done
fi

exit $FAILED
