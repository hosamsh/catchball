#!/usr/bin/env bash
set -euo pipefail

WORKER="" REVIEWER="" MAX_RETRIES=2 TASKS_DIR="./tasks"
RESULTS_DIR="./results" REVIEWS_DIR="./reviews"

invoke_claude() { claude -p "$1" 2>/dev/null; }
invoke_codex()  { codex exec --quiet "$1" 2>/dev/null; }
invoke_copilot() {
  echo "ERROR: copilot adapter is a placeholder. Edit invoke_copilot() in run.sh with your CLI command." >&2
  return 1
}

invoke() {
  case "$1" in
    claude)  invoke_claude "$2" ;;
    codex)   invoke_codex "$2" ;;
    copilot) invoke_copilot "$2" ;;
    *) echo "Unknown tool: $1" >&2; exit 1 ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --worker)   WORKER="$2";      shift 2 ;;
    --reviewer) REVIEWER="$2";    shift 2 ;;
    --retries)  MAX_RETRIES="$2"; shift 2 ;;
    --tasks)    TASKS_DIR="$2";   shift 2 ;;
    --help|-h)  echo "Usage: ./run.sh --worker <tool> --reviewer <tool> [--retries N] [--tasks dir]"; exit 0 ;;
    *) echo "Unknown option: $1. Try --help"; exit 1 ;;
  esac
done

[[ -z "$WORKER" || -z "$REVIEWER" ]] && echo "Both --worker and --reviewer required. Try --help" && exit 1
[[ ! -d "$TASKS_DIR" ]] && echo "Tasks directory not found: $TASKS_DIR" && exit 1

mkdir -p "$RESULTS_DIR" "$REVIEWS_DIR"
TASKS=($(find "$TASKS_DIR" -name '*.md' -type f | sort))
[[ ${#TASKS[@]} -eq 0 ]] && echo "No .md files in $TASKS_DIR/" && exit 1

echo "catchball | worker: $WORKER | reviewer: $REVIEWER | tasks: ${#TASKS[@]} | retries: $MAX_RETRIES"
echo ""

PASSED=0 FAILED=0

for task_file in "${TASKS[@]}"; do
  task_name=$(basename "$task_file" .md)
  echo "▶ $task_name"

  attempt=1
  task_passed=false
  feedback_file="$REVIEWS_DIR/${task_name}.feedback.md"
  rm -f "$feedback_file"

  while [ "$attempt" -le $((MAX_RETRIES + 1)) ]; do
    echo "  attempt $attempt/$((MAX_RETRIES + 1))"

    if [ -f "$feedback_file" ]; then
      worker_prompt="Implement the task in $task_file. A previous attempt failed review — read the feedback in $feedback_file and fix the issues."
    else
      worker_prompt="Implement the task described in $task_file."
    fi

    echo "  → $WORKER working..."
    worker_output=$(invoke "$WORKER" "$worker_prompt") || { attempt=$((attempt + 1)); continue; }
    printf '%s\n' "$worker_output" > "$RESULTS_DIR/${task_name}.attempt-${attempt}.md"

    echo "  → $REVIEWER reviewing..."
    reviewer_prompt="Review the implementation against the spec in $task_file. Check the actual files. Respond with PASS or FAIL on the first line, then a one-line explanation."
    review_output=$(invoke "$REVIEWER" "$reviewer_prompt") || { attempt=$((attempt + 1)); continue; }
    printf '%s\n' "$review_output" > "$REVIEWS_DIR/${task_name}.attempt-${attempt}.md"

    verdict=$(printf '%s\n' "$review_output" | grep -m1 -E '^(PASS|FAIL)' || echo "FAIL")

    if [[ "$verdict" == PASS* ]]; then
      echo "  ✅ passed"
      task_passed=true
      break
    else
      printf '%s\n' "$review_output" | tail -n +2 > "$feedback_file"
      echo "  ❌ failed"
    fi

    attempt=$((attempt + 1))
  done

  $task_passed && PASSED=$((PASSED + 1)) || FAILED=$((FAILED + 1))
  echo ""
done

echo "done | ✅ $PASSED passed | ❌ $FAILED failed | total ${#TASKS[@]}"
exit $FAILED
