#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/catchball-self-check.XXXXXX")"

cleanup() {
  rm -rf "$TMP_DIR"
}

fail() {
  printf 'self-check: %s\n' "$*" >&2
  exit 1
}

assert_output_contains() {
  local output="$1"
  local expected="$2"
  [[ "$output" == *"$expected"* ]] || fail "expected output to contain: $expected"$'\n'"$output"
}

assert_output_not_contains() {
  local output="$1"
  local unexpected="$2"
  [[ "$output" != *"$unexpected"* ]] || fail "expected output to not contain: $unexpected"$'\n'"$output"
}

assert_file_contains_line() {
  local file="$1"
  local expected="$2"
  grep -Fxq -- "$expected" "$file" || fail "expected $file to contain line: $expected"
}

create_provider_stub() {
  local name="$1"
  cat > "$TMP_DIR/bin/$name" <<EOF
#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$TMP_DIR"
name="\$(basename "\$0")"
count_file="\$tmp_dir/\${name}.count"
count=1
if [[ -f "\$count_file" ]]; then
  count=\$(( \$(cat "\$count_file") + 1 ))
fi
printf '%s' "\$count" > "\$count_file"

args_file="\$tmp_dir/\${name}.\${count}.args"
: > "\$args_file"
for arg in "\$@"; do
  printf '%q\n' "\$arg" >> "\$args_file"
done

exit 0
EOF
  chmod +x "$TMP_DIR/bin/$name"
}

run_runner() {
  PATH="$TMP_DIR/bin:$PATH" bash "$ROOT_DIR/catchball" "$@"
}

reset_stub_state() {
  rm -f "$TMP_DIR"/*.args "$TMP_DIR"/*.count 2>/dev/null || true
}

trap cleanup EXIT

mkdir -p "$TMP_DIR/bin" "$TMP_DIR/tasks"
create_provider_stub claude
create_provider_stub codex
create_provider_stub copilot

printf '# One\n' > "$TMP_DIR/tasks/010-one.md"
printf '# Two\n' > "$TMP_DIR/tasks/020-two.md"
printf 'done\n' > "$TMP_DIR/tasks/020-two.md.done"

if help_output="$(run_runner --help 2>&1)"; then
  :
else
  fail "expected --help to succeed"
fi

assert_output_contains "$help_output" "Bash on Linux or WSL only"
assert_output_contains "$help_output" "Tools must be on this shell's PATH"

if invalid_output="$(run_runner --worker claude --reviewer codex --tasks "$TMP_DIR/tasks" --worker-model nope --allow-dirty-worktree 2>&1)"; then
  fail "expected unsupported worker model to fail"
fi

assert_output_contains "$invalid_output" "Unsupported model for claude: nope"

if invalid_mode_output="$(run_runner --worker codex --reviewer claude --tasks "$TMP_DIR/tasks" --worker-mode full-auto --allow-dirty-worktree 2>&1)"; then
  fail "expected unsupported worker mode to fail"
fi

assert_output_contains "$invalid_mode_output" "Unsupported mode for codex: full-auto"

runs_parent="$TMP_DIR/runs-parent"
if runs_output="$(run_runner --worker claude --reviewer codex --tasks "$TMP_DIR/tasks" --from 020-two.md --review-passes 2 --runs-dir "$runs_parent" --allow-dirty-worktree 2>&1)"; then
  :
else
  fail "timestamped runs-dir check failed"$'\n'"$runs_output"
fi

mapfile -t run_dirs < <(find "$runs_parent" -mindepth 1 -maxdepth 1 -type d | sort)
(( ${#run_dirs[@]} == 1 )) || fail "expected one timestamped run directory under $runs_parent"
run_dir="${run_dirs[0]}"
run_name="$(basename "$run_dir")"
[[ "$run_name" =~ ^[0-9]{2}-[0-9]{2}-[0-9]{2}--[0-9]{2}--[0-9]{2}--[0-9]{2}--[0-9]+$ ]] || fail "unexpected run directory name: $run_name"
find "$run_dir/runs" -mindepth 1 -maxdepth 1 -type f -name '*.log' | grep -q . || fail "expected a run log inside $run_dir/runs"

fixed_state="$TMP_DIR/fixed-state"
if state_output="$(run_runner --worker claude --reviewer codex --tasks "$TMP_DIR/tasks" --from 020-two.md --state-dir "$fixed_state" --allow-dirty-worktree 2>&1)"; then
  :
else
  fail "fixed state-dir check failed"$'\n'"$state_output"
fi

[[ -d "$fixed_state/runs" ]] || fail "expected $fixed_state/runs to exist"
[[ "$state_output" == *"catchball | run dir: $fixed_state"* ]] || fail "expected run output to show the fixed state dir"

if conflict_output="$(run_runner --worker claude --reviewer codex --tasks "$TMP_DIR/tasks" --state-dir "$fixed_state" --runs-dir "$runs_parent" --allow-dirty-worktree 2>&1)"; then
  fail "expected --state-dir and --runs-dir together to fail"
fi

[[ "$conflict_output" == *"Use either --state-dir or --runs-dir, not both"* ]] || fail "expected a state-dir/runs-dir conflict error"

ambiguous_tasks="$TMP_DIR/ambiguous-tasks"
mkdir -p "$ambiguous_tasks"
printf '# Alpha\n' > "$ambiguous_tasks/010-alpha.md"
printf '# Alpha Extra\n' > "$ambiguous_tasks/010-alpha-extra.md"

if ambiguous_output="$(run_runner --worker claude --reviewer codex --tasks "$ambiguous_tasks" --from 010-a --allow-dirty-worktree 2>&1)"; then
  fail "expected ambiguous --from to fail"
fi

assert_output_contains "$ambiguous_output" "Start task is ambiguous: 010-a"

stale_lock_tasks="$TMP_DIR/stale-lock-tasks"
mkdir -p "$stale_lock_tasks"
printf '# Locked\n' > "$stale_lock_tasks/010-lock.md"
lock_file="$stale_lock_tasks/010-lock.md.lock"
lock_host="$(hostname 2>/dev/null || printf 'unknown')"
{
  printf 'run_id=test\n'
  printf 'owner=self-check@%s:%s\n' "$lock_host" "$$"
  printf 'host=%s\n' "$lock_host"
  printf 'pid=%s\n' "$$"
  printf 'started_at=0\n'
} > "$lock_file"
touch -t 200001010000 "$lock_file"

if locked_output="$(run_runner --worker claude --reviewer codex --tasks "$stale_lock_tasks" --allow-dirty-worktree 2>&1)"; then
  fail "expected live stale-looking lock to stop the run"
fi

assert_output_contains "$locked_output" "STOP_LOCKED 010-lock.md"
assert_output_not_contains "$locked_output" "STALE_LOCK_CLEARED"

reset_stub_state
workflow_a_tasks="$TMP_DIR/workflow-a-tasks"
workflow_a_state="$TMP_DIR/workflow-a-state"
mkdir -p "$workflow_a_tasks"
printf '# Workflow A\n' > "$workflow_a_tasks/010-run.md"

if workflow_a_output="$(run_runner --worker claude --reviewer codex --tasks "$workflow_a_tasks" --review-passes 2 --state-dir "$workflow_a_state" --allow-dirty-worktree 2>&1)"; then
  :
else
  fail "template workflow A failed"$'\n'"$workflow_a_output"
fi

assert_output_contains "$workflow_a_output" "RUN 010-run.md round 1"
assert_output_contains "$workflow_a_output" "REVIEW 010-run.md pass 1/2"
assert_output_contains "$workflow_a_output" "PASS 010-run.md"
[[ -f "$workflow_a_tasks/010-run.md.done" ]] || fail "expected workflow A task to be marked done"
assert_file_contains_line "$TMP_DIR/claude.1.args" "-p"
assert_file_contains_line "$TMP_DIR/claude.1.args" "--permission-mode"
assert_file_contains_line "$TMP_DIR/claude.1.args" "acceptEdits"
assert_file_contains_line "$TMP_DIR/codex.1.args" "exec"
assert_file_contains_line "$TMP_DIR/codex.1.args" "--sandbox"
assert_file_contains_line "$TMP_DIR/codex.1.args" "workspace-write"

reset_stub_state
workflow_b_tasks="$TMP_DIR/workflow-b-tasks"
workflow_b_state="$TMP_DIR/workflow-b-state"
mkdir -p "$workflow_b_tasks"
printf '# Workflow B\n' > "$workflow_b_tasks/010-run.md"

if workflow_b_output="$(run_runner --worker copilot --reviewer claude --tasks "$workflow_b_tasks" --review-passes 2 --state-dir "$workflow_b_state" --allow-dirty-worktree 2>&1)"; then
  :
else
  fail "template workflow B failed"$'\n'"$workflow_b_output"
fi

assert_output_contains "$workflow_b_output" "RUN 010-run.md round 1"
assert_output_contains "$workflow_b_output" "REVIEW 010-run.md pass 1/2"
assert_output_contains "$workflow_b_output" "PASS 010-run.md"
[[ -f "$workflow_b_tasks/010-run.md.done" ]] || fail "expected workflow B task to be marked done"
assert_file_contains_line "$TMP_DIR/copilot.1.args" "--allow-all"
assert_file_contains_line "$TMP_DIR/copilot.1.args" "--autopilot"
assert_file_contains_line "$TMP_DIR/copilot.1.args" "-p"
assert_file_contains_line "$TMP_DIR/claude.1.args" "-p"
assert_file_contains_line "$TMP_DIR/claude.1.args" "--permission-mode"
assert_file_contains_line "$TMP_DIR/claude.1.args" "acceptEdits"

printf 'self-check ok\n'