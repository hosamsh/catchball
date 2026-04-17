from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Callable, Sequence

import psutil

NON_WHITESPACE_RE = re.compile(r"\S")
ROLE_PERMISSION_SIGNAL_RE = re.compile(r"^permission denied (?P<target>.+)$", re.IGNORECASE)
ROLE_WRITE_BLOCK_RE = re.compile(
    r"^(?:Failed to write file\b.*|Access to the path .* is denied|Unable to create '.+index\.lock'|(?:error|fatal): .*?(?:write|save|create|index\.lock).*Permission denied)$",
    re.IGNORECASE,
)
WINDOWS_BATCH_SUFFIXES = {".bat", ".cmd"}
LOCK_PROCESS_START_TOLERANCE_SECONDS = 5
LIVE_STATUS_REFRESH_SECONDS = 0.1
ROLE_NAMES = ("worker", "fixer", "reviewer")
REQUIRED_ROLE_NAMES = ("worker", "reviewer")

@dataclass(frozen=True)
class ConsoleGlyphs:
    task: str
    worker: str
    fixer: str
    reviewer: str
    clean: str
    issues: str
    wait: str
    skip: str
    stop: str
    summary: str
    stale: str

UNICODE_GLYPHS = ConsoleGlyphs(
    task="◉",
    worker="◆",
    fixer="◆",
    reviewer="◇",
    clean="✓",
    issues="✗",
    wait="…",
    skip="↷",
    stop="!",
    summary="◆",
    stale="↻",
)

ASCII_GLYPHS = ConsoleGlyphs(
    task="*",
    worker=">",
    fixer=">",
    reviewer=">",
    clean="+",
    issues="x",
    wait="...",
    skip="-",
    stop="!",
    summary="=",
    stale="~",
)

UNICODE_SPINNER_FRAMES = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")
ASCII_SPINNER_FRAMES = ("-", "\\", "|", "/")

ANSI_RESET = "\033[0m"
ANSI_STYLES = {
    "task": "\033[1;36m",
    "worker": "\033[1;34m",
    "fixer": "\033[1;32m",
    "reviewer": "\033[1;33m",
    "clean": "\033[1;32m",
    "issues": "\033[1;31m",
    "wait": "\033[1;35m",
    "skip": "\033[2m",
    "stop": "\033[1;31m",
    "summary": "\033[1;36m",
    "stale": "\033[1;35m",
    "subtle": "\033[2m",
}

@dataclass(frozen=True)
class ToolConfig:
    spec: str
    worker_defaults: dict[str, str] = field(default_factory=dict)
    reviewer_defaults: dict[str, str] = field(default_factory=dict)
    worker_preset: str = ""
    reviewer_preset: str = ""
    output_filter: str = ""

    def defaults_for(self, role_name: str) -> dict[str, str]:
        if role_name == "reviewer":
            return self.reviewer_defaults
        return self.worker_defaults

    def preset_for(self, role_name: str) -> str:
        if role_name == "reviewer":
            return self.reviewer_preset
        return self.worker_preset

@dataclass
class RoleSettings:
    tool: str = ""
    model: str = ""
    effort: str = ""
    mode: str = ""
    instructions_file: Path | None = None
    extra_args: list[str] = field(default_factory=list)
    binary_path: str = ""

@dataclass
class AppSettings:
    worker: RoleSettings = field(default_factory=RoleSettings)
    fixer: RoleSettings = field(default_factory=RoleSettings)
    reviewer: RoleSettings = field(default_factory=RoleSettings)
    project_root: Path | None = None
    tasks_dir: Path = Path("./tasks")
    review_passes: int = 3
    continue_despite_failures: bool = False
    reset_state: bool = False
    phase_delay_seconds: int = 3
    lock_stale_timeout: int = 10800
    lock_heartbeat_interval: int = 60
    role_health_check_interval: int = 30
    role_idle_timeout: int = 600
    state_dir: Path | None = None
    allow_dirty: bool = False
    from_task: str = ""
    to_task: str = ""

class CatchballError(RuntimeError):
    pass

class CatchballHelp(RuntimeError):
    pass

ROLE_KINDS = ("model", "effort", "mode")

TOOLS: dict[str, ToolConfig] = {
    "claude": ToolConfig(
        spec=(
            "claude -p --verbose --output-format stream-json --include-partial-messages {{prompt}} "
            "[model:--model {value}:haiku|sonnet|opus|claude-haiku-4-5|claude-sonnet-4-6|claude-opus-4-6|claude-opus-4-7] "
            "[effort:--effort {value}:low|medium|high|xhigh|max] "
            "[mode:--permission-mode {value}:acceptEdits] {{extra}}"
        ),
        worker_defaults={"mode": "acceptEdits"},
        reviewer_defaults={"mode": "acceptEdits"},
        output_filter="claude-stream-json",
    ),
    "codex": ToolConfig(
        spec=(
            "codex exec --json "
            "[model:--model {value}:gpt-5.4|gpt-5.4-mini|gpt-5.3-codex|gpt-5.3-codex-spark|gpt-5.2] "
            "[effort:-c model_reasoning_effort={value}:low|medium|high|xhigh] "
            "{{preset}} {{extra}} {{prompt}}"
        ),
        worker_preset="--full-auto",
        reviewer_preset="--sandbox workspace-write",
        output_filter="codex-exec-json",
    ),
    "copilot": ToolConfig(
        spec=(
            "copilot --output-format json --no-ask-user --allow-all "
            "[model:--model {value}:gpt-5.4|gpt-5.3-Codex|gpt-5.2-Codex|gpt-5.2|gpt-5.1|gpt-5.4-mini|gpt-5-mini|gpt-4.1|claude-sonnet-4.6|claude-sonnet-4.5|claude-haiku-4.5|claude-opus-4.7|claude-opus-4.6|claude-opus-4.5|claude-sonnet-4] "
            "[effort:--reasoning-effort {value}:low|medium|high|xhigh] "
            "{{preset}} {{extra}} -p {{prompt}}"
        ),
        worker_preset="--autopilot",
        reviewer_preset="--autopilot",
        output_filter="copilot-json",
    ),
    "opencode": ToolConfig(
        spec=(
            "opencode run --format json "
            "[model:--model {value}:provider/model] "
            "{{preset}} {{extra}} {{prompt}}"
        ),
        worker_preset="--dangerously-skip-permissions",
        reviewer_preset="--dangerously-skip-permissions",
        output_filter="opencode-json",
    ),
}

class CatchballArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise CatchballError(message)

def build_argument_parser() -> CatchballArgumentParser:
    parser = CatchballArgumentParser(
        prog="catchball",
        add_help=False,
        usage="catchball --worker <tool> --reviewer <tool> [options]",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Runtime:\n"
            "  Python 3.11+\n"
            "  Windows, macOS, or Linux\n"
            "  Worker, fixer, and reviewer tools must be on this shell's PATH\n\n"
            "Tools:\n"
            + "\n".join(f"  {name}" for name in sorted(TOOLS))
        ),
    )
    parser._optionals.title = "Options"

    for role_name in ROLE_NAMES:
        parser.add_argument(f"--{role_name}", dest=f"{role_name}_tool", metavar="<tool>")
        for kind in ROLE_KINDS:
            parser.add_argument(f"--{role_name}-{kind}", metavar="<value>")
        parser.add_argument(f"--{role_name}-instructions", metavar="<file>")
        parser.add_argument(
            f"--{role_name}-arg",
            dest=f"{role_name}_args",
            action="append",
            default=[],
            metavar="<arg>",
        )

    parser.add_argument("--project-root", metavar="<dir>")
    parser.add_argument("--tasks", "--tasks-dir", dest="tasks_dir", default="./tasks", metavar="<dir>")
    parser.add_argument("--from", dest="from_task", default="", metavar="<task>")
    parser.add_argument("--to", dest="to_task", default="", metavar="<task>")
    parser.add_argument(
        "--review-passes",
        type=lambda value: parse_integer("--review-passes", value),
        default=3,
        metavar="<n>",
    )
    parser.add_argument(
        "--phase-delay",
        type=lambda value: parse_integer("--phase-delay", value, allow_zero=True),
        default=3,
        metavar="<seconds>",
    )
    parser.add_argument("--continue-despite-failures", action="store_true")
    parser.add_argument("--reset-state", action="store_true")
    parser.add_argument("--retries", type=lambda value: parse_integer("--retries", value, allow_zero=True), metavar="<n>")
    parser.add_argument("--lock-timeout", type=lambda value: parse_integer("--lock-timeout", value), default=10800, metavar="<seconds>")
    parser.add_argument("--lock-heartbeat", type=lambda value: parse_integer("--lock-heartbeat", value), default=60, metavar="<seconds>")
    parser.add_argument("--health-check-interval", type=lambda value: parse_integer("--health-check-interval", value), default=15, metavar="<seconds>")
    parser.add_argument("--idle-timeout", type=lambda value: parse_integer("--idle-timeout", value), default=600, metavar="<seconds>")
    parser.add_argument("--state-dir", metavar="<dir>")
    parser.add_argument("--allow-dirty", "--allow-dirty-worktree", dest="allow_dirty", action="store_true")
    parser.add_argument("--help", "-h", action="store_true")
    return parser

def normalize_passthrough_args(argv: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    index = 0

    while index < len(argv):
        option = argv[index]
        if option in {"--worker-arg", "--fixer-arg", "--reviewer-arg"}:
            if index + 1 >= len(argv) or not argv[index + 1] or argv[index + 1].startswith("--"):
                raise CatchballError(f"Missing value for {option}")
            normalized.append(f"{option}={argv[index + 1]}")
            index += 2
            continue
        normalized.append(option)
        index += 1

    return normalized

def parse_integer(option: str, value: str, *, allow_zero: bool = False) -> int:
    if not value.isdigit():
        if allow_zero:
            raise CatchballError(f"{option} must be a non-negative integer")
        raise CatchballError(f"{option} must be a positive integer")

    parsed = int(value)
    if allow_zero:
        return parsed
    if parsed <= 0:
        raise CatchballError(f"{option} must be greater than zero")
    return parsed

def normalize_role_value(kind: str, value: str) -> str:
    normalized = value.strip()
    if kind == "model":
        return normalized.lower()
    return normalized

def parse_cli(argv: Sequence[str]) -> AppSettings:
    parser = build_argument_parser()
    parsed = parser.parse_args(normalize_passthrough_args(argv))
    if parsed.help:
        raise CatchballHelp(parser.format_help().replace("usage:", "Usage:", 1))

    def build_role(name: str) -> RoleSettings:
        instructions = getattr(parsed, f"{name}_instructions")
        return RoleSettings(
            tool=getattr(parsed, f"{name}_tool"),
            model=normalize_role_value("model", getattr(parsed, f"{name}_model") or ""),
            effort=normalize_role_value("effort", getattr(parsed, f"{name}_effort") or ""),
            mode=normalize_role_value("mode", getattr(parsed, f"{name}_mode") or ""),
            instructions_file=Path(instructions) if instructions else None,
            extra_args=getattr(parsed, f"{name}_args"),
        )

    return AppSettings(
        worker=build_role("worker"),
        fixer=build_role("fixer"),
        reviewer=build_role("reviewer"),
        project_root=Path(parsed.project_root) if parsed.project_root else None,
        tasks_dir=Path(parsed.tasks_dir),
        review_passes=parsed.review_passes if parsed.retries is None else parsed.retries + 1,
        continue_despite_failures=parsed.continue_despite_failures,
        reset_state=parsed.reset_state,
        phase_delay_seconds=parsed.phase_delay,
        lock_stale_timeout=parsed.lock_timeout,
        lock_heartbeat_interval=parsed.lock_heartbeat,
        role_health_check_interval=parsed.health_check_interval,
        role_idle_timeout=parsed.idle_timeout,
        state_dir=Path(parsed.state_dir) if parsed.state_dir else None,
        allow_dirty=parsed.allow_dirty,
        from_task=parsed.from_task,
        to_task=parsed.to_task,
    )

class CatchballRunner:
    def __init__(
        self,
        settings: AppSettings,
        *,
        original_args: Sequence[str] = (),
        root_dir: str | Path | None = None,
        env: dict[str, str] | None = None,
        stdout: IO[str] | None = None,
        stderr: IO[str] | None = None,
    ) -> None:
        self.settings = settings
        self.original_args = tuple(original_args)
        root_source = root_dir if root_dir is not None else settings.project_root
        self.root_dir = Path(root_source or Path.cwd()).resolve()
        self.env = dict(os.environ if env is None else env)
        self.stdout = stdout or sys.stdout
        self.stderr = stderr or sys.stderr
        self.console_glyphs = self.choose_console_glyphs()
        self.console_color = self.choose_console_color()
        self.console_live_status = self.choose_console_live_status()
        self.live_status_width = 0
        self.live_status_index = 0
        self.live_status_message = ""
        self.live_status_lock = threading.Lock()
        self.live_status_stop_event: threading.Event | None = None
        self.live_status_thread: threading.Thread | None = None
        self.run_id = datetime.now().strftime("%Y%m%d%H%M%S") + f"-{os.getpid()}"
        self.run_folder = datetime.now().strftime("%d-%m-%y--%H--%M--%S") + f"--{os.getpid()}"
        self.host_name = socket.gethostname() or "unknown"
        user_name = self.env.get("USER") or self.env.get("USERNAME") or "unknown"
        self.lock_owner = f"{user_name}@{self.host_name}:{os.getpid()}"
        self.state_dir: Path | None = None
        self.task_state_dir: Path | None = None
        self.run_log: Path | None = None
        self.run_results_dir: Path | None = None
        self.run_review_outputs_dir: Path | None = None
        self.reviews_dir: Path | None = None
        self.tasks: list[Path] = []
        self.start_index = 0
        self.current_lock_file: Path | None = None
        self.last_lock_conflict_message = ""
        self.heartbeat_stop_event: threading.Event | None = None
        self.heartbeat_thread: threading.Thread | None = None
        self.current_role_process: subprocess.Popen | None = None
        self.current_display_task: str | None = None
        self.total_tasks = 0
        self.passed = 0
        self.passed_clean = 0
        self.passed_review = 0
        self.failed = 0
        self.skipped = 0
        self.stopped = False

    def run(self) -> int:
        self.validate_configuration()
        self.initialize_state()
        self.discover_tasks()
        self.print_run_header()
        self.run_tasks()
        return self.finish_run()

    def validate_configuration(self) -> None:
        if not self.settings.worker.tool or not self.settings.reviewer.tool:
            raise CatchballError("Both --worker and --reviewer are required")
        if not self.root_dir.is_dir():
            raise CatchballError(f"Project root not found: {self.root_dir}")
        for option_name, value in (
            ("--review-passes", self.settings.review_passes),
            ("--lock-timeout", self.settings.lock_stale_timeout),
            ("--lock-heartbeat", self.settings.lock_heartbeat_interval),
            ("--health-check-interval", self.settings.role_health_check_interval),
            ("--idle-timeout", self.settings.role_idle_timeout),
        ):
            if value <= 0:
                raise CatchballError(f"{option_name} must be greater than zero")
        if self.settings.phase_delay_seconds < 0:
            raise CatchballError("--phase-delay must be a non-negative integer")
        if self.settings.lock_heartbeat_interval >= self.settings.lock_stale_timeout:
            raise CatchballError("--lock-heartbeat must be smaller than --lock-timeout")
        if self.settings.role_health_check_interval > self.settings.role_idle_timeout:
            raise CatchballError(
                "--health-check-interval must be smaller than or equal to --idle-timeout"
            )

        roles_to_validate: list[str] = list(REQUIRED_ROLE_NAMES)
        if self.settings.fixer.tool:
            roles_to_validate.append("fixer")
        elif self.role_has_configuration("fixer"):
            raise CatchballError("--fixer is required when fixer-specific options are provided")
        for role_name in roles_to_validate:
            role = self.role_settings(role_name)
            self.validate_role_tool(role)
            for kind in ROLE_KINDS:
                self.validate_role_value(role.tool, kind, self.resolve_role_value(role_name, role, kind))
            self.validate_role_instructions(role_name, role)

        self.settings.tasks_dir = self.abs_dir(self.settings.tasks_dir)

    def initialize_state(self) -> None:
        self.task_state_dir = self.settings.tasks_dir / "catchball-state"
        if self.task_state_dir.exists() and not self.task_state_dir.is_dir():
            raise CatchballError(f"Task state path is not a directory: {self.task_state_dir}")

        if self.settings.state_dir is not None:
            self.state_dir = self.make_absolute_path(self.settings.state_dir)
        else:
            self.state_dir = self.root_dir / "catchball-runs" / self.run_folder

        if self.is_inside_git_worktree() and not self.settings.allow_dirty:
            if not self.is_git_worktree_clean():
                raise CatchballError(
                    "Working tree must be clean. Use --allow-dirty-worktree to override"
                )

        if self.settings.reset_state and self.task_state_dir.exists():
            try:
                shutil.rmtree(self.task_state_dir)
            except OSError as error:
                raise CatchballError(
                    f"Unable to reset task state directory: {self.task_state_dir}"
                ) from error
        self.task_state_dir.mkdir(parents=True, exist_ok=True)

        self.run_log = self.state_dir / f"{self.run_id}.log"
        self.run_results_dir = self.state_dir / "worker-output"
        self.run_review_outputs_dir = self.state_dir / "reviewer-output"
        self.reviews_dir = self.state_dir / "reviews"
        self.ensure_parent_dir(self.run_log)
        self.run_results_dir.mkdir(parents=True, exist_ok=True)
        self.run_review_outputs_dir.mkdir(parents=True, exist_ok=True)
        self.reviews_dir.mkdir(parents=True, exist_ok=True)

    def discover_tasks(self) -> None:
        self.tasks = sorted(
            (path.resolve() for path in self.settings.tasks_dir.glob("*.md") if path.is_file()),
            key=lambda path: self.task_rel(path),
        )
        if not self.tasks:
            raise CatchballError(f"No .md files in {self.settings.tasks_dir}")

        self.start_index = 0
        if self.settings.from_task:
            self.start_index = self.find_task_index(self.settings.from_task, "--from")
        self.end_index = len(self.tasks)
        if self.settings.to_task:
            self.end_index = self.find_task_index(self.settings.to_task, "--to") + 1
        if self.start_index >= self.end_index:
            raise CatchballError("--from task must come before --to task")
        self.total_tasks = self.end_index - self.start_index

    def print_run_header(self) -> None:
        assert self.state_dir is not None
        assert self.task_state_dir is not None
        assert self.reviews_dir is not None
        assert self.run_log is not None
        self.emit(
            "catchball | worker: "
            f"{self.settings.worker.tool} | fixer: {self.fixer_label()} | reviewer: {self.settings.reviewer.tool} | "
            f"tasks: {len(self.tasks)} | review-passes: {self.settings.review_passes}"
        )
        if self.settings.from_task or self.settings.to_task:
            range_parts: list[str] = []
            if self.settings.from_task:
                range_parts.append(f"from: {self.settings.from_task}")
            if self.settings.to_task:
                range_parts.append(f"to: {self.settings.to_task}")
            self.emit(f"catchball | {' | '.join(range_parts)}")
        self.emit(f"catchball | project root: {self.root_dir}")
        self.emit(f"catchball | task state: {self.display_task_state_path(self.task_state_dir)}")
        self.emit(f"catchball | run dir: {self.state_dir}")
        self.emit(
            "catchball | health: "
            f"{self.settings.role_health_check_interval}s check | "
            f"{self.settings.role_idle_timeout}s idle-timeout"
        )
        if self.settings.phase_delay_seconds > 0:
            self.emit(f"catchball | phase delay: {self.settings.phase_delay_seconds}s")
        if self.settings.reset_state:
            self.emit("catchball | task state reset: yes")
        self.emit(f"catchball | worker output: {self.display_run_path(self.run_results_dir)}")
        self.emit(f"catchball | reviewer output: {self.display_run_path(self.run_review_outputs_dir)}")
        self.emit(f"catchball | reviews: {self.display_run_path(self.reviews_dir)}")
        self.emit(f"catchball | run log: {self.display_run_path(self.run_log)}")
        for role_name in self.configured_role_names():
            for label, instructions_file in self.role_instruction_entries(role_name):
                self.emit(f"catchball | {label}: {self.display_artifact_path(instructions_file)}")
        self.emit()

        self.log_run(
            "RUN_START",
            "-",
            "worker="
            f"{self.settings.worker.tool} fixer={self.fixer_label(log=True)} reviewer={self.settings.reviewer.tool} "
            f"project_root={self.root_dir} "
            f"from={self.settings.from_task or 'start'} to={self.settings.to_task or 'end'} state_dir={self.state_dir} task_state_dir={self.task_state_dir} "
            f"health={self.settings.role_health_check_interval} idle={self.settings.role_idle_timeout} "
            f"phase_delay={self.settings.phase_delay_seconds} reset_state={int(self.settings.reset_state)}",
        )
        self.log_run("RUN_ARGS", "-", repr(list(self.original_args)))
        for role_name in self.configured_role_names():
            for label, instructions_file in self.role_instruction_entries(role_name):
                self.log_run("RUN_ROLE_INSTRUCTIONS", "-", f"{label}={self.display_artifact_path(instructions_file)}")

    def report(self, line: str, event: str, task: str, message: str = "") -> None:
        for console_line in self.console_lines_for_event(line, event, task, message):
            self.emit(console_line)
        self.log_run(event, task, message)

    def run_tasks(self) -> None:
        assert self.run_results_dir is not None
        assert self.run_review_outputs_dir is not None
        assert self.reviews_dir is not None
        pending_tasks = self.tasks[self.start_index : self.end_index]
        for task_index, task_file in enumerate(pending_tasks):
            rel_path = self.task_rel(task_file)
            key = self.task_key(task_file)
            active_review_file = self.task_sidecar(task_file, ".review", base_dir=self.reviews_dir)

            if self.task_sidecar(task_file, ".done").is_file():
                self.report(f"SKIP_DONE {rel_path}", "SKIP_DONE", rel_path)
                self.skipped += 1
                continue

            if not self.acquire_lock(task_file, rel_path):
                self.report("locked by another run -> stop", "STOP_LOCKED", rel_path, self.last_lock_conflict_message)
                self.stopped = True
                break

            task_started_at = time.monotonic()
            stage_durations = {"worker": 0.0, "fixer": 0.0, "reviewer": 0.0, "delay": 0.0}
            stage_counts = {"worker": 0, "fixer": 0, "reviewer": 0}
            task_result = "stopped"
            task_stopped = False
            had_review_issues = False
            pre_task_dirty: frozenset[str] = self.git_dirty_files()
            try:
                self.ensure_parent_dir(active_review_file)
                self.cleanup_empty_file(active_review_file)
                implementation_round = 1

                while True:
                    if self.review_passes_used(task_file) >= self.settings.review_passes:
                        task_result = "failed_continue" if self.settings.continue_despite_failures else "stopped"
                        task_stopped = self.handle_review_exhausted(task_file, rel_path)
                        break

                    has_review = self.active_review_exists(active_review_file)
                    implementation_role = self.implementation_role_name(has_review)
                    implementation_prompt = self.implementation_prompt_text(
                        implementation_role,
                        task_file,
                        active_review_file if has_review else None,
                        diff_stat=self.git_diff_stat(pre_task_dirty) if has_review else "",
                    )
                    event = "RUN_FIX" if has_review else "RUN"
                    detail = f"round={implementation_round} role={implementation_role}"
                    if has_review:
                        detail += f" review={active_review_file}"
                    self.report(
                        f"{event} {rel_path} round {implementation_round} via {implementation_role}",
                        event,
                        rel_path,
                        detail,
                    )

                    worker_output_file = self.run_results_dir / f"{key}.{implementation_role}-{implementation_round}.log"
                    stage_started_at = time.monotonic()
                    role_succeeded, should_stop_run = self.run_role_or_stop(
                        implementation_role,
                        implementation_prompt,
                        worker_output_file,
                        task_file,
                        rel_path,
                    )
                    stage_durations[implementation_role] += time.monotonic() - stage_started_at
                    stage_counts[implementation_role] += 1
                    if not role_succeeded:
                        task_stopped = should_stop_run
                        task_result = "stopped" if should_stop_run else "failed_continue"
                        break

                    previous_review_file: Path | None = None
                    if self.active_review_exists(active_review_file):
                        previous_review_file = self.archive_active_review(task_file)
                        if previous_review_file is not None:
                            self.log_run("REVIEW_ARCHIVED", rel_path, f"file={previous_review_file}")

                    stage_durations["delay"] += self.pause_before_transition(rel_path, implementation_role, "reviewer")

                    review_pass = self.next_review_pass(task_file)
                    review_output_file = self.run_review_outputs_dir / f"{key}.review-{review_pass}.log"
                    active_review_file.unlink(missing_ok=True)
                    reviewer_prompt = self.reviewer_prompt_text(
                        task_file,
                        active_review_file,
                        review_pass,
                        previous_review_file,
                        diff_stat=self.git_diff_stat(pre_task_dirty),
                    )
                    self.report(
                        f"REVIEW {rel_path} pass {review_pass}/{self.settings.review_passes}",
                        "REVIEW",
                        rel_path,
                        f"pass={review_pass}",
                    )
                    stage_started_at = time.monotonic()
                    role_succeeded, should_stop_run = self.run_role_or_stop(
                        "reviewer",
                        reviewer_prompt,
                        review_output_file,
                        task_file,
                        rel_path,
                        completion_file=active_review_file,
                    )
                    stage_durations["reviewer"] += time.monotonic() - stage_started_at
                    stage_counts["reviewer"] += 1
                    if not role_succeeded:
                        task_stopped = should_stop_run
                        task_result = "stopped" if should_stop_run else "failed_continue"
                        break

                    if not self.active_review_exists(active_review_file):
                        self.write_done(task_file)
                        self.report(f"PASS {rel_path}", "PASS", rel_path, f"pass={review_pass}")
                        self.passed += 1
                        if had_review_issues:
                            self.passed_review += 1
                            task_result = "passed_review"
                        else:
                            self.passed_clean += 1
                            task_result = "passed_clean"
                        break

                    had_review_issues = True
                    self.report(
                        f"REVIEW_FAIL {rel_path} pass {review_pass}/{self.settings.review_passes}",
                        "REVIEW_FAIL",
                        rel_path,
                        f"pass={review_pass} file={active_review_file}",
                    )
                    if review_pass >= self.settings.review_passes:
                        task_result = "failed_continue" if self.settings.continue_despite_failures else "stopped"
                        task_stopped = self.handle_review_exhausted(task_file, rel_path)
                        break

                    stage_durations["delay"] += self.pause_before_transition(
                        rel_path,
                        "reviewer",
                        self.implementation_role_name(True),
                    )
                    implementation_round += 1
            finally:
                self.release_lock()

            self.report_task_timing(rel_path, task_result, task_started_at, stage_durations, stage_counts)

            if task_stopped:
                break

            if task_index + 1 < len(pending_tasks):
                self.pause_before_transition(rel_path, "task", "next-task")

    def finish_run(self) -> int:
        self.emit()
        summary = (
            f"done | total {self.total_tasks} | passed-clean {self.passed_clean} | "
            f"passed-review {self.passed_review} | failed {self.failed} | skipped {self.skipped} | "
            f"stopped {int(self.stopped)}"
        )
        summary_message = (
            f"total={self.total_tasks} passed={self.passed} passed_clean={self.passed_clean} "
            f"passed_review={self.passed_review} failed={self.failed} skipped={self.skipped}"
        )
        if self.stopped:
            self.report(summary, "RUN_STOP", "-", f"{summary_message} stopped=1")
            return 1
        self.report(summary, "RUN_DONE", "-", f"{summary_message} stopped=0")
        return 1 if self.failed else 0

    def pause_before_transition(self, rel_path: str, from_step: str, to_step: str) -> int:
        delay = self.settings.phase_delay_seconds
        if delay <= 0:
            return 0
        target_label = to_step.replace("-", " ")
        self.report(
            f"WAIT {rel_path} {delay}s before {target_label}",
            "PHASE_DELAY",
            rel_path,
            f"from={from_step} to={to_step} seconds={delay}",
        )
        time.sleep(delay)
        return delay

    def report_task_timing(
        self,
        task_label: str,
        status: str,
        task_started_at: float,
        stage_durations: dict[str, float],
        stage_counts: dict[str, int],
    ) -> None:
        total_seconds = max(0, int(round(time.monotonic() - task_started_at)))
        worker_seconds = max(0, int(round(stage_durations.get("worker", 0.0))))
        fixer_seconds = max(0, int(round(stage_durations.get("fixer", 0.0))))
        reviewer_seconds = max(0, int(round(stage_durations.get("reviewer", 0.0))))
        delay_seconds = max(0, int(round(stage_durations.get("delay", 0.0))))
        self.report(
            "timing "
            f"total {self.format_duration(total_seconds)} | "
            f"worker {self.format_duration(worker_seconds)} x{stage_counts.get('worker', 0)} | "
            f"fixer {self.format_duration(fixer_seconds)} x{stage_counts.get('fixer', 0)} | "
            f"reviewer {self.format_duration(reviewer_seconds)} x{stage_counts.get('reviewer', 0)} | "
            f"delay {self.format_duration(delay_seconds)} | "
            f"status {status}",
            "TASK_TIMING",
            task_label,
            "status="
            f"{status} total={total_seconds} worker={worker_seconds} worker_rounds={stage_counts.get('worker', 0)} "
            f"fixer={fixer_seconds} fixer_rounds={stage_counts.get('fixer', 0)} "
            f"reviewer={reviewer_seconds} reviewer_rounds={stage_counts.get('reviewer', 0)} "
            f"delay={delay_seconds}",
        )

    def handle_review_exhausted(self, task: Path, task_label: str) -> bool:
        self.record_task_failure(task, "review_passes_exhausted")
        if self.settings.continue_despite_failures:
            self.report(f"FAIL_CONTINUE {task_label}", "FAIL_CONTINUE", task_label, "review_passes_exhausted")
            return False
        self.report(f"STOP_FAIL {task_label}", "STOP_FAIL", task_label, "review_passes_exhausted")
        self.stopped = True
        return True

    def run_role_or_stop(
        self,
        role_name: str,
        prompt: str,
        output_file: Path,
        task: Path,
        task_label: str,
        *,
        completion_file: Path | None = None,
    ) -> tuple[bool, bool]:
        status = self.run_role_to_file(role_name, prompt, output_file, task_label)
        if status == 0:
            return True, False
        if status == 73 and completion_file is not None and self.file_has_content(completion_file):
            self.log_run(
                "ROLE_BLOCKED_IGNORED",
                task_label,
                f"role={role_name} file={output_file} artifact={completion_file}",
            )
            return True, False
        event, reason, detail = self.role_failure_report(role_name, status, output_file)
        if self.settings.continue_despite_failures:
            self.record_task_failure(task, reason)
            self.report(f"FAIL_CONTINUE {task_label}", "FAIL_CONTINUE", task_label, detail)
            return False, False
        self.report(f"{event} {task_label}", event, task_label, detail)
        self.stopped = True
        return False, True

    def run_role_to_file(self, role_name: str, prompt: str, output_file: Path, task_label: str) -> int:
        self.ensure_parent_dir(output_file)
        output_file.write_text("", encoding="utf-8")

        try:
            process = self.spawn_role_process(role_name, prompt, output_file)
        except OSError as exc:
            self.append_text(output_file, f"{exc}\n")
            self.log_run("ROLE_EXIT", task_label, f"role={role_name} pid=-1 code=1 file={output_file}")
            return 1

        self.current_role_process = process
        last_activity_at = self.now()
        started_at = last_activity_at
        last_output_state = self.output_state(output_file)
        last_process_state = self.process_tree_signature(process.pid)
        self.log_run("ROLE_START", task_label, f"role={role_name} pid={process.pid} file={output_file}")
        idle_grace_used = False
        self.emit_role_health_status(role_name, "starting", started_at, 0, last_output_state[0])

        try:
            while process.poll() is None:
                time.sleep(self.settings.role_health_check_interval)
                current_output_state = self.output_state(output_file)
                current_process_state = self.process_tree_signature(process.pid)
                output_changed = int(current_output_state != last_output_state)
                process_changed = int(current_process_state != last_process_state)

                if output_changed or process_changed:
                    last_activity_at = self.now()
                    last_output_state = current_output_state
                    last_process_state = current_process_state
                    idle_grace_used = False
                    health_status = "active"
                else:
                    health_status = "idle"

                idle_for = self.now() - last_activity_at
                output_bytes = current_output_state[0]
                self.log_run(
                    "ROLE_HEALTH",
                    task_label,
                    "role="
                    f"{role_name} pid={process.pid} status={health_status} idle={idle_for} "
                    f"output_changed={output_changed} process_changed={process_changed} "
                    f"bytes={output_bytes}",
                )
                self.emit_role_health_status(role_name, health_status, started_at, idle_for, output_bytes)

                if idle_for >= self.settings.role_idle_timeout:
                    if not idle_grace_used:
                        idle_grace_used = True
                        last_activity_at = self.now()
                        self.log_run("ROLE_QUIET", task_label, f"role={role_name} pid={process.pid} idle={idle_for} grace=1 file={output_file}")
                        continue

                    self.append_text(output_file, f"\ncatchball | {role_name} stalled after {idle_for}s without process or log activity\n")
                    self.log_run("ROLE_STALL", task_label, f"role={role_name} pid={process.pid} idle={idle_for} file={output_file}")
                    self.terminate_process(process)
                    return 124

            exit_code = process.wait()
            self.current_role_process = None
            self.log_run("ROLE_EXIT", task_label, f"role={role_name} pid={process.pid} code={exit_code} file={output_file}")
            if exit_code == 0 and self.role_output_is_write_blocked(role_name, output_file):
                self.log_run("ROLE_BLOCKED", task_label, f"role={role_name} file={output_file}")
                return 73
            return exit_code
        finally:
            self.clear_live_status()

    def role_failure_details(self, role_name: str, status: int) -> tuple[str, str]:
        return {
            124: ("STOP_STALL", f"{role_name}_stalled"),
            73: ("STOP_BLOCKED", f"{role_name}_write_blocked"),
        }.get(status, ("STOP_ERROR", f"{role_name}_failed"))

    def role_failure_report(self, role_name: str, status: int, output_file: Path) -> tuple[str, str, str]:
        event, reason = self.role_failure_details(role_name, status)
        detail = f"{reason} file={output_file}"
        if event == "STOP_ERROR":
            detail = f"{reason} code={status} file={output_file}"
        return event, reason, detail

    def implementation_prompt_text(self, role_name: str, task: Path, review: Path | None, diff_stat: str = "") -> str:
        if review is not None and self.file_has_content(review):
            lines = [
                f"This task was already implemented. The latest review issues are in {review}.",
                "Start by reading that file and fix every issue listed there.",
                f"Use {task} only as the source of truth for the intended outcome.",
                "Do not re-implement the task from scratch unless a review issue requires it.",
                "",
            ]
            if diff_stat:
                lines.extend(("Changed files so far (git diff HEAD --stat):", diff_stat, ""))
        else:
            lines = [f"Implement the task in {task}.", ""]
        lines.extend(self.role_instruction_lines(role_name))
        lines.extend(self.permission_signal_lines())
        lines.append(f"Do not create, rename, or edit files under {self.reviews_dir}.")
        return "\n".join(lines) + "\n"

    def reviewer_prompt_text(self, task: Path, review: Path, review_pass: int, previous_review: Path | None, diff_stat: str = "") -> str:
        lines = [
            f"Review the implementation against the task in {task}.",
            "",
            "Do not modify application code.",
            "Do not change the task file.",
            f"If the implementation is clean, do not create {review}.",
            f"If there are issues, create exactly one non-empty file at {review} and list only the issues that must be fixed.",
            f"Write only to that file under {self.reviews_dir}.",
            f"This is review pass {review_pass}.",
        ]
        if diff_stat:
            lines.extend(("", "Changed files (git diff HEAD --stat):", diff_stat))
        lines.extend(self.role_instruction_lines("reviewer"))
        lines.extend(self.permission_signal_lines())
        if previous_review is not None:
            lines.extend(("", f"The previous resolved review comments are in {previous_review}.", "Check whether they were addressed before writing a new review."))
        return "\n".join(lines) + "\n"

    def task_relative_path(self, task: Path) -> Path:
        return task.relative_to(self.settings.tasks_dir)

    def task_rel(self, task: Path) -> str:
        return self.task_relative_path(task).as_posix()

    def task_key(self, task: Path) -> str:
        return self.sanitize_name(self.task_rel(task))

    def task_sidecar(self, task: Path, suffix: str, *, base_dir: Path | None = None) -> Path:
        target_base = self.task_state_dir if base_dir is None else base_dir
        target = task if target_base is None else target_base / self.task_relative_path(task)
        return Path(f"{target}{suffix}")

    def ensure_parent_dir(self, file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)

    def cleanup_empty_file(self, file_path: Path) -> None:
        if file_path.is_file() and not self.file_has_content(file_path):
            file_path.unlink(missing_ok=True)

    def active_review_exists(self, file_path: Path) -> bool:
        self.cleanup_empty_file(file_path)
        return self.file_has_content(file_path)

    def file_has_content(self, file_path: Path) -> bool:
        if not file_path.is_file():
            return False
        try:
            contents = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return False
        return bool(NON_WHITESPACE_RE.search(contents))

    def role_output_is_write_blocked(self, role_name: str, output_file: Path) -> bool:
        if not output_file.is_file():
            return False
        try:
            contents = output_file.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return False
        lines = contents.splitlines()
        if any(ROLE_PERMISSION_SIGNAL_RE.search(line) for line in lines):
            return True
        if self.role_settings(role_name).tool in {"claude", "codex", "copilot"}:
            return False
        return any(ROLE_WRITE_BLOCK_RE.search(line) for line in lines)

    def latest_review_archive_index(self, task: Path) -> int:
        assert self.reviews_dir is not None
        prefix = self.task_sidecar(task, ".review.done", base_dir=self.reviews_dir)
        prefix_text = str(prefix)
        max_index = 0
        for candidate in prefix.parent.glob(f"{prefix.name}.*"):
            candidate_text = str(candidate)
            suffix = candidate_text[len(prefix_text) + 1 :]
            if suffix.isdigit():
                max_index = max(max_index, int(suffix))
        return max_index

    def next_review_pass(self, task: Path) -> int:
        return self.latest_review_archive_index(task) + 1

    def review_passes_used(self, task: Path) -> int:
        count = self.latest_review_archive_index(task)
        assert self.reviews_dir is not None
        if self.file_has_content(self.task_sidecar(task, ".review", base_dir=self.reviews_dir)):
            count += 1
        return count

    def archive_active_review(self, task: Path) -> Path | None:
        assert self.reviews_dir is not None
        active_review = self.task_sidecar(task, ".review", base_dir=self.reviews_dir)
        if not self.file_has_content(active_review):
            return None
        archived_review = Path(f"{self.task_sidecar(task, '.review.done', base_dir=self.reviews_dir)}.{self.next_review_pass(task)}")
        self.ensure_parent_dir(archived_review)
        active_review.replace(archived_review)
        return archived_review

    def log_run(self, event: str, task: str, message: str = "") -> None:
        if self.run_log is None:
            return
        with self.run_log.open("a", encoding="utf-8") as handle:
            handle.write(f"{self.timestamp()} {event} {task} {message}\n")

    def write_done(self, task: Path) -> None:
        done_file = self.task_sidecar(task, ".done")
        self.ensure_parent_dir(done_file)
        done_file.write_text(f"run_id={self.run_id}\n" f"done_at={self.timestamp()}\n", encoding="utf-8")
        self.task_sidecar(task, ".failed").unlink(missing_ok=True)

    def record_task_failure(self, task: Path, reason: str) -> None:
        self.write_failed(task, reason)
        self.failed += 1

    def write_failed(self, task: Path, reason: str) -> None:
        failed_file = self.task_sidecar(task, ".failed")
        self.ensure_parent_dir(failed_file)
        failed_file.write_text(
            f"run_id={self.run_id}\n"
            f"failed_at={self.timestamp()}\n"
            f"reason={reason}\n",
            encoding="utf-8",
        )

    def find_task_index(self, target: str, option: str = "--from") -> int:
        prefix_index: int | None = None
        prefix_count = 0
        for index, task in enumerate(self.tasks):
            relative_path = self.task_rel(task)
            base_name = task.name
            if relative_path == target or base_name == target:
                return index
            if relative_path.startswith(target) or base_name.startswith(target):
                prefix_index = index
                prefix_count += 1
        if prefix_count == 1 and prefix_index is not None:
            return prefix_index
        if prefix_count > 1:
            raise CatchballError(f"{option} task is ambiguous: {target}")
        raise CatchballError(f"{option} task not found: {target}")

    def acquire_lock(self, task: Path, rel_path: str) -> bool:
        lock_path = self.task_sidecar(task, ".lock")
        self.ensure_parent_dir(lock_path)
        while True:
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                if not lock_path.exists():
                    continue
                if self.lock_is_stale(lock_path):
                    lock_path.unlink(missing_ok=True)
                    self.report(f"STALE_LOCK_CLEARED {rel_path}", "STALE_LOCK_CLEARED", rel_path)
                    continue
                self.last_lock_conflict_message = self.lock_conflict_message(lock_path)
                return False
            else:
                os.close(fd)
                self.last_lock_conflict_message = ""
                self.current_lock_file = lock_path
                self.write_lock(lock_path)
                self.start_heartbeat(lock_path)
                return True

    def write_lock(self, lock_path: Path) -> None:
        lock_path.write_text(
            f"run_id={self.run_id}\n"
            f"owner={self.lock_owner}\n"
            f"host={self.host_name}\n"
            f"pid={os.getpid()}\n"
            f"started_at={self.now()}\n",
            encoding="utf-8",
        )
        lock_path.touch()

    def lock_is_stale(self, lock_path: Path) -> bool:
        try:
            mtime = int(lock_path.stat().st_mtime)
        except OSError:
            return True
        if self.now() - mtime <= self.settings.lock_stale_timeout:
            return False
        if self.lock_holder_is_alive(lock_path):
            return False
        return True

    def lock_timeout_remaining(self, lock_path: Path) -> int:
        try:
            mtime = int(lock_path.stat().st_mtime)
        except OSError:
            return 0
        age = max(0, self.now() - mtime)
        return max(0, self.settings.lock_stale_timeout - age)

    def lock_conflict_message(self, lock_path: Path) -> str:
        return (
            f"lock_path={json.dumps(str(lock_path))} "
            f"timeout_remaining={self.lock_timeout_remaining(lock_path)}"
        )

    def start_heartbeat(self, lock_path: Path) -> None:
        self.stop_heartbeat()
        stop_event = threading.Event()

        def heartbeat() -> None:
            while not stop_event.wait(self.settings.lock_heartbeat_interval):
                try:
                    lock_path.touch()
                except OSError:
                    return

        thread = threading.Thread(target=heartbeat, name="catchball-lock-heartbeat", daemon=True)
        thread.start()
        self.heartbeat_stop_event = stop_event
        self.heartbeat_thread = thread

    def stop_heartbeat(self) -> None:
        if self.heartbeat_stop_event is not None:
            self.heartbeat_stop_event.set()
        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join(timeout=1)
        self.heartbeat_stop_event = None
        self.heartbeat_thread = None

    def release_lock(self) -> None:
        self.stop_heartbeat()
        if self.current_lock_file is not None:
            self.current_lock_file.unlink(missing_ok=True)
            self.current_lock_file = None

    def stop_role_process(self) -> None:
        if self.current_role_process is not None:
            self.terminate_process(self.current_role_process)

    def terminate_process(self, process: subprocess.Popen) -> None:
        self.kill_process_tree(process.pid)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        self.current_role_process = None

    def cleanup(self) -> None:
        self.stop_role_process()
        self.release_lock()
        self.clear_live_status()
        self.stop_live_status_thread()

    def process_tree(self, root_pid: int) -> list[psutil.Process]:
        try:
            root_process = psutil.Process(root_pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return []

        processes = [root_process]
        try:
            processes.extend(root_process.children(recursive=True))
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

        unique_processes: list[psutil.Process] = []
        seen_pids: set[int] = set()
        for process in processes:
            if process.pid in seen_pids:
                continue
            seen_pids.add(process.pid)
            unique_processes.append(process)
        return unique_processes

    def process_tree_signature(self, root_pid: int) -> tuple[str, ...]:
        lines: list[str] = []
        for process in self.process_tree(root_pid):
            try:
                cpu_times = process.cpu_times()
                total_cpu = float(getattr(cpu_times, "user", 0.0) + getattr(cpu_times, "system", 0.0))
                status = process.status()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
            lines.append(f"{process.pid}:{total_cpu:.4f}:{status}")
        return tuple(sorted(lines))

    def kill_process_tree(self, root_pid: int) -> None:
        processes = self.process_tree(root_pid)
        if not processes:
            return

        for process in reversed(processes):
            try:
                process.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        _, alive = psutil.wait_procs(processes, timeout=1)
        for process in alive:
            try:
                process.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        if alive:
            psutil.wait_procs(alive, timeout=1)

    def spawn_role_process(self, role_name: str, prompt: str, output_file: Path) -> subprocess.Popen:
        role = self.role_settings(role_name)
        command_args = self.render_tool_command(role_name, role, prompt)
        launch_command = self.prepare_launch_command(role.binary_path, command_args)
        tool_config = TOOLS[role.tool]
        filter_targets = {
            "claude-stream-json": self._claude_stream_json_filter_thread,
            "codex-exec-json": self._codex_exec_json_filter_thread,
            "copilot-json": self._copilot_json_filter_thread,
            "opencode-json": self._opencode_json_filter_thread,
        }
        filter_target = filter_targets.get(tool_config.output_filter)
        if filter_target is not None:
            with output_file.open("ab") as stderr_handle:
                proc = subprocess.Popen(
                    launch_command,
                    cwd=self.root_dir,
                    env=self.env,
                    stdout=subprocess.PIPE,
                    stderr=stderr_handle,
                    stdin=subprocess.DEVNULL,
                )
            thread = threading.Thread(
                target=filter_target,
                args=(proc, output_file),
                daemon=True,
            )
            thread.start()
            return proc
        with output_file.open("ab") as handle:
            return subprocess.Popen(
                launch_command,
                cwd=self.root_dir,
                env=self.env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
            )

    def _claude_stream_json_filter_thread(self, proc: subprocess.Popen, output_file: Path) -> None:
        line_open = False
        with output_file.open("ab") as f:
            for raw_line in proc.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                text = self.claude_event_text(event)
                if text:
                    line_open = self.write_filtered_chunk(f, text, line_open)
            self.write_filtered_break(f, line_open)

    def _codex_exec_json_filter_thread(self, proc: subprocess.Popen, output_file: Path) -> None:
        line_open = False
        with output_file.open("ab") as f:
            for raw_line in proc.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                text = self.codex_event_text(event)
                if text:
                    line_open = self.write_filtered_line(f, text, line_open)
            self.write_filtered_break(f, line_open)

    def _copilot_json_filter_thread(self, proc: subprocess.Popen, output_file: Path) -> None:
        streamed_message_ids: set[str] = set()
        stream_kind = ""
        stream_id = ""
        line_open = False
        with output_file.open("ab") as f:
            for raw_line in proc.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                data = event.get("data")
                event_type = str(event.get("type", ""))
                if event_type == "assistant.reasoning_delta" and isinstance(data, dict):
                    reasoning_id = str(data.get("reasoningId", ""))
                    delta_text = str(data.get("deltaContent", ""))
                    if not delta_text:
                        continue
                    if stream_kind != "reasoning" or stream_id != reasoning_id:
                        line_open = self.write_filtered_break(f, line_open)
                        line_open = self.write_filtered_chunk(f, "[reasoning] ", line_open)
                    stream_kind = "reasoning"
                    stream_id = reasoning_id
                    line_open = self.write_filtered_chunk(f, delta_text, line_open)
                    continue
                if event_type == "assistant.message_delta" and isinstance(data, dict):
                    message_id = str(data.get("messageId", ""))
                    delta_text = str(data.get("deltaContent", ""))
                    if not delta_text:
                        continue
                    if stream_kind != "message" or stream_id != message_id:
                        line_open = self.write_filtered_break(f, line_open)
                    stream_kind = "message"
                    stream_id = message_id
                    if message_id:
                        streamed_message_ids.add(message_id)
                    line_open = self.write_filtered_chunk(f, delta_text, line_open)
                    continue
                if event_type == "assistant.message" and isinstance(data, dict):
                    stream_kind = ""
                    stream_id = ""
                    message_id = str(data.get("messageId", ""))
                    message_text = str(data.get("content", ""))
                    if message_text and message_id not in streamed_message_ids:
                        line_open = self.write_filtered_line(f, message_text, line_open)
                    continue
                if event_type == "tool.execution_start" and isinstance(data, dict):
                    stream_kind = ""
                    stream_id = ""
                    tool_name = str(data.get("toolName", "")).strip()
                    if tool_name:
                        line_open = self.write_filtered_line(f, f"[tool] {tool_name}", line_open)
                    continue
                if event_type == "assistant.turn_end":
                    stream_kind = ""
                    stream_id = ""
                    line_open = self.write_filtered_break(f, line_open)
            self.write_filtered_break(f, line_open)

    def _opencode_json_filter_thread(self, proc: subprocess.Popen, output_file: Path) -> None:
        line_open = False
        with output_file.open("ab") as f:
            for raw_line in proc.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                text = self.opencode_event_text(event)
                if text:
                    line_open = self.write_filtered_line(f, text, line_open)
            self.write_filtered_break(f, line_open)

    def write_filtered_chunk(self, handle: IO[bytes], text: str, line_open: bool) -> bool:
        if not text:
            return line_open
        handle.write(text.encode("utf-8", errors="replace"))
        handle.flush()
        return not text.endswith("\n")

    def write_filtered_line(self, handle: IO[bytes], text: str, line_open: bool) -> bool:
        if line_open:
            handle.write(b"\n")
        rendered = text if text.endswith("\n") else f"{text}\n"
        handle.write(rendered.encode("utf-8", errors="replace"))
        handle.flush()
        return False

    def write_filtered_break(self, handle: IO[bytes], line_open: bool) -> bool:
        if line_open:
            handle.write(b"\n")
            handle.flush()
        return False

    def claude_event_text(self, event: dict[str, object]) -> str:
        if event.get("type") != "stream_event":
            return ""
        inner = event.get("event", {})
        if not isinstance(inner, dict) or inner.get("type") != "content_block_delta":
            return ""
        delta = inner.get("delta", {})
        if not isinstance(delta, dict) or delta.get("type") != "text_delta":
            return ""
        return str(delta.get("text", ""))

    def codex_event_text(self, event: dict[str, object]) -> str:
        event_type = str(event.get("type", ""))
        if event_type == "error":
            return str(event.get("message", "")).strip()
        if event_type != "item.completed":
            return ""
        item = event.get("item")
        if not isinstance(item, dict):
            return ""
        item_type = str(item.get("type", ""))
        if item_type == "agent_message":
            return str(item.get("text", "")).strip()
        if item_type == "reasoning":
            text = item.get("text") or item.get("summary") or ""
            if isinstance(text, str):
                text = text.strip()
                return f"[reasoning] {text}" if text else ""
        return ""

    def opencode_event_text(self, event: dict[str, object]) -> str:
        event_type = str(event.get("type", ""))
        if event_type in {"text", "reasoning", "tool_use", "step_start", "step_finish"}:
            part = event.get("part")
            if not isinstance(part, dict):
                return ""
            if event_type == "text":
                return str(part.get("text", "")).strip()
            if event_type == "reasoning":
                text = str(part.get("text", "")).strip()
                return f"[reasoning] {text}" if text else ""
            if event_type == "tool_use":
                tool_name = str(part.get("tool", "")).strip()
                return f"[tool] {tool_name}" if tool_name else ""
            step_type = str(part.get("type", "")).strip()
            return f"[step] {step_type}" if step_type else ""
        if event_type != "error":
            return ""
        error = event.get("error")
        if not isinstance(error, dict):
            return str(error).strip()
        data = error.get("data")
        if isinstance(data, dict):
            message = str(data.get("message", "")).strip()
            if message:
                return message
        return str(error.get("name", "")).strip()

    def render_tool_command(self, role_name: str, role: RoleSettings, prompt: str) -> list[str]:
        tool_config = TOOLS[role.tool]
        resolved_values = {
            "model": self.resolve_role_value(role_name, role, "model"),
            "effort": self.resolve_role_value(role_name, role, "effort"),
            "mode": self.resolve_role_value(role_name, role, "mode"),
        }
        preset_text = tool_config.preset_for(role_name)
        parts = self.split_spec_parts(tool_config.spec)
        if not parts:
            raise CatchballError(f"No command in spec for {role.tool}")

        output_args: list[str] = []
        for token in parts[1:]:
            if token == "{{prompt}}":
                output_args.append(prompt)
            elif token == "{{extra}}":
                output_args.extend(role.extra_args)
            elif token == "{{preset}}":
                output_args.extend(shlex.split(preset_text))
            elif self.is_spec_choice(token):
                kind, render_text, allowed_values = self.parse_spec_choice(token)
                value = resolved_values[kind]
                if not value:
                    continue
                supported_value = self.resolve_supported_value(kind, value, allowed_values)
                if not supported_value:
                    raise CatchballError(f"Unsupported {kind} for {role.tool}: {value}")
                output_args.extend(shlex.split(render_text.replace("{value}", supported_value)))
            else:
                output_args.append(token)
        return output_args

    def validate_role_value(self, tool_name: str, kind: str, resolved_value: str) -> None:
        if not resolved_value:
            return
        allowed_values = self.tool_kind_values(tool_name, kind)
        if not allowed_values:
            raise CatchballError(f"{tool_name} does not support {kind}: {resolved_value}")
        if not self.resolve_supported_value(kind, resolved_value, allowed_values):
            raise CatchballError(f"Unsupported {kind} for {tool_name}: {resolved_value}")

    def validate_role_tool(self, role: RoleSettings) -> None:
        if role.tool not in TOOLS:
            raise CatchballError(f"Unknown tool: {role.tool}")
        self.resolve_tool_for_role(role)

    def validate_role_instructions(self, role_name: str, role: RoleSettings) -> None:
        if role.instructions_file is None:
            return
        resolved_path = self.make_absolute_path(role.instructions_file)
        if not resolved_path.is_file():
            raise CatchballError(f"{role_name} instructions file not found: {role.instructions_file}")
        role.instructions_file = resolved_path

    def resolve_tool_for_role(self, role: RoleSettings) -> None:
        if role.binary_path:
            return
        command_name = self.tool_name_from_spec(TOOLS[role.tool].spec)
        binary_path = shutil.which(command_name, path=self.env.get("PATH"))
        if not binary_path:
            raise CatchballError(f"Required tool not found on PATH: {role.tool}")
        role.binary_path = binary_path

    def tool_kind_values(self, tool_name: str, kind: str) -> list[str]:
        for token in self.split_spec_parts(TOOLS[tool_name].spec):
            if not self.is_spec_choice(token):
                continue
            token_kind, _, values = self.parse_spec_choice(token)
            if token_kind == kind:
                return values
        return []

    def resolve_role_value(self, role_name: str, role: RoleSettings, kind: str) -> str:
        value = getattr(role, kind) or TOOLS[role.tool].defaults_for(role_name).get(kind, "")
        return normalize_role_value(kind, value)

    def role_settings(self, role_name: str) -> RoleSettings:
        return getattr(self.settings, role_name)

    def role_has_configuration(self, role_name: str) -> bool:
        role = self.role_settings(role_name)
        return bool(role.model or role.effort or role.mode or role.instructions_file is not None or role.extra_args)

    def configured_role_names(self) -> tuple[str, ...]:
        if self.settings.fixer.tool:
            return ("worker", "fixer", "reviewer")
        return REQUIRED_ROLE_NAMES

    def implementation_role_name(self, has_review_feedback: bool) -> str:
        if has_review_feedback and self.settings.fixer.tool:
            return "fixer"
        return "worker"

    def fixer_label(self, *, log: bool = False) -> str:
        if self.settings.fixer.tool:
            return self.settings.fixer.tool
        return "worker" if log else f"{self.settings.worker.tool} (fallback worker)"

    def role_instructions_file(self, role_name: str) -> Path | None:
        role = self.role_settings(role_name)
        if role.instructions_file is not None:
            candidate = self.make_absolute_path(role.instructions_file)
        else:
            candidate = self.root_dir / f"{role_name.upper()}.md"
        return candidate if candidate.is_file() else None

    def role_instruction_entries(self, role_name: str) -> list[tuple[str, Path]]:
        entries: list[tuple[str, Path]] = []
        seen_paths: set[Path] = set()

        if role_name == "fixer":
            worker_file = self.role_instructions_file("worker")
            if worker_file is not None:
                entries.append(("fixer base instructions", worker_file))
                seen_paths.add(worker_file)

        role_file = self.role_instructions_file(role_name)
        if role_file is not None and role_file not in seen_paths:
            entries.append((f"{role_name} instructions", role_file))
        return entries

    def role_instruction_lines(self, role_name: str) -> list[str]:
        instruction_entries = self.role_instruction_entries(role_name)
        if not instruction_entries:
            return []
        action = "review" if role_name == "reviewer" else "implementation"
        lines = [""]
        for label, instructions_file in instruction_entries:
            lines.append(f"Additional {label} are in {instructions_file}.")
            lines.append(f"Read and follow that file before continuing the {action}.")
        return lines

    def permission_signal_lines(self) -> list[str]:
        return [
            "",
            "If a real permission or access denial blocks required work, include this exact marker in your final response: permission denied <path-or-tool>.",
            "Use that marker only for an actual blocking permission problem, not for unrelated warnings.",
        ]

    def split_spec_parts(self, spec: str) -> list[str]:
        parts: list[str] = []
        current: list[str] = []
        depth = 0
        for char in spec:
            if char == "[":
                if depth == 0 and current:
                    parts.append("".join(current))
                    current = []
                current.append(char)
                depth += 1
            elif char == "]":
                current.append(char)
                if depth > 0:
                    depth -= 1
                if depth == 0:
                    parts.append("".join(current))
                    current = []
            elif char in {" ", "\t"}:
                if depth > 0:
                    current.append(char)
                elif current:
                    parts.append("".join(current))
                    current = []
            else:
                current.append(char)
        if current:
            parts.append("".join(current))
        return parts

    def is_spec_choice(self, token: str) -> bool:
        return token.startswith("[") and token.endswith("]")

    def parse_spec_choice(self, token: str) -> tuple[str, str, list[str]]:
        inner = token[1:-1]
        first_colon = inner.find(":")
        last_colon = inner.rfind(":")
        if first_colon == -1 or first_colon == last_colon:
            raise CatchballError(f"Invalid tool spec token: {token}")
        kind = inner[:first_colon]
        render_text = inner[first_colon + 1 : last_colon]
        values = inner[last_colon + 1 :]
        if not kind or not render_text:
            raise CatchballError(f"Invalid tool spec token: {token}")
        return kind, render_text, values.split("|")

    def tool_name_from_spec(self, spec: str) -> str:
        parts = self.split_spec_parts(spec)
        if not parts:
            raise CatchballError("Empty tool spec")
        return parts[0]

    def resolve_supported_value(self, kind: str, value: str, supported_values: Sequence[str]) -> str:
        normalized_value = normalize_role_value(kind, value)
        if not normalized_value:
            return ""
        for supported_value in supported_values:
            if kind == "model" and supported_value == "provider/model" and "/" in normalized_value:
                return value.strip()
            if normalize_role_value(kind, supported_value) == normalized_value:
                return supported_value
        return ""

    def prepare_launch_command(self, binary_path: str, args: list[str]) -> list[str]:
        if os.name == "nt" and Path(binary_path).suffix.lower() in WINDOWS_BATCH_SUFFIXES:
            normalized_args = [arg.replace("\r\n", "\n").replace("\n", " ") for arg in args]
            return [binary_path, *normalized_args]
        return [binary_path, *args]

    def lock_file_value(self, lock_path: Path, key_name: str) -> str:
        if not lock_path.is_file():
            return ""
        try:
            lines = lock_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            return ""
        for line in lines:
            key, separator, value = line.partition("=")
            if separator and key == key_name:
                return value
        return ""

    def lock_holder_is_alive(self, lock_path: Path) -> bool:
        pid_text = self.lock_file_value(lock_path, "pid")
        host_name = self.lock_file_value(lock_path, "host")
        started_at_text = self.lock_file_value(lock_path, "started_at")
        if not host_name:
            owner_text = self.lock_file_value(lock_path, "owner")
            if owner_text:
                host_name = owner_text.split("@", 1)[-1].split(":", 1)[0]
        if host_name != self.host_name or not pid_text.isdigit():
            return False
        try:
            process = psutil.Process(int(pid_text))
            process_create_time = process.create_time()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, ValueError):
            return False
        try:
            started_at = float(started_at_text)
        except ValueError:
            return True
        return process_create_time <= started_at + LOCK_PROCESS_START_TOLERANCE_SECONDS

    def console_lines_for_event(self, line: str, event: str, task: str, message: str) -> list[str]:
        if event == "RUN_DONE":
            return [self.colorize("summary", f"{self.console_glyphs.summary} {line}")]
        if event == "RUN_STOP":
            return [self.colorize("stop", f"{self.console_glyphs.stop} {line}")]
        if task == "-":
            return [line]

        if event not in {
            "RUN",
            "RUN_FIX",
            "REVIEW",
            "REVIEW_FAIL",
            "PASS",
            "TASK_TIMING",
            "FAIL_CONTINUE",
            "SKIP_DONE",
            "STOP_LOCKED",
            "STOP_BLOCKED",
            "STOP_FAIL",
            "STOP_STALL",
            "STOP_ERROR",
            "PHASE_DELAY",
            "STALE_LOCK_CLEARED",
        }:
            return [line]

        lines = self.console_task_header(task)
        if event in {"RUN", "RUN_FIX"}:
            role_name = self.message_value(message, "role") or ("fixer" if event == "RUN_FIX" else "worker")
            round_number = self.message_value(message, "round") or "?"
            lines.append(self.format_role_line(role_name, f"round {round_number}"))
            return lines
        if event == "REVIEW":
            review_pass = self.message_value(message, "pass") or "?"
            lines.append(self.format_role_line("reviewer", f"pass {review_pass}/{self.settings.review_passes}"))
            return lines
        if event == "REVIEW_FAIL":
            lines.append(self.format_status_line("issues", self.console_glyphs.issues, "issues found"))
            return lines
        if event == "PASS":
            lines.append(self.format_status_line("clean", self.console_glyphs.clean, "clean -> done"))
            return lines
        if event == "TASK_TIMING":
            lines.append(self.format_notice_line("  ", "summary", self.console_glyphs.summary, line))
            return lines
        if event == "FAIL_CONTINUE":
            lines.append(self.format_notice_line("    ", "stop", self.console_glyphs.stop, f"{self.failure_reason_text(message)} -> failed"))
            lines.append(self.format_notice_line("  ", "skip", self.console_glyphs.skip, "continuing to next task"))
            return lines
        if event == "SKIP_DONE":
            lines.append(self.format_notice_line("  ", "skip", self.console_glyphs.skip, "already done"))
            return lines
        if event == "STOP_LOCKED":
            lines.append(self.format_notice_line("  ", "stop", self.console_glyphs.stop, line))
            lock_path = self.message_quoted_value(message, "lock_path")
            if lock_path:
                lines.append(self.format_notice_line("    ", "wait", self.console_glyphs.wait, f"delete {lock_path} to run anyway"))
            timeout_remaining = self.message_value(message, "timeout_remaining")
            if timeout_remaining.isdigit() and int(timeout_remaining) > 0:
                lines.append(
                    self.format_notice_line(
                        "    ",
                        "wait",
                        self.console_glyphs.wait,
                        f"if the old run is gone, catchball will clear it automatically in about {self.format_duration(int(timeout_remaining))}",
                    )
                )
            return lines
        if event == "STOP_BLOCKED":
            role_name = self.role_name_from_status(message, "_write_blocked") or "role"
            lines.append(self.format_notice_line("    ", "stop", self.console_glyphs.stop, f"{role_name} write blocked -> stop"))
            return lines
        if event == "STOP_FAIL":
            lines.append(self.format_notice_line("    ", "stop", self.console_glyphs.stop, "review passes exhausted -> stop"))
            return lines
        if event == "STOP_STALL":
            role_name = self.role_name_from_status(message, "_stalled") or "role"
            lines.append(self.format_notice_line("    ", "stop", self.console_glyphs.stop, f"{role_name} stalled -> stop"))
            return lines
        if event == "STOP_ERROR":
            role_name = self.role_name_from_status(message, "_failed") or "role"
            lines.append(self.format_notice_line("    ", "stop", self.console_glyphs.stop, f"{role_name} failed -> stop"))
            return lines
        if event == "PHASE_DELAY":
            delay_seconds = self.message_value(message, "seconds") or "?"
            target_role = (self.message_value(message, "to") or "next step").replace("-", " ")
            lines.append(self.format_notice_line("    ", "wait", self.console_glyphs.wait, f"wait {delay_seconds}s before {target_role}"))
            return lines
        if event == "STALE_LOCK_CLEARED":
            lines.append(self.format_notice_line("  ", "stale", self.console_glyphs.stale, "stale lock cleared"))
            return lines
        return [line]

    def console_task_header(self, task: str) -> list[str]:
        lines: list[str] = []
        if self.current_display_task != task:
            if self.current_display_task is not None:
                lines.append("")
            lines.append(f"{self.colorize('task', f'{self.console_glyphs.task} task:')} {task}")
            self.current_display_task = task
        return lines

    def format_role_line(self, role_name: str, detail: str) -> str:
        tool_name = self.role_tool_name(role_name)
        return (
            f"  {self.colorize(role_name, self.role_glyph(role_name))} "
            f"{self.colorize(role_name, role_name)} "
            f"{self.colorize('subtle', '(')}"
            f"{self.colorize(role_name, tool_name)}"
            f"{self.colorize('subtle', f' - {detail})')}"
        )

    def format_status_line(self, style_name: str, glyph: str, text: str) -> str:
        return self.format_notice_line("    ", style_name, glyph, text)

    def format_notice_line(self, indent: str, style_name: str, glyph: str, text: str) -> str:
        return f"{indent}{self.colorize(style_name, glyph)} {self.colorize(style_name, text)}"

    def role_glyph(self, role_name: str) -> str:
        return getattr(self.console_glyphs, role_name, self.console_glyphs.worker)

    def role_tool_name(self, role_name: str) -> str:
        role = self.role_settings(role_name)
        if role.tool:
            return role.tool
        if role_name == "fixer":
            return self.settings.worker.tool
        return role_name

    def message_value(self, message: str, key_name: str) -> str:
        prefix = f"{key_name}="
        for token in message.split():
            if token.startswith(prefix):
                return token[len(prefix) :]
        return ""

    def message_quoted_value(self, message: str, key_name: str) -> str:
        match = re.search(rf'{re.escape(key_name)}=("(?:[^"\\]|\\.)*")', message)
        if not match:
            return ""
        try:
            return str(json.loads(match.group(1)))
        except json.JSONDecodeError:
            return match.group(1).strip('"')

    def role_name_from_status(self, message: str, suffix: str) -> str:
        first_token = message.split(" ", 1)[0]
        if first_token.endswith(suffix):
            return first_token[: -len(suffix)]
        return ""

    def failure_reason_text(self, message: str) -> str:
        if message.startswith("review_passes_exhausted"):
            return "review passes exhausted"
        role_name = self.role_name_from_status(message, "_write_blocked")
        if role_name:
            return f"{role_name} write blocked"
        role_name = self.role_name_from_status(message, "_stalled")
        if role_name:
            return f"{role_name} stalled"
        role_name = self.role_name_from_status(message, "_failed")
        if role_name:
            return f"{role_name} failed"
        return "task failed"

    def choose_console_glyphs(self) -> ConsoleGlyphs:
        if self.env.get("CATCHBALL_ASCII"):
            return ASCII_GLYPHS
        encoding = (getattr(self.stdout, "encoding", "") or "").lower()
        if "utf" in encoding or "65001" in encoding:
            return UNICODE_GLYPHS
        return ASCII_GLYPHS

    def display_run_path(self, path: Path | None) -> str:
        if path is None or self.state_dir is None:
            return str(path)
        try:
            relative_path = path.relative_to(self.state_dir)
        except ValueError:
            return str(path)
        relative_text = str(relative_path)
        if not relative_path.parts or relative_text == ".":
            return "<run-dir>"
        return f"<run-dir>{os.sep}{relative_text}"

    def display_artifact_path(self, path: Path | None) -> str:
        if path is None:
            return "None"
        run_path = self.display_run_path(path)
        if run_path != str(path):
            return run_path
        try:
            relative_path = path.relative_to(self.root_dir)
        except ValueError:
            return str(path)
        relative_text = str(relative_path)
        if not relative_path.parts or relative_text == ".":
            return "<project-root>"
        return f"<project-root>{os.sep}{relative_text}"

    def display_task_state_path(self, path: Path | None) -> str:
        if path is None:
            return "None"
        try:
            relative_path = path.relative_to(self.settings.tasks_dir)
        except ValueError:
            return self.display_artifact_path(path)
        relative_text = str(relative_path)
        if not relative_path.parts or relative_text == ".":
            return "<tasks-dir>"
        return f"<tasks-dir>{os.sep}{relative_text}"

    def choose_console_color(self) -> bool:
        color_mode = self.env.get("CATCHBALL_COLOR", "").strip().lower()
        if color_mode in {"1", "true", "yes", "always"}:
            return True
        if color_mode in {"0", "false", "no", "never"}:
            return False
        if self.env.get("NO_COLOR"):
            return False
        is_tty = getattr(self.stdout, "isatty", None)
        if not callable(is_tty) or not is_tty():
            return False
        term_name = self.env.get("TERM", "").lower()
        if term_name == "dumb":
            return False
        if os.name != "nt":
            return True
        return bool(
            self.env.get("WT_SESSION")
            or self.env.get("ANSICON")
            or self.env.get("COLORTERM")
            or self.env.get("ConEmuANSI", "").upper() == "ON"
            or self.env.get("TERM_PROGRAM", "").lower() == "vscode"
            or term_name.startswith(("xterm", "screen", "tmux", "vt100", "cygwin"))
        )

    def choose_console_live_status(self) -> bool:
        is_tty = getattr(self.stdout, "isatty", None)
        if not callable(is_tty) or not is_tty():
            return False
        return self.env.get("TERM", "").lower() != "dumb"

    def colorize(self, style_name: str, text: str) -> str:
        if not self.console_color:
            return text
        style = ANSI_STYLES.get(style_name, "")
        if not style:
            return text
        return f"{style}{text}{ANSI_RESET}"

    def spinner_frame(self) -> str:
        frames = ASCII_SPINNER_FRAMES if self.console_glyphs is ASCII_GLYPHS else UNICODE_SPINNER_FRAMES
        frame = frames[self.live_status_index % len(frames)]
        self.live_status_index += 1
        return frame

    def ensure_live_status_thread(self) -> None:
        if not self.console_live_status or self.live_status_thread is not None:
            return
        stop_event = threading.Event()

        def refresh_live_status() -> None:
            while not stop_event.wait(LIVE_STATUS_REFRESH_SECONDS):
                with self.live_status_lock:
                    if not self.live_status_message:
                        continue
                    self.write_live_status_locked(self.live_status_message)

        thread = threading.Thread(target=refresh_live_status, name="catchball-live-status", daemon=True)
        thread.start()
        self.live_status_stop_event = stop_event
        self.live_status_thread = thread

    def stop_live_status_thread(self) -> None:
        if self.live_status_stop_event is not None:
            self.live_status_stop_event.set()
        if self.live_status_thread is not None:
            self.live_status_thread.join(timeout=1)
        self.live_status_stop_event = None
        self.live_status_thread = None

    def write_live_status_locked(self, message: str) -> None:
        rendered = f"{self.spinner_frame()} {message}"
        self.live_status_width = max(self.live_status_width, len(rendered))
        self.stdout.write("\r" + rendered.ljust(self.live_status_width))
        self.stdout.flush()

    def format_bytes(self, byte_count: int) -> str:
        units = ("B", "KB", "MB", "GB")
        value = float(byte_count)
        for unit in units:
            if value < 1024 or unit == units[-1]:
                if unit == "B":
                    return f"{int(value)} {unit}"
                return f"{value:.1f} {unit}"
            value /= 1024
        return f"{int(byte_count)} B"

    def format_duration(self, seconds: int | float) -> str:
        total_seconds = max(0, int(round(seconds)))
        hours, remainder = divmod(total_seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        if hours:
            return f"{hours}h{minutes:02d}m{secs:02d}s"
        if minutes:
            return f"{minutes}m{secs:02d}s"
        return f"{secs}s"

    def emit_live_status(self, message: str) -> None:
        if not self.console_live_status:
            return
        self.ensure_live_status_thread()
        with self.live_status_lock:
            self.live_status_message = message
            self.write_live_status_locked(message)

    def clear_live_status(self) -> None:
        if not self.console_live_status:
            return
        with self.live_status_lock:
            self.live_status_message = ""
            if self.live_status_width <= 0:
                return
            self.stdout.write("\r" + (" " * self.live_status_width) + "\r")
            self.stdout.flush()
            self.live_status_width = 0

    def role_health_activity_text(self, health_status: str, idle_for: int, output_bytes: int) -> str:
        if health_status == "starting":
            return "starting"
        if health_status == "active":
            return "doing work"
        if output_bytes == 0:
            return f"waiting on first model response ({idle_for}s quiet)"
        if idle_for < max(self.settings.role_health_check_interval * 2, 30):
            return f"waiting on model response ({idle_for}s quiet)"
        return f"still waiting on model response ({idle_for}s quiet)"

    def emit_role_health_status(self, role_name: str, health_status: str, started_at: int, idle_for: int, output_bytes: int) -> None:
        if not self.console_live_status:
            return
        role_tool = self.role_tool_name(role_name)
        elapsed = max(0, self.now() - started_at)
        activity = self.role_health_activity_text(health_status, idle_for, output_bytes)
        self.emit_live_status(
            f"{role_name} ({role_tool}) health ok | alive {elapsed}s | {activity} | {self.format_bytes(output_bytes)} log"
        )

    def emit(self, message: str = "") -> None:
        if not self.console_live_status:
            print(message, file=self.stdout, flush=True)
            return
        with self.live_status_lock:
            self.live_status_message = ""
            if self.live_status_width > 0:
                self.stdout.write("\r" + (" " * self.live_status_width) + "\r")
                self.stdout.flush()
                self.live_status_width = 0
            print(message, file=self.stdout, flush=True)

    def append_text(self, file_path: Path, text: str) -> None:
        with file_path.open("a", encoding="utf-8") as handle:
            handle.write(text)

    def output_state(self, file_path: Path) -> tuple[int, int]:
        if not file_path.is_file():
            return (0, 0)
        stats = file_path.stat()
        return (stats.st_size, int(stats.st_mtime))

    def sanitize_name(self, value: str) -> str:
        value = value.replace("\\", "/").replace("/", "-")
        value = re.sub(r"\s+", "-", value)
        value = re.sub(r"[^A-Za-z0-9._-]+", "-", value)
        value = re.sub(r"^-+", "", value)
        value = re.sub(r"-+$", "", value)
        value = re.sub(r"-+", "-", value)
        return value

    def make_absolute_path(self, path: Path) -> Path:
        if path.is_absolute():
            return path.resolve(strict=False)
        return (self.root_dir / path).resolve(strict=False)

    def abs_dir(self, path: Path) -> Path:
        candidate = self.make_absolute_path(path)
        if not candidate.is_dir():
            raise CatchballError(f"Tasks directory not found: {path}")
        return candidate.resolve()

    def now(self) -> int:
        return int(time.time())

    def timestamp(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _git(self, *args: str) -> subprocess.CompletedProcess | None:
        try:
            return subprocess.run(["git", *args], cwd=self.root_dir, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False)
        except FileNotFoundError:
            return None

    def is_inside_git_worktree(self) -> bool:
        result = self._git("rev-parse", "--is-inside-work-tree")
        return result is not None and result.returncode == 0

    def is_git_worktree_clean(self) -> bool:
        result = self._git("status", "--porcelain")
        return result is None or not result.stdout.strip()

    def git_dirty_files(self) -> frozenset[str]:
        result = self._git("diff", "HEAD", "--name-only")
        if result is None or result.returncode != 0:
            return frozenset()
        return frozenset(line.strip() for line in result.stdout.splitlines() if line.strip())

    def git_diff_stat(self, exclude: frozenset[str] = frozenset()) -> str:
        result = self._git("diff", "HEAD", "--stat")
        if result is None or result.returncode != 0:
            return ""
        if not exclude:
            return result.stdout.strip()
        # Keep only lines that mention a file not in the pre-task baseline.
        # The summary line (e.g. "3 files changed, ...") is unconditionally excluded
        # and recomputed from the kept lines — but generating a new summary is complex,
        # so we just drop it and return the per-file lines only.
        kept: list[str] = []
        for line in result.stdout.splitlines():
            # Per-file lines look like " path/to/file | 12 +++---"
            if "|" not in line:
                continue
            file_part = line.split("|")[0].strip()
            if file_part not in exclude:
                kept.append(line)
        return "\n".join(kept)


def install_signal_handlers(runner: CatchballRunner) -> Callable[[], None]:
    previous_handlers: dict[int, object] = {}

    def handler(signum: int, frame: object) -> None:
        del frame
        runner.cleanup()
        exit_code = 130 if signum == signal.SIGINT else 143
        raise SystemExit(exit_code)

    signals = [signal.SIGINT]
    if hasattr(signal, "SIGTERM"):
        signals.append(signal.SIGTERM)

    for handled_signal in signals:
        previous_handlers[handled_signal] = signal.getsignal(handled_signal)
        signal.signal(handled_signal, handler)

    def restore() -> None:
        for handled_signal, previous_handler in previous_handlers.items():
            signal.signal(handled_signal, previous_handler)

    return restore

def main(argv: Sequence[str] | None = None, *, root_dir: str | Path | None = None, env: dict[str, str] | None = None, stdout: IO[str] | None = None, stderr: IO[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    out_stream = stdout or sys.stdout
    err_stream = stderr or sys.stderr

    try:
        settings = parse_cli(args)
    except CatchballHelp as exc:
        out_stream.write(f"{exc}\n")
        return 0
    except CatchballError as exc:
        print(str(exc), file=err_stream)
        return 1

    runner = CatchballRunner(settings, original_args=args, root_dir=root_dir, env=env, stdout=out_stream, stderr=err_stream)
    restore_handlers = install_signal_handlers(runner)

    try:
        return runner.run()
    except CatchballError as exc:
        print(str(exc), file=err_stream)
        return 1
    finally:
        restore_handlers()
        runner.cleanup()

if __name__ == "__main__":
    raise SystemExit(main())