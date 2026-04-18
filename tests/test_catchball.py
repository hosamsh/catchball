import io
import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import catchball


class DummyRunner:
    def cleanup(self) -> None:
        return


class CatchballTests(unittest.TestCase):
    def test_normalize_passthrough_args_accepts_dash_prefixed_value(self) -> None:
        self.assertEqual(
            catchball.normalize_passthrough_args(["--worker-arg", "--dangerously-skip-permissions"]),
            ["--worker-arg=--dangerously-skip-permissions"],
        )

    def test_normalize_passthrough_args_keeps_equals_form(self) -> None:
        self.assertEqual(
            catchball.normalize_passthrough_args(["--worker-arg=--dangerously-skip-permissions"]),
            ["--worker-arg=--dangerously-skip-permissions"],
        )

    def test_parse_cli_retries_sets_review_passes(self) -> None:
        settings = catchball.parse_cli(["--worker", "copilot", "--reviewer", "copilot", "--retries", "2"])
        self.assertEqual(settings.review_passes, 3)

    def test_parse_cli_rejects_conflicting_review_round_options(self) -> None:
        with self.assertRaises(catchball.CatchballError):
            catchball.parse_cli(
                [
                    "--worker",
                    "copilot",
                    "--reviewer",
                    "copilot",
                    "--review-passes",
                    "5",
                    "--retries",
                    "1",
                ]
            )

    def test_install_signal_handlers_skips_non_main_thread(self) -> None:
        errors: list[BaseException] = []

        def target() -> None:
            try:
                restore = catchball.install_signal_handlers(DummyRunner())
                restore()
            except BaseException as exc:  # pragma: no cover - failure path only
                errors.append(exc)

        thread = threading.Thread(target=target)
        thread.start()
        thread.join()
        self.assertEqual(errors, [])

    def test_discover_tasks_rejects_colliding_artifact_names(self) -> None:
        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            tasks_dir = temp_dir / "tasks"
            tasks_dir.mkdir()
            (tasks_dir / "foo bar.md").write_text("foo\n", encoding="utf-8")
            (tasks_dir / "foo-bar.md").write_text("bar\n", encoding="utf-8")
            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                    tasks_dir=tasks_dir,
                ),
                root_dir=temp_dir,
                env={},
            )
            with self.assertRaises(catchball.CatchballError):
                runner.discover_tasks()

    def test_initialize_state_places_task_state_under_runs_state(self) -> None:
        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            tasks_dir = temp_dir / "nested" / "tasks"
            tasks_dir.mkdir(parents=True)

            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                    tasks_dir=tasks_dir,
                    allow_dirty=True,
                ),
                root_dir=temp_dir,
                env={},
            )
            runner.initialize_state()

            self.assertEqual(runner.task_state_dir, temp_dir / "catchball-runs" / "state" / "nested--tasks")
            assert runner.task_state_dir is not None
            self.assertTrue(runner.task_state_dir.is_dir())

    def test_initialize_state_flattens_hidden_prefixed_tasks_path(self) -> None:
        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            tasks_dir = temp_dir / ".samples" / "sample-tasks" / "js-click-game"
            tasks_dir.mkdir(parents=True)

            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                    tasks_dir=tasks_dir,
                    allow_dirty=True,
                ),
                root_dir=temp_dir,
                env={},
            )
            runner.initialize_state()

            self.assertEqual(runner.task_state_dir, temp_dir / "catchball-runs" / "state" / "sample-tasks--js-click-game")
            assert runner.task_state_dir is not None
            self.assertTrue(runner.task_state_dir.is_dir())

    def test_initialize_state_migrates_legacy_task_state_directory(self) -> None:
        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            tasks_dir = temp_dir / "tasks"
            tasks_dir.mkdir()
            legacy_state_dir = tasks_dir / "catchball-state"
            legacy_state_dir.mkdir()
            legacy_done_file = legacy_state_dir / "010-task.md.done"
            legacy_done_file.write_text("done\n", encoding="utf-8")

            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                    tasks_dir=tasks_dir,
                    allow_dirty=True,
                ),
                root_dir=temp_dir,
                env={},
            )
            runner.initialize_state()

            migrated_done_file = temp_dir / "catchball-runs" / "state" / "tasks" / "010-task.md.done"
            self.assertFalse(legacy_state_dir.exists())
            self.assertTrue(migrated_done_file.is_file())
            self.assertEqual(migrated_done_file.read_text(encoding="utf-8"), "done\n")

    def test_initialize_state_migrates_mirrored_state_directory(self) -> None:
        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            tasks_dir = temp_dir / ".samples" / "sample-tasks" / "js-click-game"
            tasks_dir.mkdir(parents=True)
            mirrored_state_dir = temp_dir / "catchball-runs" / "state" / ".samples" / "sample-tasks" / "js-click-game"
            mirrored_state_dir.mkdir(parents=True)
            mirrored_done_file = mirrored_state_dir / "010-task.md.done"
            mirrored_done_file.write_text("done\n", encoding="utf-8")

            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                    tasks_dir=tasks_dir,
                    allow_dirty=True,
                ),
                root_dir=temp_dir,
                env={},
            )
            runner.initialize_state()

            migrated_done_file = temp_dir / "catchball-runs" / "state" / "sample-tasks--js-click-game" / "010-task.md.done"
            self.assertFalse(mirrored_state_dir.exists())
            self.assertTrue(migrated_done_file.is_file())
            self.assertEqual(migrated_done_file.read_text(encoding="utf-8"), "done\n")

    def test_validate_role_value_allows_future_model_names(self) -> None:
        runner = catchball.CatchballRunner(
            catchball.AppSettings(
                worker=catchball.RoleSettings(tool="copilot"),
                reviewer=catchball.RoleSettings(tool="copilot"),
            ),
            root_dir=".",
            env={},
        )
        runner.validate_role_value("copilot", "model", "gpt-6-preview")

    def test_render_tool_command_uses_stdin_prompt_for_codex(self) -> None:
        runner = catchball.CatchballRunner(
            catchball.AppSettings(
                worker=catchball.RoleSettings(tool="copilot"),
                reviewer=catchball.RoleSettings(tool="codex", model="gpt-5.4", effort="medium"),
            ),
            root_dir=".",
            env={},
        )

        command = runner.render_tool_command(
            "reviewer",
            runner.settings.reviewer,
            "Review the implementation against the task",
        )

        self.assertIn("--", command)
        self.assertEqual(command[-2:], ["--", "-"])
        self.assertEqual(
            runner.stdin_prompt_text("codex", "Review the implementation against the task"),
            "Review the implementation against the task",
        )

    def test_fit_live_status_text_truncates_to_terminal_width(self) -> None:
        runner = catchball.CatchballRunner(
            catchball.AppSettings(
                worker=catchball.RoleSettings(tool="copilot"),
                reviewer=catchball.RoleSettings(tool="copilot"),
            ),
            root_dir=".",
            env={"COLUMNS": "20"},
        )

        fitted = runner.fit_live_status_text("reviewer (copilot) health ok | alive 75s | doing work | 3.5 KB log")

        self.assertLessEqual(len(fitted), 19)
        self.assertTrue(fitted.endswith("..."))

    def test_raw_json_mode_line_is_tool_error_detects_copilot_launch_error_before_activity(self) -> None:
        runner = catchball.CatchballRunner(
            catchball.AppSettings(
                worker=catchball.RoleSettings(tool="copilot"),
                reviewer=catchball.RoleSettings(tool="copilot"),
            ),
            root_dir=".",
            env={},
        )

        self.assertTrue(
            runner.raw_json_mode_line_is_tool_error(
                "copilot",
                'Error: Model "gpt-5.3-Codex" from --model flag is not available.',
            )
        )
        self.assertFalse(runner.raw_json_mode_line_is_tool_error("copilot", "This is normal model output."))

    def test_raw_json_mode_line_is_tool_error_ignores_copilot_error_after_activity(self) -> None:
        runner = catchball.CatchballRunner(
            catchball.AppSettings(
                worker=catchball.RoleSettings(tool="copilot"),
                reviewer=catchball.RoleSettings(tool="copilot"),
            ),
            root_dir=".",
            env={},
        )

        self.assertFalse(
            runner.raw_json_mode_line_is_tool_error(
                "copilot",
                "Error: command returned exit code 1",
                saw_activity=True,
            )
        )

    def test_raw_json_mode_line_is_tool_error_ignores_other_json_tools(self) -> None:
        runner = catchball.CatchballRunner(
            catchball.AppSettings(
                worker=catchball.RoleSettings(tool="copilot"),
                reviewer=catchball.RoleSettings(tool="copilot"),
            ),
            root_dir=".",
            env={},
        )

        self.assertFalse(runner.raw_json_mode_line_is_tool_error("codex", "Error: model unavailable"))
        self.assertFalse(runner.raw_json_mode_line_is_tool_error("claude", "Error: model unavailable"))

    def test_role_output_has_tool_error_detects_marker(self) -> None:
        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            output_file = temp_dir / "role.log"
            output_file.write_text("[catchball-tool-error] Error: Model unavailable\n", encoding="utf-8")

            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                ),
                root_dir=temp_dir,
                env={},
            )

            self.assertTrue(runner.role_output_has_tool_error(output_file))

    def test_copilot_event_error_text_reads_structured_message(self) -> None:
        runner = catchball.CatchballRunner(
            catchball.AppSettings(
                worker=catchball.RoleSettings(tool="copilot"),
                reviewer=catchball.RoleSettings(tool="copilot"),
            ),
            root_dir=".",
            env={},
        )

        event = {
            "type": "error",
            "data": {
                "message": 'Model "gpt-5.3-Codex" from --model flag is not available.'
            },
        }

        self.assertEqual(
            runner.copilot_event_error_text(event),
            'Model "gpt-5.3-Codex" from --model flag is not available.',
        )

    def test_run_role_to_file_returns_tool_error_for_structured_copilot_error_event(self) -> None:
        class FakeProcess:
            def __init__(self, stdout_text: str) -> None:
                self.pid = os.getpid()
                self.stdout = io.StringIO(stdout_text)
                self.stdin = None

            def poll(self) -> int:
                return 0

            def wait(self, timeout: float | None = None) -> int:
                return 0

        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            output_file = temp_dir / "reviewer.log"
            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                ),
                root_dir=temp_dir,
                env={},
            )

            event_stream = json.dumps(
                {
                    "type": "error",
                    "data": {
                        "message": 'Model "gpt-5.3-Codex" from --model flag is not available.'
                    },
                }
            ) + "\n"

            def fake_spawn_role_process(role_name: str, prompt: str, destination: Path) -> catchball.RoleLaunch:
                process = FakeProcess(event_stream)
                thread = threading.Thread(
                    target=runner._copilot_json_filter_thread,
                    args=(process, destination),
                )
                thread.start()
                return catchball.RoleLaunch(process=process, output_thread=thread)

            runner.spawn_role_process = fake_spawn_role_process  # type: ignore[method-assign]

            status = runner.run_role_to_file("reviewer", "say hi", output_file, "demo")

            self.assertEqual(status, 74)
            contents = output_file.read_text(encoding="utf-8")
            self.assertIn("[catchball-tool-error]", contents)
            self.assertIn('Model "gpt-5.3-Codex" from --model flag is not available.', contents)

    def test_archive_active_round_feedback_pairs_review_and_response(self) -> None:
        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            tasks_dir = temp_dir / "tasks"
            tasks_dir.mkdir()
            task_file = tasks_dir / "010-task.md"
            task_file.write_text("task\n", encoding="utf-8")

            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                    tasks_dir=tasks_dir,
                    allow_dirty=True,
                ),
                root_dir=temp_dir,
                env={},
            )
            runner.initialize_state()

            assert runner.reviews_dir is not None
            assert runner.responses_dir is not None
            active_review = runner.task_sidecar(task_file, ".review", base_dir=runner.reviews_dir)
            active_response = runner.task_sidecar(task_file, ".response", base_dir=runner.responses_dir)
            active_review.write_text("R1: still broken\n", encoding="utf-8")
            active_response.write_text("R1: expected behavior because ...\n", encoding="utf-8")

            archived_review, archived_response = runner.archive_active_round_feedback(task_file)

            self.assertIsNotNone(archived_review)
            self.assertIsNotNone(archived_response)
            assert archived_review is not None
            assert archived_response is not None
            self.assertEqual(archived_review.name, "010-task.md.review.done.1")
            self.assertEqual(archived_response.name, "010-task.md.response.done.1")
            self.assertFalse(active_review.exists())
            self.assertFalse(active_response.exists())
            self.assertEqual(archived_review.read_text(encoding="utf-8"), "R1: still broken\n")
            self.assertEqual(archived_response.read_text(encoding="utf-8"), "R1: expected behavior because ...\n")

    def test_implementation_prompt_text_includes_response_contract(self) -> None:
        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            tasks_dir = temp_dir / "tasks"
            tasks_dir.mkdir()
            task_file = tasks_dir / "010-task.md"
            task_file.write_text("task\n", encoding="utf-8")

            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                    tasks_dir=tasks_dir,
                    allow_dirty=True,
                ),
                root_dir=temp_dir,
                env={},
            )
            runner.initialize_state()

            assert runner.reviews_dir is not None
            assert runner.responses_dir is not None
            review_file = runner.task_sidecar(task_file, ".review", base_dir=runner.reviews_dir)
            response_file = runner.task_sidecar(task_file, ".response", base_dir=runner.responses_dir)
            review_file.write_text("R1: still broken\n", encoding="utf-8")
            response_file.write_text("R1: leaving as-is\n", encoding="utf-8")

            prompt = runner.implementation_prompt_text(
                "fixer",
                task_file,
                review_file,
                response_file,
                diff_stat="catchball.py | 3 +-",
            )

            self.assertIn(str(response_file), prompt)
            self.assertIn("replace it with the current unresolved-issue response for this round", prompt)
            self.assertIn("Only include issues you are intentionally not fixing in this round", prompt)

    def test_reviewer_prompt_text_mentions_previous_response(self) -> None:
        with TemporaryDirectory() as temp_dir_text:
            temp_dir = Path(temp_dir_text)
            tasks_dir = temp_dir / "tasks"
            tasks_dir.mkdir()
            task_file = tasks_dir / "010-task.md"
            task_file.write_text("task\n", encoding="utf-8")

            runner = catchball.CatchballRunner(
                catchball.AppSettings(
                    worker=catchball.RoleSettings(tool="copilot"),
                    reviewer=catchball.RoleSettings(tool="copilot"),
                    tasks_dir=tasks_dir,
                    allow_dirty=True,
                ),
                root_dir=temp_dir,
                env={},
            )
            runner.initialize_state()

            assert runner.reviews_dir is not None
            assert runner.responses_dir is not None
            active_review = runner.task_sidecar(task_file, ".review", base_dir=runner.reviews_dir)
            previous_review = runner.task_sidecar(task_file, ".review.done.1", base_dir=runner.reviews_dir)
            previous_response = runner.task_sidecar(task_file, ".response.done.1", base_dir=runner.responses_dir)

            prompt = runner.reviewer_prompt_text(
                task_file,
                active_review,
                2,
                previous_review,
                previous_response,
                diff_stat="catchball.py | 2 ++",
            )

            self.assertIn(str(previous_review), prompt)
            self.assertIn(str(previous_response), prompt)
            self.assertIn("keep the same issue ID when practical", prompt)

    @unittest.skipUnless(os.name == "nt", "Windows-specific batch wrapper behavior")
    def test_prepare_launch_command_wraps_batch_files_with_cmd(self) -> None:
        runner = catchball.CatchballRunner(
            catchball.AppSettings(
                worker=catchball.RoleSettings(tool="copilot"),
                reviewer=catchball.RoleSettings(tool="copilot"),
            ),
            root_dir=".",
            env={"COMSPEC": "cmd.exe"},
        )
        command = runner.prepare_launch_command("tool.cmd", ["a&b"])
        self.assertEqual(command[:4], ["cmd.exe", "/d", "/v:off", "/s"])
        self.assertIn('"a&b"', command[-1])


if __name__ == "__main__":
    unittest.main()