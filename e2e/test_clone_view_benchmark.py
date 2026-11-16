import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch, Mock
from contextlib import nullcontext

spec = importlib.util.spec_from_file_location("bench", Path(__file__).with_name("clone-view-benchmark.py"))
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class BenchmarkTests(unittest.TestCase):
    def test_real_git_clone_fixture_and_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, commit = bench.fixture(root / "source", 9)
            bench.command(["git", "-c", "protocol.file.allow=always", "clone", "--no-local",
                           (root / "source").as_uri(), str(root / "clone")])
            bench.verify(root / "clone", manifest, commit)
            (root / "clone" / next(iter(manifest))).write_text("corrupt")
            with self.assertRaises(bench.http.ProbeFailure):
                bench.verify(root / "clone", manifest, commit)
            other, other_commit = bench.fixture(root / "other", 9)
            self.assertEqual((manifest, commit), (other, other_commit))

    def test_invalid_fixture_count(self):
        with tempfile.TemporaryDirectory() as directory:
            for count in (0, 513):
                with self.assertRaises(bench.http.ProbeFailure):
                    bench.fixture(Path(directory) / "source", count)

    def test_failed_or_timed_out_command_rejects_sample(self):
        for result in (subprocess.CompletedProcess(["git"], 1, b"", b"private output"),
                       subprocess.TimeoutExpired("git", 1, output=b"private output")):
            kwargs = {"side_effect": result} if isinstance(result, Exception) else {"return_value": result}
            with patch.object(bench.subprocess, "run", **kwargs):
                with self.assertRaises(bench.http.ProbeFailure) as error:
                    bench.command(["git"])
                self.assertNotIn("private output", str(error.exception))

    def test_error_classification_does_not_echo_raw_output(self):
        self.assertEqual(bench.failure_categories(b"fatal: private-path: Permission denied"), "permission")
        self.assertEqual(bench.failure_categories(b"private-path and secret bytes"), "unclassified")
        self.assertEqual(bench.failure_categories(b"x" * 65536 + b"Permission denied"), "unclassified")
        for raw, category in ((b"private child died of signal 7", "git_sigbus"),
                              (b"fatal: mmap failed on private object", "mmap_failure"),
                              (b"fatal: invalid index-pack output: private", "invalid_pack")):
            self.assertEqual(bench.failure_categories(raw), category)

    def test_error_words_are_fixed_bounded_and_unordered(self):
        self.assertEqual(bench.failure_word_counts(b"fatal: cannot read /private/secret-name"),
                         {"cannot": 1, "fatal": 1, "read": 1})
        self.assertEqual(bench.failure_word_counts(b"secret unknown words"), {})
        self.assertEqual(bench.failure_word_counts(b"x" * 65536 + b" fatal"), {})
        self.assertEqual(bench.failure_word_counts(b"fatal " * 300), {"fatal": 255})

    def test_syscall_summary_is_bounded_and_allowlisted(self):
        raw = (b'123 openat(AT_FDCWD, "/private/.git/hooks/hook", 0) = -1 EIO (Input/output error)\n'
               b'123 <... close resumed>) = -1 EIO (Input/output error)\n'
               b'123 openat(AT_FDCWD, "/secret", 0) = -1 ENOENT (No such file)\n'
               b'123 private_operation("secret") = -1 EIO (Input/output error)\n')
        rows = bench.syscall_failure_counts(raw)
        self.assertEqual(rows, [
            {"syscall": "close", "errno": "EIO", "path_class": "other", "count": 1},
            {"syscall": "openat", "errno": "EIO", "path_class": "git_hooks", "count": 1}])
        self.assertEqual(bench.syscall_failure_counts(raw + b"x" * (4 << 20)), [])
        self.assertNotIn("private", str(rows))
        self.assertNotIn("secret", str(rows))

    def test_fault_mapping_classification_is_bounded_and_private(self):
        raw = (b'12 mmap(NULL, 4096, PROT_READ, MAP_PRIVATE, 3</private/.git/objects/pack/secret>, 0) = 0x1000\n'
               b'12 --- SIGBUS {si_signo=SIGBUS, si_code=BUS_ADRERR, si_addr=0x1010} ---\n'
               b'13 --- SIGBUS {si_signo=SIGBUS, si_code=PRIVATE_CODE, si_addr=0x1010} ---\n')
        rows = bench.signal_mapping_counts(raw)
        self.assertEqual(rows, [
            {"signal": "SIGBUS", "code": "BUS_ADRERR", "mapping_class": "git_objects", "count": 1},
            {"signal": "SIGBUS", "code": "unknown", "mapping_class": "unknown", "count": 1}])
        for private in ("private", "secret", "0x1010", "PRIVATE_CODE"):
            self.assertNotIn(private, str(rows))
        self.assertEqual(bench.signal_mapping_counts(raw + b"x" * (4 << 20)), [])
        self.assertEqual(bench.signal_mapping_counts(b"malformed or private input"), [])

    def test_existing_config_error_keeps_only_known_open_flags(self):
        rows = bench.syscall_failure_counts(
            b'123 openat(AT_FDCWD, "/private/.git/config", O_RDONLY|O_CLOEXEC) = -1 EEXIST (File exists)\n'
            b'123 openat(AT_FDCWD, "/private/.git/config.lock", O_WRONLY|O_CREAT|O_EXCL|PRIVATE_FLAG, 0666) = -1 EEXIST (File exists)\n'
            b'123 rename("/private/.git/config.lock", "/private/.git/config") = -1 EEXIST (File exists)\n')
        self.assertEqual(rows, [
            {"syscall": "openat", "errno": "EEXIST", "path_class": "git_config", "count": 1,
             "open_flags": ["O_RDONLY"]},
            {"syscall": "openat", "errno": "EEXIST", "path_class": "git_config", "count": 1,
             "open_flags": ["O_WRONLY", "O_CREAT", "O_EXCL"]},
            {"syscall": "rename", "errno": "EEXIST", "path_class": "git_config", "count": 1}])
        for private in ("private", "PRIVATE_FLAG", "O_CLOEXEC", "0666"):
            self.assertNotIn(private, str(rows))

    def test_config_diagnostic_is_read_only_bounded_and_view_bound(self):
        probe = Mock(root="/private-fixture")
        work = {"layer_id": "private-view"}
        probe.fs.side_effect = lambda path, selected: "/route" + path
        probe.request.side_effect = [
            (b"private-body", {"private-header": "secret"}, 200),
            (b"fixed view operation conflicts with current state secret", {}, 409),
            (b"", {}, 404), (b"invalid filesystem view record secret", {}, 500)]
        with patch("builtins.print") as printed:
            bench.diagnose_remote_config(probe, work)
        calls = probe.request.call_args_list
        self.assertEqual([call.args[0] for call in calls], ["HEAD", "GET", "HEAD", "GET"])
        for index, call in enumerate(calls):
            name = "config" if index < 2 else "config.lock"
            self.assertEqual(call.args[1], "/route/private-fixture/repo/.git/" + name)
            self.assertIs(call.kwargs["work"], work)
            self.assertEqual(call.kwargs["limit"], 4096)
            self.assertEqual(call.kwargs["timeout"], 10)
            self.assertEqual(call.kwargs["headers"], {"Range": "bytes=0-255"} if index % 2 else {})
            self.assertTrue(call.kwargs["with_status"])
        for call in probe.fs.call_args_list:
            self.assertIs(call.args[1], work)
        output = str(printed.call_args_list)
        for category in ("categories=ok", "categories=view_conflict", "categories=invalid_view"):
            self.assertIn(category, output)
        for private in ("private", "secret", "/route"):
            self.assertNotIn(private, output)

    def test_config_diagnostic_hides_failures_and_invalid_statuses(self):
        probe = Mock(root="/private-fixture")
        probe.request.side_effect = [OSError("private credential"), (b"secret", {}, "private-status"),
                                     (b"secret", {}, 600), (b"secret", {}, True)]
        with patch("builtins.print") as printed:
            bench.diagnose_remote_config(probe, {"layer_id": "private"})
        self.assertEqual(probe.request.call_count, 4)
        self.assertEqual(str(printed.call_args_list).count("unavailable"), 4)
        self.assertNotIn("private", str(printed.call_args_list))
        self.assertNotIn("secret", str(printed.call_args_list))
        probe.fs.side_effect = ValueError("private path")
        with patch("builtins.print") as printed:
            bench.diagnose_remote_config(probe, None)
        self.assertEqual(str(printed.call_args_list).count("unavailable"), 4)
        self.assertNotIn("private", str(printed.call_args_list))

    def test_config_diagnostic_preserves_clone_failure_before_cleanup(self):
        original = bench.http.ProbeFailure("original clone failure")
        for traced in (False, True):
            probe = Mock(root="/fixture")
            probe.fs.return_value = "/route"
            probe.request.side_effect = [None] + [OSError("private diagnostic failure")] * 4
            with patch.object(bench.http, "Probe", return_value=probe), \
                    patch.object(bench.mount, "wait_retained_accounting"), \
                    patch.object(bench, "mounted", return_value=nullcontext(Path("/mount"))), \
                    patch.object(bench, "clone_fixture", side_effect=original), \
                    patch("builtins.print"):
                with self.assertRaises(bench.http.ProbeFailure) as caught:
                    bench.sample("cli", "base", Path("/source"), Path("/manifest"),
                                 "layer_cow", 1, Path("/root"), trace_primary=traced)
            self.assertIs(caught.exception, original)
            self.assertEqual(probe.request.call_count, 5 if traced else 1)
            self.assertEqual(probe.method_calls[-1][0], "cleanup")

    def test_unmapped_or_replaced_ranges_do_not_name_stale_file(self):
        start = b'12 mmap(NULL, 4096, PROT_READ, MAP_PRIVATE, 3</private/.git/index>, 0) = 0x1000\n'
        fault = b'12 --- SIGBUS {si_signo=SIGBUS, si_code=BUS_ADRERR, si_addr=0x1010} ---\n'
        for event, category in ((b'12 munmap(0x1000, 4096) = 0\n', "unknown"),
                                (b'12 mmap(NULL, 4096, PROT_READ, MAP_PRIVATE, 4</other>, 0) = 0x1000\n', "other")):
            rows = bench.signal_mapping_counts(start + event + fault)
            self.assertEqual(rows[0]["mapping_class"], category)

    def test_only_successful_exec_discards_prior_address_space(self):
        start = b'12 mmap(NULL, 4096, PROT_READ, MAP_PRIVATE, 3</private/.git/objects/pack/secret>, 0) = 0x1000\n'
        fault = b'12 --- SIGBUS {si_signo=SIGBUS, si_code=BUS_ADRERR, si_addr=0x1010} ---\n'
        for event, category in ((b'12 execve("/private/git", [], []) = 0\n', "unknown"),
                                (b'12 execveat(AT_FDCWD, "/private/git", [], [], 0) = 0\n', "unknown"),
                                (b'12 <... execve resumed>) = 0\n', "unknown"),
                                (b'12 execve("/private/git", [], []) = -1 ENOENT (missing)\n', "git_objects")):
            with self.subTest(event=event):
                rows = bench.signal_mapping_counts(start + event + fault)
                self.assertEqual(rows[0]["mapping_class"], category)
        # The new image can establish its own independently observed mapping.
        rows = bench.signal_mapping_counts(start + b'12 execve("/git", [], []) = 0\n' + start.replace(b"/.git/objects/pack/secret", b"/.git/index") + fault)
        self.assertEqual(rows[0]["mapping_class"], "git_index")

    def test_diagnostic_timeout_kills_only_its_process_group(self):
        process = Mock(pid=1234)
        process.wait.side_effect = [subprocess.TimeoutExpired("strace", 60), 0]
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(bench.shutil, "which", return_value="/bin/strace"), \
                patch.object(bench.subprocess, "Popen", return_value=process) as popen, \
                patch.object(bench.os, "killpg") as kill, patch("builtins.print") as output:
            trace = Path(directory) / "trace"
            trace.write_bytes(b'123 openat(AT_FDCWD, "/private/.git/objects/secret", 0) = -1 EIO (Input/output error)\n')
            bench.diagnose_clone_failure(["git", "clone", "source", "diagnostic-repo"], trace)
            summary = str(output.call_args_list)
            self.assertIn("summarizing partial trace", summary)
            self.assertIn('"errno": "EIO"', summary)
            self.assertNotIn("private", summary)
            self.assertNotIn("secret", summary)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        kill.assert_called_once_with(1234, bench.signal.SIGKILL)
        self.assertEqual(process.wait.call_count, 2)

    def test_diagnostic_missing_tool_does_not_spawn(self):
        with patch.object(bench.shutil, "which", return_value=None), \
                patch.object(bench.subprocess, "Popen") as popen, patch("builtins.print"):
            bench.diagnose_clone_failure(["git"], Path("unused"))
        popen.assert_not_called()

    def test_diagnostic_never_turns_failed_clone_into_success(self):
        original = bench.http.ProbeFailure("original clone failed")
        for diagnostic_error in (None, OSError("private diagnostic failure")):
            with patch.object(bench, "command", side_effect=original) as command, \
                    patch.object(bench, "diagnose_clone_failure", side_effect=diagnostic_error) as diagnostic, \
                    patch("builtins.print"):
                with self.assertRaises(bench.http.ProbeFailure) as caught:
                    bench.clone_fixture(Path("/source"), Path("/mount/repo"), Path("/trace"))
            self.assertIs(caught.exception, original)
            command.assert_called_once()
            self.assertEqual(diagnostic.call_args.args[0][-1], "/mount/diagnostic-repo")

    def test_primary_diagnostic_traces_exactly_one_original_clone(self):
        for result in (0, 128, -9, None):
            with self.subTest(result=result), \
                    patch.object(bench, "command") as command, \
                    patch.object(bench, "diagnose_clone_failure", return_value=result) as diagnostic:
                args = (Path("/source"), Path("/mount/repo"), Path("/trace"))
                if result == 0:
                    bench.clone_fixture(*args, trace_primary=True)
                else:
                    with self.assertRaises(bench.http.ProbeFailure):
                        bench.clone_fixture(*args, trace_primary=True)
                command.assert_not_called()
                diagnostic.assert_called_once()
                self.assertEqual(diagnostic.call_args.args[0][-1], "/mount/repo")
                self.assertEqual(diagnostic.call_args.kwargs, {"timeout": 180})

    def test_diagnostic_mode_never_publishes_benchmark_completion(self):
        for fails in (False, True):
            with self.subTest(fails=fails), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "report.json"
                argv = ["benchmark", "--cli", bench.sys.executable, "--output", str(output), "--diagnose-clone"]
                result = {"case": "layer_cow", "trial": 1, "verified": True, "traced": True}
                exists = bench.os.path.exists
                with patch.object(bench.sys, "argv", argv), patch.object(bench.sys, "platform", "linux"), \
                        patch.object(bench.os.path, "exists", side_effect=lambda p: p == "/dev/fuse" or exists(p)), \
                        patch.dict(bench.os.environ, {"DRIVE9_BASE": "http://127.0.0.1:9009", "DRIVE9_API_KEY": ""}), \
                        patch.object(bench, "fixture", return_value=({}, "fixture-commit")), \
                        patch.object(bench, "command", return_value=b"git version fixture\n"), \
                        patch.object(bench, "sample", return_value=result,
                                     side_effect=bench.http.ProbeFailure("failed clone") if fails else None) as sample, \
                        patch.object(bench, "summary") as summary, patch("builtins.print") as printed:
                    if fails:
                        with self.assertRaises(bench.http.ProbeFailure):
                            bench.main()
                    else:
                        bench.main()
                    sample.assert_called_once()
                    self.assertEqual(sample.call_args.args[4:6], ("layer_cow", 1))
                    self.assertEqual(sample.call_args.kwargs, {"trace_primary": True})
                    summary.assert_not_called()
                    self.assertNotIn("BENCHMARK COMPLETE", str(printed.call_args_list))
                report = json.loads(output.read_text())
                self.assertFalse(report["complete"])
                self.assertTrue(report["diagnostic"])
                self.assertEqual(report.get("diagnostic_complete", False), not fails)
                self.assertNotIn("summary", report)

    def test_shell_diagnostic_flag_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, script in (("make", "#!/bin/sh\nexit 0\n"),
                                 ("python3", '#!/bin/sh\nprintf "%s\\n" "$@" > "$BENCH_ARGV_FILE"\n')):
                executable = root / name
                executable.write_text(script)
                executable.chmod(0o700)
            argv_file = root / "argv"
            env = dict(bench.os.environ, PATH=str(root) + ":" + bench.os.environ["PATH"],
                       CLONE_BENCH_OUTPUT=str(root / "report"), BENCH_ARGV_FILE=str(argv_file))
            for flag in ("0", "1", "invalid-private-value"):
                argv_file.unlink(missing_ok=True)
                result = subprocess.run(["bash", str(Path(__file__).with_name("clone-view-benchmark.sh"))],
                                        env=dict(env, CLONE_BENCH_DIAGNOSE=flag), capture_output=True, timeout=10)
                if flag in ("0", "1"):
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual("--diagnose-clone" in argv_file.read_text().splitlines(), flag == "1")
                else:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(argv_file.exists())
                    self.assertNotIn(b"private-value", result.stderr)

    def test_summary_requires_both_verified_cases(self):
        rows = [{"case": case, "trial": 1, "clone_seconds": n, "durable_seconds": n,
                 "remount_verify_seconds": n, "verified": True}
                for case, n in (("main_no_layer", 2), ("layer_cow", 4))]
        self.assertEqual(bench.summary(rows, 1)["layer_over_main_durable_ratio"], 2)
        for incomplete in (rows[:1], rows + rows[:1], [dict(row, verified=False) for row in rows],
                           [dict(row, traced=True) for row in rows]):
            with self.assertRaises(bench.http.ProbeFailure):
                bench.summary(incomplete, 1)


if __name__ == "__main__":
    unittest.main()
