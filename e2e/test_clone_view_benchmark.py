import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch, Mock

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

    def test_summary_requires_both_verified_cases(self):
        rows = [{"case": case, "trial": 1, "clone_seconds": n, "durable_seconds": n,
                 "remount_verify_seconds": n, "verified": True}
                for case, n in (("main_no_layer", 2), ("layer_cow", 4))]
        self.assertEqual(bench.summary(rows, 1)["layer_over_main_durable_ratio"], 2)
        for incomplete in (rows[:1], rows + rows[:1], [dict(row, verified=False) for row in rows]):
            with self.assertRaises(bench.http.ProbeFailure):
                bench.summary(incomplete, 1)


if __name__ == "__main__":
    unittest.main()
