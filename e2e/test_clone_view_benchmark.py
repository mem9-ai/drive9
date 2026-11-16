import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

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
