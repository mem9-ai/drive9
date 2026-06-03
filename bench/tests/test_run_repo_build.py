import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / "bench" / "bin" / "run-repo-build.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("run_repo_build", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RepoBuildHarnessTest(unittest.TestCase):
    def setUp(self):
        self.runner = load_runner()

    def write_case(self, directory: Path) -> Path:
        case_path = directory / "case.json"
        case_path.write_text(
            json.dumps(
                {
                    "name": "mock",
                    "runs": 1,
                    "storages": ["native", "fuse"],
                    "repos": [
                        {
                            "id": "tiny",
                            "language": "go",
                            "url": "https://example.invalid/tiny.git",
                            "ref": "main",
                            "commit": "abc123",
                            "build_dir": ".",
                            "prewarm": ["echo prewarm"],
                            "build": ["echo build"],
                            "clean": ["bin", "*.out"],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return case_path

    def test_load_case_and_storage_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.runner.load_case(self.write_case(Path(tmp)))
            plan = self.runner.build_sample_plan(case, runs=2, storages=["native", "fuse"])
            got = [(run, repo.id, storage) for run, repo, storage in plan]
            self.assertEqual(
                got,
                [
                    (1, "tiny", "native"),
                    (1, "tiny", "fuse"),
                    (2, "tiny", "native"),
                    (2, "tiny", "fuse"),
                ],
            )

    def test_repo_major_sample_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            case_path = Path(tmp) / "case.json"
            raw = {
                "name": "mock",
                "runs": 1,
                "storages": ["native", "fuse"],
                "repos": [
                    {
                        "id": "a",
                        "language": "go",
                        "url": "https://example.invalid/a.git",
                        "ref": "main",
                        "build_dir": ".",
                        "prewarm": [],
                        "build": [],
                        "clean": [],
                    },
                    {
                        "id": "b",
                        "language": "go",
                        "url": "https://example.invalid/b.git",
                        "ref": "main",
                        "build_dir": ".",
                        "prewarm": [],
                        "build": [],
                        "clean": [],
                    },
                ],
            }
            case_path.write_text(json.dumps(raw), encoding="utf-8")
            case = self.runner.load_case(case_path)
            plan = self.runner.build_sample_plan(case, runs=2, storages=["native", "fuse"], repo_major=True)
            got = [(run, repo.id, storage) for run, repo, storage in plan]
            self.assertEqual(
                got,
                [
                    (1, "a", "native"),
                    (1, "a", "fuse"),
                    (2, "a", "native"),
                    (2, "a", "fuse"),
                    (1, "b", "native"),
                    (1, "b", "fuse"),
                    (2, "b", "native"),
                    (2, "b", "fuse"),
                ],
            )

    def test_filter_repos_preserves_requested_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            case_path = Path(tmp) / "case.json"
            raw = {
                "name": "mock",
                "runs": 1,
                "storages": ["native"],
                "repos": [
                    {
                        "id": "a",
                        "language": "go",
                        "url": "https://example.invalid/a.git",
                        "ref": "main",
                        "build_dir": ".",
                        "prewarm": [],
                        "build": [],
                        "clean": [],
                    },
                    {
                        "id": "b",
                        "language": "go",
                        "url": "https://example.invalid/b.git",
                        "ref": "main",
                        "build_dir": ".",
                        "prewarm": [],
                        "build": [],
                        "clean": [],
                    },
                ],
            }
            case_path.write_text(json.dumps(raw), encoding="utf-8")
            case = self.runner.load_case(case_path)
            filtered = self.runner.filter_repos(case, "b,a")
            self.assertEqual([repo.id for repo in filtered.repos], ["b", "a"])

    def test_run_one_command_writes_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            stdout = tmp_path / "out.log"
            stderr = tmp_path / "err.log"
            code = self.runner.run_one_command(
                [sys.executable, "-c", "print('ok')"],
                tmp_path,
                {},
                stdout,
                stderr,
            )
            self.assertEqual(code, 0)
            self.assertIn("ok", stdout.read_text(encoding="utf-8"))

    def test_fsync_tree_ignores_vanished_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            target = tmp_path / "tree"
            target.mkdir()
            vanished = target / "index.lock"
            vanished.write_text("temporary", encoding="utf-8")
            stdout = tmp_path / "out.log"
            stderr = tmp_path / "err.log"
            original = self.runner.fsync_one

            def fake_fsync(path: Path, *, directory: bool = False) -> None:
                if path == vanished:
                    raise FileNotFoundError(path)
                original(path, directory=directory)

            self.runner.fsync_one = fake_fsync
            try:
                code = self.runner.run_fsync_tree(target, stdout, stderr)
            finally:
                self.runner.fsync_one = original

            self.assertEqual(code, 0)
            self.assertIn("skipped vanished file", stderr.read_text(encoding="utf-8"))

    def test_safe_rmtree_refuses_outside_bench_home(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            bench_home = tmp_path / "bench"
            bench_home.mkdir()
            outside = tmp_path / "outside"
            outside.mkdir()
            with self.assertRaises(self.runner.BenchError):
                self.runner.safe_rmtree(outside, bench_home)

    def test_summarize_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp)
            events = [
                {
                    "type": "phase",
                    "repo": "tiny",
                    "language": "go",
                    "storage": "native",
                    "phase": "clone",
                    "status": "ok",
                    "duration_seconds": 1.0,
                },
                {
                    "type": "phase",
                    "repo": "tiny",
                    "language": "go",
                    "storage": "fuse",
                    "phase": "clone",
                    "status": "ok",
                    "duration_seconds": 2.0,
                },
            ]
            (result_dir / "events.jsonl").write_text(
                "\n".join(json.dumps(event) for event in events) + "\n",
                encoding="utf-8",
            )
            paths = self.runner.summarize_session(result_dir)
            self.assertTrue(paths["csv"].exists())
            summary = paths["markdown"].read_text(encoding="utf-8")
            self.assertIn("FUSE/native mean ratio 2.00x", summary)

    def test_load_completed_samples_for_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp)
            events = [
                {"type": "phase", "repo": "a", "storage": "native", "run": 1, "phase": "clone", "status": "ok"},
                {"type": "phase", "repo": "a", "storage": "native", "run": 1, "phase": "build", "status": "ok"},
                {"type": "phase", "repo": "b", "storage": "fuse", "run": 2, "phase": "clone", "status": "failed"},
                {"type": "phase", "repo": "c", "storage": "native", "run": 3, "phase": "clone", "status": "ok"},
            ]
            (result_dir / "events.jsonl").write_text(
                "\n".join(json.dumps(event) for event in events) + "\n",
                encoding="utf-8",
            )
            self.assertEqual(
                self.runner.load_completed_samples(result_dir),
                {(1, "a", "native"), (2, "b", "fuse")},
            )

    def test_cli_native_only_dry_run_outputs_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_path = self.write_case(tmp_path)
            bench_home = tmp_path / "bench-home"
            cmd = [
                sys.executable,
                str(RUNNER_PATH),
                "--case",
                str(case_path),
                "--bench-home",
                str(bench_home),
                "run",
                "--native-only",
                "--dry-run",
                "--no-resolve",
                "--skip-prewarm",
                "--runs",
                "1",
                "--session",
                "test-session",
            ]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            result_dir = bench_home / "results" / "test-session"
            self.assertTrue((result_dir / "events.jsonl").exists())
            self.assertTrue((result_dir / "summary.csv").exists())

    def test_cli_clone_only_skips_build(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_path = self.write_case(tmp_path)
            bench_home = tmp_path / "bench-home"
            cmd = [
                sys.executable,
                str(RUNNER_PATH),
                "--case",
                str(case_path),
                "--bench-home",
                str(bench_home),
                "run",
                "--native-only",
                "--dry-run",
                "--no-resolve",
                "--skip-prewarm",
                "--clone-only",
                "--runs",
                "1",
                "--session",
                "clone-only",
            ]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            events = [
                json.loads(line)
                for line in (bench_home / "results" / "clone-only" / "events.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            phases = [event["phase"] for event in events if event.get("type") == "phase"]
            self.assertEqual(phases, ["clone"])


if __name__ == "__main__":
    unittest.main()
