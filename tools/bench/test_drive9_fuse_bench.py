#!/usr/bin/env python3
"""Unit tests for drive9_fuse_bench.py."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import drive9_fuse_bench as bench


class Drive9FuseBenchTest(unittest.TestCase):
    def test_parse_target_requires_existing_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = bench.parse_target(f"tmp={tmp}")
            self.assertEqual(target.name, "tmp")
            self.assertEqual(target.path, Path(tmp).resolve())

        with self.assertRaises(Exception):
            bench.parse_target("bad=/does/not/exist")

        with self.assertRaises(Exception):
            bench.parse_target("/tmp")

    def test_summarize_runs_reports_median_and_p95(self) -> None:
        got = bench.summarize_runs([1.0, 5.0, 3.0], "throughput", "ops/s", [0.1, 0.2, 0.3])
        self.assertEqual(got["runs"], [1.0, 5.0, 3.0])
        self.assertEqual(got["median"], 3.0)
        self.assertEqual(got["p95"], 4.8)
        self.assertEqual(got["p95_method"], "linear_interpolation")
        self.assertFalse(got["p95_reliable"])
        self.assertEqual(got["sample_count"], 3)
        self.assertEqual(got["seconds"], [0.1, 0.2, 0.3])

    def test_render_summary_includes_targets_and_workloads(self) -> None:
        report = {
            "schema_version": bench.SCHEMA_VERSION,
            "version": "abc123",
            "host": "host",
            "environment": "local",
            "cache_state": "hot",
            "targets": {
                "tmp": {
                    "results": {
                        "stat": {
                            "median": 10.0,
                            "p95": 12.0,
                            "unit": "ops/s",
                        }
                    }
                }
            },
        }
        summary = bench.render_summary(report)
        self.assertIn("Drive9 benchmark summary", summary)
        self.assertIn("| tmp | stat | 10.000 | 12.000 | ops/s |", summary)

    def test_git_clone_args_default_no_local_avoids_shortcuts(self) -> None:
        got = bench.git_clone_args(Path("/src"), Path("/dst"), "no-local")
        self.assertEqual(got, ["git", "clone", "--quiet", "--no-local", "/src", "/dst"])

        got = bench.git_clone_args(Path("/src"), Path("/dst"), "local")
        self.assertEqual(got, ["git", "clone", "--quiet", "/src", "/dst"])

        with self.assertRaises(ValueError):
            bench.git_clone_args(Path("/src"), Path("/dst"), "bad")

    def test_build_report_minimal_run_has_required_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "report.json"
            summary = Path(tmp) / "summary.md"
            args = SimpleNamespace(
                server_url="http://127.0.0.1:9709",
                client_host="client",
                server_host="server",
                network_note="unit test",
                version="test-sha",
                out=str(out),
                summary_out=str(summary),
            )
            config = bench.BenchConfig(
                runs=1,
                cache_state="hot",
                environment="local",
                drop_caches_command="",
                small_count=2,
                stat_count=2,
                dir_large_count=2,
                large_mib=1,
                random_reads=2,
                macro_files=2,
                macro_total_mib=1,
                git_clone_mode="no-local",
                keep_workdirs=False,
            )
            report = bench.build_report(args, [bench.Target("tmp", Path(tmp))], config)
            out.write_text(json.dumps(report), encoding="utf-8")
            summary.write_text(bench.render_summary(report), encoding="utf-8")

            loaded = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(loaded["schema_version"], bench.SCHEMA_VERSION)
            self.assertEqual(loaded["version"], "test-sha")
            self.assertEqual(loaded["environment"], "local")
            self.assertEqual(loaded["cache_state"], "hot")
            self.assertEqual(loaded["params"]["git_clone_mode"], "no-local")
            self.assertIn("tmp:sequential_write", loaded["results"])
            self.assertIn("tmp:macro_git_clone", loaded["results"])
            self.assertEqual(loaded["results"]["tmp:macro_git_clone"]["metric"], "latency")
            self.assertEqual(loaded["results"]["tmp:macro_git_clone"]["unit"], "seconds")
            self.assertIn("tmp", loaded["targets"])
            self.assertIn("targets", loaded["mount_params"])
            self.assertIn("Drive9 benchmark summary", summary.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
