import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import compare_fuse_performance_metrics as compare


def metrics(rows_per_second=100.0, mib_per_second=50.0):
    return {
        "schema": "drive9-fuse-performance/v1",
        "generated_at_unix": 1.0,
        "params": {
            "small_files": 64,
            "small_bytes": 1024,
            "large_mb": 16,
            "large_bytes": 16 * 1024 * 1024,
            "read_passes": 1,
            "sqlite_rows": 256,
        },
        "workloads": {
            "small_file_write": {
                "mib_per_second": 1.0,
                "files_per_second": 10.0,
            },
            "small_file_read": {
                "mib_per_second": 2.0,
                "files_per_second": 20.0,
            },
            "large_file_write": {
                "mib_per_second": mib_per_second,
                "files_per_second": 4.0,
            },
            "large_file_reads": [
                {
                    "pass": 1,
                    "mib_per_second": mib_per_second,
                    "files_per_second": 4.0,
                }
            ],
            "sqlite_read_aggregate": {
                "rows_per_second": rows_per_second,
                "mib_per_second": 1.0,
                "integrity_check": "ok",
                "payload_verified_rows": 256,
            },
            "sqlite_insert_transaction": {
                "rows_per_second": rows_per_second,
                "mib_per_second": 0.0,
            },
            "sqlite_update_transaction": {
                "rows_per_second": rows_per_second,
                "mib_per_second": 0.0,
            },
            "sqlite_wal_read_aggregate": {
                "rows_per_second": rows_per_second,
                "mib_per_second": 1.0,
                "integrity_check": "ok",
                "payload_verified_rows": 256,
            },
            "sqlite_wal_insert_transaction": {
                "rows_per_second": rows_per_second,
                "mib_per_second": 0.0,
            },
            "sqlite_wal_update_transaction": {
                "rows_per_second": rows_per_second,
                "mib_per_second": 0.0,
            },
            "sqlite_wal_checkpoint_truncate": {
                "rows_per_second": rows_per_second,
                "checkpoint_busy": 0,
                "integrity_check": "ok",
            },
        },
    }


class FusePerformanceCompareTest(unittest.TestCase):
    def test_warning_only_regression_report(self):
        report = compare.compare_metrics(
            metrics(rows_per_second=60.0),
            metrics(rows_per_second=100.0),
            warning_ratio=0.30,
            current_ref="current",
            baseline_ref="baseline",
            missing_baseline_reason=None,
        )

        self.assertEqual(report["status"], "warning")
        self.assertTrue(report["warning_only"])
        regressed = [
            row for row in report["comparisons"]
            if row["workload"] == "sqlite_read_aggregate" and row["metric"] == "rows_per_second"
        ]
        self.assertEqual(len(regressed), 1)
        self.assertEqual(regressed[0]["status"], "regressed")

    def test_missing_baseline_still_validates_current_metrics(self):
        report = compare.compare_metrics(
            metrics(),
            None,
            warning_ratio=0.30,
            current_ref="current",
            baseline_ref=None,
            missing_baseline_reason="missing",
        )

        self.assertEqual(report["status"], "baseline_missing")
        self.assertEqual(report["baseline"]["missing_reason"], "missing")
        self.assertEqual(report["comparisons"], [])

    def test_expected_zero_byte_rate_metrics_do_not_warn(self):
        report = compare.compare_metrics(
            metrics(),
            metrics(),
            warning_ratio=0.30,
            current_ref="current",
            baseline_ref="baseline",
            missing_baseline_reason=None,
        )

        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["warnings"], [])

    def test_current_metrics_fail_closed_when_payload_verification_missing(self):
        current = metrics()
        del current["workloads"]["sqlite_read_aggregate"]["payload_verified_rows"]

        with self.assertRaises(compare.CompareError):
            compare.compare_metrics(
                current,
                None,
                warning_ratio=0.30,
                current_ref="current",
                baseline_ref=None,
                missing_baseline_reason="missing",
            )

    def test_current_metrics_fail_closed_when_required_workload_missing(self):
        current = metrics()
        del current["workloads"]["large_file_write"]

        with self.assertRaises(compare.CompareError):
            compare.compare_metrics(
                current,
                metrics(),
                warning_ratio=0.30,
                current_ref="current",
                baseline_ref="baseline",
                missing_baseline_reason=None,
            )

    def test_current_metrics_fail_closed_when_wal_workload_missing(self):
        current = metrics()
        del current["workloads"]["sqlite_wal_read_aggregate"]

        with self.assertRaises(compare.CompareError):
            compare.compare_metrics(
                current,
                metrics(),
                warning_ratio=0.30,
                current_ref="current",
                baseline_ref="baseline",
                missing_baseline_reason=None,
            )

    def test_current_metrics_fail_closed_when_wal_payload_verification_missing(self):
        current = metrics()
        del current["workloads"]["sqlite_wal_read_aggregate"]["payload_verified_rows"]

        with self.assertRaises(compare.CompareError):
            compare.compare_metrics(
                current,
                None,
                warning_ratio=0.30,
                current_ref="current",
                baseline_ref=None,
                missing_baseline_reason="missing",
            )

    def test_current_metrics_fail_closed_when_wal_checkpoint_busy(self):
        current = metrics()
        current["workloads"]["sqlite_wal_checkpoint_truncate"]["checkpoint_busy"] = 1

        with self.assertRaises(compare.CompareError):
            compare.compare_metrics(
                current,
                metrics(),
                warning_ratio=0.30,
                current_ref="current",
                baseline_ref="baseline",
                missing_baseline_reason=None,
            )

    def test_baseline_without_wal_metrics_warns_and_compares_existing_workloads(self):
        baseline = metrics()
        for workload in (
            "sqlite_wal_read_aggregate",
            "sqlite_wal_insert_transaction",
            "sqlite_wal_update_transaction",
            "sqlite_wal_checkpoint_truncate",
        ):
            del baseline["workloads"][workload]

        report = compare.compare_metrics(
            metrics(),
            baseline,
            warning_ratio=0.30,
            current_ref="current",
            baseline_ref="baseline",
            missing_baseline_reason=None,
        )

        self.assertEqual(report["status"], "warning")
        self.assertGreater(len(report["comparisons"]), 0)
        self.assertTrue(any("baseline missing workload sqlite_wal_read_aggregate" in warning for warning in report["warnings"]))

    def test_param_mismatch_is_warning_and_skips_comparison(self):
        current = metrics()
        baseline = metrics()
        baseline["params"]["large_mb"] = 32
        baseline["params"]["large_bytes"] = 32 * 1024 * 1024

        report = compare.compare_metrics(
            current,
            baseline,
            warning_ratio=0.30,
            current_ref="current",
            baseline_ref="baseline",
            missing_baseline_reason=None,
        )

        self.assertEqual(report["status"], "warning")
        self.assertEqual(report["comparisons"], [])
        self.assertTrue(any("params mismatch large_mb" in warning for warning in report["warnings"]))

    def test_manifest_derives_remote_metrics_path_from_legacy_manifest(self):
        manifest = {
            "archived_at": "2026-06-07T05:36:00Z",
            "branch": "dat9-dev2/fuse-performance-baseline",
            "commit_sha": "55468b8bedb2abcdef",
            "run_id": "27083815188",
            "run_attempt": "1",
            "files": [
                {"path": "mount-run.log"},
                {"path": "performance-metrics-run.json"},
            ],
        }

        remote_path = compare.metrics_remote_path_from_manifest(manifest, "/benchmarks/fuse-performance")

        self.assertEqual(
            remote_path,
            "/benchmarks/fuse-performance/2026/06/07/dat9-dev2-fuse-performance-baseline/55468b8bedb2/27083815188-1/performance-metrics-run.json",
        )

    def test_cli_writes_markdown_and_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            current = tmp_path / "current.json"
            baseline = tmp_path / "baseline.json"
            output_json = tmp_path / "report.json"
            output_md = tmp_path / "report.md"
            current.write_text(compare.json.dumps(metrics()) + "\n", encoding="utf-8")
            baseline.write_text(compare.json.dumps(metrics()) + "\n", encoding="utf-8")

            rc = compare.main([
                "compare",
                "--current", str(current),
                "--baseline", str(baseline),
                "--output-json", str(output_json),
                "--output-markdown", str(output_md),
                "--current-ref", "current",
                "--baseline-ref", "baseline",
            ])

            self.assertEqual(rc, 0)
            self.assertIn("drive9-fuse-performance-compare/v1", output_json.read_text(encoding="utf-8"))
            self.assertIn("FUSE Performance Compare", output_md.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
