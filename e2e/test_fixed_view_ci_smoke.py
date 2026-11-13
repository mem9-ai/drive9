"""Check smoke aggregation using isolated scripts, with no live services."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parent
BASELINE = ["api", "cli", "object-store", "fuse-release-gate", "fuse-patch-storage-class",
            "git-ops", "git-workspace-ondemand", "pack", "fuse-crash-recovery",
            "fuse-supervision", "fuse-write-perf-budget"]


class FixedViewCIAggregationTest(unittest.TestCase):
    def run_smokes(self, enabled=None, fail_at=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            e2e = root / "e2e"
            e2e.mkdir()
            shutil.copyfile(SOURCE / "smoke-all.sh", e2e / "smoke-all.sh")
            for script in SOURCE.glob("*.sh"):
                if script.name == "smoke-all.sh":
                    continue
                (e2e / script.name).write_text('''#!/bin/bash
printf '%s\\n' "$0" >> "$CALLS"
if [ "$0" = "$FAIL_AT" ]; then exit 9; fi
''')
            calls = root / "calls"
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("RUN_", "DRIVE9_", "FUSE_"))}
            env.update(CALLS=str(calls), FAIL_AT=fail_at)
            if enabled is not None:
                env["RUN_FIXED_VIEW_SMOKE"] = enabled
            result = subprocess.run(["bash", "e2e/smoke-all.sh"], cwd=root, env=env,
                                    capture_output=True, text=True, timeout=10)
            return result, calls.read_text().splitlines() if calls.exists() else []

    def test_baseline_still_runs_without_incompatible_layer(self):
        result, calls = self.run_smokes()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), len(BASELINE))
        self.assertNotIn("e2e/layer-fs-smoke-test.sh", calls)
        self.assertIn("SKIP [layer-fs] requires paired server CI", result.stdout)

    def test_paired_selection_runs_layer_and_baseline(self):
        result, calls = self.run_smokes("1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), len(BASELINE) + 1)
        self.assertIn("e2e/layer-fs-smoke-test.sh", calls)

    def test_layer_failure_cannot_hide_behind_baseline(self):
        result, calls = self.run_smokes("1", "e2e/layer-fs-smoke-test.sh")
        self.assertEqual(result.returncode, 1)
        self.assertIn("1 failed", result.stdout)
        self.assertEqual(len(calls), len(BASELINE) + 1)

    def test_main_failure_still_fails_without_layer(self):
        result, calls = self.run_smokes(fail_at="e2e/api-smoke-test.sh")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), len(BASELINE))

    def test_invalid_selection_fails_before_running(self):
        result, calls = self.run_smokes("yes")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
