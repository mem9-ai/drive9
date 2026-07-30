"""Unit tests for community.pjdfstest log parsing (no FUSE/mount required)."""

from __future__ import annotations

import unittest

from suites.community.pjdfstest.module import CommunityPjdfstest


class ParsePjdfstestLogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mod = CommunityPjdfstest()

    def test_pass_run_with_summary(self) -> None:
        text = """# 2026-07-26T15:03:18Z $ prove --recurse
All tests successful.
Test Summary Report
-------------------
/tmp/pjdfstest/tests/chown/00.t          (Wstat: 0 Tests: 1280 Failed: 0)
  TODO passed:   1054, 1058
Files=238, Tests=8798, 247 wallclock secs
Result: PASS
"""
        report = self.mod.parse(text, "", "log", rc=0)
        self.assertEqual(report["failed_cases"], 0)
        self.assertEqual(report["total_cases"], 8798)
        self.assertEqual(report["raw_pass_rate"], 1.0)
        self.assertNotIn("anomaly", report)

    def test_result_pass_with_failed_summaries_is_anomaly(self) -> None:
        # B2: never erase parsed failures solely because Result: PASS is present.
        text = """# 2026-07-26T15:03:18Z $ prove --recurse
/tmp/pjdfstest/tests/unlink/14.t         (Wstat: 0 Tests: 10 Failed: 2)
Files=1, Tests=10, 1 wallclock secs
Result: PASS
"""
        report = self.mod.parse(text, "", "log", rc=0)
        self.assertEqual(report["failed_cases"], 2)
        self.assertEqual(report["total_cases"], 10)
        self.assertEqual(report["anomaly"], "result_pass_with_failed_summaries")
        self.assertLess(report["raw_pass_rate"], 1.0)

    def test_zero_test_no_summary_is_anomaly(self) -> None:
        # B3: empty/skipped prove output must not look like 100% conformance.
        text = """# 2026-07-26T15:03:18Z $ prove --recurse
"""
        report = self.mod.parse(text, "", "log", rc=0)
        self.assertEqual(report["total_cases"], 0)
        self.assertEqual(report["failed_cases"], 0)
        self.assertEqual(report["raw_pass_rate"], 0.0)
        self.assertEqual(report["anomaly"], "no_test_summary")

    def test_uses_latest_command_section_only(self) -> None:
        text = """# 2026-07-26T12:16:21Z $ prove --recurse
/tmp/pjdfstest/tests/unlink/14.t         (Wstat: 0 Tests: 7 Failed: 1)
Files=238, Tests=8798, 245 wallclock secs
Result: FAIL

# 2026-07-26T15:03:18Z $ prove --recurse
All tests successful.
Files=238, Tests=8798, 247 wallclock secs
Result: PASS
"""
        report = self.mod.parse(text, "", "log", rc=0)
        self.assertEqual(report["failed_cases"], 0)
        self.assertEqual(report["total_cases"], 8798)
        self.assertNotIn("anomaly", report)

    def test_real_fail_run(self) -> None:
        text = """# 2026-07-26T12:16:21Z $ prove --recurse
not ok 7 - tried 'rmdir x', expected 0, got ENOTEMPTY
/tmp/pjdfstest/tests/unlink/14.t         (Wstat: 0 Tests: 7 Failed: 1)
Files=238, Tests=8798, 245 wallclock secs
Result: FAIL
"""
        report = self.mod.parse(text, "", "log", rc=1)
        self.assertEqual(report["failed_cases"], 1)
        self.assertEqual(report["failed_files"][0]["path"], "unlink/14.t")
        self.assertNotIn("anomaly", report)


    def test_stale_stderr_failure_not_scored_against_latest_pass(self) -> None:
        # D: stale stderr from an earlier invocation must not be counted
        # against the latest run — both streams carry per-invocation headers.
        stdout = """# 2026-07-26T12:16:21Z $ prove --recurse
/tmp/pjdfstest/tests/unlink/14.t         (Wstat: 0 Tests: 7 Failed: 1)
Files=238, Tests=8798, 245 wallclock secs
Result: FAIL

# 2026-07-26T15:03:18Z $ prove --recurse
All tests successful.
Files=238, Tests=8798, 247 wallclock secs
Result: PASS
"""
        stderr = """# 2026-07-26T12:16:21Z $ prove --recurse
/tmp/pjdfstest/tests/unlink/14.t         (Wstat: 0 Tests: 7 Failed: 1)
  Failed: 1/7 tests, 85.71% okay

# 2026-07-26T15:03:18Z $ prove --recurse
"""
        report = self.mod.parse(stdout, stderr, "log", rc=0)
        self.assertEqual(report["failed_cases"], 0)
        self.assertEqual(report["total_cases"], 8798)
        self.assertEqual(report["raw_pass_rate"], 1.0)
        self.assertNotIn("anomaly", report)

    def test_stderr_only_invocation_still_scored(self) -> None:
        # TAP failures that appear on stderr of the LATEST invocation must
        # still be counted.
        stdout = """# 2026-07-26T15:03:18Z $ prove --recurse
"""
        stderr = """# 2026-07-26T15:03:18Z $ prove --recurse
/tmp/pjdfstest/tests/unlink/14.t         (Wstat: 0 Tests: 7 Failed: 1)
Files=238, Tests=8798, 245 wallclock secs
Result: FAIL
"""
        report = self.mod.parse(stdout, stderr, "log", rc=1)
        self.assertEqual(report["failed_cases"], 1)
        self.assertEqual(report["total_cases"], 8798)


if __name__ == "__main__":
    unittest.main()
