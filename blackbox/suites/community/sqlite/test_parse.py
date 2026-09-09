"""Unit tests for community.sqlite log parsing (no FUSE/mount required)."""

from __future__ import annotations

import unittest

from suites.community.sqlite.module import (
    parse_kvtest_output,
    parse_mptest_output,
    parse_speedtest1_output,
    split_csv,
)


class ParseSpeedtest1Tests(unittest.TestCase):
    def test_verify_hash_from_latest_section(self) -> None:
        text = """# 2026-04-01T00:00:00Z $ speedtest1 --verify old.db
Verification Hash: 111 stale
# 2026-04-01T00:01:00Z $ speedtest1 --verify new.db
       TOTAL........................................................    1.234s
Verification Hash: 4242 abcdef0123456789
"""
        parsed = parse_speedtest1_output(text, "")
        self.assertTrue(parsed["ok"])
        self.assertEqual(parsed["verify_hash"], "4242")
        self.assertEqual(parsed["verify_digest"], "abcdef0123456789")

    def test_missing_hash_is_not_ok(self) -> None:
        parsed = parse_speedtest1_output("TOTAL 1.0s\n", "")
        self.assertFalse(parsed["ok"])
        self.assertEqual(parsed["reason"], "missing_verification_hash")


class ParseMptestTests(unittest.TestCase):
    def test_zero_errors_is_ok(self) -> None:
        text = """# 2026-04-01T00:00:00Z $ mptester db.db crash01.test
BEGIN: mptester db.db
Summary: 0 errors out of 42 tests
END: mptester
"""
        parsed = parse_mptest_output(text, "")
        self.assertTrue(parsed["ok"])
        self.assertEqual(parsed["errors"], 0)
        self.assertEqual(parsed["tests"], 42)

    def test_nonzero_errors_is_not_ok(self) -> None:
        text = """# 2026-04-01T00:00:00Z $ mptester db.db
Summary: 3 errors out of 40 tests
"""
        parsed = parse_mptest_output(text, "")
        self.assertFalse(parsed["ok"])
        self.assertEqual(parsed["reason"], "errors")
        self.assertEqual(parsed["errors"], 3)
        self.assertEqual(parsed["tests"], 40)

    def test_latest_summary_wins(self) -> None:
        text = """# 2026-04-01T00:00:00Z $ first
Summary: 9 errors out of 9 tests
# 2026-04-01T00:01:00Z $ second
Summary: 0 errors out of 12 tests
"""
        parsed = parse_mptest_output(text, "")
        self.assertTrue(parsed["ok"])
        self.assertEqual(parsed["tests"], 12)

    def test_missing_summary_is_not_ok(self) -> None:
        parsed = parse_mptest_output("fatal: cannot open db\n", "")
        self.assertFalse(parsed["ok"])
        self.assertEqual(parsed["reason"], "missing_summary")

    def test_threadtest3_summary_without_prefix(self) -> None:
        text = """# 2026-04-01T00:00:00Z $ threadtest3 walthread*
Running walthread1 for 20 seconds...
0 errors out of 5 tests
"""
        parsed = parse_mptest_output(text, "")
        self.assertTrue(parsed["ok"])
        self.assertEqual(parsed["tests"], 5)


class ParseKvtestTests(unittest.TestCase):
    def test_run_requires_integrity_and_elapsed(self) -> None:
        text = """# 2026-04-01T00:00:00Z $ kvtest run db
integrity-check:    ok
Total elapsed time: 1.234
"""
        parsed = parse_kvtest_output(text, "", require_integrity=True)
        self.assertTrue(parsed["ok"])

    def test_error_line_is_not_ok(self) -> None:
        parsed = parse_kvtest_output("ERROR: cannot open database\n", "", require_integrity=True)
        self.assertFalse(parsed["ok"])
        self.assertEqual(parsed["reason"], "kvtest_error")

    def test_missing_integrity_is_not_ok(self) -> None:
        parsed = parse_kvtest_output("Total elapsed time: 1.0\n", "", require_integrity=True)
        self.assertFalse(parsed["ok"])
        self.assertEqual(parsed["reason"], "missing_integrity_ok")


class SplitCsvTests(unittest.TestCase):
    def test_default_when_empty(self) -> None:
        self.assertEqual(split_csv("  ", ("wal", "delete")), ["wal", "delete"])

    def test_dedupes_and_preserves_order(self) -> None:
        self.assertEqual(split_csv("WAL, delete, wal, persist", ("wal",)), ["WAL", "delete", "persist"])


if __name__ == "__main__":
    unittest.main()
