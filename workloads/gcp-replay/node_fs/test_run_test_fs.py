"""Checks that the official TAP outcomes are not misreported as passes."""

import unittest

from run_test_fs import parse_tap


class ParseTapTest(unittest.TestCase):
    def test_pass(self):
        output = "TAP version 13\n1..1\nok 1 parallel/test-fs-access\n  ---\n  duration_ms: 8.25\n  ...\n"
        self.assertEqual(parse_tap(output, 0), ("pass", 8.25, 1))

    def test_failure_overrides_earlier_pass(self):
        output = (
            "TAP version 13\n1..2\nok 1 parallel/test-fs-a\n  ---\n"
            "  duration_ms: 2.0\n  ...\nnot ok 2 parallel/test-fs-b\n"
            "  ---\n  duration_ms: 3.0\n  ...\n"
        )
        self.assertEqual(parse_tap(output, 1), ("fail", 5.0, 2))

    def test_status_file_skip(self):
        self.assertEqual(parse_tap("No tests to run.\n", 1), ("skip", None, 0))


if __name__ == "__main__":
    unittest.main()
