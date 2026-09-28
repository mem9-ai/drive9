"""Local-only harness checks; do not execute against measured mounts."""

import pathlib
import tempfile
import unittest

from cases import CASES, execute


class HarnessTests(unittest.TestCase):
    def test_smoke_local(self):
        with tempfile.TemporaryDirectory(prefix="sixway-harness-") as tmp:
            for case in CASES:
                if case == "permissions":
                    continue  # Requires Linux sudo/nobody plus traversable ancestors.
                with self.subTest(case=case):
                    row = execute(case, pathlib.Path(tmp) / case, scale=0.02)
                    self.assertTrue(row["verified"])
                    self.assertGreater(row["duration_s"], 0)
                    self.assertAlmostEqual(row["duration_s"], sum(row["phases_s"].values()))


if __name__ == "__main__":
    unittest.main()
