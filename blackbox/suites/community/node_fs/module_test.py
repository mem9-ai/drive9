"""Unit tests for community.node_fs runner argv assembly (no FUSE/mount required)."""

from __future__ import annotations

import unittest
from pathlib import Path

from suites.community.node_fs.module import runner_cmd


class RunnerCmdTests(unittest.TestCase):
    def test_default_argv_is_all_strings(self) -> None:
        # run_cmd does " ".join(cmd) before spawning; a single int element
        # raises TypeError there and never reaches Popen. The default
        # NODE_FS_JOBS=1 path must assemble cleanly.
        cmd = runner_cmd(Path("/tmp/node"), "/tmp/node/bin/node", 1, "300")
        for element in cmd:
            self.assertIsInstance(element, str, f"argv element {element!r} is not a str")
        # The exact failure mode run_cmd would hit: joining must not raise.
        self.assertTrue(" ".join(cmd))

    def test_jobs_int_is_stringified(self) -> None:
        cmd = runner_cmd(Path("/tmp/node"), "/tmp/node/bin/node", 4, "300")
        self.assertEqual(cmd[cmd.index("-j") + 1], "4")
        self.assertIsInstance(cmd[cmd.index("-j") + 1], str)

    def test_large_jobs_value(self) -> None:
        cmd = runner_cmd(Path("/tmp/node"), "/tmp/node/bin/node", 16, "300")
        self.assertEqual(cmd[cmd.index("-j") + 1], "16")


if __name__ == "__main__":
    unittest.main()
