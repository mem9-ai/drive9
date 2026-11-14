"""Exercise the public Layer smoke entry without services or an actual mount."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ENTRY = Path(__file__).with_name("layer-fs-smoke-test.sh")


class FixedViewEntryTest(unittest.TestCase):
    def run_entry(self, fail_at=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = root / "calls"
            cli_path = root / "cli-path"
            # These stubs inspect real wrapper arguments; they start no servers.
            for name in ("python3", "make"):
                path = root / name
                path.write_text("""#!/bin/bash
set -eu
if [ "$(basename "$0")" = make ]; then
  test "$#" = 2
  test "$1" = build-cli
  case "$2" in CLI_BIN=*) ;; *) exit 90 ;; esac
  cli=${2#CLI_BIN=}
  test -f "$cli"
  printf '%s' "$cli" > "$CLI_PATH"
  stage=build
elif [ "$(basename "$1")" = fixed-view-smoke.py ]; then
  test "$#" = 2
  test "$2" = --provision-local
  stage=http
else
  test "$(basename "$1")" = fixed-view-mount-smoke.py
  test "$#" = 4
  test "$2" = --cli
  test "$3" = "$(cat "$CLI_PATH")"
  test "$4" = --write-merge
  stage=mount
fi
printf '%s\n' "$stage" >> "$CALLS"
if [ "$FAIL_AT" = "$stage" ]; then exit 17; fi
""")
                path.chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                       CALLS=str(calls), CLI_PATH=str(cli_path), FAIL_AT=fail_at)
            result = subprocess.run(["bash", str(ENTRY)], env=env, capture_output=True,
                                    text=True, timeout=10)
            observed = calls.read_text().splitlines() if calls.exists() else []
            if cli_path.exists():
                self.assertFalse(Path(cli_path.read_text()).exists(), "temporary CLI leaked")
            return result.returncode, observed

    def test_complete_new_contract(self):
        self.assertEqual(self.run_entry(), (0, ["http", "build", "mount"]))

    def test_http_failure_stops_before_build(self):
        self.assertEqual(self.run_entry("http"), (17, ["http"]))

    def test_build_failure_stops_before_mount(self):
        self.assertEqual(self.run_entry("build"), (17, ["http", "build"]))

    def test_mount_failure_is_not_success(self):
        self.assertEqual(self.run_entry("mount"), (17, ["http", "build", "mount"]))


if __name__ == "__main__":
    unittest.main()
