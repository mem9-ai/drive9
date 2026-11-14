"""Exercise the real local harness database selection with no service calls."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/e2e-local.sh'


class ExistingDatabaseRequirementTest(unittest.TestCase):
    def run_selection(self, required, reachable):
        source = SCRIPT.read_text()
        function = source.split('ensure_tidb() {\n', 1)[1].split('\nbuild_server() {', 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            calls = Path(directory) / 'calls'
            env = dict(os.environ, CALLS=str(calls), REACHABLE=str(reachable),
                       DRIVE9_LOCAL_E2E_REQUIRE_EXISTING_DB=required,
                       DRIVE9_LOCAL_DSN='root@tcp(127.0.0.1:4999)/fixture')
            script = '''set -eu
dsn_host() { echo 127.0.0.1; }
dsn_port() { echo 4999; }
mysql_handshake() { [ "$REACHABLE" = 1 ]; }
log() { :; }
pick_runtime() { echo fallback >> "$CALLS"; }
container_running() { return 0; }
wait_tcp() { return 0; }
create_database() { echo create >> "$CALLS"; }
DB_CONTAINER_NAME=fixture
DB_NAME=fixture
STARTED_DB=0
ensure_tidb() {
''' + function + '\nensure_tidb\n'
            result = subprocess.run(['bash', '-c', script], env=env,
                                    capture_output=True, text=True, timeout=5)
            return result, calls.read_text().splitlines() if calls.exists() else []

    def test_required_reachable_database_is_used(self):
        result, calls = self.run_selection('1', 1)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ['create'])

    def test_missing_required_database_never_falls_back(self):
        result, calls = self.run_selection('1', 0)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(calls, [])
        self.assertIn('refusing fallback', result.stderr)

    def test_regular_local_mode_keeps_existing_fallback(self):
        result, calls = self.run_selection('0', 0)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ['fallback', 'create'])

    def test_invalid_requirement_rejected(self):
        result, calls = self.run_selection('yes', 1)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])


if __name__ == '__main__':
    unittest.main()
