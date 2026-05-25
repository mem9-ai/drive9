import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / "bench" / "bin" / "run-localdeps-output-build.py"


def load_runner(env: dict[str, str] | None = None):
    name = "run_localdeps_output_build_test"
    spec = importlib.util.spec_from_file_location(name, RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    with mock.patch.dict(os.environ, env or {}, clear=False):
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return module


class LocalDepsOutputBuildHarnessTest(unittest.TestCase):
    def test_redact_secrets_covers_drive9_tokens_and_api_key_labels(self):
        runner = load_runner()
        redacted = runner.redact_secrets("api_key: secret-value\napi key = dat9_real-token")
        self.assertIn("api_key: [redacted]", redacted)
        self.assertNotIn("secret-value", redacted)
        self.assertNotIn("dat9_real-token", redacted)

    def test_selected_repos_accepts_comma_filter(self):
        runner = load_runner({"BENCH_REPOS": "drive9,kimi-code"})
        self.assertEqual([repo.repo_id for repo in runner.selected_repos()], ["drive9", "kimi-code"])

    def test_kimi_code_parent_bind_avoids_nested_node_sdk_bind(self):
        runner = load_runner()
        repo = next(repo for repo in runner.REPOS if repo.repo_id == "kimi-code")
        mounts = runner.bind_mounts_for(repo, Path("/checkout"), "fuse", 1)
        targets = {target.as_posix() for _, target in mounts}
        self.assertIn("/checkout/packages/node-sdk", targets)
        self.assertNotIn("/checkout/packages/node-sdk/node_modules", targets)
        self.assertIn("/checkout/apps/vis/web/dist", targets)

    def test_drive9_uses_no_repo_output_bind_mounts(self):
        runner = load_runner()
        repo = next(repo for repo in runner.REPOS if repo.repo_id == "drive9")
        self.assertEqual(runner.bind_mounts_for(repo, Path("/checkout"), "fuse", 1), [])


if __name__ == "__main__":
    unittest.main()
