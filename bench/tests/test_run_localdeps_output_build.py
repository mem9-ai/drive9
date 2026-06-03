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

    def test_kimi_code_local_policy_keeps_node_sdk_temp_local(self):
        runner = load_runner()
        repo = next(repo for repo in runner.REPOS if repo.repo_id == "kimi-code")
        patterns = runner.local_only_patterns_for(repo)
        self.assertIn("**/.git/**", patterns)
        self.assertIn("**/node_modules/**", patterns)
        self.assertIn("**/dist/**", patterns)
        self.assertIn("**/packages/node-sdk/.tmp-api-extractor/**", patterns)
        self.assertNotIn("**/packages/node-sdk/**", patterns)

    def test_drive9_uses_common_local_overlay_policy(self):
        runner = load_runner()
        repo = next(repo for repo in runner.REPOS if repo.repo_id == "drive9")
        patterns = runner.local_only_patterns_for(repo)
        self.assertIn("**/.git/**", patterns)
        self.assertIn("**/target/**", patterns)
        self.assertIn("**/bin/**", patterns)
        self.assertNotIn("**/src/kimi_cli/web/**", patterns)

    def test_kimi_cli_keeps_known_package_metadata_symlinks_local(self):
        runner = load_runner()
        repo = next(repo for repo in runner.REPOS if repo.repo_id == "kimi-cli")
        patterns = runner.local_only_patterns_for(repo)
        self.assertIn("**/packages/kimi-code/README.md", patterns)
        self.assertIn("**/src/kimi_cli/CHANGELOG.md", patterns)


if __name__ == "__main__":
    unittest.main()
