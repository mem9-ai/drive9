"""Unit tests for drive9.node_watch production-env isolation (no mount required)."""

from __future__ import annotations

import unittest

from suites.drive9.node_watch.module import production_probe_env


class ProductionProbeEnvTests(unittest.TestCase):
    def test_strips_all_watch_probe_vars(self) -> None:
        # Simulates the poisoned-runner scenario from review: the internal
        # token itself arrives via ambient env, which would satisfy the JS
        # token gate — only the production scrub keeps the probe real.
        poisoned = {
            "PATH": "/usr/bin",
            "WATCH_PROBE_TEST_ENABLE": "internal-selftest",
            "WATCH_PROBE_TEST_WATCH_BACKEND": "fake",
            "WATCH_PROBE_TEST_MUTE_GEN2": "1",
            "WATCH_PROBE_TEST_REPLAY_GEN2_AFTER_READY_MS": "30",
            "WATCH_PROBE_DELAY_READ_MS": "5000",
        }
        cleaned = production_probe_env(poisoned)
        self.assertEqual(cleaned, {"PATH": "/usr/bin"})
        for key in poisoned:
            if key.startswith("WATCH_PROBE_"):
                self.assertNotIn(key, cleaned)

    def test_preserves_unrelated_env(self) -> None:
        env = {"PATH": "/x", "HOME": "/h", "NODE_TEST_DIR": "/t"}
        self.assertEqual(production_probe_env(env), env)


if __name__ == "__main__":
    unittest.main()
