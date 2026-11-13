"""Offline switch-driver tests; no server or kernel mount is started."""
import importlib.util
import json
import os
import sys
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("fixed_view_mount", Path(__file__).with_name("fixed-view-mount-smoke.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class MountProbeTest(unittest.TestCase):
    def setUp(self):
        self.probe = smoke.http.Probe("http://127.0.0.1:9009", "unit-test-placeholder")
        self.driver = smoke.MountProbe("/paired/drive9", self.probe, "/tmp/fixture/mount")
        self.previous = {"mount_id": "mount-a", "generation": 4, "binding": smoke.binding()}
        self.work = {"layer_id": "layer-a", "work_id": "work-a", "fence": 3}

    def result(self):
        operation = self.probe.prefix + "-1"
        return {"unknown": False, "response": {"mount_id": "mount-a", "operation_id": operation,
                "receipt": {"operation_id": operation, "from_generation": 4, "generation": 5,
                            "from": smoke.binding(), "target": smoke.binding(self.work), "state": "DONE"}}}

    def test_switch_queries_and_replays_exact_identity(self):
        result = self.result()
        status = {"view": {"mount_id": "mount-a", "generation": 5, "binding": smoke.binding(self.work)}}
        with patch.object(self.driver, "call", side_effect=[result, result, result, status]) as call:
            self.assertEqual(self.driver.switch(self.previous, self.work), status["view"])
        args = [c.args[0] for c in call.call_args_list]
        self.assertEqual([a[:2] for a in args], [["mount", "switch"], ["mount", "switch-operation"],
                                               ["mount", "switch"], ["mount", "status"]])
        self.assertEqual(args[0][2:], args[1][2:])
        self.assertEqual(args[0][2:], args[2][2:])
        self.assertIn("work-a", args[0])
        self.assertIn("4", args[0])

    def test_main_switch_clears_work_binding(self):
        self.previous["binding"] = smoke.binding(self.work)
        result = self.result()
        result["response"]["receipt"].update({"from": self.previous["binding"], "target": smoke.binding()})
        status = {"view": {"mount_id": "mount-a", "generation": 5, "binding": smoke.binding()}}
        with patch.object(self.driver, "call", side_effect=[result, result, result, status]) as call:
            self.assertEqual(self.driver.switch(self.previous), status["view"])
        args = call.call_args_list[0].args[0]
        self.assertIn("main", args)
        self.assertNotIn("--work", args)
        self.assertNotIn("--fence", args)

    def test_mounted_read_requires_exact_bytes_and_success(self):
        for code, data in ((0, b"expected"), (0, b"stale"), (1, b"expected")):
            with patch.object(smoke.subprocess, "run", return_value=subprocess.CompletedProcess([], code, data, b"")) as run:
                if code == 0 and data == b"expected":
                    self.driver.read("/file", b"expected")
                else:
                    with self.assertRaises(smoke.http.ProbeFailure):
                        self.driver.read("/file", b"expected")
                self.assertEqual(run.call_args.args[0], ["cat", "/tmp/fixture/mount/file"])
                self.assertEqual(run.call_count, 1)

    def test_mounted_read_timeout_identifies_stage_without_output(self):
        with patch.object(smoke.subprocess, "run", side_effect=subprocess.TimeoutExpired(
                "sensitive path", 30, output=b"sensitive bytes")) as run:
            with self.assertRaisesRegex(smoke.http.ProbeFailure, "^mounted file read timed out$"):
                self.driver.read("/file", b"expected")
        self.assertEqual(run.call_count, 1)

    def test_invalid_receipt_never_retries_switch(self):
        for field, value in (("state", "RECOVERING"), ("generation", 6), ("from_generation", 3),
                             ("target", smoke.binding()), ("operation_id", "other")):
            with self.subTest(field=field):
                result = self.result()
                result["response"]["receipt"][field] = value
                with patch.object(self.driver, "call", return_value=result) as call:
                    with self.assertRaises(smoke.http.ProbeFailure):
                        self.driver.switch(self.previous, self.work)
                self.assertEqual(call.call_count, 1)
                self.probe.op_number = 0

    def test_unknown_switch_does_not_get_a_new_operation(self):
        with patch.object(self.driver, "call", side_effect=smoke.http.ProbeFailure("unknown")) as call:
            with self.assertRaises(smoke.http.ProbeFailure):
                self.driver.switch(self.previous, self.work)
        self.assertEqual(call.call_count, 1)
        self.assertEqual(self.probe.op_number, 1)

    def test_query_drift_and_remount_status_fail(self):
        for mode in ("query", "mount", "generation", "binding", "pending"):
            result, changed = self.result(), self.result()
            changed["response"]["receipt"]["generation"] = 8
            view = {"mount_id": "mount-a", "generation": 5, "binding": smoke.binding(self.work)}
            if mode == "mount":
                view["mount_id"] = "restarted-mount"
            elif mode == "generation":
                view["generation"] = 6
            elif mode == "binding":
                view["binding"] = smoke.binding()
            elif mode == "pending":
                view["pending"] = {"state": "RECOVERING"}
            responses = [result, changed if mode == "query" else result, result, {"view": view}]
            with patch.object(self.driver, "call", side_effect=responses):
                with self.assertRaises(smoke.http.ProbeFailure):
                    self.driver.switch(self.previous, self.work)
            self.probe.op_number = 0

    def test_cli_failure_redacts_output_and_does_not_retry(self):
        for outcome in (subprocess.CompletedProcess([], 1, b"sensitive stdout", b"sensitive stderr"),
                        subprocess.TimeoutExpired("sensitive argv", 30),
                        subprocess.CompletedProcess([], 0, b"sensitive non-json", b"")):
            with patch.object(smoke.subprocess, "run") as run:
                if isinstance(outcome, Exception):
                    run.side_effect = outcome
                else:
                    run.return_value = outcome
                with self.assertRaises(smoke.http.ProbeFailure) as error:
                    self.driver.call(["mount", "switch", "/tmp/fixture/mount"])
                self.assertNotIn("sensitive", str(error.exception))
                self.assertEqual(run.call_count, 1)
                self.assertNotIn(self.probe.key, run.call_args.args[0])
                self.assertEqual(run.call_args.kwargs["env"]["DRIVE9_API_KEY"], self.probe.key)

    def test_switch_failure_only_reports_protocol_enums(self):
        for kind in ("BUSY", "INVALID", "CONFLICT", "FORBIDDEN", "UNSUPPORTED", "RECOVERING"):
            body = json.dumps({"unknown": False, "error": "sensitive server error", "response": {
                "error_kind": kind, "error": "sensitive path", "receipt": {
                    "state": "RECOVERING", "operation_id": "sensitive identity"}}}).encode()
            with patch.object(smoke.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, body, b"sensitive stderr")) as run:
                with self.assertRaises(smoke.http.ProbeFailure) as error:
                    self.driver.call(["mount", "switch", "/tmp/fixture/mount"])
                self.assertIn(f"kind={kind} state=RECOVERING unknown=false", str(error.exception))
                self.assertNotIn("sensitive", str(error.exception))
                self.assertEqual(run.call_count, 1)

    def test_busy_reason_is_classified_without_echoing_details(self):
        cases = {
            "mount view has open handles or locks: sensitive": "handles_or_locks",
            "mount view retains kernel lookup references: sensitive": "kernel_lookups",
            "mount view retains references or private extension state (sensitive)": "final_preflight",
            "mount view extension_state: sensitive": "extension_state",
            "sensitive unknown message": "unclassified",
        }
        for error, reason in cases.items():
            with self.subTest(reason=reason):
                detail = smoke.switch_failure_details(json.dumps({"unknown": False, "response": {
                    "error_kind": "BUSY", "error": error}}).encode())
                self.assertEqual(detail, "kind=BUSY state=absent unknown=false busy=" + reason)
                self.assertNotIn("sensitive", detail)
        for error in (None, [], {"private": "sensitive"}, 42):
            detail = smoke.switch_failure_details(json.dumps({"response": {
                "error_kind": "BUSY", "error": error}}).encode())
            self.assertTrue(detail.endswith("busy=unclassified"))
            self.assertNotIn("sensitive", detail)

    def test_switch_failure_malformed_values_cannot_escape(self):
        for value in (None, [], "sensitive", {"unknown": "sensitive", "response": []},
                      {"unknown": True, "response": {"error_kind": ["sensitive"], "receipt": []}},
                      {"response": {"error_kind": "sensitive", "receipt": {"state": "sensitive"}}}):
            detail = smoke.switch_failure_details(json.dumps(value).encode())
            self.assertNotIn("sensitive", detail)
        for raw in (b"sensitive non-json", b"\xff", b"[" * 2000, b"x" * (1024 * 1024 + 1)):
            self.assertEqual(smoke.switch_failure_details(raw), "receipt=unavailable")
        self.assertEqual(smoke.switch_failure_details(b'{"unknown":true,"response":{"receipt":{"state":"DONE"}}}'),
                         "kind=unclassified state=DONE unknown=true")

    def test_cleanup_timeout_preserves_sanitized_primary_failure(self):
        for primary, label in ((smoke.http.ProbeFailure("mounted file read timed out"), "mounted file read timed out"),
                               (RuntimeError("sensitive message"), "RuntimeError")):
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                process = Mock()
                process.poll.return_value = 0
                work = dict(self.work, layer_id=self.probe.prefix + "-mount")
                with patch.object(smoke.sys, "platform", "linux"), patch.object(smoke.os.path, "exists", return_value=True), \
                     patch.object(smoke.tempfile, "mkdtemp", return_value=directory), patch.object(self.probe, "request"), \
                     patch.object(smoke.MountProbe, "call", side_effect=[{"layer_id": work["layer_id"]}, {"launch": True, "work": work}]), \
                     patch.object(smoke.subprocess, "Popen", return_value=process), \
                     patch.object(smoke, "unmount_owned_process", side_effect=subprocess.TimeoutExpired("sensitive argv", 60)), \
                     patch.object(smoke.os.path, "ismount", return_value=True), \
                     patch.object(smoke.MountProbe, "status", side_effect=primary), patch.object(smoke.shutil, "rmtree") as remove:
                    with self.assertRaisesRegex(smoke.http.ProbeFailure, "unmount cleanup timed out; prior failure: " + label) as error:
                        smoke.run("/paired/drive9", self.probe)
                    self.assertNotIn("sensitive", str(error.exception))
                    remove.assert_not_called()

    def test_cleanup_never_traverses_a_remaining_mount(self):
        with tempfile.TemporaryDirectory() as directory:
            process = Mock()
            process.poll.return_value = 0
            with patch.object(smoke.sys, "platform", "linux"), patch.object(smoke.os.path, "exists", return_value=True), \
                 patch.object(smoke.tempfile, "mkdtemp", return_value=directory), \
                 patch.object(self.probe, "request"), patch.object(smoke.MountProbe, "call", side_effect=[
                     {"layer_id": self.probe.prefix + "-mount"}, {"launch": True, "work": self.work}]), \
                 patch.object(smoke.shutil, "rmtree") as remove:
                # A wrong work identity fails before mounting; local cleanup is safe.
                with patch.object(smoke.os.path, "ismount", return_value=False):
                    with self.assertRaises(smoke.http.ProbeFailure):
                        smoke.run("/paired/drive9", self.probe)
                remove.assert_called_once_with(directory)
                smoke.os.rmdir(directory + "/mount")
                remove.reset_mock()
                work = dict(self.work, layer_id=self.probe.prefix + "-mount")
                with patch.object(smoke.MountProbe, "call", side_effect=[{"layer_id": work["layer_id"]}, {"launch": True, "work": work}]), \
                     patch.object(smoke.subprocess, "Popen", return_value=process), \
                     patch.object(smoke, "unmount_owned_process"), patch.object(smoke.os.path, "ismount", return_value=True), \
                     patch.object(smoke.MountProbe, "status", side_effect=smoke.http.ProbeFailure("status failed")):
                    with self.assertRaisesRegex(smoke.http.ProbeFailure, "cleanup incomplete"):
                        smoke.run("/paired/drive9", self.probe)
                remove.assert_not_called()


class MountedMergeTest(unittest.TestCase):
    def test_write_waits_for_fsync_and_reports_failure_without_retry(self):
        driver = smoke.MountProbe("cli", smoke.http.Probe("http://127.0.0.1:9009", ""), "/mount")
        for result in (subprocess.CompletedProcess([], 0, b"", b""),
                       subprocess.CompletedProcess([], 1, b"private", b"private"),
                       subprocess.TimeoutExpired("private", 60)):
            with patch.object(smoke.subprocess, "run") as run:
                if isinstance(result, Exception):
                    run.side_effect = result
                else:
                    run.return_value = result
                if getattr(result, "returncode", None) == 0:
                    driver.write("/file", b"content")
                else:
                    with self.assertRaises(smoke.http.ProbeFailure) as error:
                        driver.write("/file", b"content")
                    self.assertNotIn("private", str(error.exception))
                self.assertEqual(run.call_count, 1)
                self.assertIn("os.fsync", run.call_args.args[0][2])
                self.assertEqual(run.call_args.kwargs["input"], b"content")

    def test_merge_after_switch_validates_and_replays_original_request(self):
        for failure in (None, "write", "validation", "merge", "replay"):
            with self.subTest(failure=failure):
                driver = Mock()
                work = {"layer_id":"a", "work_id":"work", "fence":1}
                validation = dict(work, work_id="validation", fence=2, validation=True)
                driver.probe.seal_and_validate.return_value = validation
                driver.probe.op.return_value = "merge-original"
                done = {"state":"DONE", "layer":{"layer_id":"a", "phase":"MERGED"}}
                driver.call.side_effect = [done, done]
                if failure == "write": driver.write.side_effect = smoke.http.ProbeFailure("write")
                if failure == "validation": driver.read_validation.side_effect = smoke.http.ProbeFailure("validation")
                if failure == "merge": driver.call.side_effect = [{"state":"RUNNING"}]
                if failure == "replay": driver.call.side_effect = [done, {"state":"FAILED"}]
                if failure:
                    with self.assertRaises(smoke.http.ProbeFailure):
                        smoke.finish_write_merge(driver, {"generation":3}, work, "/file")
                else:
                    smoke.finish_write_merge(driver, {"generation":3}, work, "/file")
                    names = [c[0] for c in driver.mock_calls]
                    self.assertLess(names.index("switch"), names.index("probe.seal_and_validate"))
                    self.assertLess(names.index("read_validation"), names.index("call"))
                    self.assertEqual(driver.call.call_args_list[0], driver.call.call_args_list[1])
                    self.assertIn("validation", driver.call.call_args.args[0])
                    self.assertIn("merge-original", driver.call.call_args.args[0])
                    data = driver.write.call_args.args[1]
                    self.assertGreater(len(data), 4 << 20)
                    self.assertEqual(driver.read.call_args.args, ("/file", data))
                if failure in ("write", "validation"):
                    driver.call.assert_not_called()
                if failure == "merge": self.assertEqual(driver.call.call_count, 1)

    def test_validation_read_binds_readonly_work(self):
        driver = smoke.MountProbe("cli", smoke.http.Probe("http://127.0.0.1:9009", ""), "/mount")
        work = {"layer_id":"a", "work_id":"validation", "fence":2}
        for code, body in ((0,b"expected"),(0,b"stale"),(1,b"expected")):
            with patch.object(smoke.subprocess,"run",return_value=subprocess.CompletedProcess([],code,body,b"")) as run:
                if code == 0 and body == b"expected":
                    driver.read_validation("/file",b"expected",work)
                else:
                    with self.assertRaises(smoke.http.ProbeFailure): driver.read_validation("/file",b"expected",work)
                self.assertEqual(run.call_args.args[0], ["cli","fs","cat","--layer","a","--work","validation","--fence","2","--validation","/file"])


class UnmountOwnedProcessTest(unittest.TestCase):
    @unittest.skipIf(os.name == "nt", "Unix child process lifecycle")
    def test_reaps_foreground_child_while_unmount_waits(self):
        with tempfile.TemporaryDirectory() as directory:
            cli = Path(directory) / "fake-cli"
            cli.write_text("#!" + sys.executable + "\n" + """
import os, signal, sys, time
assert sys.argv[1:] == ["umount", "--timeout", "10s", "--no-auto-pack", "/fixture"]
pid = int(os.environ["OWNED_TEST_CHILD"])
os.kill(pid, signal.SIGTERM)
deadline = time.monotonic() + 3
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        sys.exit(0)
    time.sleep(0.01)
sys.exit(1)
""")
            cli.chmod(0o700)
            child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
            try:
                smoke.unmount_owned_process(str(cli), dict(os.environ, OWNED_TEST_CHILD=str(child.pid)),
                                            "/fixture", child)
                self.assertIsNotNone(child.returncode)
                with self.assertRaises(ProcessLookupError):
                    os.kill(child.pid, 0)
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=5)

    def test_nonzero_exit_fails_even_if_mount_already_exited(self):
        child, cleanup = Mock(), Mock()
        child.poll.return_value = 0
        cleanup.wait.return_value = 1
        cleanup.poll.return_value = 1
        with patch.object(smoke.subprocess, "Popen", return_value=cleanup):
            with self.assertRaisesRegex(smoke.http.ProbeFailure, "^unmount cleanup failed$"):
                smoke.unmount_owned_process("/paired/drive9", {}, "/fixture", child)
        cleanup.kill.assert_not_called()
        child.poll.assert_called_once()

    def test_deadline_kills_and_reaps_cleanup_command(self):
        child, cleanup = Mock(), Mock()
        cleanup.wait.side_effect = [subprocess.TimeoutExpired("sensitive argv", 0.1), 0]
        cleanup.poll.return_value = None
        with patch.object(smoke.subprocess, "Popen", return_value=cleanup), \
             patch.object(smoke.time, "monotonic", side_effect=[100, 160]):
            with self.assertRaises(subprocess.TimeoutExpired) as error:
                smoke.unmount_owned_process("/paired/drive9", {}, "/fixture", child)
        self.assertNotIn("sensitive", str(error.exception))
        cleanup.kill.assert_called_once()
        self.assertEqual(cleanup.wait.call_args.kwargs, {"timeout": 5})
        child.poll.assert_called_once()


if __name__ == "__main__":
    unittest.main()
