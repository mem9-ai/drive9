"""One locked coordinator for the extended three-case matrix; exactly one group/case at a time."""

import fcntl
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from cases import CASES

BASE = pathlib.Path(__file__).resolve().parent
BIN = pathlib.Path(os.environ.get("DRIVE9_BENCH_BIN", "/home/ubuntu/drive9-main-fe9cdcf/drive9"))
ENDPOINT = os.environ.get("DRIVE9_BENCH_SERVER", "https://drive9.example.invalid")
CRED_DIR = pathlib.Path(os.environ.get("DRIVE9_BENCH_CRED_DIR", "/home/ubuntu/drive9-sixway-20260911/credentials"))
LOCK_PATH = pathlib.Path(os.environ.get("DRIVE9_BENCH_LOCK", "/home/ubuntu/drive9-sixway-20260911/coordinator.lock"))
MOUNT_ROOT = pathlib.Path(os.environ.get("DRIVE9_BENCH_MOUNT_ROOT", "/mnt"))
REMOTE_ROOT = os.environ.get("DRIVE9_BENCH_REMOTE_ROOT", "/benchmark")
# Mount-side case root. Defaults to this harness directory's name so that a
# fresh copy of the harness never collides with case directories left behind by
# an earlier run (see workloads/AGENTS.md, "Rules and pitfalls").
RUN_PREFIX = os.environ.get("DRIVE9_BENCH_RUN_PREFIX", BASE.name)
GROUPS = ["none-a", "none-b", "coding-a", "coding-b", "efs", "ebs"]
MOUNTS = {g: MOUNT_ROOT / ("d9-six-" + g) for g in GROUPS[:4]}
MOUNTS.update(efs=MOUNT_ROOT / "efs", ebs=MOUNT_ROOT / "ebs")
RESULTS = BASE / "results"
RESULTS.mkdir(exist_ok=True)

DRAIN_TIMEOUT = "1800s"
DRAIN_PROC_TIMEOUT = 1815
CASE_TIMEOUT = 7200


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and urllib.parse.urlsplit(req.full_url).netloc != urllib.parse.urlsplit(newurl).netloc:
            redirected.remove_header("Authorization")
        return redirected


def save(path, data):
    temp = path.with_suffix(path.suffix + ".partial")
    temp.write_text(json.dumps(data, indent=2))
    os.replace(temp, path)


def request(group, remote, stat_only=False, allow404=False):
    credentials = json.loads((CRED_DIR / (group + ".json")).read_text())
    assert credentials["server"] == ENDPOINT
    url = ENDPOINT + "/v1/fs" + urllib.parse.quote(remote) + ("?stat=1" if stat_only else "")
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + credentials["api_key"]})
    try:
        with urllib.request.build_opener(SafeRedirect()).open(req, timeout=60) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        if error.code == 404 and allow404:
            return None
        raise RuntimeError("remote verification HTTP " + str(error.code)) from None


def drain(group):
    assert os.path.ismount(MOUNTS[group]), "Expected mount is absent: " + str(MOUNTS[group])
    start = time.perf_counter_ns()
    if group in GROUPS[:4]:
        proc = subprocess.run([str(BIN), "mount", "drain", "--timeout", DRAIN_TIMEOUT, "--json", str(MOUNTS[group])],
                              capture_output=True, text=True, timeout=DRAIN_PROC_TIMEOUT)
        if proc.returncode:
            raise RuntimeError("drain failed: " + proc.stderr[-1200:])
        result = json.loads(proc.stdout)
        assert result["ok"] and all(v == 0 for v in result["pending"].values()), result
    else:
        subprocess.run(["sync", "-f", str(MOUNTS[group])], check=True, capture_output=True, timeout=DRAIN_PROC_TIMEOUT)
        result = {"ok": True, "method": "sync-f"}
    return {"wall_s": (time.perf_counter_ns() - start) / 1e9, "result": result}


def run_one(group, case, stage, round_number, scale):
    label = f"{stage}-r{round_number:02d}-{group}-{case}"
    result_path = RESULTS / (label + ".json")
    if result_path.exists():
        row = json.loads(result_path.read_text())
        assert row.get("drain_after", {}).get("result", {}).get("ok"), "Previous incomplete case requires inspection"
        return row
    root = MOUNTS[group] / RUN_PREFIX / label
    raw_path = RESULTS / (label + ".workload.json")
    assert not raw_path.exists(), "Previous attempt exists; inspect before resuming"
    before = drain(group)
    status = {"state": "running", "group": group, "case": case, "stage": stage,
              "round": round_number, "start_epoch": time.time(), "label": label}
    save(BASE / "status.json", status)
    print("START " + label, flush=True)
    with (RESULTS / (label + ".log")).open("w") as log:
        proc = subprocess.Popen([sys.executable, str(BASE / "cases.py"), case, str(root), str(raw_path), "--scale", str(scale)],
                                stdout=log, stderr=log, start_new_session=True)
        try:
            code = proc.wait(timeout=CASE_TIMEOUT)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, 15)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, 9); proc.wait()
            code = -1
    row = json.loads(raw_path.read_text()) if raw_path.exists() else {"verified": False, "error": "workload timeout/no result"}
    row.update(group=group, stage=stage, round=round_number, label=label, exit_code=code,
               drain_before=before, load_average=os.getloadavg())
    try:
        row["drain_after"] = drain(group)
        if row.get("verified") and group in GROUPS[:4]:
            remote = f"{REMOTE_ROOT}/{RUN_PREFIX}/" + label
            if row.get("sample"):
                sample = row["sample"]
                response = request(group, remote + "/" + sample["path"])
                assert len(response) == sample["size"] and hashlib.sha256(response).hexdigest() == sample["sha256"], "remote sample mismatch"
                row["remote_sample_verified"] = True
            if row.get("absent"):
                assert request(group, remote + "/" + row["absent"], stat_only=True, allow404=True) is None
                row["remote_absence_verified"] = True
    except Exception as error:
        row["verified"] = False
        row["postcheck_error"] = str(error)
    save(result_path, row)
    print("RESULT " + json.dumps({k: row.get(k) for k in ("label", "duration_s", "verified", "error", "postcheck_error")}), flush=True)
    if not row.get("drain_after", {}).get("result", {}).get("ok"):
        raise RuntimeError("Cannot safely proceed while drain is unresolved: " + label)
    return row


def main():
    with LOCK_PATH.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mode = sys.argv[1]
        cases = sys.argv[2:] or CASES
        if mode == "smoke":
            schedules = [("smoke", 0, GROUPS, 0.02)]
        elif mode == "full":
            smoke = [json.loads(p.read_text()) for p in RESULTS.glob("smoke-r00-*.json") if not p.name.endswith(".workload.json")]
            required = [r for r in smoke if r["case"] in cases]
            assert len(required) == 6 * len(cases) and all(r["verified"] for r in required), "All smoke cases must pass before measuring"
            schedules = [("warmup", 0, GROUPS, 1)]
            for number in range(1, 2):
                offset = (number - 1) % 6
                order = GROUPS[offset:] + GROUPS[:offset]
                schedules.append(("measured", number, order, 1))
        else:
            raise ValueError(mode)
        failures = []
        for stage, number, order, scale in schedules:
            for case in cases:
                for group in order:
                    row = run_one(group, case, stage, number, scale)
                    if not row["verified"]:
                        failures.append(row["label"])
                        if mode == "smoke":
                            raise RuntimeError("Stop on failed smoke case: " + row["label"])
                        print("RETAIN FAILED SAMPLE " + row["label"], flush=True)
        save(BASE / "status.json", {"state": "completed" if not failures else "completed_with_failures",
                                   "mode": mode, "failures": failures, "end_epoch": time.time()})
        print("COMPLETED " + mode, flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        save(BASE / "status.json", {"state": "stopped", "error": str(error), "end_epoch": time.time()})
        print("STOPPED " + str(error), flush=True)
        raise
