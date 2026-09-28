"""Run the documented 23 dn cases; isolate D12/N11 after the normal 21."""

import argparse
import json
import os
import pathlib
import subprocess
import sys

CASES = [
    "d01_append_tail.py",
    "d02_kill_resume.py",
    "d03_patch_apply.py",
    "d04_multi_agent.py",
    "d05_lock_semantics.py",
    "d06_watcher_loop.py",
    "d07_exec_after_write.py",
    "d08_temp_storm.py",
    "d09_fd_replaced.py",
    "d10_weird_names.py",
    "d11_deep_wide_scan.py",
    "n01_pnpm_layout.py",
    "n02_workspaces.py",
    "n03_tsc_incremental.py",
    "n04_vitest.py",
    "n05_pack_tarball.py",
    "n06_ci_interrupt.py",
    "n07_cache_concurrency.py",
    "n08_rm_node_modules.py",
    "n09_node_watch.py",
    "n10_npx_corepack.py",
    "d12_nfc_nfd_alias.py",
    "n11_cache_inside_mount.py",
]
ISOLATED = {"d12", "n11"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("cases", nargs="*")
    args = parser.parse_args()
    if args.list:
        print(json.dumps(CASES))
        return 0
    if sys.platform != "linux":
        parser.error("Live cases require Linux")
    known = {s.split("_")[0] for s in CASES}
    if set(args.cases) - known:
        parser.error("Unknown case ID")
    for key in ("D9_BIN", "D9_SERVER", "D9_TARGET_VERSION", "D9_NODE_BIN"):
        if not os.environ.get(key):
            parser.error("Required configuration: " + key)
    os.environ["DRIVE9_SERVER"] = os.environ["D9_SERVER"]
    from common import BIN, MOUNT, RESULT_DIR, drain, mount_client, sh
    from config import CACHE_ROOT, DURABILITY, MOUNT_BASE
    from case_guard import boundary, run_case
    import fcntl

    lock_path = pathlib.Path(
        os.environ.get(
            "D9_REPLAY_LOCK",
            str(pathlib.Path.home() / "drive9-replays/coordinator.lock"),
        )
    )
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        rows = []
        for script in CASES:
            short = script.split("_")[0]
            if args.cases and short not in args.cases:
                continue
            case_id = script[:-3].replace("_", "-")
            result_path = RESULT_DIR / (case_id + ".json")
            if result_path.exists():
                raise RuntimeError(
                    "Existing result; use a fresh run directory: " + str(result_path)
                )
            isolated = short in ISOLATED
            mount, owned_proc = MOUNT, None
            if isolated:
                # No main mount during isolated measurements. Preserve all cache files.
                if os.path.ismount(MOUNT):
                    if not drain(MOUNT)["ok"]:
                        raise RuntimeError("Main mount dirty before isolation")
                    subprocess.run(
                        [BIN, "umount", "--no-auto-pack", str(MOUNT)],
                        check=True,
                        timeout=180,
                    )
                mount = MOUNT_BASE / ("d9-dn-" + short)
                cache = CACHE_ROOT / ("isolated-" + short)
                if cache.exists() or os.path.ismount(mount):
                    raise RuntimeError("Isolation paths already used: " + str(cache))
                owned_proc = mount_client(
                    mount,
                    cache,
                    durability=DURABILITY,
                )
            try:
                if not os.path.ismount(mount):
                    raise RuntimeError("Expected FUSE mount is absent: " + str(mount))
                boundary(mount, RESULT_DIR / (case_id + "-boundary-before.json"))
                before = drain(mount)
                if not before["ok"]:
                    raise RuntimeError("Dirty mount before " + case_id)
                env = dict(os.environ, D9_MOUNT=str(mount))
                with (RESULT_DIR / (case_id + ".log")).open("w") as log:
                    proc, case_wall = run_case(
                        [
                            sys.executable,
                            str(pathlib.Path(__file__).parent / "dn" / script),
                        ],
                        env=env,
                        stdout=log,
                        stderr=log,
                        timeout=2400,
                    )
                after = drain(mount)
                data = (
                    json.loads(result_path.read_text()) if result_path.exists() else {}
                )
                ok = (
                    proc.returncode == 0
                    and data.get("verified") is True
                    and after["ok"]
                    and all(s.get("ok") for s in data.get("sync_evidence", []))
                )
                rows.append(
                    dict(
                        scenario=case_id,
                        ok=bool(ok),
                        exit_code=proc.returncode,
                        isolated=isolated,
                        duration_s=data.get("duration_s"),
                        case_wall_s=case_wall,
                        drain_before=before,
                        drain_after=after,
                        case_wall_plus_drain_s=case_wall + after["wall_s"],
                    )
                )
                (RESULT_DIR / "run_dn.json").write_text(
                    json.dumps(rows, indent=2) + "\n"
                )
                print(case_id, "PASS" if ok else "FAIL", flush=True)
                boundary(mount, RESULT_DIR / (case_id + "-boundary-after.json"))
                if not isolated and not after["ok"]:
                    break
            finally:
                if owned_proc is not None:
                    result = sh(
                        [BIN, "umount", "--no-auto-pack", str(mount)], timeout=180
                    )
                    if result.returncode or os.path.ismount(mount):
                        raise RuntimeError(
                            "Unmount failed; stop and inspect " + str(mount)
                        )
                    if owned_proc.poll() is None:
                        owned_proc.terminate()
                    owned_proc.wait(timeout=30)
        from summarize_dn import main as summarize

        summarize()
        expected = len(set(args.cases)) if args.cases else len(CASES)
        return 0 if len(rows) == expected and all(r["ok"] for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
