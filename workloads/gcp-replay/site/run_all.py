"""Run all 18 acceptance cases; --list does not import or execute cases."""

import argparse
import json
import os
import pathlib
import sys

SCENARIOS = [
    "s01_import.py",
    "s02_save_read.py",
    "s03_watch.py",
    "s04_assets.py",
    "s05_conflict.py",
    "s06_npm.py",
    "s07_refactor.py",
    "s08_git.py",
    "s09_build.py",
    "s10_export.py",
    "s11_external.py",
    "s12_recovery.py",
    "e_attributes.py",
    "f_cache_cap.py",
    "d_sync_states.py",
    "b_mount_retry.py",
    "a_fsync_interrupt.py",
    "c_response_lost.py",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("scripts", nargs="*")
    args = parser.parse_args()
    if args.list:
        print(json.dumps(SCENARIOS))
        return 0
    if sys.platform != "linux":
        parser.error("Live cases require Linux")
    unknown = set(args.scripts) - set(SCENARIOS)
    if unknown:
        parser.error("Unknown cases: " + ", ".join(sorted(unknown)))
    for key in ("D9_BIN", "D9_SERVER", "D9_TARGET_VERSION", "D9_NODE_BIN"):
        if not os.environ.get(key):
            parser.error("Required configuration: " + key)
    os.environ["DRIVE9_SERVER"] = os.environ["D9_SERVER"]
    from common import CACHE_ROOT, MOUNT, RESULT_DIR, drain, mount_client
    from case_guard import boundary, run_case

    if not os.path.ismount(MOUNT):
        raise RuntimeError("Expected FUSE mount is absent: " + str(MOUNT))
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
        for script in SCENARIOS:
            if args.scripts and script not in args.scripts:
                continue
            case_id = script[:-3].replace("_", "-")
            result_path = RESULT_DIR / (case_id + ".json")
            if result_path.exists():
                raise RuntimeError(
                    "Existing result; use a fresh run directory: " + str(result_path)
                )
            boundary(MOUNT, RESULT_DIR / (case_id + "-boundary-before.json"))
            before = drain()
            if not before["ok"]:
                raise RuntimeError("Dirty mount before " + case_id)
            with (RESULT_DIR / (case_id + ".log")).open("w") as log:
                # On timeout stop the runner; inspect surviving children/network rules.
                proc, case_wall = run_case(
                    [sys.executable, str(pathlib.Path(__file__).with_name(script))],
                    stdout=log,
                    stderr=log,
                    timeout=7200,
                )
            restored_main = not os.path.ismount(MOUNT)
            if restored_main:
                mount_client(MOUNT, CACHE_ROOT / ("resume-" + case_id))
            after = drain()
            data = json.loads(result_path.read_text()) if result_path.exists() else {}
            ok = (
                proc.returncode == 0
                and data.get("verified") is True
                and not data.get("environment_blocks")
                and after["ok"]
                and all(x.get("ok") for x in data.get("sync_evidence", []))
            )
            row = dict(
                scenario=case_id,
                exit_code=proc.returncode,
                ok=bool(ok),
                case_wall_s=case_wall,
                main_restored_after_case=restored_main,
                duration_s=data.get("duration_s"),
                drain_before=before,
                drain_after=after,
                case_wall_plus_drain_s=case_wall + after["wall_s"],
            )
            rows.append(row)
            (RESULT_DIR / "run_all.json").write_text(json.dumps(rows, indent=2) + "\n")
            print(case_id, "PASS" if ok else "FAIL", flush=True)
            boundary(MOUNT, RESULT_DIR / (case_id + "-boundary-after.json"))
            if not after["ok"]:
                break
        from summarize import main as summarize

        summarize()
        expected = len(args.scripts) if args.scripts else len(SCENARIOS)
        return 0 if len(rows) == expected and all(r["ok"] for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
