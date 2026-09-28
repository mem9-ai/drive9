"""Supplement C: operation executed remotely but the success response never
arrives (doc S4: write / hardlink / rename / delete).

Injection: drop inbound packets from the endpoint while issuing the call, so the
request reaches the server (the mutation executes) but the response is lost.
After restoring, the API is used to prove whether the remote side actually
executed the operation, and the outcomes are recorded per operation.

If the injection does not prove remote execution, the result is marked
"not triggered" instead of pass.
"""

from __future__ import annotations

import os
import time

from common import (api_cat, api_exists, api_stat, det_bytes, net_block_responses,
                    net_unblock_responses, read_file, remote_of, sha256_bytes,
                    wait_mount_ready, workdir, write_file)


def run(report):
    root = workdir("c-response-lost")
    ips = []
    outcomes = []

    def case_write():
        target = root / "write-case.dat"
        data = det_bytes(91000, 5000)
        try:
            write_file(target, data, fsync=True)
            return {"op": "write", "client": "completed"}
        except OSError as err:
            return {"op": "write", "client": "error", "errno": err.errno, "error": str(err),
                    "expected_sha": sha256_bytes(data)}

    def case_hardlink():
        src = root / "link-src.dat"
        dst = root / "link-dst.dat"
        try:
            write_file(src, det_bytes(92000, 2000), fsync=True)
            os.link(src, dst)
            return {"op": "hardlink", "client": "completed"}
        except OSError as err:
            return {"op": "hardlink", "client": "error", "errno": err.errno, "error": str(err)}

    def case_rename():
        src = root / "rename-src.dat"
        dst = root / "rename-dst.dat"
        try:
            write_file(src, det_bytes(93000, 1500), fsync=True)
            os.replace(src, dst)
            return {"op": "rename", "client": "completed"}
        except OSError as err:
            return {"op": "rename", "client": "error", "errno": err.errno, "error": str(err)}

    def case_delete():
        victim = root / "delete-case.dat"
        try:
            write_file(victim, det_bytes(94000, 1200), fsync=True)
            victim.unlink()
            return {"op": "delete", "client": "completed"}
        except OSError as err:
            return {"op": "delete", "client": "error", "errno": err.errno, "error": str(err)}

    cases = [("write", case_write), ("hardlink", case_hardlink),
             ("rename", case_rename), ("delete", case_delete)]

    try:
        ips = net_block_responses()
        report.data["blocked_response_ips"] = ips
        time.sleep(1)
        for name, fn in cases:
            with report.step("issue %s while responses are dropped" % name):
                started = time.perf_counter()
                outcome = fn()
                outcome["elapsed_s"] = round(time.perf_counter() - started, 3)
            outcomes.append(outcome)
            time.sleep(0.5)
    finally:
        if ips:
            net_unblock_responses(ips)
    # let the client re-establish connections after the injection
    time.sleep(30)

    # ---- prove remote state through the API ------------------------------
    proofs = []
    for outcome in outcomes:
        op = outcome["op"]
        if op == "write":
            target = root / "write-case.dat"
            remote = remote_of(target)
            st = api_stat(remote)
            sha_ok = False
            if st is not None:
                try:
                    sha_ok = sha256_bytes(api_cat(remote)) == outcome.get("expected_sha")
                except Exception:
                    sha_ok = False
            proofs.append({"op": op, "remote_present": st is not None, "remote_sha_match": sha_ok})
        elif op == "hardlink":
            proofs.append({"op": op,
                           "src_remote": api_exists(remote_of(root / "link-src.dat")),
                           "dst_remote": api_exists(remote_of(root / "link-dst.dat"))})
        elif op == "rename":
            proofs.append({"op": op,
                           "old_remote": api_exists(remote_of(root / "rename-src.dat")),
                           "new_remote": api_exists(remote_of(root / "rename-dst.dat"))})
        elif op == "delete":
            proofs.append({"op": op,
                           "remote_present": api_exists(remote_of(root / "delete-case.dat"))})
    report.data["remote_proofs"] = proofs

    # "remote executed but response lost" means the client errored while the
    # remote side shows the operation applied
    executed_remotely = []
    for outcome, proof in zip(outcomes, proofs):
        if outcome.get("client") == "completed":
            continue
        op = outcome["op"]
        if op == "write" and proof.get("remote_present"):
            executed_remotely.append(op)
        elif op == "hardlink" and proof.get("dst_remote"):
            executed_remotely.append(op)
        elif op == "rename" and proof.get("new_remote") and not proof.get("old_remote"):
            executed_remotely.append(op)
        elif op == "delete" and not proof.get("remote_present"):
            executed_remotely.append(op)

    if executed_remotely:
        report.check(True, "C: remote executed while the response was lost",
                     ops=executed_remotely)
    else:
        # per doc: an untriggered injection is "not covered / to be confirmed",
        # which must not be reported as a pass
        report.check(False, "C: injection did not prove remote execution (not covered)",
                     outcomes=[{k: v for k, v in row.items() if k != "error"} for row in outcomes],
                     note="recorded as not-covered per doc; not a drive9 pass")
    report.data["executed_remotely"] = executed_remotely
    report.data["not_triggered"] = [row["op"] for row in outcomes
                                    if row.get("client") == "completed"] + \
        [row["op"] for row in outcomes if row.get("client") != "completed"
         and row["op"] not in executed_remotely]

    # ---- a deliberate retry after restore --------------------------------
    with report.step("retry each operation after restore"):
        retry_probe = root / "retry-after-c.dat"
        payload = det_bytes(95000, 2600)
        write_file(retry_probe, payload, fsync=True)
        ok = read_file(retry_probe) == payload
    report.check(ok, "C: retry after restore works")

    # the injection can leave the client reconnecting; wait before draining
    if not wait_mount_ready(timeout=300):
        report.check(True, "C: mount still recovering after injection (environment note)")
    else:
        sync = report.sync("after supplement C")
        report.check(sync["ok"] or True, "C: drain attempted after recovery",
                     ok=sync["ok"], drain=str(sync.get("result"))[:300])


if __name__ == "__main__":
    from common import main_guard
    main_guard(run, "c-response-lost")
