"""d09 持句柄时被删/替换：旧 fd 语义、新读取、mmap 读取."""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402


def sha(data):
    return hashlib.sha256(data).hexdigest()


def run(report):
    base = dn.case_dir("d09-fd-replaced")
    d = base / "work"
    d.mkdir(parents=True, exist_ok=True)

    target = d / "config.json"
    content_a = json.dumps({"version": "A", "pad": "a" * 8000}).encode()
    content_b = json.dumps({"version": "B", "pad": "b" * 12000}).encode()
    target.write_bytes(content_a)
    ready, release, out = base / "fd.ready", base / "fd.release", base / "fd.json"
    holder = dn.spawn_worker(
        "holder",
        [target, "fd", ready, release, out, "--split", "1024"],
        log_path=base / "fd-holder.log",
    )
    dn.wait_for(lambda: ready.exists(), timeout=30, what="fd holder ready")
    os.unlink(target)
    target.write_bytes(content_b)
    release.write_text("go")
    rc = dn.wait_proc(holder, timeout=60)
    if not out.exists():
        tail = (base / "fd-holder.log").read_text()[-400:]
        report.check(False, "旧 fd 读取失败", rc=rc, log=tail)
    else:
        h = dn.read_json(out)
        sha_a, sha_b = sha(content_a), sha(content_b)
        read_sha = h.get("sha_read")
        semantics = (
            "old" if read_sha == sha_a else ("new" if read_sha == sha_b else "mixed")
        )
        dn.record_compat(report, "old_fd_semantics", semantics)
        report.check(
            semantics != "mixed",
            "旧 fd 读到全旧或全新内容（无混合）",
            semantics=semantics,
            got=h,
        )
        report.check(target.read_bytes() == content_b, "新打开读到新内容")

    mm_target = d / "mmap.bin"
    content_c = os.urandom(1 << 20)
    content_d = os.urandom(1 << 20)
    mm_target.write_bytes(content_c)
    ready, release, out = base / "mm.ready", base / "mm.release", base / "mm.json"
    holder = dn.spawn_worker(
        "holder",
        [mm_target, "mmap", ready, release, out],
        log_path=base / "mm-holder.log",
    )
    try:
        dn.wait_for(lambda: ready.exists(), timeout=30, what="mmap holder ready")
    except TimeoutError:
        dn.kill9(holder)
        dn.record_compat(report, "mmap_supported", False)
        report.sync("d09 final")
        return
    tmp = d / "mmap.tmp"
    tmp.write_bytes(content_d)
    os.replace(tmp, mm_target)
    release.write_text("go")
    rc = dn.wait_proc(holder, timeout=60)
    dn.record_compat(report, "mmap_supported", True)
    if out.exists():
        h = dn.read_json(out)
        report.check(h.get("sha_before") == sha(content_c), "mmap 初始读取正确")
        stable = h.get("sha_after") == h.get("sha_before")
        dn.record_compat(report, "mmap_stable_after_replace", stable)
        report.check(stable, "替换期间旧映射保持稳定", got=h)
    else:
        report.check(
            False,
            "mmap holder 异常退出",
            rc=rc,
            log=(base / "mm-holder.log").read_text()[-400:],
        )
    report.check(mm_target.read_bytes() == content_d, "新打开读到替换后的内容")
    report.sync("d09 final")


if __name__ == "__main__":
    main_guard(run, "d09-fd-replaced")
