from __future__ import annotations

import ctypes
import os
import re
import sys
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, write_json
from harness.module_base import BaseModule, read_text
from suites.community.sqlite.deps import SqliteTools, ensure_sqlite_tools

# Official SQLite-as-filesystem probes. This is not JuiceFS-style sqlite-as-meta:
# binaries live on the host, database files live on the Drive9 FUSE mount.
# Defaults are correctness-oriented (small, skip redundant fsync) rather than
# a soak/performance run. Durability still goes through mptester --sync.
DEFAULT_SPEEDTEST_JOURNALS = ("delete", "truncate", "persist")
DEFAULT_MPTEST_JOURNALS = ("wal", "delete")
DEFAULT_MPTEST_SCRIPTS = ("multiwrite01.test", "crash01.test")
DEFAULT_THREADTEST_GLOBS = ("walthread2", "walthread5")
DEFAULT_KVTEST_JOURNALS = ("wal", "delete")
DEFAULT_SPEEDTEST_SIZE = "1"
DEFAULT_MPTEST_REPEAT = "1"
DEFAULT_CASE_TIMEOUT_S = "300"
DEFAULT_MPTEST_BUSY_MS = "30000"
DEFAULT_MMAP_SIZE = "4194304"
DEFAULT_KVTEST_COUNT = "32"
DEFAULT_KVTEST_BLOB = "4096"
SQLITE_HEADER_MAGIC = b"SQLite format 3"

HEADER_RE = re.compile(r"(?m)^# \d{4}-\d{2}-\d{2}T[^$]*\$ ")
SPEEDTEST_HASH_RE = re.compile(r"Verification Hash:\s+(\S+)(?:\s+([0-9a-fA-F]+))?")
ERRORS_OUT_OF_RE = re.compile(
    r"(?:Summary:\s+)?(?P<errors>\d+)\s+errors?\s+out of\s+(?P<tests>\d+)\s+tests",
    re.IGNORECASE,
)
KVTEST_INTEGRITY_RE = re.compile(r"integrity-check:\s+ok", re.IGNORECASE)
CREATE_MMAP_DB = (
    "import sqlite3, sys\n"
    "path, mode, mmap_size = sys.argv[1], sys.argv[2], int(sys.argv[3])\n"
    "conn = sqlite3.connect(path)\n"
    "got = conn.execute('PRAGMA journal_mode=' + mode).fetchone()[0]\n"
    "if got.lower() != mode.lower():\n"
    "    raise SystemExit(f'journal_mode={got!r} want={mode!r}')\n"
    "conn.execute(f'PRAGMA mmap_size={mmap_size}')\n"
    "conn.execute('CREATE TABLE t(x BLOB)')\n"
    "conn.execute('INSERT INTO t VALUES (zeroblob(65536))')\n"
    "conn.commit()\n"
    "conn.close()\n"
)


def latest_section(text: str) -> str:
    """Return the last run_cmd invocation in an appended stdout/stderr log."""
    headers = list(HEADER_RE.finditer(text))
    if headers:
        return text[headers[-1].start() :]
    return text


def parse_speedtest1_output(stdout_text: str, stderr_text: str = "") -> dict[str, Any]:
    text = latest_section(stdout_text) + "\n" + latest_section(stderr_text)
    match = SPEEDTEST_HASH_RE.search(text)
    if match is None:
        return {"ok": False, "reason": "missing_verification_hash", "verify_hash": "", "verify_digest": ""}
    return {
        "ok": True,
        "reason": "",
        "verify_hash": match.group(1),
        "verify_digest": match.group(2) or "",
    }


def parse_mptest_output(stdout_text: str, stderr_text: str = "") -> dict[str, Any]:
    return parse_errors_out_of_tests(stdout_text, stderr_text)


def parse_errors_out_of_tests(stdout_text: str, stderr_text: str = "") -> dict[str, Any]:
    text = latest_section(stdout_text) + "\n" + latest_section(stderr_text)
    matches = list(ERRORS_OUT_OF_RE.finditer(text))
    if not matches:
        return {"ok": False, "reason": "missing_summary", "errors": -1, "tests": 0}
    match = matches[-1]
    errors = int(match.group("errors"))
    tests = int(match.group("tests"))
    if tests <= 0:
        return {"ok": False, "reason": "zero_tests", "errors": errors, "tests": tests}
    if errors > 0:
        return {"ok": False, "reason": "errors", "errors": errors, "tests": tests}
    return {"ok": True, "reason": "", "errors": errors, "tests": tests}


def parse_kvtest_output(stdout_text: str, stderr_text: str = "", *, require_integrity: bool = False) -> dict[str, Any]:
    text = latest_section(stdout_text) + "\n" + latest_section(stderr_text)
    if re.search(r"(?m)^ERROR:", text):
        return {"ok": False, "reason": "kvtest_error"}
    if require_integrity and KVTEST_INTEGRITY_RE.search(text) is None:
        return {"ok": False, "reason": "missing_integrity_ok"}
    if require_integrity and "Total elapsed time:" not in text:
        return {"ok": False, "reason": "missing_elapsed_time"}
    return {"ok": True, "reason": ""}


def split_csv(raw: str, default: tuple[str, ...]) -> list[str]:
    if not raw.strip():
        return list(default)
    values: list[str] = []
    seen: set[str] = set()
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        values.append(item)
    return values or list(default)


def mmap_probe(db_path: Path) -> dict[str, Any]:
    """MAP_SHARED mmap of a SQLite file. SQLite itself swallows ENODEV."""
    size = db_path.stat().st_size
    if size <= 0:
        return {"ok": False, "reason": "empty_file", "errno": 0, "size": size}
    libc = _libc()
    fd = libc.open(os.fsencode(os.fspath(db_path)), 0)
    if fd < 0:
        err = ctypes.get_errno()
        raise BlackboxError(f"open({db_path}) failed: errno={err}")
    try:
        addr = libc.mmap(None, size, 1, 1, fd, 0)
        if addr is None or addr == ctypes.c_void_p(-1).value:
            err = ctypes.get_errno()
            # ENODEV is 19 on Linux and 6 on macOS (FOPEN_DIRECT_IO without mmap cap).
            if err in (19, 6):
                return {"ok": False, "reason": "enodev", "errno": err, "size": size}
            raise BlackboxError(f"mmap({db_path}) failed: errno={err}")
        try:
            magic = ctypes.string_at(addr, 15)
            if magic != SQLITE_HEADER_MAGIC:
                return {
                    "ok": False,
                    "reason": "header_mismatch",
                    "errno": 0,
                    "size": size,
                    "magic": magic.decode("latin1", errors="replace"),
                }
        finally:
            libc.munmap(addr, size)
        return {"ok": True, "reason": "", "errno": 0, "size": size}
    finally:
        libc.close(fd)


def _libc() -> ctypes.CDLL:
    if sys.platform == "darwin":
        lib = ctypes.CDLL("libc.dylib", use_errno=True)
    else:
        lib = ctypes.CDLL("libc.so.6", use_errno=True)
    lib.open.argtypes = [ctypes.c_char_p, ctypes.c_int]
    lib.open.restype = ctypes.c_int
    lib.close.argtypes = [ctypes.c_int]
    lib.close.restype = ctypes.c_int
    lib.mmap.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_longlong,
    ]
    lib.mmap.restype = ctypes.c_void_p
    lib.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.munmap.restype = ctypes.c_int
    return lib


class CommunitySqlite(BaseModule):
    description = (
        "Run official SQLite speedtest1, mptester, threadtest3, and kvtest on a "
        "Drive9 FUSE mount, plus a MAP_SHARED mmap probe of the main DB file."
    )
    labels = ("compatibility", "functional", "community")
    timeout = 1800

    def run(self, ctx: Context) -> dict[str, Any]:
        tools = ensure_sqlite_tools(ctx)
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        profile = os.environ.get("FUSE_PROFILE") or "none"
        handle = ctx.target.mount("community_sqlite", remote, profile=profile)
        try:
            work = handle.mountpoint / "sqlite"
            work.mkdir(exist_ok=True)
            cases = self._plan_cases(tools, work)
            if not cases:
                raise BlackboxError(
                    "community.sqlite has no cases; unset SQLITE_SKIP_SPEEDTEST, "
                    "SQLITE_SKIP_MPTEST, SQLITE_SKIP_THREADTEST, SQLITE_SKIP_KVTEST, "
                    "and SQLITE_SKIP_MMAP"
                )
            results: list[dict[str, Any]] = []
            fail_fast = _env_flag("SQLITE_FAIL_FAST", False)
            case_timeout = _env_int("SQLITE_TIMEOUT_S", DEFAULT_CASE_TIMEOUT_S)
            for case in cases:
                timeout = case_timeout
                if case["kind"] == "threadtest3":
                    timeout = max(timeout, 600)
                record = self._run_case(ctx, case, timeout)
                results.append(record)
                if not record["ok"] and fail_fast:
                    break
            report = self._summarize(ctx, results)
            write_json(ctx.result_dir / "sqlite-cases.json", report)
            log = ctx.artifact_dir(self.id) / "sqlite-cases.json"
            write_json(log, report)
            failed = [item for item in results if not item["ok"]]
            if failed:
                names = ", ".join(item["name"] for item in failed)
                raise BlackboxError(f"sqlite probe failures ({len(failed)}/{len(results)}): {names}; see {log}")
            return report
        finally:
            ctx.target.unmount(handle)

    def _plan_cases(self, tools: SqliteTools, work: Path) -> list[dict[str, Any]]:
        cases: list[dict[str, Any]] = []
        mmap_size = os.environ.get("SQLITE_MMAP_SIZE", DEFAULT_MMAP_SIZE).strip() or DEFAULT_MMAP_SIZE
        if not _env_flag("SQLITE_SKIP_SPEEDTEST", False):
            size = os.environ.get("SQLITE_SPEEDTEST_SIZE", DEFAULT_SPEEDTEST_SIZE).strip() or DEFAULT_SPEEDTEST_SIZE
            journals = split_csv(os.environ.get("SQLITE_SPEEDTEST_JOURNALS", ""), DEFAULT_SPEEDTEST_JOURNALS)
            for journal in journals:
                slug = journal.lower()
                cases.append(self._speedtest_case(tools, work, slug, size, mmap_size=""))
            cases.append(self._speedtest_case(tools, work, "wal", size, mmap_size=mmap_size, name="speedtest1-wal-mmap"))
        if not _env_flag("SQLITE_SKIP_MPTEST", False):
            repeat = os.environ.get("SQLITE_MPTEST_REPEAT", DEFAULT_MPTEST_REPEAT).strip() or DEFAULT_MPTEST_REPEAT
            busy_ms = os.environ.get("SQLITE_MPTEST_BUSY_MS", DEFAULT_MPTEST_BUSY_MS).strip() or DEFAULT_MPTEST_BUSY_MS
            journals = split_csv(os.environ.get("SQLITE_MPTEST_JOURNALS", ""), DEFAULT_MPTEST_JOURNALS)
            scripts = split_csv(os.environ.get("SQLITE_MPTEST_SCRIPTS", ""), DEFAULT_MPTEST_SCRIPTS)
            for script_name in scripts:
                script_path = tools.scripts_dir / script_name
                if not script_path.is_file():
                    raise BlackboxError(f"mptest script not found: {script_path}")
                stem = Path(script_name).stem.lower()
                for journal in journals:
                    slug = journal.lower()
                    db = work / f"mptest-{slug}-{stem}.db"
                    cases.append(
                        {
                            "kind": "mptest",
                            "name": f"mptest-{slug}-{stem}",
                            "journal": slug,
                            "script": script_name,
                            "db": db,
                            "cwd": tools.scripts_dir,
                            "cmds": [
                                [
                                    str(tools.mptester),
                                    str(db),
                                    "--sync",
                                    "--journalmode",
                                    slug,
                                    "--repeat",
                                    repeat,
                                    "--timeout",
                                    busy_ms,
                                    str(script_path),
                                ]
                            ],
                        }
                    )
        if not _env_flag("SQLITE_SKIP_THREADTEST", False):
            globs = split_csv(os.environ.get("SQLITE_THREADTEST_GLOBS", ""), DEFAULT_THREADTEST_GLOBS)
            for pattern in globs:
                slug = re.sub(r"[^a-zA-Z0-9]+", "-", pattern).strip("-").lower() or "all"
                cwd = work / f"threadtest-{slug}"
                cases.append(
                    {
                        "kind": "threadtest3",
                        "name": f"threadtest3-{slug}",
                        "journal": "",
                        "script": pattern,
                        "db": cwd / "test.db",
                        "cwd": cwd,
                        "cmds": [[str(tools.threadtest3), pattern]],
                    }
                )
        if not _env_flag("SQLITE_SKIP_KVTEST", False):
            count = os.environ.get("SQLITE_KVTEST_COUNT", DEFAULT_KVTEST_COUNT).strip() or DEFAULT_KVTEST_COUNT
            blob = os.environ.get("SQLITE_KVTEST_SIZE", DEFAULT_KVTEST_BLOB).strip() or DEFAULT_KVTEST_BLOB
            journals = split_csv(os.environ.get("SQLITE_KVTEST_JOURNALS", ""), DEFAULT_KVTEST_JOURNALS)
            for journal in journals:
                slug = journal.lower()
                db = work / f"kvtest-{slug}.db"
                cases.append(
                    {
                        "kind": "kvtest",
                        "name": f"kvtest-{slug}",
                        "journal": slug,
                        "script": "",
                        "db": db,
                        "cwd": work,
                        "cmds": [
                            [
                                str(tools.kvtest),
                                "init",
                                str(db),
                                "--count",
                                count,
                                "--size",
                                blob,
                                "--pagesize",
                                "4096",
                            ],
                            [
                                str(tools.kvtest),
                                "run",
                                str(db),
                                "--count",
                                count,
                                "--max-id",
                                count,
                                "--integrity-check",
                                "--mmap",
                                mmap_size,
                                "--jmode",
                                slug,
                                "--update",
                                "--stats",
                                *(["--nosync"] if _env_flag("SQLITE_KVTEST_NOSYNC", True) else []),
                            ],
                        ],
                    }
                )
        if not _env_flag("SQLITE_SKIP_MMAP", False):
            db = work / "mmap-probe.db"
            cases.append(
                {
                    "kind": "mmap",
                    "name": "mmap-probe-wal",
                    "journal": "wal",
                    "script": "",
                    "db": db,
                    "cwd": work,
                    "cmds": [["python3", "-c", CREATE_MMAP_DB, str(db), "wal", mmap_size]],
                }
            )
        return cases

    def _speedtest_case(
        self,
        tools: SqliteTools,
        work: Path,
        journal: str,
        size: str,
        *,
        mmap_size: str,
        name: str = "",
    ) -> dict[str, Any]:
        slug = journal.lower()
        suffix = "-mmap" if mmap_size else ""
        db = work / f"speedtest1-{slug}{suffix}.db"
        cmd = [
            str(tools.speedtest1),
            "--size",
            size,
            "--journal",
            slug,
            "--verify",
            "--stats",
        ]
        if _env_flag("SQLITE_SPEEDTEST_NOSYNC", True):
            cmd.append("--nosync")
        if mmap_size:
            cmd.extend(["--mmap", mmap_size])
        cmd.append(str(db))
        return {
            "kind": "speedtest1",
            "name": name or f"speedtest1-{slug}{suffix}",
            "journal": slug,
            "script": "",
            "db": db,
            "cwd": work,
            "cmds": [cmd],
        }

    def _run_case(self, ctx: Context, case: dict[str, Any], timeout: int) -> dict[str, Any]:
        cwd = case.get("cwd")
        if isinstance(cwd, Path):
            cwd.mkdir(parents=True, exist_ok=True)
        cmds: list[list[str]] = case.get("cmds") or [case["cmd"]]
        stdout_text = ""
        stderr_text = ""
        code = 0
        seconds = 0.0
        stdout_path = ""
        stderr_path = ""
        for index, cmd in enumerate(cmds):
            name = f"community-sqlite-{case['name']}"
            if len(cmds) > 1:
                name = f"{name}-{index}"
            result = ctx.target.run_cmd(
                name,
                cmd,
                cwd=cwd if isinstance(cwd, Path) else None,
                timeout=timeout,
                ok_codes=(0, 1, 124),
            )
            seconds += result.seconds
            stdout_text = read_text(result.stdout)
            stderr_text = read_text(result.stderr)
            stdout_path = str(result.stdout)
            stderr_path = str(result.stderr)
            code = result.code
            if code == 124:
                break
            if code != 0:
                break
        record: dict[str, Any] = {
            "kind": case["kind"],
            "name": case["name"],
            "journal": case["journal"],
            "script": case["script"],
            "db": str(case["db"]),
            "rc": code,
            "seconds": round(seconds, 3),
            "stdout": stdout_path,
            "stderr": stderr_path,
            "ok": False,
            "reason": "",
            "errors": 0,
            "tests": 0,
            "verify_hash": "",
            "verify_digest": "",
            "mmap_ok": False,
        }
        if code == 124:
            record["reason"] = "timeout"
            return record
        kind = case["kind"]
        if kind == "speedtest1":
            parsed = parse_speedtest1_output(stdout_text, stderr_text)
            record.update(parsed)
            if code != 0:
                record["ok"] = False
                record["reason"] = record["reason"] or f"exit_{code}"
            return record
        if kind in {"mptest", "threadtest3"}:
            parsed = parse_errors_out_of_tests(stdout_text, stderr_text)
            record.update(parsed)
            if code != 0 and record["ok"]:
                record["ok"] = False
                record["reason"] = f"exit_{code}"
            elif code != 0 and not record["reason"]:
                record["reason"] = f"exit_{code}"
            return record
        if kind == "kvtest":
            parsed = parse_kvtest_output(stdout_text, stderr_text, require_integrity=True)
            record.update(parsed)
            if code != 0:
                record["ok"] = False
                record["reason"] = record["reason"] or f"exit_{code}"
            return record
        if kind == "mmap":
            if code != 0:
                record["reason"] = f"exit_{code}"
                return record
            return self._finish_mmap_case(record, Path(case["db"]))
        record["reason"] = f"unknown_kind_{kind}"
        return record

    def _finish_mmap_case(self, record: dict[str, Any], db: Path) -> dict[str, Any]:
        probed: list[dict[str, Any]] = []
        main = mmap_probe(db)
        probed.append({"path": str(db), **main})
        shm = Path(str(db) + "-shm")
        if shm.is_file() and shm.stat().st_size > 0:
            # WAL index is mmap'd; do not require SQLite header magic.
            shm_result = self._mmap_shared(shm, require_sqlite_header=False)
            probed.append({"path": str(shm), **shm_result})
        record["mmap_probes"] = probed
        failed = [item for item in probed if not item["ok"]]
        if failed:
            reasons = ", ".join(f"{item['path']}:{item['reason']}" for item in failed)
            record["ok"] = False
            record["reason"] = reasons
            record["mmap_ok"] = False
            if any(item.get("reason") == "enodev" for item in failed):
                record["reason"] = (
                    f"{reasons}. FUSE did not negotiate CAP_DIRECT_IO_ALLOW_MMAP "
                    "so FOPEN_DIRECT_IO files cannot be mmaped (SQLite falls back to pread)."
                )
            return record
        record["ok"] = True
        record["mmap_ok"] = True
        return record

    def _mmap_shared(self, path: Path, *, require_sqlite_header: bool) -> dict[str, Any]:
        if require_sqlite_header:
            return mmap_probe(path)
        size = path.stat().st_size
        libc = _libc()
        fd = libc.open(os.fsencode(os.fspath(path)), 0)
        if fd < 0:
            err = ctypes.get_errno()
            raise BlackboxError(f"open({path}) failed: errno={err}")
        try:
            addr = libc.mmap(None, size, 1, 1, fd, 0)
            if addr is None or addr == ctypes.c_void_p(-1).value:
                err = ctypes.get_errno()
                if err in (19, 6):
                    return {"ok": False, "reason": "enodev", "errno": err, "size": size}
                raise BlackboxError(f"mmap({path}) failed: errno={err}")
            libc.munmap(addr, size)
            return {"ok": True, "reason": "", "errno": 0, "size": size}
        finally:
            libc.close(fd)

    def _summarize(self, ctx: Context, results: list[dict[str, Any]]) -> dict[str, Any]:
        passed = sum(1 for item in results if item["ok"])
        failed = len(results) - passed
        report = {
            "schema": "drive9-fuse-sqlite/v2",
            "total_cases": len(results),
            "passed_cases": passed,
            "failed_cases": failed,
            "cases": results,
        }
        ctx.metric("community.sqlite.passed_cases", float(passed), "count")
        ctx.metric("community.sqlite.failed_cases", float(failed), "count")
        return report


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: str) -> int:
    raw = os.environ.get(name, default).strip() or default
    try:
        return max(1, int(raw))
    except ValueError as exc:
        raise BlackboxError(f"{name}={raw!r} is not an integer") from exc
