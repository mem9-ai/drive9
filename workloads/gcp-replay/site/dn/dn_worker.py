#!/usr/bin/env python3
"""Helper processes for the dn corner-case suite.

Subcommands:
  append-writer <path> <lines> <interval_ms> [--fsync-every N]
  follow-reader <path> <expect> <out_json> [--timeout S] [--reopen]
  chunk-writer  <path> <total_mib> <chunk_kib> [--sleep-ms N]
  editor        <root> <worker-id> <phase> <rounds>
  scanner       <root> <out_json> <max_s> [--stop-file F]
  watcher       <dir> <out_json> <duration_s>
  holder        <path> <mode> <ready> <release> <out_json> [--split N]
  excl-racer    <path> <out_json>
  lock-holder   <path> <kind> <hold_s> <out_json>
  lock-try      <path> <kind> <out_json>      # kind = flock | fcntl
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import os
import struct
import sys
import time
from pathlib import Path


def chunk_bytes(index: int, size: int) -> bytes:
    base = ("%08d" % index).encode()
    return (base * (size // 8 + 1))[:size]


def write_out(path, obj):
    Path(path).write_text(json.dumps(obj, ensure_ascii=False))


def cmd_append_writer(a):
    path = Path(a.path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        for i in range(a.lines):
            line = json.dumps({"seq": i, "pad": "x" * 60}, separators=(",", ":")) + "\n"
            os.write(fd, line.encode())
            if a.fsync_every and (i + 1) % a.fsync_every == 0:
                os.fsync(fd)
            if a.interval_ms:
                time.sleep(a.interval_ms / 1000.0)
        os.fsync(fd)
    finally:
        os.close(fd)


def cmd_follow_reader(a):
    path = Path(a.path)
    deadline = time.time() + a.timeout
    fd = None
    offset = 0
    buf = b""
    complete = 0
    bad = 0
    dups = 0
    reads = 0
    last_seq = -1
    started = time.time()
    while time.time() < deadline and complete < a.expect:
        if fd is None:
            try:
                fd = os.open(path, os.O_RDONLY)
            except FileNotFoundError:
                time.sleep(0.05)
                continue
            if a.reopen:
                os.lseek(fd, max(0, offset - len(buf)), os.SEEK_SET)
                buf = b""
        try:
            data = os.read(fd, 1 << 16)
        except OSError as err:
            if err.errno in (errno.EAGAIN, errno.EINTR):
                time.sleep(0.02)
                continue
            raise
        if not data:
            if a.reopen:
                os.close(fd)
                fd = None
            time.sleep(0.02)
            continue
        reads += 1
        offset += len(data)
        buf += data
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if not line:
                continue
            try:
                obj = json.loads(line)
                seq = int(obj.get("seq"))
            except Exception:
                bad += 1
                continue
            if seq <= last_seq:
                dups += 1
            last_seq = seq
            complete += 1
    if fd is not None:
        os.close(fd)
    write_out(
        a.out_json,
        {
            "complete": complete,
            "expect": a.expect,
            "bad_lines": bad,
            "dups": dups,
            "last_seq": last_seq,
            "reads": reads,
            "tail_bytes": len(buf),
            "elapsed_s": round(time.time() - started, 3),
        },
    )


def cmd_chunk_writer(a):
    path = Path(a.path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    chunk = a.chunk_kib * 1024
    total = a.total_mib * 1024 * 1024
    written = 0
    index = 0
    try:
        while written < total:
            n = os.write(fd, chunk_bytes(index, chunk))
            written += n
            index += 1
            if a.sleep_ms:
                time.sleep(a.sleep_ms / 1000.0)
        os.fsync(fd)
    finally:
        os.close(fd)


def cmd_editor(a):
    root = Path(a.root)
    root.mkdir(parents=True, exist_ok=True)
    ops = 0
    atomic = root / ("atomic-%s.dat" % a.worker_id)
    inplace = root / ("inplace-%s.txt" % a.worker_id)
    for i in range(a.rounds):
        payload = json.dumps(
            {
                "file": atomic.name,
                "rev": "%s-%03d" % (a.worker_id, i),
                "pad": "y" * 3000,
            }
        )
        tmp = root / ("tmp-%s-%03d" % (a.worker_id, i))
        with open(tmp, "w") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, atomic)
        ops += 1
        if a.phase == "mixed":
            with open(inplace, "w") as fh:
                fh.write("v%03d-" % i + "z" * 2000)
                fh.flush()
            ops += 1
            new = root / ("new-%s-%03d.dat" % (a.worker_id, i))
            new.write_text("n" * 200)
            ops += 1
            old = root / ("new-%s-%03d.dat" % (a.worker_id, i - 4))
            if i >= 4 and old.exists():
                os.unlink(old)
                ops += 1
    (root / ("editor-%s.done" % a.worker_id)).write_text(str(ops))


def cmd_scanner(a):
    root = Path(a.root)
    stop = Path(a.stop_file) if a.stop_file else None
    deadline = time.time() + a.max_s
    scans = []
    errors = []
    bad = []
    vanished = 0
    while time.time() < deadline:
        start = time.time()
        n = 0
        for dirpath, _dirnames, filenames in os.walk(root):
            for name in filenames:
                p = Path(dirpath) / name
                if name.startswith("inplace-") or name.endswith(".done"):
                    n += 1
                    continue
                try:
                    data = p.read_bytes()
                except FileNotFoundError:
                    vanished += 1
                    continue
                except OSError as err:
                    errors.append("%s: %s" % (name, err))
                    continue
                n += 1
                if name.startswith("atomic-"):
                    try:
                        obj = json.loads(data)
                        if obj.get("file") != name:
                            bad.append("%s: file-field=%r" % (name, obj.get("file")))
                    except Exception as err:
                        bad.append("%s: %s" % (name, err))
        scans.append({"files": n, "elapsed_s": round(time.time() - start, 3)})
        if stop is not None and stop.exists():
            break
    write_out(
        a.out_json,
        {
            "scans": scans,
            "error_count": len(errors),
            "errors": errors[:50],
            "bad_count": len(bad),
            "bad": bad[:50],
            "vanished": vanished,
        },
    )


def cmd_watcher(a):
    import ctypes
    import select

    IN_MODIFY, IN_ATTRIB, IN_CLOSE_WRITE = 0x2, 0x4, 0x8
    IN_MOVED_FROM, IN_MOVED_TO, IN_CREATE, IN_DELETE = 0x40, 0x80, 0x100, 0x200
    IN_ISDIR = 0x40000000
    mask = (
        IN_MODIFY
        | IN_ATTRIB
        | IN_CLOSE_WRITE
        | IN_MOVED_FROM
        | IN_MOVED_TO
        | IN_CREATE
        | IN_DELETE
    )
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    fd = libc.inotify_init1(os.O_NONBLOCK)
    if fd < 0:
        write_out(
            a.out_json, {"supported": False, "error": os.strerror(ctypes.get_errno())}
        )
        return
    root = Path(a.dir)
    dirs = {}

    def add(path):
        wd = libc.inotify_add_watch(fd, str(path).encode(), ctypes.c_uint32(mask))
        if wd >= 0:
            dirs[wd] = Path(path)
        return wd

    add(root)
    stop = Path(a.stop_file) if a.stop_file else None
    jsonl_path = str(a.out_json) + ".jsonl"
    with open(jsonl_path, "w") as jsonl:
        events = 0
        started = time.time()
        while time.time() - started < a.duration_s:
            if stop is not None and stop.exists():
                break
            ready, _, _ = select.select([fd], [], [], 0.2)
            if not ready:
                continue
            data = os.read(fd, 65536)
            off = 0
            while off + 16 <= len(data):
                wd, ev_mask, _cookie, length = struct.unpack_from("iIII", data, off)
                off += 16
                name = (
                    data[off : off + length].split(b"\0", 1)[0].decode(errors="replace")
                )
                off += length
                ev = {
                    "name": name,
                    "mask": ev_mask,
                    "ts": round(time.time() - started, 3),
                }
                jsonl.write(json.dumps(ev) + "\n")
                jsonl.flush()
                events += 1
                if ev_mask & IN_ISDIR and ev_mask & (IN_CREATE | IN_MOVED_TO):
                    child = dirs.get(wd, root) / name
                    if child.is_dir():
                        add(child)
    os.close(fd)
    write_out(a.out_json, {"supported": True, "count": events})


def cmd_ghost_checker(a):
    root = Path(a.root)
    stop = Path(a.stop_file) if a.stop_file else None
    deadline = time.time() + a.max_s
    scans = 0
    vanished = 0
    errors = []
    max_entries = 0
    while time.time() < deadline:
        if stop is not None and stop.exists():
            break
        try:
            names = [
                n
                for n in os.listdir(root)
                if not n.endswith(".done") and n.startswith(a.prefix)
            ]
        except OSError as err:
            errors.append("listdir: %s" % err)
            time.sleep(0.05)
            continue
        scans += 1
        max_entries = max(max_entries, len(names))
        for name in names:
            try:
                with open(root / name, "rb") as fh:
                    fh.read(16)
            except FileNotFoundError:
                vanished += 1
            except OSError as err:
                errors.append("%s: %s" % (name, err))
        time.sleep(0.02)
    write_out(
        a.out_json,
        {
            "scans": scans,
            "vanished": vanished,
            "error_count": len(errors),
            "errors": errors[:30],
            "max_entries": max_entries,
        },
    )


def cmd_holder(a):
    path = Path(a.path)
    out = {"mode": a.mode}
    if a.mode == "mmap":
        import mmap

        fd = os.open(path, os.O_RDONLY)
        mm = mmap.mmap(fd, 0, prot=mmap.PROT_READ)
        out["sha_before"] = hashlib.sha256(mm[:]).hexdigest()
        Path(a.ready).write_text("ready")
        while not Path(a.release).exists():
            time.sleep(0.05)
        out["sha_after"] = hashlib.sha256(mm[:]).hexdigest()
        mm.close()
        os.close(fd)
    else:
        fd = os.open(path, os.O_RDONLY)
        first = os.read(fd, a.split)
        Path(a.ready).write_text("ready")
        while not Path(a.release).exists():
            time.sleep(0.05)
        rest = bytearray()
        while True:
            chunk = os.read(fd, 1 << 16)
            if not chunk:
                break
            rest += chunk
        out["sha_read"] = hashlib.sha256(first + bytes(rest)).hexdigest()
        out["bytes"] = len(first) + len(rest)
        os.close(fd)
    write_out(a.out_json, out)


def cmd_excl_racer(a):
    out = {"path": a.path}
    try:
        fd = os.open(a.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except OSError as err:
        out["won"] = False
        out["errno"] = err.errno
        out["error"] = err.strerror
        write_out(a.out_json, out)
        return
    out["won"] = True
    os.write(fd, b"winner")
    time.sleep(0.3)
    os.close(fd)
    write_out(a.out_json, out)


def cmd_lock_holder(a):
    path = Path(a.path)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    if a.kind == "flock":
        fcntl.flock(fd, fcntl.LOCK_EX)
    else:
        fcntl.lockf(fd, fcntl.LOCK_EX)
    write_out(a.out_json, {"state": "holding", "kind": a.kind})
    time.sleep(a.hold_s)
    if a.kind == "flock":
        fcntl.flock(fd, fcntl.LOCK_UN)
    else:
        fcntl.lockf(fd, fcntl.LOCK_UN)
    os.close(fd)
    write_out(a.out_json, {"state": "released", "kind": a.kind})


def cmd_lock_try(a):
    path = Path(a.path)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    out = {"kind": a.kind}
    try:
        if a.kind == "flock":
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        out["acquired"] = True
        if a.kind == "flock":
            fcntl.flock(fd, fcntl.LOCK_UN)
        else:
            fcntl.lockf(fd, fcntl.LOCK_UN)
    except OSError as err:
        out["acquired"] = False
        out["errno"] = err.errno
        out["error"] = err.strerror
    finally:
        os.close(fd)
    write_out(a.out_json, out)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    ap = sub.add_parser("append-writer")
    ap.add_argument("path")
    ap.add_argument("lines", type=int)
    ap.add_argument("interval_ms", type=int)
    ap.add_argument("--fsync-every", type=int, default=0)
    ap.set_defaults(fn=cmd_append_writer)

    ap = sub.add_parser("follow-reader")
    ap.add_argument("path")
    ap.add_argument("expect", type=int)
    ap.add_argument("out_json")
    ap.add_argument("--timeout", type=float, default=60.0)
    ap.add_argument("--reopen", action="store_true")
    ap.set_defaults(fn=cmd_follow_reader)

    ap = sub.add_parser("chunk-writer")
    ap.add_argument("path")
    ap.add_argument("total_mib", type=int)
    ap.add_argument("chunk_kib", type=int)
    ap.add_argument("--sleep-ms", type=int, default=0)
    ap.set_defaults(fn=cmd_chunk_writer)

    ap = sub.add_parser("editor")
    ap.add_argument("root")
    ap.add_argument("worker_id")
    ap.add_argument("phase", choices=["atomic", "mixed"])
    ap.add_argument("rounds", type=int)
    ap.set_defaults(fn=cmd_editor)

    ap = sub.add_parser("scanner")
    ap.add_argument("root")
    ap.add_argument("out_json")
    ap.add_argument("max_s", type=float)
    ap.add_argument("--stop-file", default="")
    ap.set_defaults(fn=cmd_scanner)

    ap = sub.add_parser("watcher")
    ap.add_argument("dir")
    ap.add_argument("out_json")
    ap.add_argument("duration_s", type=float)
    ap.add_argument("--stop-file", default="")
    ap.set_defaults(fn=cmd_watcher)

    ap = sub.add_parser("ghost-checker")
    ap.add_argument("root")
    ap.add_argument("out_json")
    ap.add_argument("max_s", type=float)
    ap.add_argument("--stop-file", default="")
    ap.add_argument("--prefix", default="")
    ap.set_defaults(fn=cmd_ghost_checker)

    ap = sub.add_parser("holder")
    ap.add_argument("path")
    ap.add_argument("mode", choices=["fd", "mmap"])
    ap.add_argument("ready")
    ap.add_argument("release")
    ap.add_argument("out_json")
    ap.add_argument("--split", type=int, default=4096)
    ap.set_defaults(fn=cmd_holder)

    ap = sub.add_parser("excl-racer")
    ap.add_argument("path")
    ap.add_argument("out_json")
    ap.set_defaults(fn=cmd_excl_racer)

    ap = sub.add_parser("lock-holder")
    ap.add_argument("path")
    ap.add_argument("kind", choices=["flock", "fcntl"])
    ap.add_argument("hold_s", type=float)
    ap.add_argument("out_json")
    ap.set_defaults(fn=cmd_lock_holder)

    ap = sub.add_parser("lock-try")
    ap.add_argument("path")
    ap.add_argument("kind", choices=["flock", "fcntl"])
    ap.add_argument("out_json")
    ap.set_defaults(fn=cmd_lock_try)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
