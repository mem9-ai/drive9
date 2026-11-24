#!/bin/sh
# drive9/fuse/concurrency: Multi-writer + multi-reader FUSE concurrency stress.
# - Parallel writer threads doing atomic write+fsync+replace, append+fsync,
#   churn create+unlink, directory replace, and open-handle read across replace
# - Concurrent reader threads asserting zero torn/partial reads
# - Drain + fresh-cache remount manifest check + remote residue check
set -eu

WORKERS="${FUSE_CONCURRENCY_WORKERS:-4}"
FILES_PER_WORKER="${FUSE_CONCURRENCY_FILES_PER_WORKER:-8}"
READERS="${FUSE_CONCURRENCY_READER_WORKERS:-2}"
PAYLOAD_KB="${FUSE_CONCURRENCY_PAYLOAD_KB:-32}"
HELPER="$RUN_DIR/concurrency_suite.py"
export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export WORKERS FILES_PER_WORKER READERS PAYLOAD_KB HELPER

cat > "$HELPER" <<'PYEOF'
import hashlib
import json
import os
import shutil
import sys
import threading
import time
import traceback

MOUNT = os.environ["MOUNT"]
RUN_DIR = os.environ["RUN_DIR"]
WORKERS = int(os.environ.get("WORKERS", "4"))
FILES = int(os.environ.get("FILES_PER_WORKER", "8"))
READERS = int(os.environ.get("READERS", "2"))
PAYLOAD_KB = int(os.environ.get("PAYLOAD_KB", "32"))
EXPECTED_JSON = os.path.join(RUN_DIR, "conc_expected.json")
ROOT = os.path.join(MOUNT, "conc-work")


def payload(worker, idx, tag):
    seed = hashlib.sha256(f"{worker}:{idx}:{tag}".encode()).digest()
    target = PAYLOAD_KB * 1024
    out = bytearray()
    c = 0
    while len(out) < target:
        out.extend(hashlib.sha256(seed + c.to_bytes(4, "big")).digest())
        c += 1
    return bytes(out[:target])


def is_transient(rel):
    if rel == ".go-fuse-epoll-hack" or rel.endswith("/.go-fuse-epoll-hack"):
        return True
    if rel.startswith("churn/"):
        return True
    parts = rel.split("/")
    return any(p.startswith(".") or ".tmp." in p or p.endswith(".tmp") for p in parts)


def atomic_write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


mode = sys.argv[1]
if mode == "run":
    os.makedirs(ROOT, exist_ok=True)
    expected = {}
    errors = []
    lock = threading.Lock()
    stop_readers = threading.Event()

    def rec_err(msg):
        with lock:
            errors.append(msg)

    for w in range(WORKERS):
        ap = bytearray()
        for i in range(FILES):
            d1 = payload(w, i, "final")
            expected[f"final/w{w}/file-{i:03d}.bin"] = {"size": len(d1), "sha256": hashlib.sha256(d1).hexdigest()}
            d2 = payload(w, i, "renamed-dir")
            expected[f"renamed-dirs/w{w}/dst-{i:03d}/payload.txt"] = {"size": len(d2), "sha256": hashlib.sha256(d2).hexdigest()}
            ap.extend(f"worker={w} index={i} append-ok\n".encode())
        expected[f"append/w{w}.log"] = {"size": len(ap), "sha256": hashlib.sha256(bytes(ap)).hexdigest()}
        dh = payload(w, FILES - 1, "handle")
        expected[f"handles/w{w}-final.bin"] = {"size": len(dh), "sha256": hashlib.sha256(dh).hexdigest()}

    def writer(w):
        try:
            for i in range(FILES):
                atomic_write(os.path.join(ROOT, f"final/w{w}/file-{i:03d}.bin"), payload(w, i, "final"))
                ap_path = os.path.join(ROOT, f"append/w{w}.log")
                os.makedirs(os.path.dirname(ap_path), exist_ok=True)
                with open(ap_path, "ab") as f:
                    f.write(f"worker={w} index={i} append-ok\n".encode())
                    f.flush()
                    os.fsync(f.fileno())

                cdir = os.path.join(ROOT, "churn", f"w{w}")
                os.makedirs(cdir, exist_ok=True)
                ctmp = os.path.join(cdir, f"del-{i:03d}.tmp")
                with open(ctmp, "wb") as f:
                    f.write(payload(w, i, "delete"))
                    f.flush()
                    os.fsync(f.fileno())
                os.unlink(ctmp)

                sdir = os.path.join(ROOT, "renamed-dirs", f"w{w}", f"src-{i:03d}")
                ddir = os.path.join(ROOT, "renamed-dirs", f"w{w}", f"dst-{i:03d}")
                os.makedirs(sdir, exist_ok=True)
                atomic_write(os.path.join(sdir, "payload.txt"), payload(w, i, "renamed-dir"))
                os.replace(sdir, ddir)

                hinit = os.path.join(ROOT, "handles", f"w{w}.bin")
                hfin = os.path.join(ROOT, f"handles/w{w}-final.bin")
                hdata = payload(w, i, "handle")
                atomic_write(hinit, hdata)
                with open(hinit, "rb") as fh:
                    os.replace(hinit, hfin)
                    seen = fh.read()
                if seen != hdata:
                    raise AssertionError(f"open handle mismatch w={w} i={i}")
        except Exception:
            rec_err(traceback.format_exc())

    def reader(rid):
        while not stop_readers.is_set():
            try:
                for cur, _, fnames in os.walk(ROOT):
                    for n in fnames:
                        p = os.path.join(cur, n)
                        rel = os.path.relpath(p, ROOT).replace(os.sep, "/")
                        if is_transient(rel) or not rel.startswith("final/"):
                            continue
                        try:
                            with open(p, "rb") as f:
                                got = f.read()
                        except (FileNotFoundError, IsADirectoryError):
                            continue
                        parts = rel.split("/")
                        w = int(parts[1][1:])
                        idx = int(parts[2].split("-")[1].split(".")[0])
                        want = payload(w, idx, "final")
                        if got != want:
                            rec_err(f"reader {rid} torn read on {rel}: len={len(got)} want={len(want)}")
            except Exception:
                rec_err(traceback.format_exc())
            time.sleep(0.01)

    rthreads = [threading.Thread(target=reader, args=(i,), daemon=True) for i in range(READERS)]
    wthreads = [threading.Thread(target=writer, args=(i,), daemon=True) for i in range(WORKERS)]
    for t in rthreads + wthreads:
        t.start()
    for t in wthreads:
        t.join(120.0)
        if t.is_alive():
            rec_err(f"writer {t.name} timed out")
    stop_readers.set()
    for t in rthreads:
        t.join(5.0)
    shutil.rmtree(os.path.join(ROOT, "churn"), ignore_errors=True)
    assert not errors, "concurrency errors:\n" + "\n".join(errors[:10])
    with open(EXPECTED_JSON, "w") as f:
        json.dump(expected, f, sort_keys=True)

elif mode == "verify":
    with open(EXPECTED_JSON) as f:
        expected = json.load(f)
    actual = {}
    for cur, dirs, fnames in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d != ".go-fuse-epoll-hack"]
        for n in fnames:
            if n == ".go-fuse-epoll-hack":
                continue
            p = os.path.join(cur, n)
            rel = os.path.relpath(p, ROOT).replace(os.sep, "/")
            assert ".tmp." not in rel and not rel.endswith(".tmp"), f"leftover temp residue: {rel}"
            data = open(p, "rb").read()
            actual[rel] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    assert actual == expected, f"missing={set(expected)-set(actual)} extra={set(actual)-set(expected)}"

print(f"ok: {mode}")
PYEOF

check shell --req 'drive9-fuse-conc-stress' --claim 'fuse-concurrency-parallel-rw-and-replace' -- python3 "$HELPER" run
check shell --req 'drive9-fuse-conc-local-verify' --claim 'fuse-concurrency-same-mount-manifest' -- python3 "$HELPER" verify

sample post_stress
drain --mode require-ok --timeout 180s
check sync-ok --req 'drive9-fuse-conc-drain'

remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-fuse-conc-remount-verify' --claim 'fuse-concurrency-fresh-remount-manifest' -- python3 "$HELPER" verify
sample remote_after --as remote
check manifest-equals --this @remote_after --against @post_stress \
    --ignore '.sim/**' --req 'drive9-fuse-conc-remote-parity'
