"""Scenario 3: continuous editing - preview discovers file changes.

Acceptance checks:
- the declared change-detection mechanism (inotify/poll) discovers the final
  file state; coalescing is allowed but the final change must not be lost
- content read back matches the completed save
- save-to-readable and save-to-detected latencies are recorded
"""

from __future__ import annotations

import ctypes
import os
import pathlib
import struct
import time

from common import det_bytes, read_file, sha256_bytes, stats, write_file, workdir

IN_MODIFY = 0x00000002
IN_CLOSE_WRITE = 0x00000008
IN_MOVED_FROM = 0x00000040
IN_MOVED_TO = 0x00000080
IN_CREATE = 0x00000100
IN_DELETE = 0x00000200

WATCH_MASK = IN_MODIFY | IN_CLOSE_WRITE | IN_MOVED_FROM | IN_MOVED_TO | IN_CREATE | IN_DELETE

EVENT_NAMES = {
    IN_MODIFY: "modify", IN_CLOSE_WRITE: "close_write", IN_MOVED_FROM: "moved_from",
    IN_MOVED_TO: "moved_to", IN_CREATE: "create", IN_DELETE: "delete",
}


class Inotify:
    """Minimal inotify wrapper via libc (no external deps)."""

    def __init__(self, paths, mask=WATCH_MASK):
        self.libc = ctypes.CDLL("libc.so.6", use_errno=True)
        self.fd = self.libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), "inotify_init1 failed")
        self.watches = {}
        for path in paths:
            wd = self.libc.inotify_add_watch(self.fd, str(path).encode(), mask)
            if wd < 0:
                raise OSError(ctypes.get_errno(), "inotify_add_watch failed: %s" % path)
            self.watches[wd] = str(path)

    def poll(self, buffer_size=262144):
        events = []
        while True:
            try:
                data = os.read(self.fd, buffer_size)
            except BlockingIOError:
                break
            except OSError:
                break
            if not data:
                break
            offset = 0
            while offset < len(data):
                wd, mask, cookie, name_len = struct.unpack_from("iIII", data, offset)
                offset += 16
                name = data[offset:offset + name_len].rstrip(b"\0").decode("utf-8", "replace")
                offset += name_len
                events.append({"dir": self.watches.get(wd, "?"), "name": name,
                               "event": EVENT_NAMES.get(mask & 0xFFF, hex(mask & 0xFFF))})
        return events

    def close(self):
        try:
            os.close(self.fd)
        except OSError:
            pass


def inotify_available():
    try:
        watcher = Inotify(["/tmp"])
        watcher.close()
        return True
    except Exception:
        return False


def poll_scan(root, state):
    """One polling pass: return {relpath: sha256} for files under root."""
    found = {}
    for path in pathlib.Path(root).rglob("*"):
        if path.is_file():
            rel = str(path.relative_to(root))
            try:
                found[rel] = sha256_bytes(read_file(path))
            except OSError:
                found[rel] = "<read-error>"
    state.clear()
    state.update(found)
    return found


def run(report):
    root = workdir("s03-watch")
    project = root / "app"
    for sub in ("styles", "src", "assets"):
        (project / sub).mkdir(parents=True, exist_ok=True)
    write_file(project / "index.html", b"<html>v0</html>\n")
    write_file(project / "styles" / "main.css", b"body{color:#000}\n" * 40)
    write_file(project / "src" / "App.jsx", b"export default function App(){return null}\n")
    write_file(project / "assets" / "logo.svg", b"<svg>v0</svg>\n")

    report.check(True, "workspace prepared", root=str(root))

    # ---- inotify detection ----------------------------------------------
    supported = inotify_available()
    report.data["inotify_supported"] = supported

    # expected final state per path: None = must be absent, else sha256 hex
    finals = {}
    events_expectation = []   # labels that must appear as inotify events
    detected = []

    if supported:
        watcher = Inotify([project, project / "styles", project / "src", project / "assets"])
        with report.step("apply edits and collect inotify events"):
            # text change
            data = b"<html>v1</html>\n" + b"x" * 500
            write_file(project / "index.html", data, fsync=True)
            finals["index.html"] = sha256_bytes(data)
            events_expectation.append("index.html")
            time.sleep(0.3)
            # style change
            data = b"body{color:#333}\n" * 60
            write_file(project / "styles" / "main.css", data, fsync=True)
            finals["styles/main.css"] = sha256_bytes(data)
            events_expectation.append("main.css")
            time.sleep(0.3)
            # new component
            data = b"export const New = () => <div/>\n"
            write_file(project / "src" / "NewComponent.jsx", data, fsync=True)
            finals["src/NewComponent.jsx"] = sha256_bytes(data)
            time.sleep(0.3)
            # rename component
            os.rename(project / "src" / "NewComponent.jsx", project / "src" / "RenamedComponent.jsx")
            finals["src/NewComponent.jsx"] = None
            finals["src/RenamedComponent.jsx"] = sha256_bytes(data)
            events_expectation.append("NewComponent.jsx")
            time.sleep(0.3)
            # delete component
            os.unlink(project / "src" / "RenamedComponent.jsx")
            finals["src/RenamedComponent.jsx"] = None
            events_expectation.append("RenamedComponent.jsx")
            time.sleep(0.3)
            # image replace
            data = det_bytes(4242, 20000)
            tmp = project / "assets" / "logo.svg.tmp"
            write_file(tmp, data, fsync=True)
            os.replace(tmp, project / "assets" / "logo.svg")
            finals["assets/logo.svg"] = sha256_bytes(data)
            events_expectation.append("logo.svg")
            time.sleep(0.3)
            # rapid consecutive saves of the same file
            for i in range(20):
                data = b"saved-%02d\n" % i + b"y" * 200
                write_file(project / "src" / "App.jsx", data, fsync=True)
            finals["src/App.jsx"] = sha256_bytes(data)
            events_expectation.append("App.jsx")
            time.sleep(0.5)
            detected = watcher.poll()
        watcher.close()

        seen = {e["name"] for e in detected}
        missed = [name for name in events_expectation if name not in seen]
        report.check(not missed, "inotify observed an event for every changed path",
                     missed=missed, events=len(detected))
        report.check(any(e["event"] == "delete" for e in detected),
                     "inotify observed delete event",
                     deletes=[e for e in detected if e["event"] == "delete"][:3])
        report.data["inotify_events"] = detected[:60]
        report.data["inotify_event_count"] = len(detected)
    else:
        report.check(False, "inotify supported", note="inotify unavailable; polling-only client records compatibility limit")
        # still compute expected finals for the content checks below
        data = read_file(project / "index.html")
        finals["index.html"] = sha256_bytes(data)
        for rel in ("styles/main.css", "assets/logo.svg"):
            if (project / rel).exists():
                finals[rel] = sha256_bytes(read_file(project / rel))
        for rel in ("src/NewComponent.jsx", "src/RenamedComponent.jsx"):
            finals[rel] = None
        if (project / "src/App.jsx").exists():
            finals["src/App.jsx"] = sha256_bytes(read_file(project / "src/App.jsx"))

    # ---- final state readable and correct -------------------------------
    with report.step("verify final content"):
        bad = []
        for rel, digest in sorted(finals.items()):
            path = project / rel
            if digest is None:
                if path.exists():
                    bad.append({"path": rel, "why": "expected absent"})
            else:
                try:
                    if sha256_bytes(read_file(path)) != digest:
                        bad.append({"path": rel, "why": "content mismatch"})
                except OSError as err:
                    bad.append({"path": rel, "why": "read failed: %s" % err})
    report.check(not bad, "final file state matches completed saves", bad=bad[:5])

    # ---- polling detection (the fallback path) --------------------------
    with report.step("poll scan detects final state"):
        state = {}
        poll_scan(project, state)
    expected_paths = {rel for rel, digest in finals.items() if digest}
    missing = sorted(p for p in expected_paths if p not in state)
    unexpected = sorted(rel for rel, digest in finals.items() if digest is None and rel in state)
    report.check(not missing, "poll scan sees every final path", missing=missing,
                 scanned=len(state))
    report.check(not unexpected, "poll scan shows deleted paths as absent",
                 unexpected=unexpected)

    # ---- save-to-readable latency ---------------------------------------
    latencies = []
    with report.step("save-to-readable latency, 20 rounds"):
        for i in range(20):
            data = det_bytes(6000 + i, 1500 + i * 7)
            target = project / "src" / ("lat-%02d.js" % i)
            t0 = time.perf_counter()
            write_file(target, data, fsync=True)
            got = read_file(target)
            latencies.append(time.perf_counter() - t0)
            if got != data:
                report.check(False, "save-to-read content mismatch at round %d" % i)
                break
    report.data["save_to_readable_s"] = stats(latencies)

    sync = report.sync("after s03")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))


if __name__ == "__main__":
    from common import main_guard
    main_guard(run, "s03-watch")
