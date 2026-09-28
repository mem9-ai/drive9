"""Resolve every fsync in a strace -T trace to the file path it targets."""

import collections
import pathlib
import re
import sys

OPEN = re.compile(
    r"^(?:\d+\s+)?(\d\d:\d\d:\d\d\.\d+)\s+(?:<\.\.\.\s+)?openat\((?:AT_FDCWD,\s+)?\"([^\"]+)\""
)
OPEN_RESULT = re.compile(r"^(?:\d+\s+)?(\d\d:\d\d:\d\d\.\d+)\s+<\.\.\. openat resumed>\)\s+=\s+(\d+)")
OPEN_INLINE = re.compile(r"^(?:\d+\s+)?(\d\d:\d\d:\d\d\.\d+)\s+openat\(.*\)\s+=\s+(\d+)\s+<([0-9.]+)>$")
FSYNC_RESULT = re.compile(r"^(?:\d+\s+)?(\d\d:\d\d:\d\d\.\d+)\s+<\.\.\. fsync resumed>\)\s+=\s+.*?<([0-9.]+)>$")
FSYNC_INLINE = re.compile(r"^(?:\d+\s+)?(\d\d:\d\d:\d\d\.\d+)\s+fsync\((\d+)\)\s+=\s+.*?<([0-9.]+)>$")
FSYNC_START = re.compile(r"^(?:\d+\s+)?(\d\d:\d\d:\d\d\.\d+)\s+(?:(\d+)\s+)?fsync\((\d+)")
FSYNC_RESUME_PID = re.compile(r"^(?:\d+\s+)?(\d\d:\d\d:\d\d\.\d+)\s+<\.\.\. fsync resumed>\)")

RESUME_FD = re.compile(r"^(?:(\d+)\s+)?(\d\d:\d\d:\d\d\.\d+)\s+<\.\.\. fsync resumed>\)\s+=\s+.*?<([0-9.]+)>$")


def main(path, start, end, limit):
    fd_path = {}
    pending_fsync = {}
    rows = []
    for raw in pathlib.Path(path).read_text().splitlines():
        line = raw.strip()
        pid = None
        rest = line
        m = re.match(r"^(\d+)\s+(.*)$", line)
        if m:
            pid, rest = m.group(1), m.group(2)
        ts_match = re.match(r"^(\d\d:\d\d:\d\d\.\d+)", rest)
        if not ts_match:
            continue
        ts = ts_match.group(1)
        if ts < start or ts > end:
            continue

        open_inline = re.search(r"openat\((?:AT_FDCWD,\s+)?\"([^\"]+)\".*\)\s+=\s+(\d+)\s+<([0-9.]+)>$", rest)
        open_resume = re.search(r"<\.\.\. openat resumed>\)\s+=\s+(\d+)\s+<([0-9.]+)>$", rest)
        if open_inline:
            fd_path[(pid, open_inline.group(2))] = open_inline.group(1)
            continue
        if open_resume:
            fd = open_resume.group(1)
            if (pid, None) in pending_fsync:
                pass
            continue
        start_open = re.search(r"openat\((?:AT_FDCWD,\s+)?\"([^\"]+)\"", rest)
        if start_open and "unfinished" in rest:
            pending_open_path = start_open.group(1)
            # the resumed line carries the fd; remember path for the next resume
            pending_fsync.setdefault(("open", pid), pending_open_path)
            continue
        if "<... openat resumed>" in rest:
            m2 = re.search(r"=\s+(\d+)", rest)
            path_saved = pending_fsync.pop(("open", pid), None)
            if m2 and path_saved:
                fd_path[(pid, m2.group(1))] = path_saved
            continue

        fsync_inline = re.search(r"fsync\((\d+)\)\s+=\s+.*?<([0-9.]+)>$", rest)
        if fsync_inline:
            fd, dur = fsync_inline.group(1), float(fsync_inline.group(2))
            rows.append((ts, fd_path.get((pid, fd), "?"), dur))
            continue
        fsync_start = re.search(r"fsync\((\d+)", rest)
        if fsync_start and "unfinished" in rest:
            pending_fsync[("fsync", pid)] = fsync_start.group(1)
            continue
        if "<... fsync resumed>" in rest:
            fd = pending_fsync.pop(("fsync", pid), None)
            dur_match = re.search(r"<([0-9.]+)>$", rest)
            if fd and dur_match:
                rows.append((ts, fd_path.get((pid, fd), "?"), float(dur_match.group(1))))
            continue

    print("fsync count:", len(rows))
    by_path = collections.Counter(r[1] for r in rows)
    for name, count in by_path.most_common(12):
        print(f"  {count:4d}  {name}")
    print("\nsequence (first rows):")
    for ts, target, dur in rows[:limit]:
        short = target if len(target) < 110 else "..." + target[-107:]
        print(f"  {ts}  {dur * 1000:7.3f}ms  {short}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]) if len(sys.argv) > 4 else 30)
