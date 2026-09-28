"""Map every fsync to its file path across a strace -T trace, and group into cycles."""

import collections
import pathlib
import re
import sys

OPEN_INLINE = re.compile(r"openat\((?:AT_FDCWD,\s+)?\"([^\"]+)\"[^)]*\)\s+=\s+(\d+)\s+<([0-9.]+)>$")
DUP_INLINE = re.compile(r"dup(?:2|3)?\((\d+)(?:,\s*\d+)?\)\s+=\s+(\d+)\s+<([0-9.]+)>$")
FSYNC_INLINE = re.compile(r"fsync\((\d+)\)\s+=\s+.*?<([0-9.]+)>$")
TS = re.compile(r"^(\d\d:\d\d:\d\d\.\d+)")


def main(path, start, end, cycles):
    fd_path = {}
    pending_open = {}
    pending_fsync = {}
    rows = []
    for raw in pathlib.Path(path).read_text().splitlines():
        line = raw.strip()
        pid = ""
        match = re.match(r"^(\d+)\s+(.*)$", line)
        if match:
            pid, line = match.group(1), match.group(2)
        ts_match = TS.match(line)
        if not ts_match:
            continue
        ts = ts_match.group(1)

        open_inline = OPEN_INLINE.search(line)
        if open_inline:
            fd_path[(pid, open_inline.group(2))] = open_inline.group(1)
            continue
        open_start = re.search(r"openat\((?:AT_FDCWD,\s+)?\"([^\"]+)\"", line)
        if open_start and "unfinished" in line:
            pending_open[pid] = open_start.group(1)
            continue
        if "<... openat resumed>" in line:
            resumed = re.search(r"=\s+(\d+)", line)
            saved = pending_open.pop(pid, None)
            if resumed and saved:
                fd_path[(pid, resumed.group(1))] = saved
            continue

        dup_inline = DUP_INLINE.search(line)
        if dup_inline:
            source = fd_path.get((pid, dup_inline.group(1)))
            if source:
                fd_path[(pid, dup_inline.group(2))] = source
            continue

        fsync_inline = FSYNC_INLINE.search(line)
        if fsync_inline:
            fd, dur = fsync_inline.group(1), float(fsync_inline.group(2))
            if start <= ts <= end:
                rows.append((ts, fd_path.get((pid, fd), "?" + fd), dur))
            continue
        fsync_start = re.search(r"fsync\((\d+)", line)
        if fsync_start and "unfinished" in line:
            pending_fsync[pid] = fsync_start.group(1)
            continue
        if "<... fsync resumed>" in line:
            fd = pending_fsync.pop(pid, None)
            dur_match = re.search(r"<([0-9.]+)>$", line)
            if fd and dur_match and start <= ts <= end:
                rows.append((ts, fd_path.get((pid, fd), "?" + fd), float(dur_match.group(1))))
            continue

    print("fsync rows:", len(rows), "total", round(sum(r[2] for r in rows), 3), "s")
    kinds = collections.Counter()
    for _, target, _ in rows:
        if target.startswith("?"):
            kinds["<unresolved fd>"] += 1
        elif target.rstrip("/").endswith("/pending"):
            kinds["pending/ dir"] += 1
        elif "/pending/" in target:
            kinds["pending/ tmp entry"] += 1
        elif "/shadow/" in target:
            kinds["shadow file"] += 1
        elif "/writeback" in target or target.endswith(".dat"):
            kinds["writeback file"] += 1
        else:
            kinds[target[-60:]] += 1
    for name, count in kinds.most_common(10):
        print(f"  {count:4d}  {name}")

    print("\nper-cycle sequences:")
    cycle = []
    last = None
    shown = 0
    for row in rows:
        if last is not None and float(row[0].replace(":", "")) - float(last.replace(":", "")) > 0.02:
            if shown < cycles:
                total = sum(r[2] for r in cycle)
                names = [r[1].split("/")[-1][:46] for r in cycle]
                print(f"  cycle {shown + 1}: {len(cycle)} fsync, {total * 1000:.2f}ms -> {names}")
                shown += 1
            cycle = []
        cycle.append(row)
        last = row[0]


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]) if len(sys.argv) > 4 else 4)
