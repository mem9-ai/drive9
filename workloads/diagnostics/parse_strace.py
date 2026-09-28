"""Aggregate a strace -T trace by syscall: count, total, mean, p95, max."""

import collections
import pathlib
import re
import statistics
import sys

INLINE = re.compile(
    r"^(?:\[pid\s+)?(?:\d+\]?\s+)?(\d\d:\d\d:\d\d\.\d+)\s+([a-zA-Z0-9_]+)\((.*)\)\s+=\s+.*?<([0-9.]+)>$"
)
RESUMED = re.compile(
    r"^(?:\[pid\s+)?(?:\d+\]?\s+)?(\d\d:\d\d:\d\d\.\d+)\s+<\.\.\.\s+([a-zA-Z0-9_]+)\s+resumed>(.*)\s+=\s+.*?<([0-9.]+)>$"
)


def main(path, slow_threshold):
    per = collections.defaultdict(list)
    slow = []
    for raw in pathlib.Path(path).read_text().splitlines():
        line = raw.strip()
        match = INLINE.match(line) or RESUMED.match(line)
        if not match:
            continue
        ts, syscall, args, dur = match.group(1), match.group(2), match.group(3), float(match.group(4))
        per[syscall].append(dur)
        if dur > slow_threshold:
            slow.append((dur, ts, syscall, args[:80]))

    header = "{:12s} {:>7s} {:>9s} {:>10s} {:>10s} {:>10s}".format(
        "syscall", "calls", "total_s", "mean_ms", "p95_ms", "max_ms"
    )
    print(header)
    for name, values in sorted(per.items(), key=lambda kv: -sum(kv[1])):
        ordered = sorted(values)
        print("{:12s} {:7d} {:9.3f} {:10.3f} {:10.3f} {:10.3f}".format(
            name, len(ordered), sum(ordered), statistics.mean(ordered) * 1000,
            ordered[int(len(ordered) * 0.95) - 1] * 1000, ordered[-1] * 1000,
        ))
    print()
    print("slowest individual calls:")
    for dur, ts, syscall, args in sorted(slow, reverse=True)[:15]:
        print("  {:8.2f}ms {} {}({})".format(dur * 1000, ts, syscall, args))


if __name__ == "__main__":
    main(sys.argv[1], float(sys.argv[2]) if len(sys.argv) > 2 else 0.003)
