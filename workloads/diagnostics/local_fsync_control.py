"""Control: cost of a plain local write+fsync sequence on the same EC2 disk,
plus an interleaved create->unlink probe on the diagnostic mount.
"""

import json
import os
import pathlib
import statistics
import sys
import time

sys.path.insert(0, os.environ.get("DRIVE9_DIAG_DIR", "/home/ubuntu/d9diag"))
import diag_probe as probe  # noqa: E402

LOCAL = pathlib.Path(os.environ.get("DRIVE9_DIAG_LOCAL", "/home/ubuntu/d9diag/local-fsync"))
MOUNT_UNLINK = probe.ROOT / "unlink-interleaved"
PAYLOAD = b"x" * 212


def local_sequence(count):
    """Mirror the drive9 flush shape: temp write+fsync, rename, dir fsync."""
    LOCAL.mkdir(parents=True, exist_ok=True)
    per_file = []
    fsync_times = []
    for index in range(count):
        start = time.perf_counter()
        temp = LOCAL / f"t{index:04d}.tmp"
        final = LOCAL / f"f{index:04d}.dat"
        fd = os.open(temp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
        os.write(fd, PAYLOAD)
        t = time.perf_counter()
        os.fsync(fd)
        fsync_times.append(time.perf_counter() - t)
        os.close(fd)
        os.rename(temp, final)
        dir_fd = os.open(LOCAL, os.O_RDONLY)
        t = time.perf_counter()
        os.fsync(dir_fd)
        fsync_times.append(time.perf_counter() - t)
        os.close(dir_fd)
        per_file.append(time.perf_counter() - start)
    return probe.stats(per_file), probe.stats(fsync_times)


def interleaved_unlink(count):
    MOUNT_UNLINK.mkdir(parents=True, exist_ok=True)
    values = []
    for index in range(count):
        path = MOUNT_UNLINK / f"u{index:04d}.bin"
        fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o644)
        os.write(fd, PAYLOAD)
        os.close(fd)
        start = time.perf_counter()
        os.unlink(path)
        values.append(time.perf_counter() - start)
    return probe.stats(values)


def main():
    per_file, fsync = local_sequence(100)
    print(json.dumps({"probe": "local_disk_write_fsync_rename_dirfsync", "per_file": per_file,
                      "per_fsync": fsync}, ensure_ascii=False), flush=True)
    probe.probe("mount_unlink_interleaved_pending", lambda: interleaved_unlink(30))


if __name__ == "__main__":
    main()
