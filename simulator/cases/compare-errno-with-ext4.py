#!/usr/bin/env python3
"""errno differential probe (issue #1006).

Runs the same syscall scenarios against two directory roots — one on a real
ext4 filesystem, one on the drive9 mount — and emits a per-case comparison
table plus per-family verdict marker files:

    <out>/fail-<FAM>   one line per FAIL row (parity mismatch, or divergence
                       drifted away from the documented value)
    <out>/info-<FAM>   one line per record-only row (no verdict, information)
    <out>/table.txt    the full table (also printed to stdout)

Row classes:
    parity   drive9 errno MUST equal the ext4 errno observed on this host.
    diverge  ext4 supports the feature and drive9 does not; drive9 MUST keep
             returning the documented divergence errno (drift = FAIL).
    record   no verdict; both sides recorded (kernel-boundary questions,
             timing-dependent behavior, contract cases ext4 has no twin for).

Exit code: 0 = table produced (FAIL rows are findings, not harness errors);
           2 = harness error (bad roots, unsupported platform, ...).
"""

import ctypes
import ctypes.util
import errno as _errno
import os
import platform
import shutil
import subprocess
import sys
import time

RENAME_NOREPLACE = 1
RENAME_EXCHANGE = 2
XATTR_CREATE = 1
XATTR_REPLACE = 2
FALLOC_KEEP_SIZE = 0x01
FALLOC_PUNCH_HOLE = 0x02
FALLOC_COLLAPSE_RANGE = 0x08
FALLOC_ZERO_RANGE = 0x10

SYS_RENAMEAT2 = {"x86_64": 316, "aarch64": 276}
AT_FDCWD = -100

_libc = None


def libc():
    global _libc
    if _libc is None:
        _libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    return _libc


def err_of(fn, *args, **kwargs):
    """Run fn; return 0 on success or the numeric errno raised."""
    try:
        r = fn(*args, **kwargs)
        if isinstance(r, int) and not isinstance(r, bool) and r != 0:
            return r  # nested probes that report errno as a value
        return 0
    except OSError as e:
        return e.errno if e.errno is not None else -1


def renameat2(old, new, flags):
    nr = SYS_RENAMEAT2.get(platform.machine())
    if nr is None:
        raise RuntimeError("renameat2 syscall number unknown on %s" % platform.machine())
    r = libc().syscall(nr, AT_FDCWD, os.fsencode(old), AT_FDCWD, os.fsencode(new), flags)
    if r != 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))


def c_setxattr(path, name, value, flags):
    b = value if isinstance(value, bytes) else value.encode()
    r = libc().setxattr(os.fsencode(path), name, b, len(b), flags)
    if r != 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))


def c_getxattr(path, name, size):
    buf = ctypes.create_string_buffer(size)
    r = libc().getxattr(os.fsencode(path), name, buf, size)
    if r < 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))


def c_listxattr(path, size):
    buf = ctypes.create_string_buffer(size)
    r = libc().listxattr(os.fsencode(path), buf, size)
    if r < 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))


def c_fallocate(fd, mode, off, length):
    r = libc().fallocate(fd, mode, off, length)
    if r != 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))


# ---------------------------------------------------------------------------
# scenario helpers: each runs inside a fresh per-case workspace directory


def _w(root, cid):
    return os.path.join(root, cid)


def _mkfile(path, data=b"x" * 16):
    with open(path, "wb") as f:
        f.write(data)


def _mkdir(path):
    os.mkdir(path)


# ---------------------------------------------------------------------------
# case registry


CASES = []


def case(cid, fam, cls, run, doc=None, note=""):
    CASES.append(dict(id=cid, fam=fam, cls=cls, run=run, doc=doc, note=note))


def simple_op(op):
    """Wrap a zero-arg op into the (root, ws) -> int errno shape."""

    def run(root, ws):
        return err_of(op, ws)

    return run


def dual_op(op):
    """Wrap an op taking (root, ws) into the (root, ws) -> int errno shape."""

    def run(root, ws):
        return err_of(op, root, ws)

    return run


# --- A: path resolution ------------------------------------------------------

case("A1-stat-missing-leaf", "A", "parity",
     simple_op(lambda ws: os.stat(os.path.join(ws, "nope"))))
case("A2-stat-missing-mid", "A", "parity", simple_op(
    lambda ws: os.stat(os.path.join(ws, "a", "b"))),
     note="a/ does not exist")
case("A3-stat-through-file", "A", "parity",
     lambda root, ws: (_mkfile(os.path.join(ws, "f")),
                       err_of(os.stat, os.path.join(ws, "f", "x")))[ -1])
case("A4-name-256-bytes", "A", "parity",
     simple_op(lambda ws: os.mkdir(os.path.join(os.fsencode(ws), b"n" * 256))))
case("A5-name-exactly-255", "A", "parity",
     lambda root, ws: (_mkdir(os.path.join(os.fsencode(ws), b"n" * 255)), 0)[-1])
case("A6-path-beyond-pathmax", "A", "parity",
     simple_op(lambda ws: os.stat(os.path.join(ws, "d" * 4096, "x"))))
case("A7-trailing-slash-on-file", "A", "parity",
     lambda root, ws: (_mkfile(os.path.join(ws, "f")),
                       err_of(os.stat, os.path.join(ws, "f") + "/"))[-1])

# --- B: mkdir / rmdir / unlink ----------------------------------------------


def _b1(ws):
    _mkdir(os.path.join(ws, "d"))
    os.mkdir(os.path.join(ws, "d"))


case("B1-mkdir-existing", "B", "parity", simple_op(_b1))
case("B2-mkdir-missing-parent", "B", "parity",
     simple_op(lambda ws: os.mkdir(os.path.join(ws, "a", "b"))))


def _b3(ws):
    _mkfile(os.path.join(ws, "f"))
    os.mkdir(os.path.join(ws, "f", "sub"))


case("B3-mkdir-parent-is-file", "B", "parity", simple_op(_b3))
case("B4-rmdir-missing", "B", "parity", simple_op(lambda ws: os.rmdir(os.path.join(ws, "d"))))


def _b5(ws):
    _mkfile(os.path.join(ws, "f"))
    os.rmdir(os.path.join(ws, "f"))


case("B5-rmdir-a-file", "B", "parity", simple_op(_b5))


def _b6(ws):
    _mkdir(os.path.join(ws, "d"))
    _mkfile(os.path.join(ws, "d", "kid"))
    os.rmdir(os.path.join(ws, "d"))


case("B6-rmdir-nonempty", "B", "parity", simple_op(_b6),
     note="known local-only divergence: EEXIST (issue #1006)")
case("B7-unlink-missing", "B", "parity", simple_op(lambda ws: os.unlink(os.path.join(ws, "f"))))


def _b8(ws):
    _mkdir(os.path.join(ws, "d"))
    os.unlink(os.path.join(ws, "d"))


case("B8-unlink-directory", "B", "parity", simple_op(_b8))
case("B9-unlink-dot", "B", "parity", simple_op(lambda ws: os.unlink(os.path.join(ws, "."))),
     note="kernel-side check")

# --- C: create / open flags --------------------------------------------------


def _c1(ws):
    _mkfile(os.path.join(ws, "f"))
    os.open(os.path.join(ws, "f"), os.O_CREAT | os.O_EXCL | os.O_WRONLY)


case("C1-oexcl-existing", "C", "parity", simple_op(_c1))


def _c2(ws):
    _mkdir(os.path.join(ws, "d"))
    os.open(os.path.join(ws, "d"), os.O_CREAT | os.O_WRONLY, 0o644)


case("C2-creat-on-directory", "C", "parity", simple_op(_c2))
def _c3(ws):
    fd = os.open(os.path.join(ws, "a", "f"), os.O_CREAT | os.O_WRONLY, 0o644)
    os.close(fd)


case("C3-creat-missing-parent", "C", "parity", simple_op(_c3))
case("C4-open-missing", "C", "parity", simple_op(lambda ws: os.open(os.path.join(ws, "f"), os.O_RDONLY)))


def _c5(ws):
    _mkdir(os.path.join(ws, "d"))
    os.open(os.path.join(ws, "d"), os.O_RDWR)


case("C5-open-dir-o-rdwr", "C", "parity", simple_op(_c5))


def _c6(ws):
    _mkfile(os.path.join(ws, "f"))
    fd = os.open(os.path.join(ws, "f"), os.O_RDONLY)
    try:
        os.ftruncate(fd, 4)
    finally:
        os.close(fd)


case("C6-ftruncate-readonly-fd", "C", "parity", simple_op(_c6), note="kernel-side check")


def _c7(ws):
    os.symlink("target", os.path.join(ws, "l"))
    os.open(os.path.join(ws, "l"), os.O_RDONLY | os.O_NOFOLLOW)


case("C7-onofollow-symlink", "C", "parity", simple_op(_c7), note="kernel-side check")


def _c8(ws):
    _mkfile(os.path.join(ws, "f"))
    os.open(os.path.join(ws, "f"), os.O_RDONLY | os.O_DIRECTORY)


case("C8-odirectory-on-file", "C", "parity", simple_op(_c8), note="kernel-side check")

# --- D: rename ---------------------------------------------------------------


def _mk_d_tree(ws):
    _mkdir(os.path.join(ws, "d1"))
    _mkdir(os.path.join(ws, "d2"))
    _mkfile(os.path.join(ws, "d1", "kid"))
    _mkfile(os.path.join(ws, "d2", "kid"))


def _d1(ws):
    _mkdir(os.path.join(ws, "dd"))
    _mkfile(os.path.join(ws, "f"))
    os.rename(os.path.join(ws, "f"), os.path.join(ws, "dd"))


case("D1-file-over-dir", "D", "parity", simple_op(_d1))


def _d2(ws):
    _mkdir(os.path.join(ws, "dd"))
    _mkfile(os.path.join(ws, "f"))
    os.rename(os.path.join(ws, "dd"), os.path.join(ws, "f"))


case("D2-dir-over-file", "D", "parity", simple_op(_d2))


def _d3(ws):
    _mk_d_tree(ws)
    os.rename(os.path.join(ws, "d1"), os.path.join(ws, "d2"))


case("D3-dir-over-nonempty", "D", "parity", simple_op(_d3))


def _d4(ws):
    _mkdir(os.path.join(ws, "d1"))
    _mkdir(os.path.join(ws, "d1", "d2"))
    os.rename(os.path.join(ws, "d1"), os.path.join(ws, "d1", "d2", "d3"))


case("D4-rename-into-descendant", "D", "parity", simple_op(_d4))
case("D5-source-missing", "D", "parity",
     simple_op(lambda ws: os.rename(os.path.join(ws, "old"), os.path.join(ws, "new"))))


def _d6(ws):
    _mkdir(os.path.join(ws, "d1"))
    os.rename(os.path.join(ws, "d1"), os.path.join(ws, "nodir", "new"))


case("D6-dest-parent-missing", "D", "parity", simple_op(_d6))


def _d7(ws):
    _mkfile(os.path.join(ws, "f1"), b"one")
    _mkfile(os.path.join(ws, "f2"), b"two")
    os.rename(os.path.join(ws, "f1"), os.path.join(ws, "f2"))
    return 0


case("D7-file-over-file", "D", "parity", simple_op(_d7))


def _d8(ws):
    _mkfile(os.path.join(ws, "f1"))
    os.rename(os.path.join(ws, "f1"), os.path.join(ws, "f1"))


case("D8-rename-same-path", "D", "parity", simple_op(_d8), note="kernel: OK no-op")


def _d9(ws):
    _mkfile(os.path.join(ws, "f1"))
    _mkfile(os.path.join(ws, "f2"))
    renameat2(os.path.join(ws, "f1"), os.path.join(ws, "f2"), RENAME_NOREPLACE)


case("D9-noreplace-existing", "D", "parity", simple_op(_d9), note="kernel-side EEXIST")


def _d10(ws):
    _mkfile(os.path.join(ws, "f1"))
    renameat2(os.path.join(ws, "f1"), os.path.join(ws, "f2"), RENAME_NOREPLACE)


case("D10-noreplace-dest-missing", "D", "diverge", simple_op(_d10), doc=_errno.EINVAL,
     note="ext4 succeeds; drive9 documented divergence EINVAL (#1006)")


def _d11(ws):
    _mkfile(os.path.join(ws, "f1"), b"one")
    _mkfile(os.path.join(ws, "f2"), b"two")
    renameat2(os.path.join(ws, "f1"), os.path.join(ws, "f2"), RENAME_EXCHANGE)


case("D11-exchange-existing", "D", "diverge", simple_op(_d11), doc=_errno.EINVAL,
     note="ext4 succeeds; drive9 documented divergence EINVAL (#1006)")


def _d12(ws):
    _mkfile(os.path.join(ws, "f1"))
    renameat2(os.path.join(ws, "f1"), os.path.join(ws, "f2"), RENAME_EXCHANGE)


case("D12-exchange-dest-missing", "D", "parity", simple_op(_d12), note="kernel-side ENOENT")

# --- E: link -----------------------------------------------------------------


def _e1(ws):
    _mkdir(os.path.join(ws, "d"))
    os.link(os.path.join(ws, "d"), os.path.join(ws, "l"))


case("E1-link-directory", "E", "parity", simple_op(_e1))


def _e2(ws):
    _mkfile(os.path.join(ws, "f"))
    os.link(os.path.join(ws, "f"), os.path.join(ws, "l"))
    return 0


case("E2-link-file", "E", "parity", simple_op(_e2))
case("E3-link-source-missing", "E", "parity",
     simple_op(lambda ws: os.link(os.path.join(ws, "f"), os.path.join(ws, "l"))))


def _e4(ws):
    _mkfile(os.path.join(ws, "f"))
    _mkfile(os.path.join(ws, "l"))
    os.link(os.path.join(ws, "f"), os.path.join(ws, "l"))


case("E4-link-dest-exists", "E", "parity", simple_op(_e4))
def _e5(ws):
    _mkfile(os.path.join(ws, "f"))
    os.link(os.path.join(ws, "f"), os.path.join(ws, "nodir", "l"))


case("E5-link-dest-parent-missing", "E", "parity", simple_op(_e5))

# --- F: symlink --------------------------------------------------------------


def _f1(ws):
    os.symlink("target-string", os.path.join(ws, "l"))
    got = os.readlink(os.path.join(ws, "l"))
    if got != "target-string":
        raise OSError(_errno.EINVAL, "readlink content mismatch")


case("F1-symlink-create-readlink", "F", "parity", simple_op(_f1))


def _f2(ws):
    _mkfile(os.path.join(ws, "f"))
    os.symlink("t", os.path.join(ws, "f"))


case("F2-symlink-dest-exists", "F", "parity", simple_op(_f2))
case("F3-symlink-parent-missing", "F", "parity",
     simple_op(lambda ws: os.symlink("t", os.path.join(ws, "nodir", "l"))))


def _f4(ws):
    os.symlink("nowhere", os.path.join(ws, "l"))
    os.open(os.path.join(ws, "l"), os.O_RDONLY)


case("F4-open-dangling-symlink", "F", "parity", simple_op(_f4))


def _f5(ws):
    os.symlink("loop", os.path.join(ws, "loop"))
    os.open(os.path.join(ws, "loop"), os.O_RDONLY)


case("F5-open-symlink-loop", "F", "parity", simple_op(_f5), note="kernel-side ELOOP")


def _f6(ws):
    _mkfile(os.path.join(ws, "f"))
    os.readlink(os.path.join(ws, "f"))


case("F6-readlink-regular-file", "F", "parity", simple_op(_f6))


def _f7(ws):
    os.symlink("/" + "p" * 4096, os.path.join(ws, "l"))


case("F7-symlink-target-too-long", "F", "record", simple_op(_f7),
     note="ext4 stores long targets; verdict deferred (E2BIG/ENAMETOOLONG varies by fs)")

# --- G: filename charset (deferred-error divergence shape) -------------------


def _staged_name(ws, name):
    """create+write+fsync+close a file with a raw-byte name; return stage map"""
    stages = {}

    def stageread(k, fn, *a):
        stages[k] = err_of(fn, *a)

    p = os.path.join(os.fsencode(ws), name)
    fd = None
    try:
        try:
            fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
            stages["open"] = 0
        except OSError as e:
            stages["open"] = e.errno
            return stages
        stageread("write", lambda: (os.write(fd, b"data"), 0)[-1])
        stageread("fsync", os.fsync, fd)
    finally:
        if fd is not None:
            stageread("close", os.close, fd)
    stages["stat"] = err_of(os.stat, p)
    stages["unlink"] = err_of(os.unlink, p)
    return stages


def _g(name):
    def run(root, ws):
        return _staged_name(ws, name)

    return run


case("G1-name-backslash", "G", "diverge", _g(b"back\\slash.txt"), doc="deferred-eio",
     note="ext4: all stages OK. drive9 documented shape: open/write OK, fsync/close EIO")
case("G2-name-control-byte", "G", "diverge", _g(b"ct\x01rl.txt"), doc="deferred-eio",
     note="same documented shape as G1")
case("G3-name-invalid-utf8", "G", "diverge", _g(b"\xff\xfe.txt"), doc="deferred-eio",
     note="same documented shape as G1")

# --- H: write / truncate -----------------------------------------------------

case("H1-write-truncate-basics", "H", "parity", simple_op(
    lambda ws: (_mkfile(os.path.join(ws, "f"), b"0123456789"),
                os.truncate(os.path.join(ws, "f"), 4), 0)[-1]))
case("H3-truncate-negative", "H", "parity",
     simple_op(lambda ws: os.truncate(os.path.join(ws, "f"), -1)))
def _h4(ws):
    _mkfile(os.path.join(ws, "f"))
    os.truncate(os.path.join(ws, "f"), 2 ** 62)


case("H4-truncate-huge", "H", "record", simple_op(_h4),
     note="ext4: kernel EFBIG at s_maxbytes; FUSE passes 2^62 to the daemon (boundary question)")
case("H5-truncate-missing", "H", "parity", simple_op(lambda ws: os.truncate(os.path.join(ws, "f"), 4)))

# --- I: fsync ----------------------------------------------------------------


def _i1(ws):
    _mkfile(os.path.join(ws, "f"))
    fd = os.open(os.path.join(ws, "f"), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


case("I1-fsync-readonly-fd", "I", "parity", simple_op(_i1),
     note="Linux allows fsync on read-only fds")


def _i2(ws):
    _mkdir(os.path.join(ws, "d"))
    fd = os.open(os.path.join(ws, "d"), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


case("I2-fsync-directory", "I", "parity", simple_op(_i2))


def _i3(ws):
    _mkfile(os.path.join(ws, "f"))
    fd = os.open(os.path.join(ws, "f"), os.O_RDONLY)
    try:
        os.fdatasync(fd)
    finally:
        os.close(fd)


case("I3-fdatasync", "I", "parity", simple_op(_i3))

# --- J: permissions (mode-bit based, single uid) -----------------------------


def _j1(ws):
    _mkdir(os.path.join(ws, "d"))
    os.chmod(os.path.join(ws, "d"), 0o555)
    try:
        os.open(os.path.join(ws, "d", "new"), os.O_CREAT | os.O_WRONLY, 0o644)
    finally:
        os.chmod(os.path.join(ws, "d"), 0o755)


case("J1-create-in-unwritable-dir", "J", "parity", simple_op(_j1))


def _j2(ws):
    _mkdir(os.path.join(ws, "d"))
    os.chmod(os.path.join(ws, "d"), 0o555)
    try:
        os.mkdir(os.path.join(ws, "d", "sub"))
    finally:
        os.chmod(os.path.join(ws, "d"), 0o755)


case("J2-mkdir-in-unwritable-dir", "J", "parity", simple_op(_j2))


def _j3(ws):
    _mkdir(os.path.join(ws, "d"))
    _mkfile(os.path.join(ws, "d", "f"))
    os.chmod(os.path.join(ws, "d"), 0o000)
    try:
        os.stat(os.path.join(ws, "d", "f"))
    finally:
        os.chmod(os.path.join(ws, "d"), 0o755)


case("J3-traverse-unsearchable-dir", "J", "parity", simple_op(_j3),
     note="kernel-side EACCES with default_permissions (parity since the mount option is unconditional)")

# --- K: xattr ----------------------------------------------------------------

def _k1(ws):
    _mkfile(os.path.join(ws, "f"))
    os.getxattr(os.path.join(ws, "f"), b"user.probe")


def _k2(ws):
    _mkfile(os.path.join(ws, "f"))
    c_setxattr(os.path.join(ws, "f"), b"user.probe", b"v", XATTR_REPLACE)


case("K1-getxattr-missing", "K", "parity", simple_op(_k1))
case("K2-setxattr-replace-missing", "K", "parity", simple_op(_k2))


def _k3(ws):
    _mkfile(os.path.join(ws, "f"))
    c_setxattr(os.path.join(ws, "f"), b"user.probe", b"value", 0)
    got = os.getxattr(os.path.join(ws, "f"), b"user.probe")
    if got != b"value":
        raise OSError(_errno.EINVAL, "xattr roundtrip mismatch")


case("K3-user-xattr-roundtrip", "K", "parity", simple_op(_k3))
def _k4(ws):
    _mkfile(os.path.join(ws, "f"))
    c_setxattr(os.path.join(ws, "f"), b"bogus.ns.probe", b"v", 0)


case("K4-unknown-namespace", "K", "parity", simple_op(_k4),
     note="ext4 refuses unknown namespaces; drive9 historically returned OK (#1006 finding)")


def _k5(ws):
    _mkfile(os.path.join(ws, "f"))
    c_setxattr(os.path.join(ws, "f"), b"user.probe", b"0123456789", 0)
    c_getxattr(os.path.join(ws, "f"), b"user.probe", 4)


def _k6(ws):
    _mkfile(os.path.join(ws, "f"))
    c_setxattr(os.path.join(ws, "f"), b"user.probe", b"v", 0)
    c_listxattr(os.path.join(ws, "f"), 1)


def _k7(ws):
    _mkfile(os.path.join(ws, "f"))
    os.removexattr(os.path.join(ws, "f"), b"user.probe")


def _k8(ws):
    _mkfile(os.path.join(ws, "f"))
    c_setxattr(os.path.join(ws, "f"), b"trusted.probe", b"v", 0)


def _k9(ws):
    _mkfile(os.path.join(ws, "f"))
    c_setxattr(os.path.join(ws, "f"), b"user." + b"a" * 300, b"v", 0)


case("K5-getxattr-small-buffer", "K", "parity", simple_op(_k5))
case("K6-listxattr-small-buffer", "K", "parity", simple_op(_k6))
case("K7-removexattr-missing", "K", "parity", simple_op(_k7))
case("K8-trusted-ns-nonroot", "K", "parity", simple_op(_k8),
     note="kernel-side EPERM for non-CAP_SYS_ADMIN")
case("K9-xattr-name-too-long", "K", "record", simple_op(_k9),
     note="ext4: ERANGE; record drive9 answer")

# --- L: fallocate & friends --------------------------------------------------


def _l_alloc(mode):
    def run(ws):
        _mkfile(os.path.join(ws, "f"), b"y" * 8192)
        fd = os.open(os.path.join(ws, "f"), os.O_RDWR)
        try:
            c_fallocate(fd, mode, 0, 4096)
        finally:
            os.close(fd)

    return run


case("L1-fallocate-default", "L", "diverge", simple_op(_l_alloc(0)), doc=_errno.ENOTSUP,
     note="ext4: OK; drive9 documented divergence ENOTSUP (#1006)")
case("L2-fallocate-punch-hole", "L", "diverge",
     simple_op(_l_alloc(FALLOC_PUNCH_HOLE | FALLOC_KEEP_SIZE)),
     doc=_errno.ENOTSUP, note="ext4: OK; drive9 documented divergence ENOTSUP")
case("L3-fallocate-collapse", "L", "diverge", simple_op(_l_alloc(FALLOC_COLLAPSE_RANGE)),
     doc=_errno.ENOTSUP, note="ext4: OK (aligned); drive9 documented divergence ENOTSUP")
case("L4-fallocate-zero-range", "L", "diverge", simple_op(_l_alloc(FALLOC_ZERO_RANGE)),
     doc=_errno.ENOTSUP, note="ext4: OK; drive9 documented divergence ENOTSUP")


def _l5(ws):
    _mkfile(os.path.join(ws, "src"), b"z" * 4096)
    _mkfile(os.path.join(ws, "dst"))
    s = os.open(os.path.join(ws, "src"), os.O_RDONLY)
    d = os.open(os.path.join(ws, "dst"), os.O_WRONLY)
    try:
        os.copy_file_range(s, d, 4096)
    finally:
        os.close(s)
        os.close(d)


case("L5-copy-file-range", "L", "record", simple_op(_l5),
     note="ext4: OK; drive9 unimplemented — record resulting errno")


def _l6(ws):
    _mkfile(os.path.join(ws, "f"), b"w" * 8192)
    fd = os.open(os.path.join(ws, "f"), os.O_RDONLY)
    try:
        os.lseek(fd, 0, os.SEEK_HOLE)
    finally:
        os.close(fd)


case("L6-lseek-seek-hole", "L", "record", simple_op(_l6),
     note="ext4: OK; drive9 answer recorded (affects cp/rsync --sparse)")

# --- M: special files --------------------------------------------------------


def _m1(ws):
    os.mkfifo(os.path.join(ws, "fifo"))


case("M1-mkfifo", "M", "record", simple_op(_m1),
     note="ext4: OK; drive9 answer recorded")


def _m2(ws):
    os.mknod(os.path.join(ws, "dev"), 0o0600 | 0o020000, 0)


case("M2-mknod-device-nonroot", "M", "parity", simple_op(_m2), note="kernel-side EPERM (CAP_MKNOD)")

# --- X: cross-layer rename / hardlink (local-only overlay ↔ remote) --------
# Registered only when --alt-root is given (a d9 directory on the OTHER
# layer). ext4 reference: a rename/link between two directories of the same
# filesystem succeeds.

_EXT4_ROOT = []
_ALT_ROOT = {"ext4": None, "d9": None}


def _alt_for_root(root):
    return _ALT_ROOT["ext4"] if root == _EXT4_ROOT[0] else _ALT_ROOT["d9"]


def _x1(root, ws):
    _mkfile(os.path.join(ws, "f"))
    os.rename(os.path.join(ws, "f"), os.path.join(_alt_for_root(root), "x1-target"))


def _x2(root, ws):
    _mkfile(os.path.join(ws, "f"))
    os.link(os.path.join(ws, "f"), os.path.join(_alt_for_root(root), "x2-target"))


def register_cross_layer_cases():
    case("X1-cross-layer-rename", "X", "crosslayer", dual_op(_x1), doc=_errno.EXDEV,
         note="ext4: same-fs rename OK; drive9 documented divergence EXDEV (#1006)")
    case("X2-cross-layer-hardlink", "X", "crosslayer", dual_op(_x2), doc=_errno.EXDEV,
         note="ext4: same-fs hardlink OK; drive9 documented divergence EXDEV (#1006)")


# --- W: git workspace hardlinks ---------------------------------------------
# Registered only when --git-ws-root is given (a git workspace previously
# registered via `drive9 git clone --fast`). ext4 reference: hardlinks to
# regular files are supported.

_GIT_WS = {"ext4": None, "d9": None}


def _gitws_for_root(root):
    return _GIT_WS["ext4"] if root == _EXT4_ROOT[0] else _GIT_WS["d9"]


def _w1(root, ws):
    os.link(os.path.join(_gitws_for_root(root), ".git", "config"), os.path.join(ws, "l"))


def _w2(root, ws):
    _mkfile(os.path.join(ws, "f"))
    os.link(os.path.join(ws, "f"), os.path.join(_gitws_for_root(root), ".git", "hl-out"))


def register_git_ws_cases():
    # Runtime finding (supersedes the static prediction in issue #1006): once
    # `drive9 git clone --fast` registers the workspace, .git is overlaid on
    # the local filesystem and hardlinks succeed through the overlay — which
    # matches ext4. The daemon-side ENOTSUP branch is only reachable before
    # the .git overlay materializes, so parity is the contract to lock.
    case("W1-hardlink-from-gitws-dotgit", "W", "parity", dual_op(_w1),
         note="git-ws .git hardlink succeeds via local overlay; ext4 parity")
    case("W2-hardlink-into-gitws-dotgit", "W", "parity", dual_op(_w2),
         note="git-ws .git hardlink succeeds via local overlay; ext4 parity")

# ---------------------------------------------------------------------------
# engine


def fmt(v):
    if isinstance(v, int):
        if v == 0:
            return "OK"
        try:
            return "%d(%s)" % (v, _errno.errorcode.get(v, "?"))
        except Exception:
            return str(v)
    return " ".join("%s=%s" % (k, fmt(x)) for k, x in sorted(v.items()))


def verdict(row, diverge_as_record=False):
    """return (verdict, detail)

    diverge_as_record downgrades diverge-class rows to INFO: documented
    divergence values were recorded on the remote branch and do not apply
    to other mount layers (e.g. the local-only overlay, which is a plain
    local filesystem underneath).
    """
    ext4v, d9v, cls, doc = row["ext4"], row["d9"], row["cls"], row["doc"]
    if cls == "record":
        return "INFO", ""
    if cls == "diverge" and diverge_as_record:
        return "INFO", "divergence doc is remote-branch only"
    if cls in ("diverge", "crosslayer"):
        if doc == "deferred-eio":
            ok = (isinstance(d9v, dict) and d9v.get("open") == 0 and d9v.get("write") == 0
                  and (d9v.get("fsync") == _errno.EIO or d9v.get("close") == _errno.EIO))
            return ("PASS" if ok else "FAIL"), "doc-shape=deferred-eio"
        ok = d9v == doc
        return ("PASS" if ok else "FAIL"), "doc=%s" % fmt(doc)
    return ("PASS" if ext4v == d9v else "FAIL"), ""


def detect_ext4_base(explicit):
    cands = [explicit] if explicit else [os.environ.get("EXT4_BASE"), "/mnt/ebs", os.path.expanduser("~")]
    for c in cands:
        if not c:
            continue
        c = os.path.abspath(c)
        if not os.path.isdir(c):
            continue
        try:
            out = subprocess.run(["findmnt", "-no", "FSTYPE", "-T", c],
                                 capture_output=True, text=True, timeout=10)
            fs = out.stdout.strip()
        except Exception:
            fs = ""
        if fs == "ext4":
            return c
        if explicit:
            sys.stderr.write("EXT4_BASE=%s is on %s, not ext4\n" % (c, fs or "unknown"))
    return None


def run_side(root, c):
    ws = os.path.join(root, c["id"])
    try:
        os.makedirs(ws, exist_ok=True)
        r = c["run"](root, ws)
        if not isinstance(r, int) and not isinstance(r, dict):
            r = 0
    except Exception as e:  # harness bug in the case itself
        r = -1
        sys.stderr.write("case %s raised %r\n" % (c["id"], e))
    finally:
        try:
            shutil.rmtree(ws, ignore_errors=True)
        except OSError:
            pass
    return r


def d9_version():
    p = shutil.which("drive9")
    if not p:
        return "unknown"
    try:
        out = subprocess.run([p, "--version"], capture_output=True, text=True, timeout=10)
        return out.stdout.strip().replace("\n", " ")[:80]
    except Exception:
        return "unknown"


def main():
    args = sys.argv[1:]
    ext4_base = d9_root = out_dir = alt_root = git_ws_root = None
    suffix = ""
    diverge_as_record = False
    i = 0
    while i < len(args):
        if args[i] == "--ext4-base":
            ext4_base = args[i + 1]
        elif args[i] == "--d9-root":
            d9_root = args[i + 1]
        elif args[i] == "--out":
            out_dir = args[i + 1]
        elif args[i] == "--suffix":
            suffix = args[i + 1]
        elif args[i] == "--alt-root":
            alt_root = args[i + 1]
        elif args[i] == "--git-ws-root":
            git_ws_root = args[i + 1]
        elif args[i] == "--diverge-as-record":
            diverge_as_record = True
            i += 1
            continue
        else:
            sys.stderr.write("unknown arg %r\n" % args[i])
            return 2
        i += 2
    if not (d9_root and out_dir):
        sys.stderr.write("usage: probe.py --d9-root DIR --out DIR [--ext4-base DIR]"
                         " [--suffix S] [--alt-root DIR] [--diverge-as-record]\n")
        return 2
    ext4_base = detect_ext4_base(ext4_base)
    if not ext4_base:
        sys.stderr.write("no ext4-backed base directory found (set EXT4_BASE)\n")
        return 2
    os.makedirs(out_dir, exist_ok=True)

    stamp = "%d-%d" % (int(time.time()), os.getpid())
    ext4_root = os.path.join(ext4_base, ".errno-cmp-" + stamp)
    d9ws = os.path.join(d9_root, ".errno-cmp-" + stamp)
    os.makedirs(ext4_root)
    os.makedirs(d9ws)
    _EXT4_ROOT.insert(0, ext4_root)
    ext4_alt = None
    if alt_root:
        ext4_alt = os.path.join(ext4_base, ".errno-cmp-alt-" + stamp)
        os.makedirs(ext4_alt)
        os.makedirs(alt_root, exist_ok=True)
        _ALT_ROOT["ext4"] = ext4_alt
        _ALT_ROOT["d9"] = alt_root
        register_cross_layer_cases()
    if git_ws_root:
        ext4_gitws = os.path.join(ext4_base, ".errno-cmp-gitws-" + stamp)
        os.makedirs(os.path.join(ext4_gitws, ".git"))
        _mkfile(os.path.join(ext4_gitws, ".git", "config"))
        _GIT_WS["ext4"] = ext4_gitws
        _GIT_WS["d9"] = git_ws_root
        register_git_ws_cases()

    rows = []
    for c in CASES:
        rows.append(dict(id=c["id"], fam=c["fam"], cls=c["cls"], doc=c["doc"], note=c["note"],
                         ext4=run_side(ext4_root, c), d9=run_side(d9ws, c)))

    fam_summary = {}
    for r in rows:
        v, detail = verdict(r, diverge_as_record)
        r["verdict"], r["detail"] = v, detail
        fam_summary.setdefault(r["fam"], []).append(r)

    lines = []
    lines.append("kernel: %s" % platform.release())
    lines.append("ext4 root: %s" % ext4_root)
    lines.append("drive9 root: %s%s" % (d9ws, suffix))
    lines.append("drive9 version: %s" % d9_version())
    lines.append("")
    lines.append("| case | class | ext4 | drive9 | verdict | note |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    fails = infos = passes = 0
    for r in rows:
        v = r["verdict"]
        if v == "FAIL":
            fails += 1
        elif v == "INFO":
            infos += 1
        else:
            passes += 1
        d = (" [" + r["detail"] + "]") if r["detail"] else ""
        lines.append("| %s | %s | %s | %s | %s%s | %s |" % (
            r["id"], r["cls"], fmt(r["ext4"]), fmt(r["d9"]), v, d, r["note"]))
    lines.append("")
    lines.append("total=%d pass=%d fail=%d info=%d" % (len(rows), passes, fails, infos))

    table = "\n".join(lines)
    sys.stdout.write("===ERRNO-TABLE-BEGIN%s===\n" % suffix + table + "\n===ERRNO-TABLE-END%s===\n" % suffix)
    with open(os.path.join(out_dir, "table%s.txt" % suffix), "w") as f:
        f.write(table + "\n")

    for fam, rs in sorted(fam_summary.items()):
        with open(os.path.join(out_dir, "fail-" + fam + suffix), "w") as ff, \
                open(os.path.join(out_dir, "info-" + fam + suffix), "w") as fi:
            for r in rs:
                target = ff if r["verdict"] == "FAIL" else fi if r["verdict"] == "INFO" else None
                if target:
                    target.write("%s ext4=%s d9=%s %s\n" % (r["id"], fmt(r["ext4"]), fmt(r["d9"]), r["detail"]))

    shutil.rmtree(ext4_root, ignore_errors=True)
    shutil.rmtree(d9ws, ignore_errors=True)
    if ext4_alt:
        shutil.rmtree(ext4_alt, ignore_errors=True)
        # best-effort cleanup of the d9-side alt directory contents
        for name in ("x1-target", "x2-target"):
            try:
                os.unlink(os.path.join(alt_root, name))
            except OSError:
                pass
        try:
            os.rmdir(alt_root)
        except OSError:
            pass
    if git_ws_root:
        try:
            os.unlink(os.path.join(git_ws_root, ".git", "hl-out"))
        except OSError:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
