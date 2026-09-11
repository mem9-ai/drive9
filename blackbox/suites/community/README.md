# Community Modules

This directory holds the `community.*` blackbox modules — open source filesystem
test suites and tools run against a Drive9 FUSE mount. The generic harness lives
under `blackbox/harness`; drive9-specific environment wiring (server/CLI/FUSE
mount, dependency preparation) lives under `blackbox/env`.

## Modules

Each subdirectory is one auto-discovered module (`module.py`), optionally with a
`deps.py` and data files.

| Module | Profile | Description |
|---|---|---|
| `community.fio` | performance | fio sequential and random I/O workloads. |
| `community.fsx` | functional | fsx randomized file operation stress. |
| `community.lock` | compatibility | POSIX advisory lock checks. |
| `community.ltp.fs` | compatibility | LTP filesystem scenario (`drive9-fs-smoke`). |
| `community.ltp.syscalls` | compatibility | LTP filesystem-sensitive syscall subset (`drive9-syscalls-fs`). |
| `community.mdtest` | performance | mdtest metadata create/stat/remove workload. |
| `community.pjdfstest` | compatibility | pjdfstest POSIX pass rate. |
| `community.pyxattr` | compatibility | pyxattr-backed extended attribute checks. |
| `community.sqlite` | compatibility | Official SQLite `speedtest1`, `mptester`, `threadtest3`, and `kvtest` on a FUSE mount, plus a MAP_SHARED mmap probe. |

The two LTP modules each have their own `module.py` but share the LTP
dependency logic (`ltp_fs/deps.py`, re-exported by `ltp_syscalls/deps.py`).

## Selection

Modules are selected with `python3 blackbox/run.py`. The `community` directory
group selects every module here:

```bash
python3 blackbox/run.py --group community
```

Label filters narrow within a selection. Community-relevant labels include
`posix` (`community.pjdfstest`), `performance` (`community.fio`,
`community.mdtest`), and `compatibility`
(`community.pjdfstest`, `community.sqlite`, LTP, lock, pyxattr):

```bash
python3 blackbox/run.py --group community --label performance
python3 blackbox/run.py --group community --label posix
python3 blackbox/run.py --module community.pjdfstest
python3 blackbox/run.py --module community.sqlite
```

## Dependencies

The harness prefers already-installed tools, then `*_BIN` / `*_DIR` environment
overrides, then auto-fetch under the work-dir cache. On a clean Linux host with
passwordless `sudo`, it also auto-installs OS packages needed to *build* those
tools:

| Package manager | Distros | Notes |
|---|---|---|
| `apt-get` | Debian / Ubuntu | Installs Debian package names as written in deps |
| `pacman` | Arch Linux / Arch-based (incl. Orb Arch) | Maps Debian-style names (`build-essential` → `base-devel`, `pkg-config` → `pkgconf`, `mpich` → `openmpi`, …) |

Disable OS package install with `AUTO_INSTALL_SYSTEM_DEPS=0`. Without passwordless
`sudo`, install build deps yourself and re-run (tools may still auto-fetch/build
from source when compilers are present).

Direct environment overrides (read without prefix):

```bash
PJDFSTEST_DIR=/path/to/pjdfstest
PJDFSTEST_TESTS=/path/to/pjdfstest/tests
PJDFSTEST_BIN=/path/to/pjdfstest
PJDFSTEST_ALLOW_NONROOT=1          # pjdfstest normally requires root
LTP_ROOT=/path/to/ltp              # installed tree with kirk (or runltp), runtest/, testcases/bin/
LTP_INSTALL_ROOT=/path/to/ltp-install
FIO_BIN=/path/to/fio
MDTEST_BIN=/path/to/mdtest
MPICC=/path/to/mpicc
FSX_BIN=/path/to/fsx
SPEEDTEST1_BIN=/path/to/speedtest1
MPTEST_BIN=/path/to/mptester          # upstream binary name is mptester
KVTEST_BIN=/path/to/kvtest
THREADTEST3_BIN=/path/to/threadtest3
SQLITE_SRC=/path/to/sqlite            # source tree with mptest/*.test
```

Tunables read via the harness `env_value` helper accept a `BLACKBOX_` prefix
(`BLACKBOX_LTP_REF` is equivalent to the documented base name):

```bash
LTP_REF=20260529
LTP_RUNNER=kirk                     # kirk (default) or runltp
LTP_FS_CASES="openfile01 stream01 ftest01 lftest01 writetest01"  # allow-list override
LTP_FS_EXCLUDE="gf01 gf02 ... read_all_dev proc01"               # deny-list override
LTP_SYSCALL_DIRS="access chmod chown close ..."
LTP_SYSCALL_CASES="access01 chmod01 open01 write01 ..."          # allow-list override
LTP_SYSCALL_EXCLUDE="alarm02 bind01 ..."                          # deny-list override
LTP_SYSCALLS_SHARDS=3               # split syscalls into N shard files
LTP_MAKE_JOBS=2
LTP_BUILD_TIMEOUT_S=1800
FIO_REF=fio-3.42
FIO_MAKE_JOBS=2
FIO_BUILD_TIMEOUT_S=1800
IOR_REF=4.0.0                       # IOR provides mdtest
IOR_MAKE_JOBS=2
IOR_BUILD_TIMEOUT_S=1800
SECFS_TEST_REF=master               # fsx fallback source
SQLITE_REF=master                   # sqlite clone ref
SQLITE_MAKE_JOBS=2
SQLITE_BUILD_TIMEOUT_S=1800
SQLITE_SPEEDTEST_SIZE=1             # correctness scale (upstream default is 100)
SQLITE_SPEEDTEST_JOURNALS=delete,truncate,persist  # WAL is the extra --mmap case
SQLITE_SPEEDTEST_NOSYNC=1           # speedtest1 skips fsync; mptester --sync keeps durability
SQLITE_MPTEST_SCRIPTS=multiwrite01.test,crash01.test
SQLITE_MPTEST_JOURNALS=wal,delete
SQLITE_MPTEST_REPEAT=1              # upstream `make mptest` uses 20
SQLITE_MPTEST_BUSY_MS=30000
SQLITE_TIMEOUT_S=300                # per-case run_cmd timeout
SQLITE_MMAP_SIZE=4194304            # speedtest1/kvtest --mmap and mmap probe
SQLITE_THREADTEST_GLOBS=walthread2,walthread5  # short WAL sidecar/copy tests; walthread* is a soak
SQLITE_KVTEST_COUNT=32
SQLITE_KVTEST_SIZE=4096             # blob bytes for kvtest init
SQLITE_KVTEST_JOURNALS=wal,delete
SQLITE_KVTEST_NOSYNC=1              # kvtest skips fsync; mptester --sync keeps durability
SQLITE_SKIP_SPEEDTEST=0
SQLITE_SKIP_MPTEST=0
SQLITE_SKIP_THREADTEST=0
SQLITE_SKIP_KVTEST=0
SQLITE_SKIP_MMAP=0
SQLITE_FAIL_FAST=0
```

`LTP_ROOT` must point to an installed LTP tree containing `kirk` (or `runltp`),
`runtest/`, and `testcases/bin/`. When LTP is auto-fetched, the
source checkout is kept under `<work-dir>/cache/tools/ltp/<ref>` and the runnable
install tree under `<work-dir>/cache/tools/ltp-install/<ref>`.

The auto-fetched LTP build (LTP `20260529`) uses the **kirk** runner by default
(LTP's modern Python-based executor), not the legacy `runltp` wrapper. Set
`LTP_RUNNER=runltp` to fall back to `runltp` if kirk is unavailable. The build
fetches the `tools/kirk/kirk-src` submodule automatically so `make -C tools
install` installs kirk into the tree.

The fs and syscall test selections use **deny-lists** aligned with the
[JuiceFS LTP CI](https://github.com/juicedata/juicefs/blob/main/.github/workflows/ltpfs.yml)
intent — long-running tests (`growfiles`, `rwtest`, `iogen`, `fs_fill`,
`fsx-linux`, `fs_racer`, `open04`) and host-specific tests (`isofs`,
`quota_remount`, `read_all_*`, `proc01`) are excluded. `lftest01` is retained (JuiceFS's `rm_list.sh` had a
tokenisation bug that incidentally stripped it; we do not replicate that bug).
`read_all_dev/proc/sys` and `proc01` are explicitly excluded because they read
host `/dev`, `/proc`, `/sys` (not the FUSE mount) and `read_all_dev` can hang
forever reading `/dev/fuse`.

`community.ltp.fs` runs `drive9-fs-smoke` (deny-list from `runtest/fs`) by
default; set `LTP_FS_CASES` to switch to an explicit allow-list for debugging,
or `LTP_FS_SCENARIO=fs` for the full upstream filesystem scenario when
`LTP_ROOT` points to a full installation. Default timeout: 1800s
(`LTP_FS_TIMEOUT_S`).

`community.ltp.syscalls` runs `drive9-syscalls-fs` (deny-list from
`runtest/syscalls`, aligned with JuiceFS `rm_syscalls`) by default. The
surviving tests are split into `LTP_SYSCALLS_SHARDS` (default 3) shard files
(`drive9-syscalls-fs-0/1/2`), each run sequentially with its own 1800s timeout
(`LTP_SYSCALLS_TIMEOUT_S`). Set `LTP_SYSCALL_CASES` to switch to an
allow-list (no deny, no sharding), or `LTP_SYSCALLS_SCENARIO=syscalls` for full
syscall coverage against a full LTP install.

`community.fio` auto-fetches and builds fio when `fio` is not already available.
`community.mdtest` auto-fetches and builds IOR/mdtest when `mdtest` is not
already available; IOR requires an MPI compiler, so with auto system-deps the
harness installs `mpich`/`libmpich-dev` (Debian) or `openmpi` (Arch) when
`mpicc` is missing. The run sets `UCX_TLS=tcp,sm,self` (and OpenMPI
`btl=tcp,self`) so MPI_Init does not probe InfiniBand — GitHub-hosted
runners have no IB and MPICH/UCX otherwise fails with `ibv_create_srq`. The IOR source is patched in-cache for newer compiler
compatibility before building mdtest. `community.fsx` fetches and builds
`secfs.test` to obtain the `fsx` binary, and patches `fsx.c` for glibc builds
that already provide `strlcpy`/`strlcat` (common on Arch).
`community.sqlite` fetches [sqlite/sqlite](https://github.com/sqlite/sqlite),
runs `./configure && make sqlite3.c`, then compiles official `speedtest1`,
`mptester`, `kvtest`, and `threadtest3`. It is an SQLite-as-filesystem
**correctness** probe (journal/WAL/locks/mmap/crash), not a throughput
benchmark. Defaults stay small and skip redundant fsync; `mptester --sync`
is what still exercises durable commits.

Default cases:

- `speedtest1 --size 1 --nosync --verify` across `delete,truncate,persist`, plus
  `speedtest1 --journal wal --mmap 4M --nosync`
- `mptester --sync` running `multiwrite01.test` and `crash01.test` against
  `wal` and `delete` (the fsync/crash path)
- `threadtest3 walthread2` + `walthread5` (WAL vs rollback sidecar, WAL copy;
  ~21s designed. `SQLITE_THREADTEST_GLOBS=walthread*` restores the 81s soak)
- `kvtest init` + `kvtest run --integrity-check --mmap --update --nosync` for
  `wal` and `delete` (32×4KiB)
- a WAL-mode fixture plus a **direct `mmap(MAP_SHARED)` probe** of the main DB
  (and `.db-shm` if present). SQLite's own `PRAGMA mmap_size` / `--mmap`
  swallows `ENODEV` and falls back to pread, so the probe is what actually
  verifies FUSE `CAP_DIRECT_IO_ALLOW_MMAP`.

Binaries stay on the host; only the database files are created on the mount.
This is separate from the in-house `drive9.sqlite` WAL+mmap remount module.

Dependency metadata (name, source, license, ref) is embedded in each
module's own `deps.py` and written as `.drive9-blackbox-dependency.json`
next to the cached dependency when a module prepares it.

## Platform Notes

Linux requirements: `/dev/fuse` and `fusermount3` or `fusermount`.

## Adding A Community Module

1. Create `suites/community/<module_name>/` with `__init__.py` and `module.py`.
2. Give the class a stable `id` (`community.<name>`), `category`, `description`,
   `labels`, and `timeout`.
3. Implement `ensure_dependencies(ctx)` when it needs external tools, either
   inline or via a `deps.py` in the module directory.
4. Implement `run(ctx)` returning a small metrics/details dict; mount through
   `ctx.target.mount(...)` and always unmount in `finally`.
5. The module is auto-discovered — no registration step.

Keep module IDs stable; CI reports and dashboards can depend on them.

## Notices / Third-party

This blackbox framework is Drive9 test code. It integrates or can fetch several
open source filesystem test suites and tools at runtime. Those dependencies
retain their own licenses and notices.

- **pjdfstest**: https://github.com/pjd/pjdfstest — BSD-2-Clause
- **Linux Test Project**: https://github.com/linux-test-project/ltp — GPL-2.0-or-later
- **secfs.test / fsx**: https://github.com/billziss-gh/secfs.test — Apache-2.0
- **fio**: https://github.com/axboe/fio — GPL-2.0-only
- **IOR / mdtest**: https://github.com/hpc/ior — GPL-2.0-only
- **SQLite** (`speedtest1`, `mptester`): https://github.com/sqlite/sqlite — public domain ([blessing](https://sqlite.org/copyright.html))

fio, mdtest/IOR, Python xattr bindings, and platform tools may be
provided by the host environment or installed by CI. Their own distribution
licenses apply.