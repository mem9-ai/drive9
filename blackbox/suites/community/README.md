# Community Modules

This directory holds the `community.*` blackbox modules — open source filesystem
test suites and tools run against a Drive9 FUSE mount. The generic harness lives
under `blackbox/harness`; drive9-specific environment wiring (server/CLI/FUSE
mount, dependency preparation) lives under `blackbox/env`.

## Modules

Each subdirectory is one auto-discovered module (`module.py`), optionally with a
`config.json`, `deps.py`, and data files (e.g. `allowlist.json`).

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
| `community.vdbench` | performance | vdbench file workload (manual dependency). |

The two LTP modules share one `module.py` (`community.ltp.fs` + `community.ltp.syscalls`).

## Selection

Modules are selected with `python3 blackbox/run.py`. The `community` directory
group selects every module here:

```bash
python3 blackbox/run.py --group community
```

Label filters narrow within a selection. Community-relevant labels include
`posix` (`community.pjdfstest`), `performance` (`community.fio`,
community.mdtest, community.vdbench`), and `compatibility`:

```bash
python3 blackbox/run.py --group community --label performance
python3 blackbox/run.py --group community --label posix
python3 blackbox/run.py --module community.pjdfstest
```

`community.vdbench` is a manual-dependency module and is excluded from broad
selectors unless `INCLUDE_MANUAL=1` is set or it is selected explicitly with
`--module`.

## Dependencies

The harness prefers already-installed tools, then `*_BIN` / `*_DIR` environment
overrides, then auto-fetch under the work-dir cache. On Linux hosts with
`apt-get` and passwordless `sudo`, it can also bootstrap the system packages
needed to build these suites (build-essential, autotools, Perl/prove, MPICH,
Python headers). Disable that with `AUTO_INSTALL_SYSTEM_DEPS=0`.

Direct environment overrides (read without prefix):

```bash
PJDFSTEST_DIR=/path/to/pjdfstest
PJDFSTEST_TESTS=/path/to/pjdfstest/tests
PJDFSTEST_BIN=/path/to/pjdfstest
PJDFSTEST_ALLOW_NONROOT=1          # pjdfstest normally requires root
LTP_ROOT=/path/to/ltp              # installed tree with runltp, bin/ltp-pan, runtest/, testcases/bin/
LTP_INSTALL_ROOT=/path/to/ltp-install
FIO_BIN=/path/to/fio
MDTEST_BIN=/path/to/mdtest
MPICC=/path/to/mpicc
VDBENCH_BIN=/path/to/vdbench       # vdbench is never auto-fetched (Oracle download)
FSX_BIN=/path/to/fsx
```

Tunables read via the harness `env_value` helper accept a `BLACKBOX_` prefix
(`BLACKBOX_LTP_REF` is equivalent to the documented base name):

```bash
LTP_REF=20240129
LTP_FS_CASES="openfile01 stream01 ftest01 lftest01 writetest01"
LTP_SYSCALL_DIRS="access chmod chown close ..."
LTP_SYSCALL_CASES="access01 chmod01 open01 write01 ..."
LTP_MAKE_JOBS=2
LTP_BUILD_TIMEOUT_S=1800
FIO_REF=fio-3.42
FIO_MAKE_JOBS=2
FIO_BUILD_TIMEOUT_S=1800
IOR_REF=4.0.0                       # IOR provides mdtest
IOR_MAKE_JOBS=2
IOR_BUILD_TIMEOUT_S=1800
SECFS_TEST_REF=master               # fsx fallback source
```

`LTP_ROOT` must point to an installed LTP tree containing `runltp`,
`bin/ltp-pan`, `runtest/`, and `testcases/bin/`. When LTP is auto-fetched, the
source checkout is kept under `<work-dir>/cache/tools/ltp/<ref>` and the runnable
install tree under `<work-dir>/cache/tools/ltp-install/<ref>`.

The auto-fetched LTP build intentionally does not build the full upstream tree.
It prepares the required runtime tools, a bounded filesystem smoke scenario
(`drive9-fs-smoke`), and a filesystem-oriented syscall subset
(`drive9-syscalls-fs`). `community.ltp.fs` runs `drive9-fs-smoke` by default;
set `LTP_FS_CASES` to choose a different bounded set, or
`LTP_FS_SCENARIO=fs` for the full upstream filesystem scenario when `LTP_ROOT`
points to a full installation. `community.ltp.syscalls` runs `drive9-syscalls-fs`
by default; widen it with `LTP_SYSCALL_CASES` / `LTP_SYSCALL_DIRS`, or set
`LTP_SYSCALLS_SCENARIO=syscalls` for full syscall coverage against a full LTP
install.

`community.fio` auto-fetches and builds fio when `fio` is not already available.
`community.mdtest` auto-fetches and builds IOR/mdtest when `mdtest` is not
already available; IOR requires an MPI compiler, so on Linux with `apt-get` and
passwordless `sudo` the harness installs `mpich libmpich-dev` when `mpicc` is
missing. The IOR source is patched in-cache for newer compiler compatibility
before building mdtest. `community.fsx` fetches and builds `secfs.test` to obtain the `fsx` binary.

Dependency metadata (name, source, license, ref) is embedded in each
module's own `deps.py` and written as `.drive9-blackbox-dependency.json`
next to the cached dependency when a module prepares it.

## Platform Notes

Linux requirements: `/dev/fuse` and `fusermount3` or `fusermount`.

Some POSIX cases are not expected to behave identically on macOS (permissions,
ownership, flags, case-sensitivity-adjacent behavior). Modules classify these as
`SKIP` or `XFAIL` through capability checks or allowlists rather than failing the
whole run as a Drive9 regression.

## Adding A Community Module

1. Create `suites/community/<module_name>/` with `__init__.py` and `module.py`.
2. Give the class a stable `id` (`community.<name>`), `category`, `description`,
   `labels`, and `timeout`.
3. Implement `ensure_dependencies(ctx)` when it needs external tools, either
   inline or via a `deps.py` in the module directory.
4. Implement `run(ctx)` returning a small metrics/details dict; mount through
   `ctx.target.mount(...)` and always unmount in `finally`.
5. Optionally add `config.json` for tunables and `*.json` data files.
6. The module is auto-discovered — no registration step.

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
- **vdbench**: Oracle distribution (manual download, not auto-fetched).

fio, mdtest/IOR, vdbench, Python xattr bindings, and platform tools may be
provided by the host environment or installed by CI. Their own distribution
licenses apply.