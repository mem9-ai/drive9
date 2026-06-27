# JuiceFS-Inspired Modules

This directory holds the `juicefs.*` blackbox modules — Drive9-owned equivalent
rewrites inspired by generic filesystem behaviors tested in the
[JuiceFS](https://github.com/juicedata/juicefs) project. They are **not** vendored
JuiceFS test code; they are independent implementations targeting Drive9 FUSE
semantics (cache visibility, random I/O verification, concurrent metadata
stress, recursive remove).

## Modules

Each subdirectory is one auto-discovered module (`module.py`), optionally with a
`config.json` for tunables.

| Module | Category | Description |
|---|---|---|
| `juicefs.cache_consistency` | cache | Two-mount cache visibility checks: writes on one mount are immediately readable on a second mount of the same remote root. |
| `juicefs.fsrand` | consistency | Deterministic random filesystem model test (`seed`/`ops` configurable). Creates, writes, renames, and removes entries in a reproducible sequence. |
| `juicefs.random_rw` | io | Random write/read verification: writes N blocks of fixed size, then reads them back and verifies content integrity. |
| `juicefs.random_stress` | stress | Concurrent create/read/rename/remove stress with configurable worker count and files-per-worker. |
| `juicefs.rmr` | metadata | Recursive remove workload: creates a nested directory tree, then removes it recursively and verifies the root is gone. |

## Config

| Module | Config key | Default | Description |
|---|---|---|---|
| `juicefs.fsrand` | `seed` | `9` | RNG seed for deterministic op sequence. |
| `juicefs.fsrand` | `ops` | `1000` | Number of random filesystem operations. |
| `juicefs.random_rw` | `size_bytes` | `4194304` (4 MiB) | Block size for each write. |
| `juicefs.random_rw` | `ops` | `1024` | Number of write/read cycles. |
| `juicefs.random_stress` | `workers` | `4` | Concurrent worker threads. |
| `juicefs.random_stress` | `files_per_worker` | `64` | Files each worker creates. |

## Selection

```bash
python3 blackbox/run.py --group juicefs
python3 blackbox/run.py --module juicefs.rmr
python3 blackbox/run.py --group juicefs --label functional
```

## Notices / Third-party

These modules are Drive9 test code. They are inspired by generic filesystem
behaviors tested in the JuiceFS project but contain no vendored JuiceFS source
code. JuiceFS source: https://github.com/juicedata/juicefs (Apache-2.0).

If future work ever copies actual JuiceFS test files into this directory, those
files must preserve the original copyright header and license notice, and this
section must be updated accordingly.