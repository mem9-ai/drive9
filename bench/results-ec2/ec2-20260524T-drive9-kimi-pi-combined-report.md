# drive9 / kimi-cli / pi New-Instance Benchmark Summary

Run date: 2026-05-24

EC2: `i-073e6d574b0d04ce1`, `c6i.2xlarge`, `ap-southeast-1`

Original drive9 context restored after runs: `8msnxlu`

## Common Setup

- Runs: `1` per repo/storage
- Storages: `native`, `fuse`
- FUSE flags: `--mode=fuse --durability=interactive --perf-counters`
- Git clone mode: `git clone --no-checkout` followed by `git read-tree -mu <sha>`
- Git fsync boundaries: enabled
- Clone/build/prewarm timeout: `1800s`
- Dependency prewarm was outside measured clone/build timing.
- Each repo used a fresh drive9 production instance for its FUSE run.

## Instances And Commits

| repo | drive9 context | session | commit |
| --- | --- | --- | --- |
| `kimi-cli` | `bkimi123403` | `ec2-20260524T123221Z-kimi-bkimi123403` | `33d7b4f8a012953e73ed625e45dcbea42048248d` |
| `drive9` | `bd9123922` | `ec2-20260524T123221Z-drive9-bd9123922` | `91e2eb9726e0782fbba6fc743a7888b8747b04d6` |
| `pi` | `bpi133006` | `ec2-20260524T133006Z-pi-bpi133006` | `9600ded92253ae0aba564481caafbe5ef31dfc8f` |

## Results

| repo | storage | clone | build | status |
| --- | --- | ---: | ---: | --- |
| `drive9` | native | 6.804s | 19.589s | ok |
| `drive9` | fuse | 46.227s | 58.440s | ok |
| `kimi-cli` | native | 9.671s | 47.526s | ok |
| `kimi-cli` | fuse | 104.863s | failed after 0.414s | clone ok, build failed |
| `pi` | native | 14.048s | 9.655s | ok |
| `pi` | fuse | 96.406s | failed after 621.559s | clone ok, build failed |

## Ratios

| repo | clone FUSE/native | build FUSE/native |
| --- | ---: | ---: |
| `drive9` | 6.79x | 2.98x |
| `kimi-cli` | 10.84x | n/a, FUSE build failed |
| `pi` | 6.86x | n/a, FUSE build failed |

For `pi`, the FUSE build time-to-failure was 64.38x the native successful build duration.

## Observations

- `drive9` is the only one of the three that completed both FUSE clone and FUSE build.
- `kimi-cli` FUSE clone succeeded on a fresh drive9 instance, so the earlier clone issue was not reproduced there.
- `kimi-cli` FUSE build failed immediately in `uv sync` because Python virtualenv setup tried to create a symlink and the FUSE layer returned `ENOSYS`.
- `pi` FUSE clone succeeded, but FUSE build failed during `npm ci --ignore-scripts`, also because package installation needed symlink support.
- `pi` also produced many `EAGAIN` cleanup warnings while npm tried to remove directories under `node_modules`.
- All FUSE runs used interactive/writeback behavior via `--durability=interactive`.

## Failure Details

### kimi-cli FUSE Build

Failing command:

```text
uv sync --frozen --all-extras --all-packages
```

Core error:

```text
failed to symlink file from .../.venv/bin/python to .../python3.14:
Function not implemented (os error 38)
```

### pi FUSE Build

Failing command:

```text
npm ci --ignore-scripts
```

Core error:

```text
npm error code ENOSYS
npm error syscall symlink
npm error path ../packages/coding-agent/examples/extensions/with-deps
npm error dest .../node_modules/pi-extension-with-deps
npm error ENOSYS: function not implemented, symlink ...
```

Additional cleanup symptom:

```text
EAGAIN: resource temporarily unavailable, rmdir .../node_modules/...
```

## Cleanup Notes

Each measured session wrote its events and summary successfully. Several FUSE cleanup paths left `drive9 umount --timeout 900s` stuck after the mountpoint had already disappeared; I killed only the stuck `drive9 umount` child process so the runner could finish summaries and restore the original context. After each run, I verified there were no remaining `/mnt/drive9-bench/mounts` mounts or benchmark processes.

## Artifacts

- Combined `drive9` / `kimi-cli` orchestrator: `bench/results-ec2/ec2-20260524T123221Z-newinst-kimi-drive9-orchestrator/`
- `drive9` raw results: `bench/results-ec2/ec2-20260524T123221Z-drive9-bd9123922/`
- `kimi-cli` raw results: `bench/results-ec2/ec2-20260524T123221Z-kimi-bkimi123403/`
- `pi` orchestrator: `bench/results-ec2/ec2-20260524T133006Z-newinst-pi-orchestrator/`
- `pi` raw results: `bench/results-ec2/ec2-20260524T133006Z-pi-bpi133006/`
- Original combined report for `drive9` and `kimi-cli`: `bench/results-ec2/ec2-20260524T123221Z-newinst-kimi-drive9-report.md`
- Original `pi` report: `bench/results-ec2/ec2-20260524T133006Z-newinst-pi-report.md`
