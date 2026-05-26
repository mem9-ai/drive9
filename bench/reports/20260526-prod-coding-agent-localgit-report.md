# Prod Coding-Agent Local Overlay Benchmark

Date: 2026-05-26 UTC

EC2: `i-073e6d574b0d04ce1`, `c6i.2xlarge`, `ap-southeast-1`

Drive9 backend: production endpoint `https://api.drive9.ai`

Drive9 CLI: self-built from the current `feat/bench` source snapshot, version
`feat-bench-coding-agent`, git hash
`5b9c505faf643ecffdc685a1e1aa114f513b226f`.

## Summary

All three target repos completed `git clone` and build on native disk and on
Drive9 FUSE using PR #464's coding-agent local overlay feature.

The important behavior change is that `.git` is no longer written through
Drive9 FUSE. It is routed to the local overlay with `--profile=coding-agent
--local-root ...`, together with dependency install trees, caches, temporary
directories, and generated output directories.

Compared with the previous 2026-05-25 bind-mount workaround, FUSE clone time
improved materially for all three repos:

| repo | previous FUSE clone | coding-agent FUSE clone | change |
| --- | ---: | ---: | ---: |
| `drive9` | 62.066s | 18.453s | -70.3% |
| `kimi-cli` | 103.539s | 44.786s | -56.7% |
| `kimi-code` | 102.806s | 58.347s | -43.2% |

These are single-run measurements. `drive9` and `kimi-code` commits changed
because the benchmark resolves `main` at run time, so the comparison is
directional rather than a strict A/B on identical source revisions.

## Method

- Runs: `1` measured repeat per repo/storage.
- Storages:
  - native disk under `/mnt/drive9-bench/work/...`
  - Drive9 FUSE under `/mnt/drive9-bench/mounts/prod-coding-agent-localgit-v2`
- Clone timing: `git clone --no-checkout <url> <dir>` plus
  `git checkout --detach <commit>`.
- Build timing: repo-specific build commands inside the fresh checkout.
- Not timed: dependency prewarm, `drive9 create`, FUSE mount/unmount, checkout
  cleanup, and context setup.
- FUSE flags: `--mode=fuse --allow-other --profile=coding-agent --local-root
  <BENCH_HOME/local-overlay/...> --durability=interactive --perf-counters`.
- Prod Drive9 context: one fresh `drive9 create --server https://api.drive9.ai`
  context per repo.
- CLI source: self-built on EC2 with `make build-cli`; the runner used
  `BENCH_DRIVE9_CLI=/mnt/drive9-bench/src/drive9-coding-agent-20260526T044200Z/bin/drive9`.

## Local-Only Policy

Common local-only patterns:

`**/.git/**`, `**/node_modules/**`, `**/.venv/**`, `**/dist/**`,
`**/build/**`, `**/target/**`, `**/bin/**`, cache directories, temp
directories, and common Python cache directories.

Repo-specific additions:

| repo | extra local-only paths |
| --- | --- |
| `kimi-cli` | `src/kimi_cli/deps/{bin,tmp}`, `src/kimi_cli/{web,vis}`, `packages/kimi-code/README.md`, `src/kimi_cli/CHANGELOG.md` |
| `kimi-code` | `packages/node-sdk/.tmp-api-extractor` |
| `drive9` | none beyond common policy |

The two `kimi-cli` metadata paths are symlinks in the target commit. An initial
coding-agent run without them failed during `uv sync` because
`packages/kimi-code/README.md -> ../../README.md` was not reliably readable
through prod FUSE. Keeping those symlink entries local fixed the build.

## Final Results

Final session: `ec2-20260526T050107Z-prod-coding-agent-localgit-v2`

| repo | commit | storage | clone s | build s | clone+build s | status |
| --- | --- | --- | ---: | ---: | ---: | --- |
| `drive9` | `a7f48a5bda2566137220d5a0915bc8db9517f2be` | native | 1.129 | 18.234 | 19.364 | ok |
| `drive9` | `a7f48a5bda2566137220d5a0915bc8db9517f2be` | Drive9 FUSE | 18.453 | 55.981 | 74.434 | ok |
| `kimi-cli` | `33d7b4f8a012953e73ed625e45dcbea42048248d` | native | 3.983 | 44.724 | 48.708 | ok |
| `kimi-cli` | `33d7b4f8a012953e73ed625e45dcbea42048248d` | Drive9 FUSE | 44.786 | 218.348 | 263.134 | ok |
| `kimi-code` | `b2854353e7dacc4daf9f7cc19f4be62e6e62b6a9` | native | 1.380 | 23.543 | 24.923 | ok |
| `kimi-code` | `b2854353e7dacc4daf9f7cc19f4be62e6e62b6a9` | Drive9 FUSE | 58.347 | 229.152 | 287.498 | ok |

## FUSE / Native Ratios

| repo | clone ratio | build ratio | clone+build ratio |
| --- | ---: | ---: | ---: |
| `drive9` | 16.34x | 3.07x | 3.84x |
| `kimi-cli` | 11.24x | 4.88x | 5.40x |
| `kimi-code` | 42.29x | 9.73x | 11.54x |

## Previous Run Comparison

Previous report: `bench/reports/20260525-prod-three-repo-localdeps-output-report.md`

| repo | previous FUSE build | coding-agent FUSE build | change |
| --- | ---: | ---: | ---: |
| `drive9` | 83.226s | 55.981s | -32.7% |
| `kimi-cli` | 196.890s | 218.348s | +10.9% |
| `kimi-code` | 334.084s | 229.152s | -31.4% |

Clone improved for all three repos because `.git` writes moved to local disk.
Build improved for `drive9` and `kimi-code`; `kimi-cli` build became slightly
slower in this single run, likely because its `uv sync` and package build still
scan a mix of remote source and local overlay metadata while also using the
extra symlink local-only workaround.

## Verification Evidence

- `.git` local overlay probe succeeded for all repos:
  - `drive9`: `.git/config` existed under
    `/mnt/drive9-bench/local-overlay/ec2-20260526T050107Z-prod-coding-agent-localgit-v2/drive9/run-1/.../.git`
  - `kimi-cli`: `.git/config` existed under
    `/mnt/drive9-bench/local-overlay/ec2-20260526T050107Z-prod-coding-agent-localgit-v2/kimi-cli/run-1/.../.git`
  - `kimi-code`: `.git/config` existed under
    `/mnt/drive9-bench/local-overlay/ec2-20260526T050107Z-prod-coding-agent-localgit-v2/kimi-code/run-1/.../.git`
- FUSE perf counters showed local policy hits:
  - `drive9`: `local_only=6512`, `remote_default=13612`
  - `kimi-cli`: `local_only=1204940`, `remote_default=20393`
  - `kimi-code`: `local_only=540446`, `remote_default=39978`
- Final run had no failed phase events.

## Artifacts

- Final raw artifacts:
  `bench/results-ec2/ec2-20260526T050107Z-prod-coding-agent-localgit-v2/`
- Initial failed coding-agent run before adding `kimi-cli` symlink metadata
  local-only paths:
  `bench/results-ec2/ec2-20260526T044427Z-prod-coding-agent-localgit/`

## Cleanup

FUSE mount cleanup was verified with
`findmnt /mnt/drive9-bench/mounts/prod-coding-agent-localgit-v2`, which returned
no mount. The EC2 Drive9 config was restored to its previous dev context after
the prod run. The EC2 machine was left running for follow-up experiments.
