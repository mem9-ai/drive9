# JuiceFS Cloud FUSE Writeback Repo Build Benchmark

Date: 2026-05-26 UTC

## Executive Summary

This benchmark compares native disk against JuiceFS Cloud FUSE on three real
coding-agent-style repo workloads: cloning a fresh checkout and running each
repo's build command. JuiceFS was mounted with writeback/cache enabled.

All three repos completed `git clone` on JuiceFS, but clone latency was much
higher than the native baseline. The Go repo (`mem9-ai/drive9`) also completed
build successfully. The Python/Node repo (`MoonshotAI/kimi-cli`) and TypeScript
repo (`MoonshotAI/kimi-code`) did not finish within the 30 minute build timeout.
Both were still in dependency materialization; their actual total build time is
therefore unknown and higher than the reported lower bound.

| repo | clone ratio | build ratio | clone+build ratio | JuiceFS build status |
| --- | ---: | ---: | ---: | --- |
| `drive9` | 95.1x | 3.0x | 8.6x | completed |
| `kimi-cli` | 110.4x | >41.6x | >45.7x | timed out during `uv sync` |
| `kimi-code` | 198.6x | >74.0x | >81.7x | timed out during `pnpm install` |

The comparison is not technically identical to the Drive9 coding-agent local
overlay run. This JuiceFS run used normal JuiceFS cache/writeback behavior, and
the repo working tree remained on the FUSE mount, including `.git`, dependency
install directories, temporary trees, and generated outputs. Dependency package
caches were prewarmed outside measured time, but install materialization still
happened inside the JuiceFS working tree.

These are single-run measurements. They are suitable for pass/fail validation
and directional performance comparison, not for statistical claims or tight
regression thresholds.

## Environment

| item | value |
| --- | --- |
| Host class | Linux EC2, 8 vCPU, 15 GiB RAM |
| Native baseline | local `/tmp` workspace |
| JuiceFS | JuiceFS Cloud FUSE |
| JuiceFS version | `juicefs version 5.3.8 (2026-05-11 678ddf9)` |
| Mount flags | `--writeback --cache-dir <BENCH_HOME/cache/juicefs> --cache-size 4096 --buffer-size 1024 -o allow_other,writeback_cache` |
| Go | `go1.25.1 linux/amd64` |
| Git | `2.53.0` |
| Node | `v24.16.0` |
| npm | `11.13.0` |
| pnpm | `10.33.0` via Corepack |
| corepack | `0.35.0` |
| uv | `0.11.16` |
| Python | `3.14.4` |

## Methodology

- Runs: `1` measured repeat per repo/storage.
- Storages:
  - native disk under a local `/tmp/juicefs-bench/work/...` workspace
  - JuiceFS Cloud FUSE under `/tmp/juicefs-bench/mount/...`
- Clone timing: `git clone --no-checkout <url> <dir>` followed by
  `git checkout --detach <commit>`.
- Build timing: repo-specific build commands inside the fresh checkout.
- Not timed: dependency prewarm, JuiceFS mount/unmount, checkout cleanup, and
  benchmark setup.
- Timeout: clone, build, and prewarm were each capped at `1800s`.
- Cache policy: shared package caches were stored under the benchmark cache
  directory and prewarmed before measured phases.
- Working-tree policy: `.git`, dependency install directories, temporary trees,
  and generated outputs remained inside the JuiceFS FUSE working tree.

Note: the native baseline used `/tmp` because the host root disk had limited
free space. On this image, `/tmp` is tmpfs, so the native baseline is especially
fast.

## Repo Matrix

| repo | language | commit | build command |
| --- | --- | --- | --- |
| `mem9-ai/drive9` | Go | `a7f48a5bda2566137220d5a0915bc8db9517f2be` | `make build` |
| `MoonshotAI/kimi-cli` | Python/Node | `33d7b4f8a012953e73ed625e45dcbea42048248d` | `uv sync --frozen --all-extras --all-packages && make build` |
| `MoonshotAI/kimi-code` | TypeScript | `9d037168d34699f575f61b8f592af6ccb25eea79` | `corepack pnpm install --frozen-lockfile --store-dir "$PNPM_STORE_DIR" --package-import-method=copy && corepack pnpm run build` |

## Timing Results

| repo | storage | clone s | build s | clone+build s | status |
| --- | --- | ---: | ---: | ---: | --- |
| `drive9` | native | 1.180 | 18.341 | 19.521 | ok |
| `drive9` | JuiceFS | 112.149 | 55.795 | 167.943 | ok |
| `kimi-cli` | native | 2.732 | 43.264 | 45.996 | ok |
| `kimi-cli` | JuiceFS | 301.523 | >1800.094 | >2101.617 | build timeout |
| `kimi-code` | native | 1.580 | 24.308 | 25.888 | ok |
| `kimi-code` | JuiceFS | 313.841 | >1800.126 | >2113.967 | build timeout |

## Build Progress At Timeout

| repo | top-level build commands | last observable marker |
| --- | --- | --- |
| `drive9` | `1/1` completed | `make build` produced `bin/drive9-server` and `bin/drive9` |
| `kimi-cli` | `0/2` completed | `uv sync` prepared 5 local Python packages, did not return, and `make build` did not start |
| `kimi-code` | `0/2` completed | `pnpm install` reached `394/665` packages added, or `59.2%`; `pnpm run build` did not start |

## FUSE Overhead

| repo | clone overhead s | build overhead s | clone+build overhead s |
| --- | ---: | ---: | ---: |
| `drive9` | 110.969 | 37.454 | 148.423 |
| `kimi-cli` | 298.791 | >1756.830 | >2055.621 |
| `kimi-code` | 312.261 | >1775.818 | >2088.079 |

## Interpretation

- JuiceFS writeback/cache was enough for the Go repo to complete, but clone was
  still about `95x` slower than the local `/tmp` baseline.
- Python and TypeScript builds were dominated by dependency tree materialization
  inside the FUSE working tree. Both timed out before the actual build command
  ran.
- Because this run has no path-local overlay equivalent, it exercises a heavier
  workload on FUSE than the Drive9 coding-agent profile, where `.git`,
  dependencies, temporary paths, and generated outputs are routed to local disk.
- The timeout rows should be read as lower bounds: the true JuiceFS build and
  clone+build ratios for `kimi-cli` and `kimi-code` are higher than reported.

## Validation

| check | result |
| --- | --- |
| Clone status | all three JuiceFS clone phases completed successfully |
| Build status | `drive9` completed; `kimi-cli` and `kimi-code` timed out after 30 minutes |
| Failure mode | dependency materialization still in progress at timeout |
| Artifact capture | events, manifests, and per-phase logs were copied locally |

## Artifacts

| session | contents |
| --- | --- |
| `ec2-20260526T070756Z-juicefs-cloud-writeback` | `drive9` and `kimi-cli` events/logs |
| `ec2-20260526T075415Z-juicefs-cloud-writeback-kimicode` | `kimi-code` events/logs/summary |

Local artifact copies:

- `bench/results-ec2/ec2-20260526T070756Z-juicefs-cloud-writeback/`
- `bench/results-ec2/ec2-20260526T075415Z-juicefs-cloud-writeback-kimicode/`
