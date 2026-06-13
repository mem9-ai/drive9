# Prod Drive9 FUSE Coding-Agent Local Overlay Benchmark

Date: 2026-05-26 UTC

## Executive Summary

This benchmark compares native disk against Drive9 FUSE on three real coding
agent workloads: cloning a fresh checkout and running each repo's build command.
The Drive9 FUSE sample used the `coding-agent` local overlay profile, which
routes Git metadata, dependency installs, caches, temporary trees, and generated
outputs to local disk while leaving the remaining source tree on the tested
storage.

All three repos completed successfully on both native disk and Drive9 FUSE. With
the local overlay enabled, Drive9 FUSE was still slower than native disk:

| repo | clone ratio | build ratio | clone+build ratio |
| --- | ---: | ---: | ---: |
| `drive9` | 16.34x | 3.07x | 3.84x |
| `kimi-cli` | 11.24x | 4.88x | 5.40x |
| `kimi-code` | 42.29x | 9.73x | 11.54x |

The most expensive relative phase remains clone/checkout. Because `.git` is
local-only in this profile, this is not a pure remote-Git benchmark; the measured
FUSE cost is primarily source working-tree materialization plus ordinary
source-tree reads and metadata activity.

These are single-run measurements. They are suitable for pass/fail validation
and directional performance comparison, not for statistical claims or tight
regression thresholds.

## Environment

| item | value |
| --- | --- |
| Host class | Linux EC2, `c6i.2xlarge`, `ap-southeast-1` |
| Drive9 endpoint | production, `https://api.drive9.ai` |
| Drive9 CLI | self-built from `feat/bench` |
| Drive9 CLI version | `feat-bench-coding-agent` |
| Drive9 CLI git hash | `5b9c505faf643ecffdc685a1e1aa114f513b226f` |
| Go | `go1.25.1 linux/amd64` |
| Git | `2.43.0` |
| Node | `v24.16.0` |
| npm | `11.13.0` |
| corepack | `0.35.0` |
| uv | `0.11.16` |

## Methodology

- Runs: `1` measured repeat per repo/storage.
- Storages:
  - native disk under the benchmark work directory
  - Drive9 FUSE mounted with `--profile=coding-agent --local-root <...>`
- Drive9 context strategy: one fresh production Drive9 context per repo via
  `drive9 create`; context creation time is excluded.
- Clone timing: `git clone --no-checkout <url> <dir>` followed by
  `git checkout --detach <commit>`.
- Build timing: repo-specific build commands inside the fresh checkout.
- Not timed: dependency prewarm, `drive9 create`, FUSE mount/unmount, checkout
  cleanup, context setup, and local overlay setup.
- Timeout: clone, build, and prewarm were each capped at `1800s`.
- FUSE flags: `--mode=fuse --allow-other --profile=coding-agent --local-root
  <BENCH_HOME/local-overlay/...> --durability=interactive --perf-counters`.
- Git safety: benchmark environment set `safe.directory=*` because the FUSE
  mount was root-started with `allow_other`.

## Repo Matrix

| repo | language | commit | build command |
| --- | --- | --- | --- |
| `mem9-ai/drive9` | Go | `a7f48a5bda2566137220d5a0915bc8db9517f2be` | `make build` |
| `MoonshotAI/kimi-cli` | Python/Node | `33d7b4f8a012953e73ed625e45dcbea42048248d` | `uv sync --frozen --all-extras --all-packages && make build` |
| `MoonshotAI/kimi-code` | TypeScript | `b2854353e7dacc4daf9f7cc19f4be62e6e62b6a9` | `corepack pnpm install --frozen-lockfile --store-dir "$PNPM_STORE_DIR" && corepack pnpm run build` |

## Local Overlay Policy

Common local-only categories:

- VCS metadata: `.git`, `.hg`, `.svn`
- Dependency installs and package caches: `node_modules`, `.pnpm-store`,
  `.venv`, `.gradle`, cache directories
- Generated outputs: `dist`, `build`, `target`, `bin`
- Temporary and language cache trees: `tmp`, `.tmp`, `__pycache__`,
  `.pytest_cache`, `.mypy_cache`, `.ruff_cache`

Repo-specific additions:

| repo | extra local-only paths |
| --- | --- |
| `drive9` | none beyond the common policy |
| `kimi-cli` | `src/kimi_cli/deps/{bin,tmp}`, `src/kimi_cli/{web,vis}`, `packages/kimi-code/README.md`, `src/kimi_cli/CHANGELOG.md` |
| `kimi-code` | `packages/node-sdk/.tmp-api-extractor` |

## Timing Results

| repo | storage | clone s | build s | clone+build s | status |
| --- | --- | ---: | ---: | ---: | --- |
| `drive9` | native | 1.129 | 18.234 | 19.364 | ok |
| `drive9` | Drive9 FUSE | 18.453 | 55.981 | 74.434 | ok |
| `kimi-cli` | native | 3.983 | 44.724 | 48.708 | ok |
| `kimi-cli` | Drive9 FUSE | 44.786 | 218.348 | 263.134 | ok |
| `kimi-code` | native | 1.380 | 23.543 | 24.923 | ok |
| `kimi-code` | Drive9 FUSE | 58.347 | 229.152 | 287.498 | ok |

## FUSE Overhead

| repo | clone overhead s | build overhead s | clone+build overhead s |
| --- | ---: | ---: | ---: |
| `drive9` | 17.324 | 37.747 | 55.070 |
| `kimi-cli` | 40.803 | 173.624 | 214.426 |
| `kimi-code` | 56.967 | 205.609 | 262.575 |

## Interpretation

- The coding-agent local overlay removes the write-heavy paths that usually
  dominate coding-agent workloads: `.git`, dependency trees, caches, temporary
  trees, and generated outputs.
- Clone/checkout remains the largest relative slowdown because the working tree
  source files still need to be materialized through the tested storage.
- Build slowdown varies by repo. `drive9` has the smallest build ratio because
  Go module/build caches are outside the measured source tree. `kimi-code` is
  the most expensive build workload in this run, even with dependency and
  generated-output paths local-only.
- The result should be compared against future runs only when host class,
  target commits, CLI build, profile, and prewarm policy are kept consistent.

## Validation

| check | result |
| --- | --- |
| Phase status | all clone/build phases completed successfully |
| Failed phase events | none |
| `.git` local overlay probe | `.git/config` was present in the local overlay for all repos |
| FUSE cleanup | final mount check returned no active mount |

FUSE perf counters also confirmed that the local-only policy was exercised:

| repo | local-only hits | remote-default hits |
| --- | ---: | ---: |
| `drive9` | 6,512 | 13,612 |
| `kimi-cli` | 1,204,940 | 20,393 |
| `kimi-code` | 540,446 | 39,978 |

## Artifacts

- Result session: `ec2-20260526T050107Z-prod-coding-agent-localgit-v2`
- Local artifact copy:
  `bench/results-ec2/ec2-20260526T050107Z-prod-coding-agent-localgit-v2/`
- Main files: `events.jsonl`, `manifest.json`, `summary.csv`, `summary.md`,
  and `logs/`
