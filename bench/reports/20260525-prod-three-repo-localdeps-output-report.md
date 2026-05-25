# Prod Drive9 FUSE vs Native Repo Build Benchmark

Date: 2026-05-25 UTC

EC2: `i-073e6d574b0d04ce1`, `c6i.2xlarge`, `ap-southeast-1`

Drive9: production endpoint `https://api.drive9.ai`; each repo used a fresh
`drive9 create` instance for its FUSE sample.

## Executive Summary

With third-party dependency directories and known generated-output directories
kept on the native disk, all three target repos completed both `git clone` and
build on native disk and on Drive9 FUSE.

The main remaining cost is source checkout and source-tree metadata activity on
FUSE. Clone is the largest relative slowdown: `32.03x` to `83.63x` across the
three repos. Build is less extreme for `drive9` and `kimi-cli` (`4.45x` and
`4.07x`), but `kimi-code` remains expensive at `10.57x` even after dependency
and output trees are moved off FUSE.

`mem9-ai/drive9` did not need repo output bind mounts. Go module cache and the
per-sample Go build cache were already on the native disk through `GOMODCACHE`
and `GOCACHE`.

## Method

- Runs: `1` measured repeat per repo/storage.
- Storages:
  - native disk under `/mnt/drive9-bench/work/...`
  - Drive9 FUSE mount under `/mnt/drive9-bench/mounts/...`
- Clone timing: `git clone --no-checkout <url> <dir>` followed by
  `git checkout --detach <commit>`.
- Build timing: repo-specific build commands inside the fresh checkout.
- Not timed: dependency prewarm, `drive9 create`, FUSE mount, FUSE unmount,
  bind-mount setup/teardown, checkout cleanup.
- Timeout: `1800s` each for clone, build, and prewarm.
- FUSE flags: `--mode=fuse --allow-other --profile=interactive
  --durability=interactive --perf-counters`.
- Mount process: started through `sudo env HOME=/home/ubuntu ... drive9 mount`
  so Linux bind mounts can be layered into the checkout without editing
  `/etc/fuse.conf`.
- Git ownership: `safe.directory=*` was set in the benchmark environment because
  the FUSE mount process is root-started with `allow_other`.
- Dependency policy: shared package caches live under `/mnt/drive9-bench/cache`;
  install directories such as `.venv` and `node_modules` are fresh per sample
  and bind-mounted from native disk.
- Output policy: known generated-output directories are bind-mounted from native
  disk for repos that need it, so source files stay on the tested storage while
  generated build artifacts avoid Drive9 FUSE.

## Repo Matrix

| repo | language | commit | build command | FUSE bind policy |
| --- | --- | --- | --- | --- |
| `mem9-ai/drive9` | Go | `29f076c7047c17704be4cd2ce50994bb8525c9fb` | `make build` | no repo output bind; Go caches on native disk |
| `MoonshotAI/kimi-cli` | Python/Node | `33d7b4f8a012953e73ed625e45dcbea42048248d` | `uv sync --frozen --all-extras --all-packages && make build` | `.venv`, `web/node_modules`, `vis/node_modules`, `src/kimi_cli/deps/{bin,tmp}`, `web/dist`, `vis/dist`, `src/kimi_cli/{web,vis}`, `dist`, `build` |
| `MoonshotAI/kimi-code` | TypeScript | `f84678c09493009cb9b9828c00456d5c2d53cf89` | `pnpm install --frozen-lockfile && pnpm run build` | root/app/package `node_modules`, app/package/docs `dist`, and parent-bound `packages/node-sdk` |

## Timing Results

| repo | storage | clone s | build s | clone+build s | status |
| --- | --- | ---: | ---: | ---: | --- |
| `drive9` | native | 1.179 | 18.714 | 19.893 | ok |
| `drive9` | Drive9 FUSE | 62.066 | 83.226 | 145.292 | ok |
| `kimi-cli` | native | 3.232 | 48.377 | 51.609 | ok |
| `kimi-cli` | Drive9 FUSE | 103.539 | 196.890 | 300.428 | ok |
| `kimi-code` | native | 1.229 | 31.610 | 32.839 | ok |
| `kimi-code` | Drive9 FUSE | 102.806 | 334.084 | 436.890 | ok |

## FUSE / Native Ratios

| repo | clone ratio | build ratio | clone+build ratio |
| --- | ---: | ---: | ---: |
| `drive9` | 52.63x | 4.45x | 7.30x |
| `kimi-cli` | 32.03x | 4.07x | 5.82x |
| `kimi-code` | 83.63x | 10.57x | 13.30x |

## Observations

- The local dependency/output bind strategy is enough to make the three selected
  repos pass on production Drive9 FUSE.
- Clone still exercises the full source checkout path on Drive9 FUSE, including
  Git object writes, index operations, checkout writes, and metadata visibility.
  That is why clone remains the largest relative slowdown.
- `drive9` is the cleanest workload after this change: dependency downloads are
  outside the measured build, Go caches are native, and no repo-local generated
  output directory needed a bind mount.
- `kimi-cli` previously failed when `scripts/build_web.py` copied generated web
  artifacts into the Python package tree. Binding both the web/vis output dirs
  and their final package destinations moved that fragile generated-output path
  to native disk.
- `kimi-code` previously got past `apps/vis` output copying after output binds,
  then failed in `packages/node-sdk/.tmp-api-extractor` with `EAGAIN` on
  `rmdir`. The final successful run bind-mounted the whole `packages/node-sdk`
  parent to native disk because the build removes and recreates the temporary
  API Extractor tree.
- These are single-run measurements. They are good for pass/fail and directional
  performance comparison, but not for tight statistical claims.

## Sessions And Artifacts

| repo | Drive9 context | result session |
| --- | --- | --- |
| `drive9` | `bplgd920260525T142237` | `bench/results-ec2/ec2-20260525T142235Z-prod-localdeps-outputs-drive9-newinst/` |
| `kimi-cli` | `bplokcli20260525T134317` | `bench/results-ec2/ec2-20260525T134255Z-prod-localdeps-outputs-newinst/` |
| `kimi-code` | `bplnkcode20260525T140610` | `bench/results-ec2/ec2-20260525T140605Z-prod-localdeps-outputs-node-sdk-newinst/` |

Tool versions recorded in manifests:

| tool | version |
| --- | --- |
| `drive9` | `32f2766` / `32f27669134f15fafa33752a434739540b89714b` |
| `git` | `2.43.0` |
| `go` | `go1.25.1 linux/amd64` |
| `node` | `v24.16.0` |
| `npm` | `11.13.0` |
| `corepack` | `0.35.0` |
| `uv` | `0.11.16` |

## Cleanup

FUSE samples were unmounted after each measured sample. The last prod run was
checked with `findmnt /mnt/drive9-bench/mounts/prod-localdeps-outputs-drive9-newinst`,
which returned no mount. The EC2 Drive9 config was restored to the dev context
after the prod runs. The EC2 machine is intentionally left running for follow-up
experiments.
