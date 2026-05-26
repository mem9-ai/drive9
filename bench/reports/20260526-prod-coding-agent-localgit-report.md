# Prod Coding-Agent Local Overlay Benchmark

Date: 2026-05-26 UTC

EC2: `i-073e6d574b0d04ce1`, `c6i.2xlarge`, `ap-southeast-1`

Drive9 backend: production endpoint `https://api.drive9.ai`

Drive9 CLI: self-built from the current `feat/bench` source snapshot, version
`feat-bench-coding-agent`, git hash
`5b9c505faf643ecffdc685a1e1aa114f513b226f`.

## Conclusion

All three target repos completed `git clone` and build on both native disk and
Drive9 FUSE with the coding-agent local overlay enabled.

The final setup routes `.git`, dependency install trees, caches, temporary
directories, and generated output directories to native disk through Drive9's
`--profile=coding-agent --local-root ...` mount policy. The remaining source
tree is kept under the tested storage, so the result still measures native disk
versus Drive9 FUSE for ordinary source-tree reads and metadata activity.

At one measured run per repo/storage, Drive9 FUSE remained slower than native
disk, with clone ratios from `11.24x` to `42.29x` and build ratios from `3.07x`
to `9.73x`. The full matrix succeeded without failed phase events.

## Test Method

- Runs: `1` measured repeat per repo/storage.
- Storages:
  - native disk under `/mnt/drive9-bench/work/...`
  - Drive9 FUSE under `/mnt/drive9-bench/mounts/prod-coding-agent-localgit-v2`
- Clone timing: `git clone --no-checkout <url> <dir>` plus
  `git checkout --detach <commit>`.
- Build timing: repo-specific build commands inside the fresh checkout.
- Not timed: dependency prewarm, `drive9 create`, FUSE mount/unmount, checkout
  cleanup, context setup, and local overlay setup.
- FUSE flags: `--mode=fuse --allow-other --profile=coding-agent --local-root
  <BENCH_HOME/local-overlay/...> --durability=interactive --perf-counters`.
- Drive9 context strategy: one fresh prod context per repo via
  `drive9 create --server https://api.drive9.ai`.
- CLI selection: `BENCH_DRIVE9_CLI` pointed to the self-built EC2 binary at
  `/mnt/drive9-bench/src/drive9-coding-agent-20260526T044200Z/bin/drive9`.
- Timeouts: clone, build, and prewarm were each capped at `1800s`.

## Test Process

1. Synced the current `feat/bench` source snapshot to the EC2 machine.
2. Built the Drive9 CLI on EC2 with `make build-cli`.
3. Activated prod configuration from `/home/ubuntu/.drive9/config_prod_bak_codex`.
4. Resolved target commits for `drive9`, `kimi-cli`, and `kimi-code`.
5. Prewarmed dependency caches outside measured timings.
6. For each repo, created a fresh prod Drive9 context, ran native clone/build,
   then ran Drive9 FUSE clone/build with coding-agent local overlay enabled.
7. Captured raw events, per-phase logs, summary CSV/Markdown, manifest,
   `.git` local overlay probes, and FUSE perf counters.
8. Unmounted the FUSE mount after each FUSE sample and restored the previous
   Drive9 config after the run.

## Local-Only Policy

Common local-only patterns:

`**/.git/**`, `**/node_modules/**`, `**/.venv/**`, `**/dist/**`,
`**/build/**`, `**/target/**`, `**/bin/**`, cache directories, temp
directories, and common Python cache directories.

Repo-specific additions:

| repo | extra local-only paths |
| --- | --- |
| `drive9` | none beyond common policy |
| `kimi-cli` | `src/kimi_cli/deps/{bin,tmp}`, `src/kimi_cli/{web,vis}`, `packages/kimi-code/README.md`, `src/kimi_cli/CHANGELOG.md` |
| `kimi-code` | `packages/node-sdk/.tmp-api-extractor` |

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
- FUSE cleanup was verified with
  `findmnt /mnt/drive9-bench/mounts/prod-coding-agent-localgit-v2`, which
  returned no mount.

## Artifacts

- Final raw artifacts:
  `bench/results-ec2/ec2-20260526T050107Z-prod-coding-agent-localgit-v2/`
- Main result files in that directory:
  `events.jsonl`, `manifest.json`, `summary.csv`, `summary.md`, and `logs/`.
