# JuiceFS Cloud FUSE Repo Build Benchmark - 2026-05-26

## Conclusion

On the EC2 host `47.129.127.73`, JuiceFS Cloud FUSE with writeback/cache enabled successfully completed `git clone` for all three target repos, but clone latency was much higher than the native `/tmp` baseline. The Go repo (`mem9-ai/drive9`) also completed build successfully on JuiceFS. The Python and TypeScript repos did not finish their build phase within the 30 minute timeout because dependency materialization inside the FUSE working tree was still in progress.

For `kimi-code`, the timeout happened during `pnpm install`; the observable package-add progress reached `394/665`, or `59.2%`, before the 30 minute limit. For `kimi-cli`, `uv sync` does not expose a numeric install denominator in the captured log; at timeout it had prepared the five local Python packages but had not returned from `uv sync`, and the subsequent `make build` command had not started. By top-level build-command completion, both `kimi-cli` and `kimi-code` were still at `0/2` commands complete when the 30 minute timeout fired.

## Environment

| item | value |
| --- | --- |
| Host | EC2 at `47.129.127.73` |
| CPU / memory | 8 vCPU, 15 GiB RAM |
| Native workspace | `/tmp/juicefs-bench/work/...` |
| JuiceFS mountpoint | `/tmp/juicefs-bench/mount` |
| JuiceFS Cloud volume | `qiffang` |
| JuiceFS version | `juicefs version 5.3.8 (2026-05-11 678ddf9)` |
| Mount flags | `--writeback --cache-dir /tmp/juicefs-bench/cache/juicefs --cache-size 4096 --buffer-size 1024 -o allow_other,writeback_cache` |
| Node | `v24.16.0` |
| Corepack | `0.35.0` |
| npm | `11.13.0` |
| pnpm | `10.33.0` via Corepack |
| Go | `go1.25.1 linux/amd64` |
| uv | `uv 0.11.16` |
| Python | `Python 3.14.4` |

Notes:

- The native baseline used `/tmp` on this host because the root disk had limited free space. On this EC2 image `/tmp` is tmpfs, so the native baseline is very fast.
- I did not find a JuiceFS Cloud equivalent of Drive9's coding-agent path-local overlay. This run used JuiceFS' normal cache/writeback mechanisms. Dependency caches were stored under `/tmp/juicefs-bench/cache`, but the repo working tree and generated directories such as `.git`, `.venv`, and `node_modules` were on the JuiceFS FUSE mount.
- Each phase had a 1800 second timeout. Dependency prewarm was run before measured phases and was not counted in clone/build timing.

## Method

For each repo/storage pair, the runner executed:

1. `git clone --no-checkout <repo> <checkout>`
2. `git -C <checkout> checkout --detach <resolved-commit>`
3. build commands inside the fresh checkout

Storage locations:

| storage | path |
| --- | --- |
| native | `/tmp/juicefs-bench/work/<session>/native/<repo>-run-1` |
| juicefs | `/tmp/juicefs-bench/mount/bench/<session>/<repo>-run-1` |

Resolved commits:

| repo | commit |
| --- | --- |
| `mem9-ai/drive9` | `a7f48a5bda2566137220d5a0915bc8db9517f2be` |
| `MoonshotAI/kimi-cli` | `33d7b4f8a012953e73ed625e45dcbea42048248d` |
| `MoonshotAI/kimi-code` | `9d037168d34699f575f61b8f592af6ccb25eea79` |

## Results

| repo | storage | clone | build | total clone+build | top-level build commands | build progress at 30 min |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `mem9-ai/drive9` | native | 1.18s | 18.34s | 19.52s | 1/1, 100% | `make build` completed |
| `mem9-ai/drive9` | JuiceFS | 112.15s | 55.79s | 167.94s | 1/1, 100% | `make build` completed |
| `MoonshotAI/kimi-cli` | native | 2.73s | 43.26s | 46.00s | 2/2, 100% | `uv sync` + `make build` completed |
| `MoonshotAI/kimi-cli` | JuiceFS | 301.52s | timeout at 1800.09s | >2101.62s | 0/2, 0% | Numeric percentage unavailable from `uv`; `uv sync` prepared 5 local packages, did not return, `make build` did not start |
| `MoonshotAI/kimi-code` | native | 1.58s | 24.31s | 25.89s | 2/2, 100% | `pnpm install` + `pnpm run build` completed |
| `MoonshotAI/kimi-code` | JuiceFS | 313.84s | timeout at 1800.13s | >2113.97s | 0/2, 0% | `pnpm install` reached `394/665` packages, 59.2%; `pnpm run build` did not start |

## Ratios

| repo | metric | JuiceFS / native |
| --- | --- | ---: |
| `mem9-ai/drive9` | clone | 95.1x |
| `mem9-ai/drive9` | build | 3.0x |
| `mem9-ai/drive9` | clone+build | 8.6x |
| `MoonshotAI/kimi-cli` | clone | 110.4x |
| `MoonshotAI/kimi-cli` | build | >41.6x |
| `MoonshotAI/kimi-cli` | clone+build | >45.7x |
| `MoonshotAI/kimi-code` | clone | 198.6x |
| `MoonshotAI/kimi-code` | build | >74.0x |
| `MoonshotAI/kimi-code` | clone+build | >81.7x |

## Build Progress Details

| repo | JuiceFS build status after 30 min | last observable log marker |
| --- | --- | --- |
| `mem9-ai/drive9` | Completed in 55.79s | `go build` produced `bin/drive9-server` and `bin/drive9` |
| `MoonshotAI/kimi-cli` | Timed out during `uv sync` | `Prepared 5 packages in 1.26s`; no numeric install denominator was emitted before timeout |
| `MoonshotAI/kimi-code` | Timed out during `pnpm install` | `Progress: resolved 665, reused 665, downloaded 0, added 394` |

## Artifacts

Raw artifacts were copied back under ignored local result directories:

| session | contents |
| --- | --- |
| `bench/results-ec2/ec2-20260526T070756Z-juicefs-cloud-writeback` | `drive9` and `kimi-cli` events/logs |
| `bench/results-ec2/ec2-20260526T075415Z-juicefs-cloud-writeback-kimicode` | `kimi-code` events/logs/summary |

The key event files are:

- `bench/results-ec2/ec2-20260526T070756Z-juicefs-cloud-writeback/events.jsonl`
- `bench/results-ec2/ec2-20260526T075415Z-juicefs-cloud-writeback-kimicode/events.jsonl`
