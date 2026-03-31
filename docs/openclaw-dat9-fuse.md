---
name: dat9-fuse-openclaw
version: 0.1.0
description: Use dat9 FUSE as OpenClaw workspace storage for persistent files.
homepage: https://dat9.ai
---

# dat9 FUSE for OpenClaw

Mount a dat9 tenant locally, then point OpenClaw workspace paths into the mount so files survive host restarts and can be queried through dat9.

## Prerequisites

- `dat9` CLI installed and authenticated (`DAT9_SERVER`, `DAT9_API_KEY` or `dat9 ctx`)
- `openclaw` CLI installed
- Linux: `fuse3` package installed (`fusermount3` available)

Install `dat9` using the standard entrypoint:

```bash
curl -fsSL https://dat9.ai/install | sh
dat9 --version
```

## 1) Provision (or reuse) a dat9 tenant

```bash
dat9 create --name openclaw
dat9 ctx openclaw
```

## 2) Mount dat9

```bash
mkdir -p "$HOME/dat9-openclaw"
dat9 mount "$HOME/dat9-openclaw"
```

Keep this process running in a tmux/screen session or as a service.

## 3) Run OpenClaw in an isolated shell rooted on the mount

Do not overwrite your global shell `HOME`. Use a subshell so only OpenClaw
state/config writes into dat9.

```bash
mkdir -p "$HOME/dat9-openclaw/openclaw-home"
(
  export HOME="$HOME/dat9-openclaw/openclaw-home"
  openclaw --version
)
```

## 4) Skill/plugin operations (example)

```bash
(
  export HOME="$HOME/dat9-openclaw/openclaw-home"
  openclaw plugins --help
  openclaw plugins install @tencent-weixin/openclaw-weixin
  openclaw plugins list
)
```

If remote registry throttles (`429`) or plugin install is memory-heavy, retry later or use a larger instance.

## 5) Verify data is really in dat9

```bash
# Write via mounted path
echo "openclaw-fuse-ok" > "$HOME/dat9-openclaw/openclaw-home/fuse-check.txt"

# Read via dat9 API/CLI path
dat9 fs cat /openclaw-home/fuse-check.txt
```

If output is `openclaw-fuse-ok`, OpenClaw-visible files are persisted in dat9.

## 6) Unmount safely

```bash
dat9 umount "$HOME/dat9-openclaw"
```

On Linux, dat9 now prefers `fusermount3`, then falls back to `fusermount`, then `umount`.
