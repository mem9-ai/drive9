---
name: drive9-fuse-openclaw
version: 0.1.0
description: Use drive9 FUSE as OpenClaw workspace storage for persistent files.
homepage: https://drive9.ai
---

# drive9 FUSE for OpenClaw

Mount a drive9 tenant locally, then point OpenClaw workspace paths into the mount so files survive host restarts and can be queried through drive9.

## Prerequisites

- `drive9` CLI installed and authenticated (`DRIVE9_SERVER`, `DRIVE9_API_KEY` or `drive9 ctx`)
- `openclaw` CLI installed
- Linux: `fuse3` package installed (`fusermount3` available)

Install `drive9` using the standard entrypoint:

```bash
curl -fsSL https://drive9.ai/install.sh | sh
drive9 --version
```

## 1) Provision (or reuse) a drive9 tenant

```bash
drive9 create --name openclaw
drive9 ctx openclaw
```

## 2) Mount drive9

```bash
mkdir -p "$HOME/drive9-openclaw"
drive9 mount "$HOME/drive9-openclaw"
```

Keep this process running in a tmux/screen session or as a service.

## 3) Run OpenClaw in an isolated shell rooted on the mount

Do not overwrite your global shell `HOME`. Use a subshell so only OpenClaw
state/config writes into drive9.

```bash
mkdir -p "$HOME/drive9-openclaw/openclaw-home"
(
  export HOME="$HOME/drive9-openclaw/openclaw-home"
  openclaw --version
)
```

## 4) Skill/plugin operations (example)

```bash
(
  export HOME="$HOME/drive9-openclaw/openclaw-home"
  openclaw plugins --help
  openclaw plugins install @tencent-weixin/openclaw-weixin
  openclaw plugins list
)
```

If remote registry throttles (`429`) or plugin install is memory-heavy, retry later or use a larger instance.

## 5) Verify data is really in drive9

```bash
# Write via mounted path
echo "openclaw-fuse-ok" > "$HOME/drive9-openclaw/openclaw-home/fuse-check.txt"

# Read via drive9 API/CLI path
drive9 fs cat /openclaw-home/fuse-check.txt
```

If output is `openclaw-fuse-ok`, OpenClaw-visible files are persisted in drive9.

## 6) Unmount safely

```bash
drive9 umount "$HOME/drive9-openclaw"
```

On Linux, drive9 now prefers `fusermount3`, then falls back to `fusermount`, then `umount`.
