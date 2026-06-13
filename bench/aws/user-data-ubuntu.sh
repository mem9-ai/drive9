#!/usr/bin/env bash
set -euo pipefail

# Cloud-init friendly bootstrap for an Ubuntu 24.04 EC2 benchmark host.
# Override these through cloud-init variable substitution or by editing the
# user-data payload before launch.
DRIVE9_BENCH_REPO_URL="${DRIVE9_BENCH_REPO_URL:-https://github.com/mem9-ai/drive9.git}"
DRIVE9_BENCH_REF="${DRIVE9_BENCH_REF:-main}"
DRIVE9_BENCH_DIR="${DRIVE9_BENCH_DIR:-/opt/drive9}"
BENCH_HOME="${BENCH_HOME:-/mnt/drive9-bench}"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ca-certificates curl git sudo

if [[ ! -d "$DRIVE9_BENCH_DIR/.git" ]]; then
  rm -rf "$DRIVE9_BENCH_DIR"
  git clone "$DRIVE9_BENCH_REPO_URL" "$DRIVE9_BENCH_DIR"
fi
git -C "$DRIVE9_BENCH_DIR" fetch --all --tags
git -C "$DRIVE9_BENCH_DIR" checkout "$DRIVE9_BENCH_REF"

mkdir -p "$BENCH_HOME"
chmod 0777 "$BENCH_HOME"

cd "$DRIVE9_BENCH_DIR"
BENCH_HOME="$BENCH_HOME" bash bench/bin/bootstrap-host.sh

cat >/etc/profile.d/drive9-bench.sh <<EOF
export BENCH_HOME="$BENCH_HOME"
export PATH="\$HOME/.local/bin:\$HOME/.bun/bin:\$HOME/.cargo/bin:\$PATH"
EOF

if [[ -n "${DRIVE9_API_KEY:-}" ]]; then
  install -d -m 0700 /root/drive9-bench
  cat >/root/drive9-bench/env.sh <<EOF
export BENCH_HOME="$BENCH_HOME"
export DRIVE9_API_KEY="$(printf '%q' "$DRIVE9_API_KEY")"
EOF
fi

bench/bin/run-repo-build.py doctor --dry-run || true

echo "drive9 benchmark host ready"
echo "repo: $DRIVE9_BENCH_DIR"
echo "bench home: $BENCH_HOME"
echo "next:"
echo "  cd $DRIVE9_BENCH_DIR"
echo "  source /etc/profile.d/drive9-bench.sh"
echo "  bench/bin/run-repo-build.py doctor"
echo "  bench/bin/run-repo-build.py run"
