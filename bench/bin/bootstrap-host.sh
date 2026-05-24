#!/usr/bin/env bash
set -euo pipefail

GO_VERSION="${GO_VERSION:-1.25.1}"
NODE_MAJOR="${NODE_MAJOR:-22}"
INSTALL_ROOT="${INSTALL_ROOT:-$HOME/.local/share/drive9-bench}"
BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"
NO_INSTALL=false

export PATH="$BIN_DIR:$HOME/.bun/bin:$HOME/.cargo/bin:$PATH"

usage() {
  cat <<'USAGE'
usage: bench/bin/bootstrap-host.sh [--no-install]

Installs or checks the host tools needed by bench/bin/run-repo-build.py:
git, Python 3, make/build tools, FUSE, the drive9 CLI, Bun, uv, Rust, and Go.

Options:
  --no-install   only print missing tools; do not install packages.
USAGE
}

while (($#)); do
  case "$1" in
    --no-install)
      NO_INSTALL=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

log() {
  printf 'bench bootstrap: %s\n' "$*"
}

have() {
  command -v "$1" >/dev/null 2>&1
}

need_sudo() {
  if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

append_path_hint() {
  mkdir -p "$BIN_DIR"
  case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *) log "add this to your shell profile: export PATH=\"$BIN_DIR:\$PATH\"" ;;
  esac
}

install_os_packages() {
  if $NO_INSTALL; then
    log "--no-install set; skipping OS package installation"
    return
  fi

  if have apt-get; then
    log "installing Ubuntu/Debian packages"
    need_sudo apt-get update
    need_sudo apt-get install -y \
      ca-certificates curl git jq make build-essential pkg-config libssl-dev \
      python3 python3-venv unzip tar xz-utils fuse3 libcap-dev
    return
  fi

  if have dnf; then
    log "installing Amazon Linux/Fedora packages"
    need_sudo dnf install -y \
      ca-certificates curl git jq make gcc gcc-c++ pkgconf-pkg-config openssl-devel \
      python3 nodejs npm unzip tar xz fuse3 libcap-devel
    return
  fi

  if have yum; then
    log "installing yum packages"
    need_sudo yum install -y \
      ca-certificates curl git jq make gcc gcc-c++ pkgconfig openssl-devel \
      python3 nodejs npm unzip tar xz fuse3 libcap-devel
    return
  fi

  if have brew; then
    log "installing macOS Homebrew packages"
    brew install git jq go node bun uv rustup-init || true
    return
  fi

  log "no supported package manager found; continuing with language installers"
}

install_node() {
  local major
  if have node; then
    major="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)"
    if [[ "$major" =~ ^[0-9]+$ ]] && ((major >= 20)); then
      log "node: $(node --version)"
      if have npm; then
        log "npm: $(npm --version)"
      fi
      return
    fi
    log "node $(node --version) is too old; installing Node ${NODE_MAJOR}.x"
  fi
  if $NO_INSTALL; then
    log "missing Node >=20"
    return
  fi

  if have apt-get; then
    mkdir -p "$INSTALL_ROOT"
    log "installing Node ${NODE_MAJOR}.x from NodeSource"
    curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" -o "$INSTALL_ROOT/nodesource-setup.sh"
    need_sudo bash "$INSTALL_ROOT/nodesource-setup.sh"
    need_sudo apt-get install -y nodejs
    log "node: $(node --version)"
    log "npm: $(npm --version)"
    return
  fi

  if have brew; then
    brew install node || true
    log "node: $(node --version)"
    return
  fi

  log "no Node installer configured for this OS; install Node >=20 before running kimi-cli"
}

install_bun() {
  if have bun; then
    log "bun: $(bun --version)"
    return
  fi
  if $NO_INSTALL; then
    log "missing bun"
    return
  fi
  log "installing Bun"
  curl -fsSL https://bun.sh/install | bash
  log "Bun installer usually writes to ~/.bun/bin; reload your shell if bun is not on PATH"
}

install_uv() {
  if have uv; then
    log "uv: $(uv --version)"
    return
  fi
  if $NO_INSTALL; then
    log "missing uv"
    return
  fi
  log "installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
}

install_rust() {
  if have cargo && have rustc; then
    log "cargo: $(cargo --version)"
    log "rustc: $(rustc --version)"
    return
  fi
  if $NO_INSTALL; then
    log "missing cargo/rustc"
    return
  fi
  log "installing Rust toolchain"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
  fi
  rustup component add rustfmt clippy || true
}

install_go() {
  if have go; then
    log "go: $(go version)"
    return
  fi
  if $NO_INSTALL; then
    log "missing go"
    return
  fi

  local os arch suffix url dest
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) echo "unsupported architecture for Go install: $arch" >&2; exit 1 ;;
  esac
  suffix="${os}-${arch}"
  url="https://go.dev/dl/go${GO_VERSION}.${suffix}.tar.gz"
  dest="$INSTALL_ROOT/go${GO_VERSION}"
  mkdir -p "$INSTALL_ROOT" "$BIN_DIR"
  log "installing Go ${GO_VERSION} from $url"
  curl -fsSL "$url" -o "$INSTALL_ROOT/go.tgz"
  rm -rf "$dest"
  mkdir -p "$dest"
  tar -C "$dest" --strip-components=1 -xzf "$INSTALL_ROOT/go.tgz"
  ln -sf "$dest/bin/go" "$BIN_DIR/go"
  ln -sf "$dest/bin/gofmt" "$BIN_DIR/gofmt"
  append_path_hint
}

install_drive9_cli() {
  if have drive9; then
    log "drive9: $(drive9 --version 2>/dev/null || drive9 version 2>/dev/null || echo present)"
    return
  fi
  if $NO_INSTALL; then
    log "missing drive9"
    return
  fi
  log "installing drive9 CLI from https://drive9.ai"
  curl -fsSL https://drive9.ai/install.sh | sh
}

check_fuse() {
  case "$(uname -s)" in
    Linux)
      if [[ -e /dev/fuse ]]; then
        log "FUSE: /dev/fuse present"
      else
        log "FUSE: /dev/fuse missing"
      fi
      ;;
    Darwin)
      if [[ -d /Library/Filesystems/macfuse.fs ]] || have mount_macfuse; then
        log "macFUSE: present"
      else
        log "macFUSE not found; install it before FUSE runs"
      fi
      ;;
  esac
}

main() {
  install_os_packages
  install_node
  install_bun
  install_uv
  install_rust
  install_go
  install_drive9_cli
  check_fuse
  log "bootstrap complete"
  log "next: bench/bin/run-repo-build.py doctor --dry-run"
}

main "$@"
