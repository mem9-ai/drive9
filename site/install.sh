#!/bin/sh
set -e

# dat9 installer
# Usage: curl -fsSL https://dat9.ai/install | sh

BASE_URL="https://dat9.ai"
API_URL="https://api.dat9.ai"
DEFAULT_INSTALL_DIR="/usr/local/bin"
INSTALL_DIR=""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
DIM='\033[2m'
BOLD='\033[1m'
RESET='\033[0m'

info()    { printf "  ${DIM}%s${RESET}\n" "$1"; }
success() { printf "  ${GREEN}%s${RESET}\n" "$1"; }
warn()    { printf "  ${YELLOW}%s${RESET}\n" "$1"; }
error()   { printf "  ${RED}error:${RESET} %s\n" "$1" >&2; exit 1; }

detect_os() {
  case "$(uname -s)" in
    Linux*)   echo "linux" ;;
    Darwin*)  echo "darwin" ;;
    MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
    *) error "Unsupported OS: $(uname -s)" ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)   echo "amd64" ;;
    aarch64|arm64)   echo "arm64" ;;
    *) error "Unsupported architecture: $(uname -m)" ;;
  esac
}

download() {
  if command -v curl > /dev/null 2>&1; then
    if [ -t 2 ]; then
      curl -fSL --progress-bar -o "$2" "$1"
    else
      curl -fsSL -o "$2" "$1"
    fi
  elif command -v wget > /dev/null 2>&1; then
    if [ -t 2 ]; then
      wget --show-progress -q -O "$2" "$1" 2>&1
    else
      wget -q -O "$2" "$1"
    fi
  else
    error "Neither curl nor wget found."
  fi
}

download_quiet() {
  if command -v curl > /dev/null 2>&1; then
    curl -fsSL -o "$2" "$1"
  elif command -v wget > /dev/null 2>&1; then
    wget -q -O "$2" "$1"
  else
    error "Neither curl nor wget found."
  fi
}

fetch_version() {
  LATEST_VERSION=""
  VERSION_URL="${BASE_URL}/releases/version"
  if command -v curl > /dev/null 2>&1; then
    LATEST_VERSION=$(curl -fsSL "$VERSION_URL" 2>/dev/null | tr -d '[:space:]') || true
  elif command -v wget > /dev/null 2>&1; then
    LATEST_VERSION=$(wget -q -O - "$VERSION_URL" 2>/dev/null | tr -d '[:space:]') || true
  fi
}

is_user_managed_dir() {
  case "$1" in
    "$HOME/.local/bin"|"$HOME/.cargo/bin"|"$HOME/bin"|"/usr/local/bin") return 0 ;;
    *) return 1 ;;
  esac
}

resolve_install_dir() {
  if [ -n "${DAT9_INSTALL_DIR:-}" ]; then
    INSTALL_DIR="$DAT9_INSTALL_DIR"
    info "Install dir: ${INSTALL_DIR} (from DAT9_INSTALL_DIR)"
    return
  fi

  EXISTING=$(command -v dat9 2>/dev/null || true)
  if [ -n "$EXISTING" ] && [ -x "$EXISTING" ]; then
    EXISTING_DIR=$(dirname "$EXISTING")
    if is_user_managed_dir "$EXISTING_DIR"; then
      INSTALL_DIR="$EXISTING_DIR"
      info "Upgrading active dat9 in ${INSTALL_DIR}"
      return
    fi

    INSTALL_DIR="$DEFAULT_INSTALL_DIR"
    warn "dat9 currently resolves to ${EXISTING}"
    warn "Installing to ${INSTALL_DIR}; set DAT9_INSTALL_DIR=${EXISTING_DIR} to replace the active binary"
    return
  fi

  INSTALL_DIR="$DEFAULT_INSTALL_DIR"
  info "Install dir: ${INSTALL_DIR}"
}

report_path_status() {
  INSTALLED="${INSTALL_DIR}/dat9"
  ACTIVE=$(command -v dat9 2>/dev/null || true)

  if [ -z "$ACTIVE" ]; then
    warn "dat9 is installed at ${INSTALLED}, but ${INSTALL_DIR} is not on your PATH"
    warn "Run ${INSTALLED} directly or add ${INSTALL_DIR} to PATH"
    return
  fi

  if [ "$ACTIVE" != "$INSTALLED" ]; then
    warn "PATH shadowing detected: dat9 resolves to ${ACTIVE}"
    warn "Installed binary: ${INSTALLED}"
    warn "Re-run with DAT9_INSTALL_DIR=$(dirname "$ACTIVE") to replace the active binary"
  fi
}

bootstrap_config() {
  if [ -z "${HOME:-}" ]; then
    return
  fi
  CONFIG_DIR="${HOME}/.dat9"
  CONFIG_FILE="${CONFIG_DIR}/config"
  mkdir -p "${CONFIG_DIR}" 2>/dev/null || true
  if [ ! -f "${CONFIG_FILE}" ]; then
    cat > "${CONFIG_FILE}" <<CONF
{
  "server": "${API_URL}",
  "contexts": {}
}
CONF
    chmod 600 "${CONFIG_FILE}"
  fi
}

main() {
  printf "\n"
  printf "  ${BOLD}dat9${RESET} installer\n"
  printf "  ${DIM}────────────────────────────${RESET}\n"
  printf "\n"

  OS=$(detect_os)
  ARCH=$(detect_arch)
  info "Platform: ${OS}/${ARCH}"

  LATEST_VERSION=""
  fetch_version
  if [ -n "$LATEST_VERSION" ]; then
    info "Latest version: v${LATEST_VERSION}"
  fi

  resolve_install_dir

  OLD_VERSION=""
  if [ -x "${INSTALL_DIR}/dat9" ]; then
    OLD_VERSION=$("${INSTALL_DIR}/dat9" --version 2>/dev/null | sed 's/^dat9[[:space:]]*//' | tr -d '[:space:]') || true
  fi

  if [ ! -d "$INSTALL_DIR" ]; then
    mkdir -p "$INSTALL_DIR" 2>/dev/null || sudo mkdir -p "$INSTALL_DIR"
  fi

  TMP_DIR=$(mktemp -d)
  trap 'rm -rf "$TMP_DIR"' EXIT

  # Download dat9 (FUSE support is built-in, no separate binary needed)
  if [ -n "$LATEST_VERSION" ]; then
    info "Downloading dat9 v${LATEST_VERSION}..."
  else
    info "Downloading dat9..."
  fi
  if ! download "${BASE_URL}/releases/dat9-${OS}-${ARCH}" "$TMP_DIR/dat9"; then
    error "No pre-built binary available for ${OS}/${ARCH}.\n  Available: linux/amd64, linux/arm64, darwin/arm64, darwin/amd64\n  Visit https://dat9.ai for more info."
  fi
  chmod +x "$TMP_DIR/dat9"

  if [ -w "$INSTALL_DIR" ]; then
    mv "$TMP_DIR/dat9" "$INSTALL_DIR/dat9"
  else
    info "Installing to ${INSTALL_DIR} (requires sudo)..."
    sudo mv "$TMP_DIR/dat9" "$INSTALL_DIR/dat9"
  fi

  printf "\n"
  NEW_VERSION=$(${INSTALL_DIR}/dat9 --version 2>/dev/null | sed 's/^dat9[[:space:]]*//' | tr -d '[:space:]') || true
  if [ -n "$OLD_VERSION" ] && [ -n "$NEW_VERSION" ] && [ "$OLD_VERSION" != "$NEW_VERSION" ]; then
    success "dat9 upgraded successfully! (v${OLD_VERSION} -> v${NEW_VERSION})"
  elif [ -n "$NEW_VERSION" ]; then
    success "dat9 installed successfully! (v${NEW_VERSION})"
  else
    success "dat9 installed successfully!"
  fi
  bootstrap_config
  report_path_status
  printf "\n"
  printf "  Get started:\n"
  printf "\n"
  printf "    ${BOLD}1.${RESET} Create a workspace\n"
  printf "       ${DIM}\$${RESET} dat9 create\n"
  printf "\n"
  printf "    ${BOLD}2.${RESET} Verify it's ready\n"
  printf "       ${DIM}\$${RESET} dat9 fs ls :/\n"
  printf "\n"
  printf "    ${BOLD}3.${RESET} Start using dat9\n"
  printf "       ${DIM}\$${RESET} dat9 fs cp ./file.txt :/data/file.txt\n"
  printf "       ${DIM}\$${RESET} dat9 fs grep \"search term\" /\n"
  printf "       ${DIM}\$${RESET} dat9 mount ~/dat9\n"
  printf "\n"
  printf "  Docs: ${DIM}https://dat9.ai/skill.md${RESET}\n"
  printf "\n"
}

main
