#!/bin/bash

# Podman-backed testcontainers environment for dat9-3 on macOS.
#
# testcontainers-go normally talks to a Docker-compatible socket; we point it at
# the current Podman machine socket via DOCKER_HOST.
#
# Ryuk is testcontainers-go's cleanup sidecar. On macOS + Podman, Ryuk fails
# when it tries to mount the Podman machine socket into its container, so the
# test suite cannot even start. Disabling Ryuk avoids that socket-mount path and
# lets the MySQL test container start successfully.

set -euo pipefail

fail() {
  printf 'macos-podman.sh: %s\n' "$1" >&2
  return 1 2>/dev/null || exit 1
}

if [ "$(uname -s)" != "Darwin" ]; then
  fail "this helper is intended for macOS hosts"
fi

if ! command -v podman >/dev/null 2>&1; then
  fail "podman is not installed"
fi

podman_socket="$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null || true)"
if [ -z "$podman_socket" ]; then
  fail "could not determine the Podman machine socket; is podman machine initialized?"
fi

export DOCKER_HOST="unix://$podman_socket"
export TESTCONTAINERS_RYUK_DISABLED='true'

if ! podman info >/dev/null 2>&1; then
  fail "podman is not reachable; start the Podman machine first"
fi

unset podman_socket
