#!/bin/bash

# Podman-backed testcontainers environment for dat9.
#
# testcontainers-go normally talks to a Docker-compatible socket; we point it at
# the active Podman socket via DOCKER_HOST.
#
# Ryuk is testcontainers-go's cleanup sidecar. It can fail against Podman on
# both macOS and Linux depending on socket-mount support, so we disable it for
# this test target.

set -euo pipefail

fail() {
  printf 'test-podman.sh: %s\n' "$1" >&2
  return 1 2>/dev/null || exit 1
}

if ! command -v podman >/dev/null 2>&1; then
  fail "podman is not installed"
fi

case "$(uname -s)" in
  Darwin)
    podman_socket="$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null || true)"
    if [ -z "$podman_socket" ]; then
      fail "could not determine the Podman machine socket; is podman machine initialized?"
    fi
    ;;
  Linux)
    podman_socket="$(podman info --format '{{.Host.RemoteSocket.Path}}' 2>/dev/null || true)"
    podman_socket_exists="$(podman info --format '{{.Host.RemoteSocket.Exists}}' 2>/dev/null || true)"
    if [ -z "$podman_socket" ]; then
      fail "could not determine the Podman socket path from podman info"
    fi
    if [ "$podman_socket_exists" != "true" ]; then
      fail "Podman reports remote socket unavailable at $podman_socket; start the Podman API service (for example: systemctl --user start podman.socket or podman system service --time=0 unix://$podman_socket)"
    fi
    if [ ! -S "$podman_socket" ]; then
      fail "Podman socket path does not exist or is not a Unix socket: $podman_socket"
    fi
    ;;
  *)
    fail "unsupported host OS: $(uname -s)"
    ;;
esac

export DOCKER_HOST="unix://$podman_socket"
export TESTCONTAINERS_RYUK_DISABLED='true'

if ! podman info >/dev/null 2>&1; then
  fail "podman is not reachable; start the Podman machine first"
fi

unset podman_socket
unset podman_socket_exists
