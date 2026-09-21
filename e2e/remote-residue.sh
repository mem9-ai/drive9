#!/usr/bin/env bash
# Shared remote-root residue verdict for e2e cleanup acceptance.
#
# Sourcing scripts must provide a `drive9` command (shell function or binary
# on PATH) and may set REQUEST_MAX_RETRIES / REQUEST_RETRY_SLEEP_S (same knobs
# as drive9_retry). remote_root_not_found succeeds only when the CLI states
# the path is gone — an explicit "not found" or HTTP 404. A resolvable path
# (stat exits 0) and any other non-zero result (HTTP 5xx, timeout, auth
# failure, connection error) fail the residue check, with bounded retries for
# transient errors so a briefly unavailable backend cannot false-pass a
# residue assertion (issue #936 cleanup acceptance).

remote_root_not_found() {
  if [ "$#" -ne 1 ]; then
    echo "remote_root_not_found requires exactly one path argument" >&2
    return 64
  fi
  local target="$1"
  local attempt=1 out rc
  while :; do
    set +e
    out="$(drive9 fs stat "$target" 2>&1)"
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      echo "remote root residue: $target is still resolvable (stat: $out)" >&2
      return 1
    fi
    if [[ "$out" == *"HTTP 404"* || "$out" == *"404 Not Found"* || "$out" =~ [Nn]ot\ [Ff]ound ]]; then
      return 0
    fi
    if [ "$attempt" -lt "${REQUEST_MAX_RETRIES:-8}" ]; then
      attempt=$((attempt + 1))
      sleep "${REQUEST_RETRY_SLEEP_S:-2}"
      continue
    fi
    echo "remote root residue check could not confirm deletion of $target after $attempt attempts (stat: $out)" >&2
    return 1
  done
}
