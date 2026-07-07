#!/usr/bin/env bash
# Shared temporary-directory helpers for live e2e scripts.

DRIVE9_TMP_HELPER_LOADED=1

drive9_e2e_init_tmpdir() {
  local tmp_root="${DRIVE9_E2E_TMPDIR:-${TMPDIR:-/tmp}}"

  if [ -z "$tmp_root" ]; then
    echo "DRIVE9_E2E_TMPDIR/TMPDIR resolved to an empty path" >&2
    return 1
  fi

  umask 077
  mkdir -p "$tmp_root" || return 1
  export DRIVE9_E2E_TMPDIR="$tmp_root"
  export TMPDIR="$tmp_root"
}

drive9_e2e_tmp_path() {
  local rel="${1:?relative temp path required}"
  local tmp_root="${DRIVE9_E2E_TMPDIR:-${TMPDIR:-/tmp}}"

  case "$tmp_root" in
    /) printf '/%s' "$rel" ;;
    */) printf '%s%s' "$tmp_root" "$rel" ;;
    *) printf '%s/%s' "$tmp_root" "$rel" ;;
  esac
}
