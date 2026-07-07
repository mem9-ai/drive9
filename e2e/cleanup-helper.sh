#!/usr/bin/env bash
# Shared cleanup registry helpers for live e2e scripts.
#
# Safety model:
# - never discover tenants from the server;
# - only delete resources explicitly registered by this local e2e run;
# - require e2e source markers and drive9-e2e-* run IDs before deletion;
# - keep raw registries owner-readable only because they contain API keys.

_drive9_cleanup_helper_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -z "${DRIVE9_TMP_HELPER_LOADED:-}" ]; then
  . "$_drive9_cleanup_helper_dir/tmp-helper.sh"
fi

DRIVE9_CLEANUP_HELPER_LOADED=1
DRIVE9_E2E_CLEANUP_SOURCE="drive9-e2e-cleanup-v1"

drive9_cleanup_mode() {
  printf '%s' "${DRIVE9_E2E_CLEANUP:-0}"
}

drive9_cleanup_is_enabled() {
  case "$(drive9_cleanup_mode)" in
    always|success) return 0 ;;
    0|off|false|"") return 1 ;;
    *)
      echo "invalid DRIVE9_E2E_CLEANUP=${DRIVE9_E2E_CLEANUP}; expected 0, always, or success" >&2
      return 2
      ;;
  esac
}

drive9_cleanup_cache_root() {
  printf '%s' "${DRIVE9_E2E_CLEANUP_CACHE_DIR:-${XDG_CACHE_HOME:-${HOME:?HOME is required}/.cache}/drive9-smoke}"
}

drive9_cleanup_timestamp() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

drive9_cleanup_generate_run_id() {
  printf 'drive9-e2e-%s-%s' "$(date -u '+%Y%m%dT%H%M%SZ')" "$$"
}

drive9_cleanup_validate_run_id() {
  case "$1" in
    drive9-e2e-*) return 0 ;;
    *)
      echo "refusing cleanup for run_id without drive9-e2e- prefix: $1" >&2
      return 1
      ;;
  esac
}

drive9_cleanup_validate_registry_name() {
  local registry="$1" name

  name="$(basename "$registry")"
  case "$name" in
    drive9-e2e-*.jsonl) ;;
    *)
      echo "refusing cleanup for registry without drive9-e2e-*.jsonl name: $registry" >&2
      return 1
      ;;
  esac
  if [ -L "$registry" ]; then
    echo "refusing cleanup registry symlink: $registry" >&2
    return 1
  fi
}

drive9_cleanup_registry_run_id() {
  basename "$1" .jsonl
}

drive9_cleanup_validate_registry_run_id() {
  local registry="$1" run_id="$2" registry_run_id

  registry_run_id="$(drive9_cleanup_registry_run_id "$registry")"
  if [ "$registry_run_id" != "$run_id" ]; then
    echo "refusing cleanup registry/run_id mismatch: registry=$registry run_id=$run_id" >&2
    return 1
  fi
}

drive9_cleanup_validate_registry_file() {
  local registry="$1" mode owner uid perms stat_out

  drive9_cleanup_validate_registry_name "$registry" || return 1
  [ -f "$registry" ] || {
    echo "cleanup registry not found: $registry" >&2
    return 1
  }

  uid="$(id -u)"
  stat_out="$(stat -c '%a %u' "$registry" 2>/dev/null || stat -f '%Lp %u' "$registry" 2>/dev/null)" || {
    echo "failed to stat cleanup registry: $registry" >&2
    return 1
  }
  mode="${stat_out%% *}"
  owner="${stat_out##* }"
  if [ "$owner" != "$uid" ]; then
    echo "refusing cleanup registry not owned by current user: $registry" >&2
    return 1
  fi
  case "$mode" in
    ''|*[!0-7]*)
      echo "refusing cleanup registry with unreadable mode: $registry" >&2
      return 1
      ;;
  esac
  perms=$((8#$mode))
  if [ $((perms & 077)) -ne 0 ]; then
    echo "refusing cleanup registry readable or writable by group/other: $registry mode=$mode" >&2
    return 1
  fi
}

drive9_cleanup_lock_active() {
  local lock_file="$1" pid
  [ -f "$lock_file" ] || return 1
  pid="$(sed -n '1p' "$lock_file" 2>/dev/null || true)"
  case "$pid" in
    ''|*[!0-9]*) return 1 ;;
  esac
  kill -0 "$pid" >/dev/null 2>&1
}

drive9_cleanup_init_run() {
  local mode run_id cache_root pending_dir completed_dir retained_dir registry lock_file run_dir old_umask

  mode="$(drive9_cleanup_mode)"
  case "$mode" in
    0|off|false|"") return 0 ;;
    always|success) ;;
    *)
      echo "invalid DRIVE9_E2E_CLEANUP=$mode; expected 0, always, or success" >&2
      return 1
      ;;
  esac

  run_id="${DRIVE9_E2E_RUN_ID:-$(drive9_cleanup_generate_run_id)}"
  drive9_cleanup_validate_run_id "$run_id" || return 1

  cache_root="$(drive9_cleanup_cache_root)"
  pending_dir="${DRIVE9_E2E_CLEANUP_PENDING_DIR:-$cache_root/cleanup-pending}"
  completed_dir="${DRIVE9_E2E_CLEANUP_COMPLETED_DIR:-$cache_root/cleanup-completed}"
  retained_dir="${DRIVE9_E2E_CLEANUP_RETAINED_DIR:-$cache_root/cleanup-retained}"
  drive9_e2e_init_tmpdir || return 1
  run_dir="${DRIVE9_E2E_RUN_DIR:-$(drive9_e2e_tmp_path "drive9-smoke/$run_id")}"
  registry="${DRIVE9_E2E_CLEANUP_REGISTRY:-$pending_dir/$run_id.jsonl}"
  lock_file="${registry}.lock"
  drive9_cleanup_validate_registry_name "$registry" || return 1
  drive9_cleanup_validate_registry_run_id "$registry" "$run_id" || return 1

  old_umask="$(umask)"
  umask 077
  mkdir -p "$pending_dir" "$completed_dir" "$retained_dir" "$run_dir" || {
    umask "$old_umask"
    return 1
  }
  if [ -s "$registry" ]; then
    echo "cleanup registry already exists and is non-empty: $registry" >&2
    umask "$old_umask"
    return 1
  fi
  : >"$registry" || {
    umask "$old_umask"
    return 1
  }
  chmod 600 "$registry" || {
    umask "$old_umask"
    return 1
  }
  drive9_cleanup_validate_registry_file "$registry" || {
    umask "$old_umask"
    return 1
  }
  printf '%s\n' "$$" >"$lock_file" || {
    umask "$old_umask"
    return 1
  }
  chmod 600 "$lock_file" || {
    umask "$old_umask"
    return 1
  }
  umask "$old_umask"

  export DRIVE9_E2E_RUN_ID="$run_id"
  export DRIVE9_E2E_RUN_DIR="$run_dir"
  export DRIVE9_E2E_CLEANUP_REGISTRY="$registry"
  export DRIVE9_E2E_CLEANUP_PENDING_DIR="$pending_dir"
  export DRIVE9_E2E_CLEANUP_COMPLETED_DIR="$completed_dir"
  export DRIVE9_E2E_CLEANUP_RETAINED_DIR="$retained_dir"
  export DRIVE9_E2E_CLEANUP_LOCK="$lock_file"

  echo "cleanup registry: $registry"
}

drive9_cleanup_require_ready() {
  local mode registry run_id parent

  mode="$(drive9_cleanup_mode)"
  case "$mode" in
    0|off|false|"") return 0 ;;
    always|success) ;;
    *)
      echo "invalid DRIVE9_E2E_CLEANUP=$mode; expected 0, always, or success" >&2
      return 1
      ;;
  esac

  run_id="${DRIVE9_E2E_RUN_ID:-}"
  registry="${DRIVE9_E2E_CLEANUP_REGISTRY:-}"
  if [ -z "$run_id" ] || [ -z "$registry" ]; then
    echo "DRIVE9_E2E_RUN_ID and DRIVE9_E2E_CLEANUP_REGISTRY are required when cleanup is enabled" >&2
    return 1
  fi
  drive9_cleanup_validate_run_id "$run_id" || return 1
  drive9_cleanup_validate_registry_run_id "$registry" "$run_id" || return 1

  parent="$(dirname "$registry")"
  if [ ! -d "$parent" ] || [ ! -w "$parent" ]; then
    echo "cleanup registry parent is not writable: $parent" >&2
    return 1
  fi
  if [ ! -f "$registry" ] || [ ! -w "$registry" ]; then
    echo "cleanup registry is not writable: $registry" >&2
    return 1
  fi
  drive9_cleanup_validate_registry_file "$registry" || return 1
}

drive9_cleanup_register() {
  local kind="$1" suite="$2" base="$3" tenant_id="$4" api_key="$5"
  local registry run_id created_at public_key private_key cleanup_requires_tidbcloud=false

  if ! drive9_cleanup_require_ready; then
    return 1
  fi
  if ! drive9_cleanup_is_enabled; then
    return 0
  fi

  registry="${DRIVE9_E2E_CLEANUP_REGISTRY:-}"
  run_id="${DRIVE9_E2E_RUN_ID:-}"
  created_at="$(drive9_cleanup_timestamp)"

  case "$kind" in
    live|fork|admin_live) ;;
    *)
      echo "invalid cleanup kind: $kind" >&2
      return 1
      ;;
  esac
  case "$base" in
    http://*|https://*) ;;
    *)
      echo "refusing cleanup registration for non-http base: $base" >&2
      return 1
      ;;
  esac
  if [ -z "$tenant_id" ] || [ -z "$api_key" ]; then
    echo "cleanup registration requires tenant_id and api_key for suite=$suite kind=$kind" >&2
    return 1
  fi
  if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required for cleanup registration" >&2
    return 1
  fi
  public_key="${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-${DRIVE9_PUBLIC_KEY:-}}"
  private_key="${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-${DRIVE9_PRIVATE_KEY:-}}"
  if [ -n "$public_key" ] || [ -n "$private_key" ]; then
    if [ -z "$public_key" ] || [ -z "$private_key" ]; then
      echo "cleanup registration requires both TiDBCloud public and private keys, or neither for default-key endpoints" >&2
      return 1
    fi
    cleanup_requires_tidbcloud=true
  fi

  jq -cn \
    --arg source "$DRIVE9_E2E_CLEANUP_SOURCE" \
    --arg run_id "$run_id" \
    --arg kind "$kind" \
    --arg suite "$suite" \
    --arg base "$base" \
    --arg tenant_id "$tenant_id" \
    --arg api_key "$api_key" \
    --arg created_at "$created_at" \
    --argjson cleanup_requires_tidbcloud "$cleanup_requires_tidbcloud" \
    '{source:$source, run_id:$run_id, kind:$kind, suite:$suite, base:$base, tenant_id:$tenant_id, api_key:$api_key, created_at:$created_at, cleanup_requires_tidbcloud:$cleanup_requires_tidbcloud}' \
    >>"$registry"
  chmod 600 "$registry" >/dev/null 2>&1 || true
  echo "registered cleanup: suite=$suite kind=$kind tenant_id=$tenant_id" >&2
}

drive9_cleanup_register_live() {
  drive9_cleanup_register live "$@"
}

drive9_cleanup_register_fork() {
  drive9_cleanup_register fork "$@"
}

drive9_cleanup_register_admin_live() {
  drive9_cleanup_register admin_live "$@"
}

drive9_cleanup_validate_registry() {
  local registry="$1" expected_run_id line n=0

  drive9_cleanup_validate_registry_file "$registry" || return 1
  expected_run_id="$(drive9_cleanup_registry_run_id "$registry")"
  if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required for cleanup" >&2
    return 1
  fi

  while IFS= read -r line || [ -n "$line" ]; do
    n=$((n + 1))
    [ -n "$line" ] || continue
    if ! jq -e \
      --arg source "$DRIVE9_E2E_CLEANUP_SOURCE" \
      --arg run_id "$expected_run_id" \
      '(.source == $source)
       and (.run_id == $run_id)
       and (.kind == "live" or .kind == "fork" or .kind == "admin_live")
       and (.suite | type == "string")
       and (.base | type == "string" and (startswith("http://") or startswith("https://")))
       and (.tenant_id | type == "string" and length > 0)
       and (.api_key | type == "string" and length > 0)
       and ((.cleanup_requires_tidbcloud // false) | type == "boolean")' \
      >/dev/null 2>&1 <<<"$line"; then
      echo "refusing cleanup: invalid registry record at $registry:$n" >&2
      return 1
    fi
  done <"$registry"
}

drive9_cleanup_body_args() {
  local public_key="${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-${DRIVE9_PUBLIC_KEY:-}}"
  local private_key="${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-${DRIVE9_PRIVATE_KEY:-}}"
  local body_file="$1"
  local requires_tidbcloud="${2:-false}"

  if [ -z "$public_key" ] && [ -z "$private_key" ]; then
    if [ "$requires_tidbcloud" = "true" ]; then
      echo "TiDBCloud cleanup requires DRIVE9_TIDBCLOUD_PUBLIC_KEY and DRIVE9_TIDBCLOUD_PRIVATE_KEY for this pending registry" >&2
      return 1
    fi
    return 0
  fi
  if [ -z "$public_key" ] || [ -z "$private_key" ]; then
    echo "TiDBCloud cleanup requires both public and private keys, or neither for default-key endpoints" >&2
    return 1
  fi
  jq -cn --arg pk "$public_key" --arg sk "$private_key" '{public_key:$pk, private_key:$sk}' >"$body_file"
  chmod 600 "$body_file" >/dev/null 2>&1 || true
  printf '%s' "1"
}

drive9_cleanup_verify_deleted() {
  local base="$1" api_key="$2" body_file code status

  body_file="$(mktemp)"
  code=$(curl -sS -o "$body_file" -w "%{http_code}" -H "Authorization: Bearer $api_key" "$base/v1/status" 2>/dev/null || true)
  case "$code" in
    401|403|404)
      rm -f "$body_file"
      printf '%s' "verified"
      return 0
      ;;
    200)
      status="$(jq -r '.status // empty' "$body_file" 2>/dev/null || true)"
      rm -f "$body_file"
      case "$status" in
        deleting|deleted)
          printf '%s' "verified"
          return 0
          ;;
        *)
          printf '%s' "active"
          return 1
          ;;
      esac
      ;;
    *)
      rm -f "$body_file"
      printf '%s' "unknown"
      return 1
      ;;
  esac
}

drive9_cleanup_delete_record() {
  local record="$1" dry_run="${DRIVE9_E2E_CLEANUP_DRY_RUN:-0}"
  local kind suite base tenant_id api_key requires_tidbcloud endpoint resp_file body_file body_present code reason verify

  kind="$(jq -r '.kind' <<<"$record")"
  suite="$(jq -r '.suite' <<<"$record")"
  base="$(jq -r '.base' <<<"$record")"
  tenant_id="$(jq -r '.tenant_id' <<<"$record")"
  api_key="$(jq -r '.api_key' <<<"$record")"
  requires_tidbcloud="$(jq -r '.cleanup_requires_tidbcloud // false' <<<"$record")"

  case "$kind" in
    live|admin_live) endpoint="$base/v1/tenant" ;;
    fork) endpoint="$base/v1/fork" ;;
    *)
      echo "cleanup delete failed: invalid kind=$kind suite=$suite tenant_id=$tenant_id" >&2
      return 1
      ;;
  esac

  if [ "$dry_run" = "1" ]; then
    echo "cleanup dry-run delete accepted: suite=$suite kind=$kind tenant_id=$tenant_id endpoint=$endpoint"
    return 0
  fi

  resp_file="$(mktemp)"
  body_file="$(mktemp)"
  body_present="$(drive9_cleanup_body_args "$body_file" "$requires_tidbcloud")" || {
    rm -f "$resp_file" "$body_file"
    return 1
  }

  if [ -n "$body_present" ]; then
    code=$(curl -sS -o "$resp_file" -w "%{http_code}" -X DELETE \
      -H "Authorization: Bearer $api_key" \
      -H "Content-Type: application/json" \
      --data-binary "@$body_file" \
      "$endpoint" 2>/dev/null || true)
  else
    code=$(curl -sS -o "$resp_file" -w "%{http_code}" -X DELETE \
      -H "Authorization: Bearer $api_key" \
      "$endpoint" 2>/dev/null || true)
  fi
  rm -f "$body_file"

  case "$code" in
    202)
      ;;
    401|403|404)
      rm -f "$resp_file"
      echo "cleanup already deleted or inaccessible: suite=$suite kind=$kind tenant_id=$tenant_id http=$code"
      return 0
      ;;
    *)
      reason="$(jq -r '.error // empty' "$resp_file" 2>/dev/null || true)"
      if [ -z "$reason" ]; then
        reason="$(tr '\n' ' ' <"$resp_file" | sed 's/[[:space:]]\+/ /g' | cut -c1-240)"
      fi
      rm -f "$resp_file"
      echo "cleanup delete failed: suite=$suite kind=$kind tenant_id=$tenant_id http=$code reason=$reason" >&2
      return 1
      ;;
  esac
  rm -f "$resp_file"

  verify="$(drive9_cleanup_verify_deleted "$base" "$api_key" || true)"
  case "$verify" in
    verified)
      echo "cleanup delete accepted: suite=$suite kind=$kind tenant_id=$tenant_id verified=$verify"
      return 0
      ;;
    *)
      echo "cleanup delete accepted but verification is $verify: suite=$suite kind=$kind tenant_id=$tenant_id" >&2
      return 1
      ;;
  esac
}

drive9_cleanup_registry_records_ordered() {
  local registry="$1"
  jq -c -s '
    (map(select(.kind == "fork")) | reverse)
    + (map(select(.kind != "fork")) | reverse)
    | .[]
  ' "$registry"
}

drive9_cleanup_write_summary() {
  local registry="$1" outcome="$2" delete_failed="$3" verify_failed="$4" summary_file="$5"

  jq -s \
    --arg source "$DRIVE9_E2E_CLEANUP_SOURCE" \
    --arg outcome "$outcome" \
    --argjson delete_failed "$delete_failed" \
    --argjson verify_failed "$verify_failed" \
    --arg completed_at "$(drive9_cleanup_timestamp)" \
    '{
      source: $source,
      outcome: $outcome,
      completed_at: $completed_at,
      created_live: (map(select(.kind == "live" or .kind == "admin_live")) | length),
      created_fork: (map(select(.kind == "fork")) | length),
      delete_failed: $delete_failed,
      verify_failed: $verify_failed,
      records: map({run_id, suite, kind, base, tenant_id, created_at})
    }' "$registry" >"$summary_file"
  chmod 600 "$summary_file" >/dev/null 2>&1 || true
}

drive9_cleanup_retain_registry() {
  local registry="$1" reason="$2" retained_dir="${DRIVE9_E2E_CLEANUP_RETAINED_DIR:-$(drive9_cleanup_cache_root)/cleanup-retained}"
  local dest

  mkdir -p "$retained_dir"
  dest="$retained_dir/$(basename "$registry").$reason"
  mv "$registry" "$dest"
  rm -f "${registry}.lock"
  echo "cleanup registry retained: $dest"
}

drive9_cleanup_run_registry() {
  local registry="$1" run_rc="${2:-0}" mode created_live created_fork record failures=0 summary_file completed_dir

  mode="$(drive9_cleanup_mode)"
  case "$mode" in
    0|off|false|"")
      echo "cleanup skipped: DRIVE9_E2E_CLEANUP=$mode"
      return 0
      ;;
    always|success) ;;
    *)
      echo "invalid DRIVE9_E2E_CLEANUP=$mode; expected 0, always, or success" >&2
      return 1
      ;;
  esac

  drive9_cleanup_validate_registry "$registry" || return 1

  created_live="$(jq -s '[.[] | select(.kind == "live" or .kind == "admin_live")] | length' "$registry")"
  created_fork="$(jq -s '[.[] | select(.kind == "fork")] | length' "$registry")"

  if [ "$mode" = "success" ] && [ "$run_rc" -ne 0 ]; then
    echo "cleanup retained because run failed and DRIVE9_E2E_CLEANUP=success"
    echo "cleanup: created_live=$created_live created_fork=$created_fork delete_skipped=$((created_live + created_fork))"
    drive9_cleanup_retain_registry "$registry" "run-failed"
    return 0
  fi

  while IFS= read -r record || [ -n "$record" ]; do
    [ -n "$record" ] || continue
    if ! drive9_cleanup_delete_record "$record"; then
      failures=$((failures + 1))
    fi
  done < <(drive9_cleanup_registry_records_ordered "$registry")

  echo "cleanup: created_live=$created_live created_fork=$created_fork delete_failed=$failures net_remaining=$failures"

  if [ "$failures" -eq 0 ]; then
    completed_dir="${DRIVE9_E2E_CLEANUP_COMPLETED_DIR:-$(drive9_cleanup_cache_root)/cleanup-completed}"
    mkdir -p "$completed_dir"
    summary_file="$completed_dir/$(basename "$registry" .jsonl).summary.json"
    drive9_cleanup_write_summary "$registry" "deleted" 0 0 "$summary_file"
    rm -f "$registry" "${registry}.lock"
    echo "cleanup summary: $summary_file"
    return 0
  fi

  echo "cleanup incomplete; registry remains pending: $registry" >&2
  rm -f "${registry}.lock"
  return 1
}

drive9_cleanup_finish() {
  local run_rc="${1:-0}" registry="${DRIVE9_E2E_CLEANUP_REGISTRY:-}"

  if drive9_cleanup_is_enabled; then
    :
  else
    case "$?" in
      1) return 0 ;;
      *) return 1 ;;
    esac
  fi
  if [ -z "$registry" ] || [ ! -f "$registry" ]; then
    return 0
  fi
  drive9_cleanup_run_registry "$registry" "$run_rc"
}

drive9_cleanup_run_pending() {
  local pending_dir="${DRIVE9_E2E_CLEANUP_PENDING_DIR:-$(drive9_cleanup_cache_root)/cleanup-pending}"
  local registry rc=0 mode current_registry="${DRIVE9_E2E_CLEANUP_REGISTRY:-}"

  mode="$(drive9_cleanup_mode)"
  case "$mode" in
    0|off|false|"") return 0 ;;
    always|success) ;;
    *)
      echo "invalid DRIVE9_E2E_CLEANUP=$mode; expected 0, always, or success" >&2
      return 1
      ;;
  esac

  [ -d "$pending_dir" ] || return 0
  for registry in "$pending_dir"/drive9-e2e-*.jsonl; do
    [ -e "$registry" ] || continue
    [ "$registry" != "$current_registry" ] || continue
    if drive9_cleanup_lock_active "${registry}.lock"; then
      echo "cleanup pending skip active run: $registry"
      continue
    fi
    rm -f "${registry}.lock"
    echo "cleanup pending registry: $registry"
    DRIVE9_E2E_CLEANUP=always drive9_cleanup_run_registry "$registry" 0 || rc=1
  done
  return "$rc"
}
