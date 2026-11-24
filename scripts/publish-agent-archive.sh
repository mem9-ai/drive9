#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "error: $*" >&2
  exit 1
}

info() {
  echo "archive: $*"
}

warn() {
  echo "archive: warning: $*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

require_cmd git
require_cmd go
require_cmd jq
require_cmd sha256sum

publisher="${DRIVE9_PUBLISHER:-drive9}"
archive_root="${DRIVE9_ARCHIVE_ROOT:-/drive9}"
archive_root="${archive_root%/}"
latest_index="${archive_root}/latest.json"
archive_lock_path="${archive_root}/.publish-lock"
archive_staging_root="${archive_root}/.staging"
cli_targets="${DRIVE9_ARCHIVE_CLI_TARGETS:-linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64}"
legacy_agfs_module="github.com/c4pt0r/agfs/agfs-server"
legacy_agfs_version="${DRIVE9_ARCHIVE_LEGACY_AGFS_VERSION:-v0.0.0-20260410081414-678f51854d2a}"
work_base="${RUNNER_TEMP:-$(pwd)/.tmp}"
work_root=""
artifact_root=""
worktree_root=""
lock_state_dir=""
lock_lease_file=""
lock_token_file=""
lock_token_exp_file=""
lock_token_id=""
repository="${GITHUB_REPOSITORY:-mem9-ai/drive9}"
branch="${DRIVE9_ARCHIVE_BRANCH:-main}"
latest_index_branch="${DRIVE9_ARCHIVE_LATEST_BRANCH:-main}"
retain_commits="${DRIVE9_ARCHIVE_RETAIN_COMMITS:-20}"
incomplete_grace_seconds="${DRIVE9_ARCHIVE_INCOMPLETE_GRACE_SECONDS:-3600}"
quota_retry_attempts="${DRIVE9_ARCHIVE_QUOTA_RETRY_ATTEMPTS:-6}"
quota_retry_delay="${DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY:-10}"
lock_token_ttl_seconds="${DRIVE9_ARCHIVE_LOCK_TOKEN_TTL_SECONDS:-3600}"
lock_wait_seconds="${DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS:-1800}"
lock_poll_seconds="${DRIVE9_ARCHIVE_LOCK_POLL_SECONDS:-5}"
lock_break_owner="${DRIVE9_ARCHIVE_LOCK_BREAK_OWNER:-}"
lock_bootstrap_recovery_seconds=60
remote_auth_status=41
remote_missing_status=42
remote_conflict_status=43
active_worktree=""
active_remote_staging=""
pruned_any=0
deferred_latest_prune=0
lock_held=0
lock_cleanup_needed=0
mutation_in_flight=0
aborted_by_signal=0
mutating_child_pid=""

current_epoch() {
  if [ -n "${DRIVE9_ARCHIVE_NOW_FILE:-}" ] && [ -f "${DRIVE9_ARCHIVE_NOW_FILE}" ]; then
    local fixed
    fixed="$(cat "${DRIVE9_ARCHIVE_NOW_FILE}" 2>/dev/null || true)"
    if [[ "$fixed" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$fixed"
      return 0
    fi
  fi
  date -u '+%s'
}

wall_epoch() {
  date -u '+%s'
}

if [ -n "${GITHUB_RUN_ID:-}" ]; then
  lock_owner_prefix="$(printf 'gha-%s-%s' "${GITHUB_RUN_ID}" "${GITHUB_RUN_ATTEMPT:-1}" | tr -cd 'A-Za-z0-9_-' | cut -c1-32)"
else
  lock_owner_host="$(hostname -s 2>/dev/null | tr -cd 'A-Za-z0-9_-' | cut -c1-24 || true)"
  lock_owner_prefix="${lock_owner_host:-publisher}"
fi
lock_owner_id="${lock_owner_prefix}-$$-$(wall_epoch)-${RANDOM:-0}"

if [ -z "${DRIVE9_SERVER:-}" ]; then
  die "DRIVE9_SERVER is required"
fi
if [ -z "${DRIVE9_API_KEY:-}" ]; then
  die "DRIVE9_API_KEY is required"
fi
owner_api_key="${DRIVE9_API_KEY}"
if [ -z "$branch" ]; then
  die "DRIVE9_ARCHIVE_BRANCH must not be empty"
fi
if [ "${branch#-}" != "$branch" ] || ! git check-ref-format "refs/heads/${branch}" >/dev/null; then
  die "invalid DRIVE9_ARCHIVE_BRANCH: ${branch}"
fi
if [ -z "$latest_index_branch" ] || [ "${latest_index_branch#-}" != "$latest_index_branch" ] || ! git check-ref-format "refs/heads/${latest_index_branch}" >/dev/null; then
  die "invalid DRIVE9_ARCHIVE_LATEST_BRANCH: ${latest_index_branch}"
fi
if ! [[ "$retain_commits" =~ ^(0|[1-9][0-9]{0,5})$ ]]; then
  die "invalid DRIVE9_ARCHIVE_RETAIN_COMMITS: ${retain_commits}"
fi
if ! [[ "$incomplete_grace_seconds" =~ ^(0|[1-9][0-9]{0,6})$ ]]; then
  die "invalid DRIVE9_ARCHIVE_INCOMPLETE_GRACE_SECONDS: ${incomplete_grace_seconds}"
fi
if ! [[ "$quota_retry_attempts" =~ ^[1-9][0-9]{0,2}$ ]]; then
  die "invalid DRIVE9_ARCHIVE_QUOTA_RETRY_ATTEMPTS: ${quota_retry_attempts}"
fi
if ! [[ "$quota_retry_delay" =~ ^(0|[1-9][0-9]{0,3})$ ]]; then
  die "invalid DRIVE9_ARCHIVE_QUOTA_RETRY_DELAY: ${quota_retry_delay}"
fi
if ! [[ "$lock_token_ttl_seconds" =~ ^[1-9][0-9]{0,5}$ ]]; then
  die "invalid DRIVE9_ARCHIVE_LOCK_TOKEN_TTL_SECONDS: ${lock_token_ttl_seconds}"
fi
if ! [[ "$lock_wait_seconds" =~ ^(0|[1-9][0-9]{0,5})$ ]]; then
  die "invalid DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS: ${lock_wait_seconds}"
fi
if ! [[ "$lock_poll_seconds" =~ ^[1-9][0-9]{0,3}$ ]]; then
  die "invalid DRIVE9_ARCHIVE_LOCK_POLL_SECONDS: ${lock_poll_seconds}"
fi
if [ -n "$lock_break_owner" ]; then
  if ! [[ "$lock_break_owner" =~ ^[A-Za-z0-9_-]+$ ]] || [ "$lock_break_owner" = "init" ] || [[ "$lock_break_owner" == free-* ]]; then
    die "invalid DRIVE9_ARCHIVE_LOCK_BREAK_OWNER: ${lock_break_owner}"
  fi
fi

init_work_root() {
  mkdir -p "$work_base"
  work_root="$(mktemp -d "${work_base}/drive9-agent-archive.XXXXXX")"
  artifact_root="${work_root}/artifacts"
  worktree_root="${work_root}/worktrees"
  lock_state_dir="${work_root}/lock-state"
  lock_lease_file="${lock_state_dir}/lease-name"
  lock_token_file="${lock_state_dir}/scoped-token"
  lock_token_exp_file="${lock_state_dir}/scoped-token-exp"
  mkdir -p "$artifact_root" "$worktree_root" "$lock_state_dir"
  chmod 700 "$lock_state_dir"
}

remote_owner() {
  DRIVE9_API_KEY="$owner_api_key" "${publisher}" "$@"
}

remote_scoped() {
  local tok=""
  if [ -n "${lock_token_file:-}" ] && [ -f "$lock_token_file" ]; then
    tok="$(cat "$lock_token_file" 2>/dev/null || true)"
  fi
  if [ -z "$tok" ]; then
    die "missing active scoped archive token; refusing unfenced archive operation"
  fi
  DRIVE9_API_KEY="$tok" "${publisher}" "$@"
}

remote() {
  remote_scoped "$@"
}

remote_ref() {
  printf ':%s' "$1"
}

remote_cat() {
  local path=$1
  local out=$2
  local err="${out}.stderr"
  if remote fs cat "$(remote_ref "$path")" >"$out" 2>"$err"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$out" "$err"
    return "$remote_missing_status"
  fi
  cat "$err" >&2
  rm -f "$out" "$err"
  return 1
}

remote_ls() {
  local path=$1
  local out=$2
  local err="${out}.stderr"
  if remote fs ls "$(remote_ref "$path")" >"$out" 2>"$err"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$out" "$err"
    return "$remote_missing_status"
  fi
  cat "$err" >&2
  rm -f "$out" "$err"
  return 1
}

remote_manifest_exists() {
  local commit_path=$1
  local out="${work_root}/stat-manifest.out"
  local err="${out}.stderr"
  if remote fs stat "$(remote_ref "${commit_path}/manifest.json")" >"$out" 2>"$err"; then
    rm -f "$out" "$err"
    return 0
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$out" "$err"
    return "$remote_missing_status"
  fi
  cat "$err" >&2
  rm -f "$out" "$err"
  return 1
}

remote_stat_mtime() {
  local path=$1
  local out="${work_root}/stat-mtime.json"
  local err="${out}.stderr"
  local mtime
  if remote fs stat -o json "$(remote_ref "$path")" >"$out" 2>"$err"; then
    mtime="$(jq -r 'if type == "object" and (.mtime | type == "number") then (.mtime | floor) else "" end' "$out" 2>/dev/null || true)"
    rm -f "$out" "$err"
    if [[ "$mtime" =~ ^[0-9]+$ ]] && [ "$mtime" -gt 0 ]; then
      printf '%s\n' "$mtime"
      return 0
    fi
    return 2
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$out" "$err"
    return "$remote_missing_status"
  fi
  cat "$err" >&2
  rm -f "$out" "$err"
  return 1
}

run_mutating_remote() {
  local stdout_file=$1
  local stderr_file=$2
  shift 2
  local rc=0

  mutation_in_flight=1
  remote "$@" >"$stdout_file" 2>"$stderr_file" &
  mutating_child_pid=$!
  set +e
  wait "$mutating_child_pid"
  rc=$?
  set -e
  mutating_child_pid=""
  if [ "$aborted_by_signal" -eq 0 ]; then
    mutation_in_flight=0
  fi
  return "$rc"
}

remote_rm_rf() {
  local path=$1
  local err="${work_root}/rm.stderr"
  if run_mutating_remote /dev/null "$err" fs rm -r "$(remote_ref "$path")"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(HTTP 401|HTTP 403|invalid API key|token expired|token revoked|unauthorized)' "$err"; then
    cat "$err" >&2
    rm -f "$err"
    return "$remote_auth_status"
  fi
  cat "$err" >&2
  rm -f "$err"
  return 1
}

remote_cp_to_local() {
  local remote_path=$1
  local local_path=$2
  remote fs cp "$(remote_ref "$remote_path")" "$local_path" >/dev/null
}

remote_mkdir() {
  local path=$1
  local err="${work_root}/mkdir.stderr"
  if run_mutating_remote /dev/null "$err" fs mkdir "$(remote_ref "$path")"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(path already exists|already exists|HTTP 409)' "$err"; then
    rm -f "$err"
    return 0
  fi
  cat "$err" >&2
  rm -f "$err"
  return 1
}

remote_mv() {
  local old_path=$1
  local new_path=$2
  local err="${work_root}/mv.stderr"
  if run_mutating_remote /dev/null "$err" fs mv "$(remote_ref "$old_path")" "$(remote_ref "$new_path")"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$err"
    return "$remote_missing_status"
  fi
  if grep -qiE '(path already exists|already exists|HTTP 409)' "$err"; then
    rm -f "$err"
    return "$remote_conflict_status"
  fi
  if grep -qiE '(HTTP 401|HTTP 403|invalid API key|token expired|token revoked|unauthorized)' "$err"; then
    cat "$err" >&2
    rm -f "$err"
    return "$remote_auth_status"
  fi
  cat "$err" >&2
  rm -f "$err"
  return 1
}

lock_ls() {
  local path=$1
  local out=$2
  local err="${out}.stderr"
  if remote_owner fs ls "$(remote_ref "$path")" >"$out" 2>"$err"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$out" "$err"
    return "$remote_missing_status"
  fi
  cat "$err" >&2
  rm -f "$out" "$err"
  return 1
}

lock_stat_exists() {
  local path=$1
  local out="${lock_state_dir}/lock-stat.$$.out"
  local err="${out}.stderr"
  if remote_owner fs stat "$(remote_ref "$path")" >"$out" 2>"$err"; then
    rm -f "$out" "$err"
    return 0
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$out" "$err"
    return "$remote_missing_status"
  fi
  rm -f "$out" "$err"
  return 1
}

lock_stat_mtime() {
  local path=$1
  local out="${lock_state_dir}/lock-mtime.$$.json"
  local err="${out}.stderr"
  local mtime
  if remote_owner fs stat -o json "$(remote_ref "$path")" >"$out" 2>"$err"; then
    mtime="$(jq -r 'if type == "object" and (.mtime | type == "number") then (.mtime | floor) else "" end' "$out" 2>/dev/null || true)"
    rm -f "$out" "$err"
    if [[ "$mtime" =~ ^[0-9]+$ ]] && [ "$mtime" -gt 0 ]; then
      printf '%s\n' "$mtime"
      return 0
    fi
    return 2
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$out" "$err"
    return "$remote_missing_status"
  fi
  rm -f "$out" "$err"
  return 1
}

lock_mkdir_exclusive() {
  local path=$1
  local err="${lock_state_dir}/mkdir-exclusive.$$.stderr"
  if remote_owner fs mkdir "$(remote_ref "$path")" >/dev/null 2>"$err"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(path already exists|already exists|HTTP 409)' "$err"; then
    rm -f "$err"
    return "$remote_conflict_status"
  fi
  cat "$err" >&2
  rm -f "$err"
  return 1
}

lock_mv() {
  local old_path=$1
  local new_path=$2
  local err="${lock_state_dir}/mv.$$.stderr"
  if remote_owner fs mv "$(remote_ref "$old_path")" "$(remote_ref "$new_path")" >/dev/null 2>"$err"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(^|[^[:alnum:]])(not found|HTTP 404)([^[:alnum:]]|$)' "$err"; then
    rm -f "$err"
    return "$remote_missing_status"
  fi
  if grep -qiE '(path already exists|already exists|HTTP 409)' "$err"; then
    rm -f "$err"
    return "$remote_conflict_status"
  fi
  cat "$err" >&2
  rm -f "$err"
  return 1
}

issue_scoped_archive_token() {
  local out_file="${lock_state_dir}/issued-token.$$.json"
  local err_file="${out_file}.stderr"
  local subject="archive-${lock_owner_id}"
  subject="${subject:0:64}"
  local raw_tok tok_id exp_iso exp_epoch

  if ! remote_owner token issue \
    --subject "$subject" \
    --ttl "${lock_token_ttl_seconds}s" \
    --allow "${archive_root}:read,list,write,delete" \
    --json >"$out_file" 2>"$err_file"; then
    cat "$err_file" >&2
    rm -f "$out_file" "$err_file"
    return 1
  fi
  rm -f "$err_file"

  raw_tok="$(jq -r 'if type == "object" and (.token | type == "string") then .token else "" end' "$out_file" 2>/dev/null || true)"
  tok_id="$(jq -r 'if type == "object" and (.token_id | type == "string") then .token_id else "" end' "$out_file" 2>/dev/null || true)"
  exp_iso="$(jq -r 'if type == "object" and (.expires_at | type == "string") then .expires_at else "" end' "$out_file" 2>/dev/null || true)"
  rm -f "$out_file"

  if [ -z "$raw_tok" ] || [ -z "$tok_id" ] || ! [[ "$tok_id" =~ ^[A-Za-z0-9_-]+$ ]]; then
    warn "scoped token issue response missing token or valid token_id"
    return 1
  fi
  exp_epoch="$(jq -nr --arg ts "$exp_iso" '($ts | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601)' 2>/dev/null || true)"
  if ! [[ "$exp_epoch" =~ ^[0-9]+$ ]]; then
    warn "scoped token issue response missing valid expires_at"
    return 1
  fi

  printf '%s\t%s\t%s\n' "$tok_id" "$exp_epoch" "$raw_tok"
}

revoke_scoped_token_id() {
  local tok_id=$1
  local err="${lock_state_dir}/revoke-token.$$.stderr"
  [ -n "$tok_id" ] || return 0
  if remote_owner token revoke "$tok_id" >/dev/null 2>"$err"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE '(not found|already revoked|HTTP 404)' "$err"; then
    rm -f "$err"
    return 0
  fi
  rm -f "$err"
  return 1
}

commit_local_lock_state() {
  local lease_name=$1
  local tok_exp=$2
  local raw_tok=$3

  (
    umask 077
    printf '%s\n' "$raw_tok" >"${lock_token_file}.tmp"
    mv "${lock_token_file}.tmp" "$lock_token_file"
    printf '%s\n' "$tok_exp" >"${lock_token_exp_file}.tmp"
    mv "${lock_token_exp_file}.tmp" "$lock_token_exp_file"
    printf '%s\n' "$lease_name" >"${lock_lease_file}.tmp"
    mv "${lock_lease_file}.tmp" "$lock_lease_file"
  )
}

parse_lease_entry() {
  local name=$1
  local rest started rest2 tid owner
  case "$name" in
  lease.*)
    rest="${name#lease.}"
    started="${rest%%.*}"
    rest2="${rest#*.}"
    if ! [[ "$started" =~ ^[0-9]+$ ]] || [ -z "$rest2" ] || [ "$rest2" = "$rest" ]; then
      return 1
    fi
    if [[ "$rest2" == *.* ]]; then
      tid="${rest2%%.*}"
      owner="${rest2#*.}"
    else
      tid="-"
      owner="$rest2"
    fi
    [ -n "$tid" ] || tid="-"
    [ -n "$owner" ] || return 1
    printf '%s\t%s\t%s\n' "$started" "$tid" "$owner"
    return 0
    ;;
  *)
    return 1
    ;;
  esac
}

is_revocable_token_id() {
  local tok_id=$1
  [ -n "$tok_id" ] && [ "$tok_id" != "free" ] && [ "$tok_id" != "none" ] && [ "$tok_id" != "-" ]
}

verify_exclusive_lock_entry() {
  local expected_entry=$1
  local ls_out="${lock_state_dir}/verify-ls.$$.txt"
  local entry name found_expected=0 other_lease_count=0
  if ! lock_ls "$archive_lock_path" "$ls_out"; then
    return 1
  fi
  while IFS= read -r entry; do
    entry="${entry%$'\r'}"
    [ -n "$entry" ] || continue
    name="${entry%/}"
    if [ "$name" = "$expected_entry" ]; then
      found_expected=1
    elif [[ "$name" == lease.* ]]; then
      other_lease_count=$((other_lease_count + 1))
    fi
  done <"$ls_out"
  rm -f "$ls_out"
  [ "$found_expected" -eq 1 ] && [ "$other_lease_count" -eq 0 ]
}

acquire_archive_lock() {
  local wall_deadline rc ls_out entry name parsed started tid owner
  local active_found active_entry active_started active_tid active_owner active_desc
  local free_entry candidate_entry breaking_active initialized_found
  local mtime age issued new_tid new_tok_exp new_tok started_epoch lease_name mv_rc stat_rc mv_confirmed
  local logged_wait=0
  local free_lease="lease.0.free.${lock_owner_id}"

  mkdir -p "$lock_state_dir"
  rm -f "$lock_lease_file" "$lock_token_file" "$lock_token_exp_file"
  lock_token_id=""
  wall_deadline=$(( $(wall_epoch) + lock_wait_seconds ))
  ls_out="${lock_state_dir}/lock-ls.txt"

  # Permanent fail-closed lock container directory: never deleted on release so
  # DeleteEmptyDir can never race a newly committed child lease entry. Inside it
  # live one permanent `initialized` bootstrap marker and one circulating
  # generation-unique `lease.<started>.<tid>.<owner>` entry.
  #
  # Automatic lease-expiry takeover is intentionally disabled: drive9-server does
  # not carry a lock generation into mutation commits or drain already-admitted
  # requests on token revocation, so stealing an active/ambiguous lock automatically
  # could overlap an in-flight server mutation. Only an explicit operator break-glass
  # matching the exact confirmed-dead owner (`DRIVE9_ARCHIVE_LOCK_BREAK_OWNER`) may
  # take over a held lock.
  remote_owner fs mkdir "$(remote_ref "$archive_root")" >/dev/null 2>&1 || true
  remote_owner fs mkdir "$(remote_ref "$archive_lock_path")" >/dev/null 2>&1 || true

  while true; do
    active_found=0
    active_entry=""
    active_started=0
    active_tid=""
    active_owner=""
    active_desc=""
    free_entry=""
    candidate_entry=""
    breaking_active=0
    initialized_found=0

    if lock_ls "$archive_lock_path" "$ls_out"; then
      while IFS= read -r entry; do
        entry="${entry%$'\r'}"
        [ -n "$entry" ] || continue
        name="${entry%/}"
        if [ "$name" = "initialized" ]; then
          initialized_found=1
        elif parsed="$(parse_lease_entry "$name")"; then
          IFS=$'\t' read -r started tid owner <<<"$parsed"
          if [ "$started" -gt 0 ]; then
            active_found=1
            if [ -z "$active_entry" ]; then
              active_entry="$name"
              active_started="$started"
              active_tid="$tid"
              active_owner="$owner"
              active_desc="owner=${owner} started=${started}"
            fi
          elif [ -z "$free_entry" ]; then
            free_entry="$name"
          fi
        fi
      done <"$ls_out"
      rm -f "$ls_out"

      if [ "$active_found" -eq 1 ]; then
        if [ -n "$lock_break_owner" ] && [ "$active_owner" = "$lock_break_owner" ]; then
          # Operator-confirmed manual break-glass pinned to the exact active owner:
          # require confirmed revocation of the prior token BEFORE committing the rename.
          info "breaking confirmed-dead archive lock ${active_entry} (owner=${active_owner})"
          if is_revocable_token_id "$active_tid"; then
            if revoke_scoped_token_id "$active_tid"; then
              candidate_entry="$active_entry"
              breaking_active=1
            else
              warn "failed to revoke archive lock token ${active_tid} for owner ${active_owner}; retrying before break-glass takeover"
              active_desc="revoking token for owner=${active_owner}"
            fi
          else
            candidate_entry="$active_entry"
            breaking_active=1
          fi
        elif [ -n "$lock_break_owner" ]; then
          active_desc="owner=${active_owner} started=${active_started} (DRIVE9_ARCHIVE_LOCK_BREAK_OWNER=${lock_break_owner} did not match)"
        fi
      elif [ -n "$free_entry" ]; then
        candidate_entry="$free_entry"
      fi

      if [ -n "$candidate_entry" ]; then
        if issued="$(issue_scoped_archive_token)"; then
          IFS=$'\t' read -r new_tid new_tok_exp new_tok <<<"$issued"
          lock_token_id="$new_tid"
          lock_cleanup_needed=1
          started_epoch="$(current_epoch)"
          if [ "$started_epoch" -le 0 ]; then
            started_epoch=1
          fi
          lease_name="lease.${started_epoch}.${new_tid}.${lock_owner_id}"
          mv_rc=0
          mv_confirmed=0
          if lock_mv "${archive_lock_path}/${candidate_entry}" "${archive_lock_path}/${lease_name}"; then
            mv_confirmed=1
          else
            mv_rc=$?
            if [ "$mv_rc" -ne "$remote_missing_status" ] && [ "$mv_rc" -ne "$remote_conflict_status" ]; then
              # Ambiguous mv failure (e.g. 503 after server rename): confirm before revoking new_tid.
              while true; do
                stat_rc=0
                lock_stat_exists "${archive_lock_path}/${lease_name}" || stat_rc=$?
                if [ "$stat_rc" -eq 0 ]; then
                  mv_confirmed=1
                  break
                elif [ "$stat_rc" -eq "$remote_missing_status" ]; then
                  break
                fi
                if [ "$(wall_epoch)" -ge "$wall_deadline" ]; then
                  break
                fi
                sleep "$lock_poll_seconds"
              done
            fi
          fi

          if [ "$mv_confirmed" -eq 1 ]; then
            if verify_exclusive_lock_entry "$lease_name"; then
              commit_local_lock_state "$lease_name" "$new_tok_exp" "$new_tok"
              lock_held=1
              if [ "$breaking_active" -eq 1 ]; then
                info "acquired archive lock ${archive_lock_path} via manual break-glass (previous_owner=${active_owner}, owner=${lock_owner_id}, token_ttl=${lock_token_ttl_seconds}s)"
              else
                info "acquired archive lock ${archive_lock_path} (owner=${lock_owner_id}, token_ttl=${lock_token_ttl_seconds}s)"
              fi
              return 0
            fi
            # Relinquish back to free sentinel rather than deleting the circulating token.
            lock_mv "${archive_lock_path}/${lease_name}" "${archive_lock_path}/${free_lease}" >/dev/null 2>&1 || true
          fi
          revoke_scoped_token_id "$new_tid" >/dev/null 2>&1 || true
          lock_token_id=""
        fi
        if [ "$breaking_active" -eq 1 ]; then
          active_desc="break-glass lock contention"
        else
          active_desc="lock acquisition contention"
        fi
      elif [ "$active_found" -eq 0 ] && [ "$initialized_found" -eq 0 ]; then
        # One-time bootstrap: race atomic mkdir on permanent `initialized` marker;
        # the single winner creates the initial circulating sentinel `lease.0.none.init`.
        if lock_mkdir_exclusive "${archive_lock_path}/initialized"; then
          if ! lock_mkdir_exclusive "${archive_lock_path}/lease.0.none.init"; then
            rc=$?
            if [ "$rc" -ne "$remote_conflict_status" ]; then
              die "failed to initialize archive lock token under ${archive_lock_path}"
            fi
          fi
          continue
        else
          rc=$?
          if [ "$rc" -ne "$remote_conflict_status" ]; then
            die "failed to bootstrap archive lock ${archive_lock_path}"
          fi
        fi
        active_desc="initializing"
      elif [ "$active_found" -eq 0 ] && [ "$initialized_found" -eq 1 ]; then
        # Crash recovery only if the initial bootstrapper died between creating
        # `initialized` and creating `lease.0.none.init`.
        if mtime="$(lock_stat_mtime "${archive_lock_path}/initialized")"; then
          age=$(( $(wall_epoch) - mtime ))
          if [ "$age" -ge "$lock_bootstrap_recovery_seconds" ]; then
            info "recovering incomplete archive lock bootstrap under ${archive_lock_path} (age=${age}s)"
            if ! lock_mkdir_exclusive "${archive_lock_path}/lease.0.none.init"; then
              rc=$?
              if [ "$rc" -ne "$remote_conflict_status" ]; then
                die "failed to recover archive lock token under ${archive_lock_path}"
              fi
            fi
            continue
          fi
        fi
        active_desc="initializing"
      fi
    else
      rc=$?
      if [ "$rc" -eq "$remote_missing_status" ]; then
        remote_owner fs mkdir "$(remote_ref "$archive_root")" >/dev/null 2>&1 || true
        remote_owner fs mkdir "$(remote_ref "$archive_lock_path")" >/dev/null 2>&1 || true
        active_desc="initializing"
      else
        die "failed to inspect archive lock ${archive_lock_path}"
      fi
    fi

    # Route every unsuccessful attempt through one unified wall-clock deadline + backoff block
    # so frozen test clocks (DRIVE9_ARCHIVE_NOW_FILE) still honor DRIVE9_ARCHIVE_LOCK_WAIT_SECONDS.
    if [ "$(wall_epoch)" -ge "$wall_deadline" ]; then
      if [ "$active_found" -eq 1 ] && [ -n "$active_owner" ]; then
        die "archive lock ${archive_lock_path} is held by another active publisher (${active_desc}); timed out after ${lock_wait_seconds}s. Confirm the prior run and all in-flight requests have finished, then re-run with DRIVE9_ARCHIVE_LOCK_BREAK_OWNER=${active_owner} (workflow_dispatch input: break_lock_owner) to break a stuck lock"
      fi
      die "archive lock ${archive_lock_path} is held by another active publisher (${active_desc:-busy}); timed out after ${lock_wait_seconds}s"
    fi

    if [ "$logged_wait" -eq 0 ]; then
      info "waiting for archive lock ${archive_lock_path} (${active_desc:-active})"
      logged_wait=1
    fi
    sleep "$lock_poll_seconds"
  done
}

check_archive_lock_held() {
  local cur_lease cur_tok_exp now

  if [ "$lock_held" -ne 1 ]; then
    return 1
  fi

  cur_lease="$(cat "$lock_lease_file" 2>/dev/null || true)"
  cur_tok_exp="$(cat "$lock_token_exp_file" 2>/dev/null || true)"
  if [ -z "$cur_lease" ] || ! [[ "$cur_tok_exp" =~ ^[0-9]+$ ]]; then
    lock_held=0
    return 1
  fi

  now="$(current_epoch)"
  if [ "$now" -ge "$cur_tok_exp" ]; then
    lock_held=0
    return 1
  fi

  if lock_stat_exists "${archive_lock_path}/${cur_lease}"; then
    return 0
  fi

  lock_held=0
  return 1
}

assert_archive_lock_held() {
  if ! check_archive_lock_held; then
    die "lost archive lock ownership on ${archive_lock_path}; aborting to prevent concurrent archive corruption"
  fi
}

release_archive_lock() {
  local cur_lease ls_out entry name parsed started tid owner
  local free_lease="lease.0.free.${lock_owner_id}"

  # Track cleanup eligibility separately from lock_held so a transient stat failure
  # still releases surviving active lease entries owned by this exact lock_owner_id.
  # Release moves our active lease to a unique free sentinel (lease.0.free.<owner>)
  # instead of deleting it, preserving the single circulating lock token invariant.
  # If interrupted while a remote mutation was still in flight, retain the lock
  # fail-closed so no subsequent publisher can overlap the already-admitted request.
  if [ "$lock_cleanup_needed" -eq 1 ]; then
    cur_lease="$(cat "$lock_lease_file" 2>/dev/null || true)"
    if [ "$mutation_in_flight" -eq 1 ]; then
      warn "interrupted while remote mutation was in flight; retaining archive lock ${cur_lease:-unknown} (owner=${lock_owner_id}) fail-closed until prior request completes"
      if [ -n "${lock_token_id:-}" ]; then
        revoke_scoped_token_id "$lock_token_id" >/dev/null 2>&1 || true
      fi
    else
      if [ -n "$cur_lease" ] && [ "$cur_lease" != "$free_lease" ]; then
        lock_mv "${archive_lock_path}/${cur_lease}" "${archive_lock_path}/${free_lease}" >/dev/null 2>&1 || true
      fi
      ls_out="${lock_state_dir}/release-ls.txt"
      if lock_ls "$archive_lock_path" "$ls_out" >/dev/null 2>&1; then
        while IFS= read -r entry; do
          entry="${entry%$'\r'}"
          [ -n "$entry" ] || continue
          name="${entry%/}"
          if parsed="$(parse_lease_entry "$name")"; then
            IFS=$'\t' read -r started tid owner <<<"$parsed"
            if [ "$owner" = "$lock_owner_id" ] && [ "$started" -gt 0 ]; then
              lock_mv "${archive_lock_path}/${name}" "${archive_lock_path}/${free_lease}" >/dev/null 2>&1 || true
            fi
          fi
        done <"$ls_out"
        rm -f "$ls_out"
      fi

      if [ -n "${lock_token_id:-}" ]; then
        revoke_scoped_token_id "$lock_token_id" >/dev/null 2>&1 || true
      fi
    fi

    lock_held=0
    lock_cleanup_needed=0
    lock_token_id=""
  fi

  if [ -n "${lock_state_dir:-}" ]; then
    rm -rf "$lock_state_dir" 2>/dev/null || true
  fi
}

remote_upload() {
  local local_path=$1
  local remote_path=$2
  local err="${work_root}/upload.stderr"
  local max_attempts=1
  local attempt=1
  if [ "$pruned_any" -eq 1 ]; then
    max_attempts="$quota_retry_attempts"
  fi

  info "upload ${remote_path}"
  while true; do
    assert_archive_lock_held
    if run_mutating_remote /dev/null "$err" fs cp "$local_path" "$(remote_ref "$remote_path")"; then
      rm -f "$err"
      return 0
    fi
    if [ "$pruned_any" -eq 1 ] && [ "$attempt" -lt "$max_attempts" ] && grep -qi 'tenant storage quota exceeded' "$err"; then
      info "storage quota still draining after prune; waiting ${quota_retry_delay}s before retrying ${remote_path} (attempt ${attempt}/${max_attempts})"
      rm -f "$err"
      sleep "$quota_retry_delay"
      attempt=$((attempt + 1))
      continue
    fi
    cat "$err" >&2
    rm -f "$err"
    return 1
  done
}

fetch_archive_branch() {
  git fetch --quiet origin "refs/heads/${branch}:refs/remotes/origin/${branch}"
  if [ "$branch" != "$latest_index_branch" ]; then
    git fetch --quiet origin "refs/heads/${latest_index_branch}:refs/remotes/origin/${latest_index_branch}" 2>/dev/null || true
  fi
}

archive_branch_tip() {
  fetch_archive_branch
  git rev-parse "refs/remotes/origin/${branch}^{commit}"
}

checksums_object() {
  local checksums_file=$1
  jq -Rn '
    reduce inputs as $line ({};
      if ($line | length) == 0 then .
      else
        ($line | capture("^(?<sha>[0-9a-f]+)  (?<path>.+)$")) as $m
        | . + {($m.path): $m.sha}
      end
    )
  ' <"$checksums_file"
}

validate_remote_commit_manifest() {
  local sha=$1
  local manifest=$2

  if ! jq -e --arg sha "$sha" '
    def checksum: type == "string" and test("^[0-9a-f]{64}$");
    def target_component: type == "string" and test("^[A-Za-z0-9][A-Za-z0-9_.-]*$");
    def binary_entry:
      (.goos | target_component)
      and (.goarch | target_component)
      and (.path == ("bin/drive9-" + .goos + "-" + .goarch + (if .goos == "windows" then ".exe" else "" end)));

    .commit_sha == $sha
    and .source_archive == "source.tar.gz"
    and (.checksums | type == "object")
    and ((.checksums | length) > 0)
    and (.binaries | type == "array")
    and ((.binaries | length) > 0)
    and all(.binaries[]; binary_entry)
    and (([.binaries[].path] | unique | length) == (.binaries | length))
    and (. as $manifest
      | (["source.tar.gz"] + [.binaries[].path]) as $allowed
      | all($allowed[]; ($manifest.checksums[.] | checksum))
      and (($manifest.checksums | keys | sort) == ($allowed | sort)))
  ' "$manifest" >/dev/null; then
    die "remote manifest exists for ${sha} but is malformed"
  fi
}

binary_metadata() {
  local artifact_dir=$1
  local jsonl="${artifact_dir}/binaries.jsonl"
  : >"$jsonl"

  while IFS= read -r rel; do
    local name os arch_ext arch
    name="${rel#bin/drive9-}"
    os="${name%%-*}"
    arch_ext="${name#*-}"
    arch="${arch_ext%.exe}"
    jq -n \
      --arg path "$rel" \
      --arg goos "$os" \
      --arg goarch "$arch" \
      '{path: $path, goos: $goos, goarch: $goarch}' >>"$jsonl"
  done < <(cd "$artifact_dir" && find bin -type f -name 'drive9-*' | sort)

  jq -s '.' "$jsonl"
}

write_commit_manifest() {
  local sha=$1
  local artifact_dir=$2
  local short_sha published_at checksums_json binaries_json
  short_sha="${sha:0:7}"
  published_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  checksums_json="$(checksums_object "${artifact_dir}/checksums.txt")"
  binaries_json="$(binary_metadata "$artifact_dir")"

  jq -n \
    --arg repository "$repository" \
    --arg branch "$branch" \
    --arg sha "$sha" \
    --arg short_sha "$short_sha" \
    --arg published_at "$published_at" \
    --argjson checksums "$checksums_json" \
    --argjson binaries "$binaries_json" \
    '{
      schema_version: 1,
      repository: $repository,
      branch: $branch,
      commit_sha: $sha,
      short_sha: $short_sha,
      published_at: $published_at,
      source_archive: "source.tar.gz",
      checksums: $checksums,
      binaries: $binaries
    }' >"${artifact_dir}/manifest.json"
}

prepare_agfs_dependency() {
  local worktree=$1
  local parent target marker module_dir
  parent="$(dirname "$worktree")"
  target="${parent}/agfs/agfs-server"
  marker="${target}/.drive9-agfs-version"
  if ! grep -q '../agfs' "${worktree}/go.mod"; then
    return
  fi
  if [ -f "$marker" ] && [ "$(cat "$marker")" = "$legacy_agfs_version" ]; then
    return
  fi
  info "checkout pinned agfs dependency ${legacy_agfs_version} for legacy local replace"
  rm -rf "${parent}/agfs"
  module_dir="$(go mod download -json "${legacy_agfs_module}@${legacy_agfs_version}" | jq -r '.Dir')"
  if [ -z "$module_dir" ] || [ "$module_dir" = "null" ]; then
    die "failed to resolve ${legacy_agfs_module}@${legacy_agfs_version}"
  fi
  mkdir -p "$target"
  cp -R "${module_dir}/." "$target/"
  chmod -R u+w "$target"
  printf '%s\n' "$legacy_agfs_version" >"$marker"
}

cleanup_worktree() {
  local worktree=$1
  git worktree remove --force "$worktree" >/dev/null 2>&1 || true
  rm -rf "$worktree"
  git worktree prune >/dev/null 2>&1 || true
}

cleanup_active_worktree() {
  if [ -n "${active_worktree:-}" ]; then
    cleanup_worktree "$active_worktree"
  fi
}

build_artifacts() {
  local sha=$1
  local artifact_dir=$2
  local worktree="${worktree_root}/${sha}"
  local build_dist="${artifact_dir}/dist-build"
  local short_sha="${sha:0:7}"

  rm -rf "$artifact_dir"
  cleanup_worktree "$worktree"
  mkdir -p "$artifact_dir" "$worktree_root"

  info "create source archive for ${sha}"
  git archive --format=tar.gz --prefix="drive9-${sha}/" -o "${artifact_dir}/source.tar.gz" "$sha"

  info "create worktree for ${sha}"
  active_worktree="$worktree"
  git worktree add --detach "$worktree" "$sha" >/dev/null

  prepare_agfs_dependency "$worktree"

  info "build CLI binaries for ${sha}"
  make -C "$worktree" build-cli-release DIST_DIR="$build_dist" VERSION="$short_sha" CLI_TARGETS="$cli_targets"

  mkdir -p "${artifact_dir}/bin"
  cp "${build_dist}"/drive9-* "${artifact_dir}/bin/"

  (
    cd "$artifact_dir"
    sha256sum source.tar.gz bin/drive9-* >checksums.txt
  )

  write_commit_manifest "$sha" "$artifact_dir"

  cleanup_worktree "$worktree"
  active_worktree=""
}

verify_remote_artifacts() {
  local commit_path=$1
  local checksums_file=$2
  local verify_dir="${work_root}/verify"
  if [ ! -s "$checksums_file" ]; then
    die "empty checksum list for ${commit_path}"
  fi
  rm -rf "$verify_dir"
  mkdir -p "$verify_dir"

  while read -r expected rel; do
    local out actual
    out="${verify_dir}/${rel}"
    mkdir -p "$(dirname "$out")"
    remote_cp_to_local "${commit_path}/${rel}" "$out"
    actual="$(sha256sum "$out" | awk '{print $1}')"
    if [ "$actual" != "$expected" ]; then
      die "remote checksum mismatch for ${commit_path}/${rel}"
    fi
  done <"$checksums_file"
}

remote_commit_state() {
  local sha=$1
  local artifact_dir=$2
  local commit_path="${archive_root}/commits/${sha}"
  local remote_manifest="${work_root}/remote-manifest-${sha}.json"
  local checksums_json cat_status

  if remote_cat "${commit_path}/manifest.json" "$remote_manifest"; then
    :
  else
    cat_status=$?
    if [ "$cat_status" -eq "$remote_missing_status" ]; then
      echo "absent"
      return
    fi
    die "failed to read remote manifest for ${sha}"
  fi

  checksums_json="$(checksums_object "${artifact_dir}/checksums.txt")"
  if ! jq -e --arg sha "$sha" --argjson checksums "$checksums_json" \
    '.commit_sha == $sha and .checksums == $checksums' "$remote_manifest" >/dev/null; then
    die "remote manifest exists for ${sha} but does not match local artifact contract"
  fi

  verify_remote_artifacts "$commit_path" "${artifact_dir}/checksums.txt"
  echo "complete"
}

remote_commit_self_state() {
  local sha=$1
  local commit_path="${archive_root}/commits/${sha}"
  local remote_manifest="${work_root}/remote-manifest-${sha}.json"
  local remote_checksums="${work_root}/remote-checksums-${sha}.txt"
  local cat_status

  if remote_cat "${commit_path}/manifest.json" "$remote_manifest"; then
    :
  else
    cat_status=$?
    if [ "$cat_status" -eq "$remote_missing_status" ]; then
      echo "absent"
      return
    fi
    die "failed to read remote manifest for ${sha}"
  fi

  validate_remote_commit_manifest "$sha" "$remote_manifest"
  jq -r '.checksums | to_entries[] | [.value, .key] | @tsv' "$remote_manifest" >"$remote_checksums"
  verify_remote_artifacts "$commit_path" "$remote_checksums"
  echo "complete"
}

publish_commit() {
  local sha=$1
  local artifact_dir="${artifact_root}/${sha}"
  local commit_path="${archive_root}/commits/${sha}"
  local staging_path="${archive_staging_root}/${sha}.${lock_owner_id}"
  local state mv_rc

  state="$(remote_commit_self_state "$sha")"
  if [ "$state" = "complete" ]; then
    info "commit ${sha} already complete"
    return
  fi

  build_artifacts "$sha" "$artifact_dir"
  state="$(remote_commit_state "$sha" "$artifact_dir")"
  if [ "$state" = "complete" ]; then
    info "commit ${sha} already complete"
    return
  fi

  assert_archive_lock_held

  remote_mkdir "$archive_root" || true
  remote_mkdir "${archive_root}/commits" || true
  remote_mkdir "$archive_staging_root" || true
  remote_mkdir "$staging_path" || true
  remote_mkdir "${staging_path}/bin" || true

  active_remote_staging="$staging_path"
  remote_upload "${artifact_dir}/source.tar.gz" "${staging_path}/source.tar.gz"
  while IFS= read -r binary; do
    remote_upload "${artifact_dir}/${binary}" "${staging_path}/${binary}"
  done < <(cd "$artifact_dir" && find bin -type f -name 'drive9-*' | sort)
  remote_upload "${artifact_dir}/checksums.txt" "${staging_path}/checksums.txt"
  verify_remote_artifacts "$staging_path" "${artifact_dir}/checksums.txt"
  remote_upload "${artifact_dir}/manifest.json" "${staging_path}/manifest.json"

  assert_archive_lock_held
  info "promote ${staging_path} -> ${commit_path}"
  if remote_mv "$staging_path" "$commit_path"; then
    active_remote_staging=""
    return 0
  else
    mv_rc=$?
    if [ "$mv_rc" -eq "$remote_conflict_status" ]; then
      remote_rm_rf "$staging_path" >/dev/null 2>&1 || true
      active_remote_staging=""
      state="$(remote_commit_state "$sha" "$artifact_dir")"
      if [ "$state" = "complete" ]; then
        info "commit ${sha} already complete"
        return 0
      fi
      die "cannot promote ${staging_path} to ${commit_path}: target already exists and is incomplete"
    fi
    die "failed to promote ${staging_path} to ${commit_path}"
  fi
}

publish_latest_index() {
  local sha=$1
  local commit_path="${archive_root}/commits/${sha}"
  local remote_manifest="${work_root}/latest-source-manifest-${sha}.json"
  local latest_manifest="${work_root}/latest.json"
  local branch_tip

  if [ "$branch" != "$latest_index_branch" ]; then
    info "skip latest index ${latest_index}; branch ${branch} does not own ${latest_index} (expected ${latest_index_branch})"
    return
  fi

  branch_tip="$(archive_branch_tip)"
  if [ "$sha" != "$branch_tip" ]; then
    info "skip latest index ${latest_index}; ${sha} is not current origin/${branch} tip ${branch_tip}"
    return
  fi

  remote_cat "${commit_path}/manifest.json" "$remote_manifest" >/dev/null
  jq -n \
    --arg commit_path "$commit_path" \
    --arg manifest_path "${commit_path}/manifest.json" \
    --arg source_archive_path "${commit_path}/source.tar.gz" \
    --arg checksums_path "${commit_path}/checksums.txt" \
    --slurpfile manifest "$remote_manifest" \
    '($manifest[0]) as $m
    | {
        schema_version: 1,
        repository: $m.repository,
        branch: $m.branch,
        commit_sha: $m.commit_sha,
        short_sha: $m.short_sha,
        published_at: $m.published_at,
        commit_path: $commit_path,
        manifest_path: $manifest_path,
        source_archive_path: $source_archive_path,
        checksums_path: $checksums_path,
        binaries: [
          $m.binaries[]
          | . + {path: ($commit_path + "/" + .path)}
        ]
      }' >"$latest_manifest"

  info "upload latest index ${latest_index} -> ${commit_path}"
  remote_upload "$latest_manifest" "$latest_index"
}

remote_latest_commit_sha() {
  local latest_file="${work_root}/remote-latest.json"
  local cat_status sha
  if remote_cat "$latest_index" "$latest_file"; then
    sha="$(jq -r 'if type == "object" and (.commit_sha | type == "string") then .commit_sha else "" end' "$latest_file" 2>/dev/null || true)"
    rm -f "$latest_file"
    if [[ "$sha" =~ ^[0-9a-f]{40}$ ]]; then
      printf '%s\n' "$sha"
    else
      warn "remote latest index ${latest_index} is malformed; proceeding without latest protection"
    fi
    return 0
  else
    cat_status=$?
    if [ "$cat_status" -eq "$remote_missing_status" ]; then
      return 0
    fi
    return 1
  fi
}

remote_commit_timestamp() {
  local sha=$1
  local commit_path="${archive_root}/commits/${sha}"
  local manifest_file="${work_root}/rank-manifest-${sha}.json"
  local cat_status ts mtime

  if remote_cat "${commit_path}/manifest.json" "$manifest_file"; then
    ts="$(jq -r 'if type == "object" and (.published_at | type == "string") then (.published_at | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) else "" end' "$manifest_file" 2>/dev/null || true)"
    rm -f "$manifest_file"
    if [[ "$ts" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$ts"
      return 0
    fi
    if mtime="$(remote_stat_mtime "${commit_path}/manifest.json")"; then
      printf '%s\n' "$mtime"
      return 0
    fi
    return 2
  else
    cat_status=$?
    if [ "$cat_status" -eq "$remote_missing_status" ]; then
      if mtime="$(remote_stat_mtime "$commit_path")"; then
        printf '%s\n' "$mtime"
        return 0
      fi
      return 2
    fi
    return 1
  fi
}

incomplete_commit_eligible_for_prune() {
  local sha=$1
  local now_epoch=$2
  local mtime mtime_status age

  if [ "$incomplete_grace_seconds" -eq 0 ]; then
    return 0
  fi

  if mtime="$(remote_stat_mtime "${archive_root}/commits/${sha}")"; then
    age=$((now_epoch - mtime))
    if [ "$age" -ge "$incomplete_grace_seconds" ]; then
      return 0
    fi
    info "preserve recent incomplete commit ${sha} (age=${age}s < grace=${incomplete_grace_seconds}s)"
    return 1
  else
    mtime_status=$?
    if [ "$mtime_status" -eq "$remote_missing_status" ]; then
      return 1
    fi
    warn "cannot verify age of incomplete commit ${sha}; preserving it during this prune pass"
    return 1
  fi
}

sort_remote_commits_newest_first() {
  local remote_shas_file=$1
  local sorted_out=$2
  local unknown_age_out=$3
  local branch_revlist="${work_root}/branch-revlist.txt"
  local all_commits_ts="${work_root}/all-commits-ts.txt"
  local ranked_file="${work_root}/remote-commits-ranked.tsv"
  local unranked_file="${work_root}/remote-commits-unranked.txt"

  git rev-list "refs/remotes/origin/${branch}" >"$branch_revlist"
  git log --all --format='%H %ct' >"$all_commits_ts"
  : >"$ranked_file"
  : >"$unranked_file"
  : >"$unknown_age_out"

  # Rank every commit known to git globally by committer timestamp descending so
  # dispatching a stale branch never prefers ancient ancestors over newer main
  # archives; use origin/${branch} ancestry position only to break timestamp ties.
  awk -v f1="$branch_revlist" -v f2="$all_commits_ts" -v unranked="$unranked_file" '
    FILENAME == f1 {
      branch_rank[$1] = FNR
      next
    }
    FILENAME == f2 {
      if (!($1 in ts)) {
        ts[$1] = $2
      }
      next
    }
    {
      if ($1 in ts) {
        br = ($1 in branch_rank) ? branch_rank[$1] : 999999999
        printf "0\t%012d\t%09d\t%s\n", (9999999999 - ts[$1]), br, $1
      } else {
        print $1 > unranked
      }
    }
  ' "$branch_revlist" "$all_commits_ts" "$remote_shas_file" >"$ranked_file"

  if [ -s "$unranked_file" ]; then
    local sha ct inv_ct
    while IFS= read -r sha; do
      [ -n "$sha" ] || continue
      ct="$(git show -s --format=%ct "${sha}^{commit}" 2>/dev/null || true)"
      if [[ "$ct" =~ ^[0-9]+$ ]]; then
        inv_ct=$((9999999999 - ct))
        printf '0\t%012d\t999999999\t%s\n' "$inv_ct" "$sha" >>"$ranked_file"
      elif ct="$(remote_commit_timestamp "$sha")"; then
        inv_ct=$((9999999999 - ct))
        printf '0\t%012d\t999999999\t%s\n' "$inv_ct" "$sha" >>"$ranked_file"
      else
        printf '%s\n' "$sha" >>"$unknown_age_out"
        printf '1\t999999999999\t999999999\t%s\n' "$sha" >>"$ranked_file"
      fi
    done <"$unranked_file"
  fi

  LC_ALL=C sort -t $'\t' -k1,1n -k2,2n -k3,3n -k4,4 "$ranked_file" | cut -f4 >"$sorted_out"
}

is_sha_in_list() {
  local needle=$1
  shift
  local item
  for item in "$@"; do
    if [ "$item" = "$needle" ]; then
      return 0
    fi
  done
  return 1
}

is_sha_in_file() {
  local needle=$1
  local file=$2
  [ -s "$file" ] && grep -Fxq "$needle" "$file"
}

prune_lock_fence() {
  local lock_fatal=$1
  if check_archive_lock_held; then
    return 0
  fi
  if [ "$lock_fatal" -eq 1 ]; then
    die "lost archive lock ownership on ${archive_lock_path}; aborting prune"
  fi
  warn "lost archive lock ownership on ${archive_lock_path}; aborting post-publish prune"
  return 1
}

prune_remote_staging() {
  local lock_fatal=$1
  local ls_out="${work_root}/remote-staging-ls.txt"
  local ls_status entry name mtime mtime_status age now_epoch rm_status

  prune_lock_fence "$lock_fatal" || return 0

  if remote_ls "$archive_staging_root" "$ls_out"; then
    :
  else
    ls_status=$?
    if [ "$ls_status" -eq "$remote_missing_status" ]; then
      return 0
    fi
    warn "failed to list ${archive_staging_root}; skipping orphaned staging cleanup"
    return 0
  fi

  now_epoch="$(current_epoch)"
  while IFS= read -r entry; do
    entry="${entry%$'\r'}"
    case "$entry" in
    */)
      name="${entry%/}"
      if ! [[ "$name" =~ ^[0-9a-f]{40}\.[A-Za-z0-9_-]+$ ]]; then
        continue
      fi
      if [ "$incomplete_grace_seconds" -gt 0 ]; then
        if mtime="$(remote_stat_mtime "${archive_staging_root}/${name}")"; then
          age=$((now_epoch - mtime))
          if [ "$age" -lt "$incomplete_grace_seconds" ]; then
            continue
          fi
        else
          mtime_status=$?
          if [ "$mtime_status" -ne "$remote_missing_status" ]; then
            warn "cannot verify age of orphaned staging ${name}; preserving it"
          fi
          continue
        fi
      fi
      prune_lock_fence "$lock_fatal" || return 0
      info "remove ${archive_staging_root}/${name} (orphaned staging)"
      if remote_rm_rf "${archive_staging_root}/${name}"; then
        pruned_any=1
      else
        rm_status=$?
        if [ "$rm_status" -eq "$remote_auth_status" ]; then
          lock_held=0
          if [ "$lock_fatal" -eq 1 ]; then
            die "remote server rejected cleanup of ${archive_staging_root}/${name} (expired/revoked scoped token)"
          fi
          warn "remote server rejected post-publish cleanup of ${archive_staging_root}/${name}; stopping"
          return 0
        fi
        warn "failed to remove orphaned staging ${archive_staging_root}/${name}; continuing"
      fi
      ;;
    esac
  done <"$ls_out"
  rm -f "$ls_out"
}

prune_remote_commits() {
  local keep_budget=$1
  local lock_fatal=$2
  shift 2
  local protected_shas=("$@")
  local ls_out="${work_root}/remote-commits-ls.txt"
  local remote_shas_file="${work_root}/remote-commits-shas.txt"
  local sorted_shas_file="${work_root}/remote-commits-sorted.txt"
  local unknown_age_file="${work_root}/remote-commits-unknown-age.txt"
  local incomplete_target_file="${work_root}/remote-commits-prune-incomplete-target.txt"
  local incomplete_file="${work_root}/remote-commits-prune-incomplete.txt"
  local expired_file="${work_root}/remote-commits-prune-expired.txt"
  local ls_status latest_sha kept_complete sha entry name stat_status now_epoch
  local incomplete_target_count=0
  local incomplete_count=0
  local expired_count=0
  local pruned_ok=0
  local prune_failures=0

  deferred_latest_prune=0

  if [ "$keep_budget" -eq 0 ]; then
    info "retention pruning disabled (DRIVE9_ARCHIVE_RETAIN_COMMITS=0)"
    return 0
  fi

  prune_lock_fence "$lock_fatal" || return 0
  prune_remote_staging "$lock_fatal"

  if remote_ls "${archive_root}/commits" "$ls_out"; then
    :
  else
    ls_status=$?
    if [ "$ls_status" -eq "$remote_missing_status" ]; then
      return 0
    fi
    warn "failed to list ${archive_root}/commits; skipping retention prune"
    return 0
  fi

  : >"$remote_shas_file"
  while IFS= read -r entry; do
    entry="${entry%$'\r'}"
    case "$entry" in
    */)
      name="${entry%/}"
      if [[ "$name" =~ ^[0-9a-f]{40}$ ]]; then
        printf '%s\n' "$name" >>"$remote_shas_file"
      fi
      ;;
    esac
  done <"$ls_out"

  if [ ! -s "$remote_shas_file" ]; then
    return 0
  fi

  if latest_sha="$(remote_latest_commit_sha)"; then
    :
  else
    warn "failed to read remote latest index ${latest_index}; skipping retention prune"
    return 0
  fi

  sort_remote_commits_newest_first "$remote_shas_file" "$sorted_shas_file" "$unknown_age_file"
  now_epoch="$(current_epoch)"

  # Reserve one retention slot per target commit being published so pre-publish
  # pruning leaves room for new commits without over-pruning idempotent reruns.
  kept_complete="${#protected_shas[@]}"
  : >"$incomplete_target_file"
  : >"$incomplete_file"
  : >"$expired_file"

  while IFS= read -r sha; do
    [ -n "$sha" ] || continue

    if [ "${#protected_shas[@]}" -gt 0 ] && is_sha_in_list "$sha" "${protected_shas[@]}"; then
      if remote_manifest_exists "${archive_root}/commits/${sha}"; then
        :
      else
        stat_status=$?
        if [ "$stat_status" -eq "$remote_missing_status" ]; then
          if [ -z "$latest_sha" ] || [ "$sha" != "$latest_sha" ]; then
            printf '%s\n' "$sha" >>"$incomplete_target_file"
            incomplete_target_count=$((incomplete_target_count + 1))
          fi
        else
          warn "failed to inspect remote manifest for target ${sha}; preserving it before publish"
        fi
      fi
      continue
    fi

    if [ -n "$latest_sha" ] && [ "$sha" = "$latest_sha" ]; then
      if [ "$kept_complete" -lt "$keep_budget" ]; then
        if remote_manifest_exists "${archive_root}/commits/${sha}"; then
          kept_complete=$((kept_complete + 1))
        else
          stat_status=$?
          if [ "$stat_status" -eq "$remote_missing_status" ]; then
            deferred_latest_prune=1
          else
            warn "failed to inspect remote manifest for latest ${sha}; preserving it during this prune pass"
          fi
        fi
      else
        deferred_latest_prune=1
      fi
      continue
    fi

    if is_sha_in_file "$sha" "$unknown_age_file"; then
      if remote_manifest_exists "${archive_root}/commits/${sha}"; then
        warn "cannot determine age for commit ${sha}; preserving it during this prune pass"
      else
        stat_status=$?
        if [ "$stat_status" -eq "$remote_missing_status" ]; then
          if incomplete_commit_eligible_for_prune "$sha" "$now_epoch"; then
            printf '%s\n' "$sha" >>"$incomplete_file"
            incomplete_count=$((incomplete_count + 1))
          fi
        else
          warn "failed to inspect remote manifest for ${sha}; preserving it during this prune pass"
        fi
      fi
      continue
    fi

    if remote_manifest_exists "${archive_root}/commits/${sha}"; then
      if [ "$kept_complete" -lt "$keep_budget" ]; then
        kept_complete=$((kept_complete + 1))
      else
        printf '%s\n' "$sha" >>"$expired_file"
        expired_count=$((expired_count + 1))
      fi
    else
      stat_status=$?
      if [ "$stat_status" -eq "$remote_missing_status" ]; then
        if incomplete_commit_eligible_for_prune "$sha" "$now_epoch"; then
          printf '%s\n' "$sha" >>"$incomplete_file"
          incomplete_count=$((incomplete_count + 1))
        fi
      else
        warn "failed to inspect remote manifest for ${sha}; preserving it during this prune pass"
      fi
    fi
  done <"$sorted_shas_file"

  if [ "$incomplete_target_count" -eq 0 ] && [ "$incomplete_count" -eq 0 ] && [ "$expired_count" -eq 0 ]; then
    info "retention check: no commits to prune (retaining up to ${keep_budget} complete commits)"
    return 0
  fi

  info "prune ${incomplete_target_count} incomplete target, ${incomplete_count} incomplete, and ${expired_count} expired remote commit(s) (retaining up to ${keep_budget} complete commits)"

  local rm_status

  if [ "$incomplete_target_count" -gt 0 ]; then
    while IFS= read -r sha; do
      [ -n "$sha" ] || continue
      prune_lock_fence "$lock_fatal" || return 0
      info "remove ${archive_root}/commits/${sha} (incomplete target)"
      if remote_rm_rf "${archive_root}/commits/${sha}"; then
        pruned_any=1
        pruned_ok=$((pruned_ok + 1))
      else
        rm_status=$?
        if [ "$rm_status" -eq "$remote_auth_status" ]; then
          lock_held=0
          if [ "$lock_fatal" -eq 1 ]; then
            die "remote server rejected prune of ${archive_root}/commits/${sha} (expired/revoked scoped token)"
          fi
          warn "remote server rejected post-publish prune of ${archive_root}/commits/${sha} (expired/revoked scoped token); stopping"
          return 0
        fi
        warn "failed to remove ${archive_root}/commits/${sha} (incomplete target); continuing"
        prune_failures=$((prune_failures + 1))
      fi
    done <"$incomplete_target_file"
  fi

  if [ "$incomplete_count" -gt 0 ]; then
    while IFS= read -r sha; do
      [ -n "$sha" ] || continue
      prune_lock_fence "$lock_fatal" || return 0
      info "remove ${archive_root}/commits/${sha} (incomplete)"
      if remote_rm_rf "${archive_root}/commits/${sha}"; then
        pruned_any=1
        pruned_ok=$((pruned_ok + 1))
      else
        rm_status=$?
        if [ "$rm_status" -eq "$remote_auth_status" ]; then
          lock_held=0
          if [ "$lock_fatal" -eq 1 ]; then
            die "remote server rejected prune of ${archive_root}/commits/${sha} (expired/revoked scoped token)"
          fi
          warn "remote server rejected post-publish prune of ${archive_root}/commits/${sha} (expired/revoked scoped token); stopping"
          return 0
        fi
        warn "failed to remove ${archive_root}/commits/${sha} (incomplete); continuing"
        prune_failures=$((prune_failures + 1))
      fi
    done <"$incomplete_file"
  fi

  if [ "$expired_count" -gt 0 ]; then
    # Delete oldest expired commits first so an interrupted prune preserves the newest history.
    while IFS= read -r sha; do
      [ -n "$sha" ] || continue
      prune_lock_fence "$lock_fatal" || return 0
      info "remove ${archive_root}/commits/${sha} (expired)"
      if remote_rm_rf "${archive_root}/commits/${sha}"; then
        pruned_any=1
        pruned_ok=$((pruned_ok + 1))
      else
        rm_status=$?
        if [ "$rm_status" -eq "$remote_auth_status" ]; then
          lock_held=0
          if [ "$lock_fatal" -eq 1 ]; then
            die "remote server rejected prune of ${archive_root}/commits/${sha} (expired/revoked scoped token)"
          fi
          warn "remote server rejected post-publish prune of ${archive_root}/commits/${sha} (expired/revoked scoped token); stopping"
          return 0
        fi
        warn "failed to remove ${archive_root}/commits/${sha} (expired); continuing"
        prune_failures=$((prune_failures + 1))
      fi
    done < <(awk '{ lines[NR] = $0 } END { for (i = NR; i >= 1; i--) print lines[i] }' "$expired_file")
  fi

  if [ "$prune_failures" -gt 0 ]; then
    warn "retention prune completed with partial failures: removed=${pruned_ok} failed=${prune_failures}"
  fi
}

resolve_commits() {
  if [ "$#" -gt 0 ]; then
    printf '%s\n' "$@"
    return
  fi

  case "${GITHUB_EVENT_NAME:-}" in
  workflow_dispatch)
    git rev-parse "refs/remotes/origin/${branch}^{commit}"
    ;;
  *)
    git rev-parse HEAD
    ;;
  esac
}

cleanup_on_exit() {
  local exit_code=$?
  if [ -n "${active_remote_staging:-}" ] && [ "$mutation_in_flight" -eq 0 ] && [ "$aborted_by_signal" -eq 0 ] && [ "$lock_held" -eq 1 ]; then
    remote_rm_rf "$active_remote_staging" >/dev/null 2>&1 || true
    active_remote_staging=""
  fi
  release_archive_lock || true
  cleanup_active_worktree || true
  if [ -n "${work_root:-}" ] && [ -d "$work_root" ]; then
    rm -rf "$work_root" || true
  fi
  exit "$exit_code"
}

on_signal() {
  local sig_code=$1
  aborted_by_signal=1
  exit "$sig_code"
}

main() {
  local raw_commits=()
  local commits=()
  local sha resolved

  init_work_root
  fetch_archive_branch

  while IFS= read -r sha; do
    [ -n "$sha" ] && raw_commits+=("$sha")
  done < <(resolve_commits "$@")
  if [ "${#raw_commits[@]}" -eq 0 ]; then
    die "no commits to archive"
  fi

  for sha in "${raw_commits[@]}"; do
    resolved="$(git rev-parse "${sha}^{commit}")"
    if [ "${#commits[@]}" -eq 0 ] || ! is_sha_in_list "$resolved" "${commits[@]}"; then
      commits+=("$resolved")
    fi
  done

  acquire_archive_lock

  prune_remote_commits "$retain_commits" 1 "${commits[@]}"

  for sha in "${commits[@]}"; do
    assert_archive_lock_held
    publish_commit "$sha"
    publish_latest_index "$sha"
  done

  if [ "$deferred_latest_prune" -eq 1 ]; then
    prune_remote_commits "$retain_commits" 0 "${commits[@]}"
  fi
}

trap cleanup_on_exit EXIT
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

main "$@"
