#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "error: $*" >&2
  exit 1
}

info() {
  echo "archive: $*"
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
cli_targets="${DRIVE9_ARCHIVE_CLI_TARGETS:-linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64}"
legacy_agfs_module="github.com/c4pt0r/agfs/agfs-server"
legacy_agfs_version="${DRIVE9_ARCHIVE_LEGACY_AGFS_VERSION:-v0.0.0-20260410081414-678f51854d2a}"
work_root="${RUNNER_TEMP:-$(pwd)/.tmp}/drive9-agent-archive"
artifact_root="${work_root}/artifacts"
worktree_root="${work_root}/worktrees"
repository="${GITHUB_REPOSITORY:-mem9-ai/drive9}"
branch="${DRIVE9_ARCHIVE_BRANCH:-main}"
remote_missing_status=42
active_worktree=""

if [ -z "${DRIVE9_SERVER:-}" ]; then
  die "DRIVE9_SERVER is required"
fi
if [ -z "${DRIVE9_API_KEY:-}" ]; then
  die "DRIVE9_API_KEY is required"
fi
if [ -z "$branch" ]; then
  die "DRIVE9_ARCHIVE_BRANCH must not be empty"
fi
if [ "${branch#-}" != "$branch" ] || ! git check-ref-format "refs/heads/${branch}" >/dev/null; then
  die "invalid DRIVE9_ARCHIVE_BRANCH: ${branch}"
fi

remote() {
  "${publisher}" "$@"
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

remote_cp_to_local() {
  local remote_path=$1
  local local_path=$2
  remote fs cp "$(remote_ref "$remote_path")" "$local_path" >/dev/null
}

remote_mkdir() {
  local path=$1
  remote fs mkdir "$(remote_ref "$path")" >/dev/null
}

remote_upload() {
  local local_path=$1
  local remote_path=$2
  info "upload ${remote_path}"
  remote fs cp "$local_path" "$(remote_ref "$remote_path")"
}

fetch_archive_branch() {
  git fetch --quiet origin "refs/heads/${branch}:refs/remotes/origin/${branch}"
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
  local state

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

  remote_mkdir "$archive_root" || true
  remote_mkdir "${archive_root}/commits" || true
  remote_mkdir "$commit_path" || true
  remote_mkdir "${commit_path}/bin" || true

  remote_upload "${artifact_dir}/source.tar.gz" "${commit_path}/source.tar.gz"
  while IFS= read -r binary; do
    remote_upload "${artifact_dir}/${binary}" "${commit_path}/${binary}"
  done < <(cd "$artifact_dir" && find bin -type f -name 'drive9-*' | sort)
  remote_upload "${artifact_dir}/checksums.txt" "${commit_path}/checksums.txt"
  verify_remote_artifacts "$commit_path" "${artifact_dir}/checksums.txt"
  remote_upload "${artifact_dir}/manifest.json" "${commit_path}/manifest.json"
}

publish_latest_index() {
  local sha=$1
  local commit_path="${archive_root}/commits/${sha}"
  local remote_manifest="${work_root}/latest-source-manifest-${sha}.json"
  local latest_manifest="${work_root}/latest.json"

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

main() {
  mkdir -p "$artifact_root" "$worktree_root"
  fetch_archive_branch

  mapfile -t commits < <(resolve_commits "$@")
  if [ "${#commits[@]}" -eq 0 ]; then
    die "no commits to archive"
  fi

  for sha in "${commits[@]}"; do
    sha="$(git rev-parse "${sha}^{commit}")"
    publish_commit "$sha"
    publish_latest_index "$sha"
  done
}

trap cleanup_active_worktree EXIT

main "$@"
