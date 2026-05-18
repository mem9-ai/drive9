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
require_cmd jq
require_cmd sha256sum

publisher="${DRIVE9_PUBLISHER:-drive9}"
archive_root="${DRIVE9_ARCHIVE_ROOT:-/drive9}"
archive_root="${archive_root%/}"
recent_commits="${DRIVE9_ARCHIVE_RECENT_COMMITS:-20}"
cli_targets="${DRIVE9_ARCHIVE_CLI_TARGETS:-linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64}"
work_root="${RUNNER_TEMP:-$(pwd)/.tmp}/drive9-agent-archive"
artifact_root="${work_root}/artifacts"
worktree_root="${work_root}/worktrees"
repository="${GITHUB_REPOSITORY:-mem9-ai/drive9}"
branch="main"

if [ -z "${DRIVE9_SERVER:-}" ]; then
  die "DRIVE9_SERVER is required"
fi
if [ -z "${DRIVE9_API_KEY:-}" ]; then
  die "DRIVE9_API_KEY is required"
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
  remote fs cat "$(remote_ref "$path")" >"$out" 2>/dev/null
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

write_latest_manifest() {
  local sha=$1
  local artifact_dir=$2
  local published_at checksums_json
  published_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  checksums_json="$(checksums_object "${artifact_dir}/checksums.txt")"

  jq -n \
    --arg repository "$repository" \
    --arg branch "$branch" \
    --arg sha "$sha" \
    --arg commit_path "${archive_root}/commits/${sha}/" \
    --arg published_at "$published_at" \
    --argjson checksums "$checksums_json" \
    '{
      schema_version: 1,
      repository: $repository,
      branch: $branch,
      commit_sha: $sha,
      commit_path: $commit_path,
      published_at: $published_at,
      checksums: $checksums
    }' >"${artifact_dir}/latest-manifest.json"
}

write_latest_manifest_from_remote_commit() {
  local sha=$1
  local out=$2
  local remote_manifest="${work_root}/latest-source-${sha}.json"
  local published_at checksums_json

  remote_cat "${archive_root}/commits/${sha}/manifest.json" "$remote_manifest"
  published_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  checksums_json="$(jq '.checksums' "$remote_manifest")"

  jq -n \
    --arg repository "$repository" \
    --arg branch "$branch" \
    --arg sha "$sha" \
    --arg commit_path "${archive_root}/commits/${sha}/" \
    --arg published_at "$published_at" \
    --argjson checksums "$checksums_json" \
    '{
      schema_version: 1,
      repository: $repository,
      branch: $branch,
      commit_sha: $sha,
      commit_path: $commit_path,
      published_at: $published_at,
      checksums: $checksums
    }' >"$out"
}

prepare_agfs_dependency() {
  local worktree=$1
  local parent
  parent="$(dirname "$worktree")"
  if ! grep -q '../agfs' "${worktree}/go.mod"; then
    return
  fi
  if [ -d "${parent}/agfs/.git" ]; then
    return
  fi
  info "checkout agfs dependency for legacy local replace"
  git clone --depth 1 https://github.com/c4pt0r/agfs "${parent}/agfs"
}

build_artifacts() {
  local sha=$1
  local artifact_dir=$2
  local worktree="${worktree_root}/${sha}"
  local build_dist="${artifact_dir}/dist-build"
  local short_sha="${sha:0:7}"

  rm -rf "$artifact_dir" "$worktree"
  mkdir -p "$artifact_dir" "$worktree_root"

  info "create source archive for ${sha}"
  git archive --format=tar.gz --prefix="drive9-${sha}/" -o "${artifact_dir}/source.tar.gz" "$sha"

  info "create worktree for ${sha}"
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
  write_latest_manifest "$sha" "$artifact_dir"

  git worktree remove --force "$worktree" >/dev/null
}

verify_remote_artifacts() {
  local commit_path=$1
  local checksums_file=$2
  local verify_dir="${work_root}/verify"
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
  local checksums_json

  if ! remote_cat "${commit_path}/manifest.json" "$remote_manifest"; then
    echo "absent"
    return
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

  if ! remote_cat "${commit_path}/manifest.json" "$remote_manifest"; then
    echo "absent"
    return
  fi

  if ! jq -e --arg sha "$sha" \
    '.commit_sha == $sha and (.checksums | type == "object")' "$remote_manifest" >/dev/null; then
    die "remote manifest exists for ${sha} but is malformed"
  fi

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
  remote_upload "${artifact_dir}/manifest.json" "${commit_path}/manifest.json"
}

update_latest_if_head() {
  local sha=$1
  local artifact_dir="${artifact_root}/${sha}"
  local latest_manifest="${artifact_dir}/latest-manifest.json"
  local newest
  newest="$(git rev-parse origin/main)"
  if [ "$sha" != "$newest" ]; then
    info "skip latest for ${sha}; origin/main is ${newest}"
    return
  fi

  if [ "$(remote_commit_self_state "$sha")" != "complete" ]; then
    die "cannot update latest because ${sha} is not complete"
  fi

  if [ ! -f "$latest_manifest" ]; then
    mkdir -p "$artifact_dir"
    write_latest_manifest_from_remote_commit "$sha" "$latest_manifest"
  fi

  remote_mkdir "$archive_root" || true
  remote_mkdir "${archive_root}/latest" || true
  remote_upload "$latest_manifest" "${archive_root}/latest/manifest.json"
}

resolve_commits() {
  if [ "$#" -gt 0 ]; then
    printf '%s\n' "$@"
    return
  fi

  if [ -n "${DRIVE9_ARCHIVE_COMMIT_SHA:-}" ]; then
    printf '%s\n' "$DRIVE9_ARCHIVE_COMMIT_SHA"
    return
  fi

  case "${GITHUB_EVENT_NAME:-}" in
  schedule | workflow_dispatch)
    git rev-list --reverse --max-count="$recent_commits" origin/main
    ;;
  *)
    git rev-parse HEAD
    ;;
  esac
}

main() {
  mkdir -p "$artifact_root" "$worktree_root"
  git fetch --quiet origin main

  mapfile -t commits < <(resolve_commits "$@")
  if [ "${#commits[@]}" -eq 0 ]; then
    die "no commits to archive"
  fi

  for sha in "${commits[@]}"; do
    sha="$(git rev-parse "${sha}^{commit}")"
    publish_commit "$sha"
    update_latest_if_head "$sha"
  done
}

main "$@"
