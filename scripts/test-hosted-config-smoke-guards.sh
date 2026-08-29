#!/usr/bin/env bash
# Regression checks for credential-bearing hosted smoke preconditions.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAKE_BIN="$(mktemp -d)"
CURL_CALLED_FILE="$FAKE_BIN/curl-called"
trap 'rm -rf "$FAKE_BIN"' EXIT

printf '%s\n' \
  '#!/usr/bin/env bash' \
  ': >"${CURL_CALLED_FILE:?}"' \
  'exit 99' >"$FAKE_BIN/curl"
chmod +x "$FAKE_BIN/curl"

common_env=(
  "PATH=$FAKE_BIN:$PATH"
  "CURL_CALLED_FILE=$CURL_CALLED_FILE"
  DRIVE9_TIDBCLOUD_PUBLIC_KEY=dummy-public
  DRIVE9_TIDBCLOUD_PRIVATE_KEY=dummy-private
  DRIVE9_E2E_IMAGE_EXTRACT_API_BASE=https://provider.invalid/v1
  DRIVE9_E2E_IMAGE_EXTRACT_API_KEY=dummy-image-key
  DRIVE9_E2E_IMAGE_EXTRACT_MODEL=dummy-image-model
  DRIVE9_E2E_VIDEO_EXTRACT_API_BASE=https://provider.invalid/v1
  DRIVE9_E2E_VIDEO_EXTRACT_API_KEY=dummy-video-key
  DRIVE9_E2E_VIDEO_EXTRACT_MODEL=dummy-video-model
  DRIVE9_E2E_VIDEO_FIXTURE_PATH=/fixture-not-read-before-base-validation.mp4
  DRIVE9_E2E_VIDEO_EXPECTED_MARKER=fixture-specific-marker
  DRIVE9_E2E_EMBED_API_BASE=https://provider.invalid/v1
  DRIVE9_E2E_EMBED_API_KEY=dummy-embed-key
  DRIVE9_E2E_EMBED_MODEL=dummy-embed-model
)
scripts=(
  e2e/image-extract-config-smoke-test.sh
  e2e/video-extract-config-smoke-test.sh
  e2e/embedding-config-smoke-test.sh
)
http_bases=(
  http://127.0.0.1:9
  http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com
)

for script in "${scripts[@]}"; do
  for base in "${http_bases[@]}"; do
    rm -f "$CURL_CALLED_FILE"
    output_file="$FAKE_BIN/output"
    if env "${common_env[@]}" DRIVE9_BASE="$base" bash "$REPO_ROOT/$script" >"$output_file" 2>&1; then
      printf 'FAIL: %s accepted insecure DRIVE9_BASE=%s\n' "$script" "$base" >&2
      exit 1
    fi
    grep -q 'DRIVE9_BASE must use https://' "$output_file" || {
      printf 'FAIL: %s did not report the HTTPS requirement\n' "$script" >&2
      exit 1
    }
    [ ! -e "$CURL_CALLED_FILE" ] || {
      printf 'FAIL: %s called curl for insecure DRIVE9_BASE=%s\n' "$script" "$base" >&2
      exit 1
    }
  done

  rm -f "$CURL_CALLED_FILE"
  output_file="$FAKE_BIN/output"
  env -u DRIVE9_BASE "${common_env[@]}" bash "$REPO_ROOT/$script" >"$output_file" 2>&1
  grep -q '^SKIP:' "$output_file" || {
    printf 'FAIL: %s did not skip when DRIVE9_BASE was absent\n' "$script" >&2
    exit 1
  }
  [ ! -e "$CURL_CALLED_FILE" ] || {
    printf 'FAIL: %s called curl when DRIVE9_BASE was absent\n' "$script" >&2
    exit 1
  }
done

rm -f "$CURL_CALLED_FILE"
output_file="$FAKE_BIN/output"
if env "${common_env[@]}" DRIVE9_BASE=https://drive9.invalid DRIVE9_E2E_VIDEO_EXPECTED_MARKER=video \
  bash "$REPO_ROOT/e2e/video-extract-config-smoke-test.sh" >"$output_file" 2>&1; then
  printf 'FAIL: video smoke accepted a marker present in its prompt\n' >&2
  exit 1
fi
grep -q 'must not appear in the video prompt' "$output_file" || {
  printf 'FAIL: video smoke did not report the marker/prompt conflict\n' >&2
  exit 1
}
[ ! -e "$CURL_CALLED_FILE" ] || {
  printf 'FAIL: video smoke called curl for a marker/prompt conflict\n' >&2
  exit 1
}

printf 'PASS: hosted config smoke guards fail closed without network access\n'
