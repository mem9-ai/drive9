#!/usr/bin/env bash

set -u

case_dir=$(cd "${0%/*}" && pwd) || exit 1
repo_root=$(cd "$case_dir/../../.." && pwd) || exit 1
bundle_dir="$case_dir/bundle"

mkdir -p "$bundle_dir" || exit 1
cp "$repo_root/e2e/fuse-s3-express-append-log.sh" \
	"$bundle_dir/fuse-s3-express-append-log.sh" || exit 1
chmod 0755 "$bundle_dir/fuse-s3-express-append-log.sh" || exit 1
