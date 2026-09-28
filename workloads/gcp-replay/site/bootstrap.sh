#!/usr/bin/env bash
# Check Linux prerequisites; generate fixtures only after all checks pass.
set -uo pipefail
cd -- "${BASH_SOURCE[0]%/*}" || exit 1
set -a
# shellcheck source=config.example.env
source config.env || exit 1
set +a
export PATH="${D9_NODE_BIN:?}:/usr/sbin:/sbin:$PATH"
[[ "$(uname -s)" == "Linux" ]] || exit 1
missing=0
for tool in python3 git tar unzip zip patch fusermount3 mountpoint \
	iptables tc ss tmux curl node npm corepack; do
	if ! command -v "$tool" >/dev/null; then
		printf 'Missing: %s\n' "$tool" >&2
		missing=1
	fi
done
[[ -e /dev/fuse ]] || missing=1
sudo -n true || missing=1
grep -Eq '^[[:space:]]*user_allow_other([[:space:]]|$)' \
	/etc/fuse.conf || missing=1
[[ -x "${D9_BIN:?}" ]] || missing=1
((missing == 0)) || exit 1
python3 -c 'import sys; assert sys.version_info >= (3, 9)' || exit 1
[[ "$(node --version)" == "v22.13.0" ]] || exit 1
[[ "$(npm --version)" == "10.9.2" ]] || exit 1
"$D9_BIN" version || exit 1
http_code=$(curl -sS --max-time 15 -o /dev/null -w '%{http_code}' \
	"${D9_SERVER:?}/v1/status") || exit 1
[[ "$http_code" == 200 || "$http_code" == 401 ]] || exit 1
[[ "${1:-}" == "--check-only" ]] && exit 0
python3 fixtures_gen.py || exit 1
