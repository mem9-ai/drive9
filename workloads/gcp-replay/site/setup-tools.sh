#!/usr/bin/env bash
set -uo pipefail
cd -- "${BASH_SOURCE[0]%/*}" || exit 1
set -a
# shellcheck source=config.example.env
source config.env || exit 1
set +a
export PATH="${D9_NODE_BIN:?}:$PATH"
[[ "$(node --version)" == "v22.13.0" ]] || exit 1
[[ "$(npm --version)" == "10.9.2" ]] || exit 1
mkdir -p -- "${D9_STATE:?}/tools" || exit 1
npm install --prefix "$D9_STATE/tools" --no-audit --no-fund \
	pnpm@9.15.1 typescript@5.6.3 vitest@2.1.9 || exit 1
"$D9_STATE/tools/node_modules/.bin/pnpm" --version || exit 1
"$D9_STATE/tools/node_modules/.bin/tsc" --version || exit 1
"$D9_STATE/tools/node_modules/.bin/vitest" --version || exit 1
