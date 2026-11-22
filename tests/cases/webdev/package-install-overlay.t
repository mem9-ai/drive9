#!/bin/sh
# Package manager lifecycle in overlay-local topology (`--overlay 'node_modules/**'`):
# `node_modules` stays in local overlay while `package.json` and `package-lock.json`
# sync remotely. Verifies cold install, `npm ci`, upgrade, removal, and that an
# installed `node_modules/tiny` remains strictly absent from the remote vantage.
set -eu

require --claim 'npm-available' -- sh -c 'command -v node >/dev/null && command -v npm >/dev/null'

T="$RUN_DIR/package-install"
export npm_config_cache="$RUN_DIR/npm-cache"
rm -rf "$T" "$npm_config_cache"
mkdir -p "$T/tiny-1.0.0/bin" "$T/tiny-1.1.0/bin" "$npm_config_cache"

printf '{"name":"tiny","version":"1.0.0","main":"index.js","bin":{"tiny":"bin/tiny.js"}}\n' > "$T/tiny-1.0.0/package.json"
printf 'module.exports = { hello: () => "hi" };\n' > "$T/tiny-1.0.0/index.js"
printf '#!/usr/bin/env node\nconsole.log("tiny-bin-1.0.0");\n' > "$T/tiny-1.0.0/bin/tiny.js"
cp -a "$T/tiny-1.0.0/." "$T/tiny-1.1.0/"
printf '{"name":"tiny","version":"1.1.0","main":"index.js","bin":{"tiny":"bin/tiny.js"}}\n' > "$T/tiny-1.1.0/package.json"
printf 'module.exports = { hello: () => "hi2" };\n' > "$T/tiny-1.1.0/index.js"
printf '#!/usr/bin/env node\nconsole.log("tiny-bin-1.1.0");\n' > "$T/tiny-1.1.0/bin/tiny.js"
( cd "$T/tiny-1.0.0" && npm pack --silent >/dev/null )
( cd "$T/tiny-1.1.0" && npm pack --silent >/dev/null )
for f in "$T"/tiny-*/tiny-*.tgz; do
  printf '%s  %s\n' "$(d9-stat sha256 "$f")" "$f"
done

npm install --no-audit --no-fund "$T/tiny-1.0.0/tiny-1.0.0.tgz"
node -e 'const t=require("tiny"); if (t.hello()!=="hi") process.exit(3)'
./node_modules/.bin/tiny | grep -qx tiny-bin-1.0.0
[ -e node_modules/.bin/tiny ]

npm install --no-audit --no-fund "$T/tiny-1.0.0/tiny-1.0.0.tgz"
rm -rf node_modules
npm ci --no-audit --no-fund
node -e 'const t=require("tiny"); if (t.hello()!=="hi") process.exit(3)'

# Upgrade to 1.1.0 and verify while `node_modules/tiny` IS present locally that
# `--overlay 'node_modules/**'` keeps `node_modules` off the remote side.
npm install --no-audit --no-fund "$T/tiny-1.1.0/tiny-1.1.0.tgz"
node -e 'const t=require("tiny"); if (t.hello()!=="hi2") process.exit(3)'
./node_modules/.bin/tiny | grep -qx tiny-bin-1.1.0
[ -f node_modules/tiny/index.js ]
drain --mode require-ok --timeout 180s
sample remote-with-pkg --as remote

# Removal must update local enumeration immediately.
npm remove tiny --no-audit --no-fund
[ ! -e node_modules/tiny ] && [ ! -e node_modules/.bin/tiny ]
if node -e 'require("tiny")' 2>/dev/null; then
  echo "REMOVED-PACKAGE-STILL-LOADABLE"; exit 1
fi

sample final
drain --mode require-ok --timeout 180s
sample remote-final --as remote

check manifest-absent --this @remote-with-pkg \
    --paths 'node_modules/tiny/index.js' --req 'section2-1'
check manifest-equals --this @remote-final --against @final \
    --ignore '.sim/**,node_modules/**' --req 'scenario6-check1'
