#!/bin/sh
# Deterministic project export via tar across v1 and v2 trees, verifying
# complete traversal with zero stale-cache leakage and remote deliverable parity.
set -eu
PAYLOAD_DIR=${PAYLOAD_DIR:-}

S="$RUN_DIR/export-snap"
rm -rf "$S" && mkdir -p "$S/x1" "$S/x2" "$S/dist"

hash_tree() {
  (
    cd "$1"
    find . -type f | sort | while IFS= read -r f; do
      printf '%s  %s\n' "$(d9-stat sha256 "$f")" "$f"
    done
  )
}

if [ -n "$PAYLOAD_DIR" ]; then
  mkdir -p site
  cp -a "$PAYLOAD_DIR/." site/
  [ -n "$(find site -type f | head -1)" ] || {
    echo "PAYLOAD-EMPTY: $PAYLOAD_DIR has no files"; exit 1
  }
else
  mkdir -p site/src site/assets
  printf '<html>export v1</html>\n'     > site/index.html
  printf 'body{}\n'                     > site/styles.css
  printf 'app v1\n'                     > site/src/app.js
  head -c 65536 /dev/zero | tr '\0' 'e' > site/assets/hero.bin
fi
hash_tree site > "$S/exp-v1"

# v1 export -> unpack -> verify digest manifest.
tar cf "$S/e1.tar" site
tar xf "$S/e1.tar" -C "$S/x1"
hash_tree "$S/x1/site" > "$S/got-v1"
cmp "$S/got-v1" "$S/exp-v1"

# Mutate v1 -> v2 (overwrite index, delete styles, add pages/about.html).
removed=""
idx=$(find site -maxdepth 1 -type f -name 'index.html' | head -1)
printf '<html>export v2</html>\n' > "${idx:-site/index.html}"
if [ -e site/styles.css ]; then removed=styles.css; rm site/styles.css; fi
mkdir -p site/pages
printf '<html>about</html>\n' > site/pages/about.html

# v2 export must reflect fresh content and directory entries.
tar cf "$S/e2.tar" site
tar xf "$S/e2.tar" -C "$S/x2"
[ "$(cat "$S/x2/site/index.html")" = "<html>export v2</html>" ]
[ -e "$S/x2/site/pages/about.html" ]
if [ -n "$removed" ]; then [ ! -e "$S/x2/site/styles.css" ]; fi
stable=$(cd site && find . -type f ! -name 'index.html' ! -name 'styles.css' ! -name 'about.html' | sort | head -1)
if [ -n "$stable" ]; then cmp "$S/x2/site/$stable" "site/$stable"; fi

printf 'deliverable artifact\n' > "$S/dist/app.js"
mkdir -p deliverables
cp -a "$S/dist/app.js" deliverables/app.js
cmp deliverables/app.js "$S/dist/app.js"

sample final
drain --mode require-ok --timeout 120s
sample remote-final --as remote

check manifest-equals --this @remote-final --against @final \
    --ignore '.sim/**' --req 'scenario10-step5'
