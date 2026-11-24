#!/bin/sh
# Git branch/commit switching across multi-file adds, deletes, rewrites, directory
# renames (`git mv`), 0755 permissions, and symlinks, including back-to-back switches.
set -eu

require --claim 'git-available' -- command -v git

S="$RUN_DIR/vswitch"
rm -rf "$S" && mkdir -p "$S"

printf '<html>A</html>\n'            > index.html
printf "console.log('app A');\n"     > app.js
printf 'body{color:#111}\n'          > styles.css
mkdir -p assets tools
head -c 8192 /dev/zero | tr '\0' 'A' > assets/img.bin
printf '#!/bin/sh\necho run-a\n'     > tools/run.sh
chmod 755 tools/run.sh
ln -sf ../tools/run.sh tools/run-link

git init -q -b main
git config user.email t@t && git config user.name t
git add -A && git commit -qm A
git ls-files | sort > "$S/expect-a"
[ -z "$(git status --porcelain)" ]

git rm -q styles.css assets/img.bin
printf "console.log('app B');\n"     > app.js
mkdir -p assets
head -c 8192 /dev/zero | tr '\0' 'B' > assets/img-b.bin
git mv tools scripts
printf '<html>B extra page</html>\n' > page2.html
git add -A && git commit -qm B
git ls-files | sort > "$S/expect-b"
[ -z "$(git status --porcelain)" ]

# B -> A
git checkout -q main~1
[ -z "$(git status --porcelain)" ]
git ls-files | sort | diff - "$S/expect-a"
[ "$(cat app.js)" = "console.log('app A');" ]
[ -x tools/run.sh ]
[ "$(readlink tools/run-link)" = "../tools/run.sh" ]
[ ! -e page2.html ] && [ -e assets/img.bin ] && [ ! -e scripts/run.sh ]

# A -> B
git checkout -q main
[ -z "$(git status --porcelain)" ]
git ls-files | sort | diff - "$S/expect-b"
[ "$(cat app.js)" = "console.log('app B');" ]
[ -x scripts/run.sh ]
[ "$(readlink scripts/run-link)" = "../tools/run.sh" ]
[ ! -e tools ] && [ ! -e styles.css ] && [ ! -e assets/img.bin ]

# Fast double switch B -> A -> B
git checkout -q main~1 && git checkout -q main
[ -z "$(git status --porcelain)" ]
git ls-files | sort | diff - "$S/expect-b"
head -c 8192 /dev/zero | tr '\0' 'B' | cmp - assets/img-b.bin

find . -path './.git' -prune -o -type f -name '*.js' -exec grep -q "app B" {} +
git ls-files | while IFS= read -r f; do
  [ "$(readlink "$f" 2>/dev/null || true)" = "../tools/run.sh" ] && continue
  cat "$f" > /dev/null
done

sample final
drain --mode require-ok --timeout 180s
sample remote-final --as remote

check manifest-equals --this @remote-final --against @final \
    --ignore '.git/**,.sim/**' --req 'scenario8-step5'
check manifest-absent --this @remote-final \
    --paths 'tools/run.sh,styles.css,assets/img.bin' --req 'scenario8-check2'
