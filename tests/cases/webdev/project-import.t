#!/bin/sh
# Bulk project import via both `unzip` and `git clone` of a deep tree containing
# empty files, dotfiles, space-in-name files, CJK filenames, 0755 scripts, and
# symlinks, followed by an independent per-file SHA-256 + attribute scan.
set -eu

require --claim 'zip-and-git-available' -- sh -c '
  command -v zip >/dev/null && command -v unzip >/dev/null &&
  command -v git >/dev/null && command -v python3 >/dev/null
'

FILES=${FILES:-100}
PAYLOAD_DIR=${PAYLOAD_DIR:-}
export FILES

T="$RUN_DIR/project-import"
rm -rf "$T" && mkdir -p "$T/staged"

cat > "$T/hash_tree.py" <<'EOF'
import hashlib, os, sys
root = sys.argv[1]
rows = []
for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
    if ".git" in dirnames:
        dirnames.remove(".git")
    dirnames.sort()
    for fn in sorted(filenames):
        full = os.path.join(dirpath, fn)
        if os.path.islink(full) or not os.path.isfile(full):
            continue
        rel = os.path.relpath(full, root)
        h = hashlib.sha256()
        with open(full, "rb") as fp:
            while True:
                b = fp.read(65536)
                if not b:
                    break
                h.update(b)
        rows.append(f"{h.hexdigest()}  {rel}\n")
rows.sort(key=lambda s: s.split("  ", 1)[1])
sys.stdout.writelines(rows)
EOF

if [ -n "$PAYLOAD_DIR" ]; then
  cp -a "$PAYLOAD_DIR/." "$T/staged/"
  [ -n "$(find "$T/staged" \( -type f -o -type l \) | head -1)" ] || {
    echo "PAYLOAD-EMPTY: $PAYLOAD_DIR has no files"; exit 1
  }
else
  mkdir -p "$T/staged/src/deep/a/b/c" "$T/staged/pages" "$T/staged/assets"
  i=0
  while [ "$i" -lt "$FILES" ]; do
    k=$((i + 1))
    printf 'payload %s\n' "$k"           > "$T/staged/src/deep/a/b/c/mod-$k.js"
    :                                    > "$T/staged/src/empty-$k"
    printf 'dot %s\n' "$k"               > "$T/staged/src/.hidden-$k"
    printf 'space %s\n' "$k"             > "$T/staged/pages/note $k.md"
    printf '页面 %s\n' "$k"              > "$T/staged/pages/页面-$k.html"
    head -c 4096 /dev/zero | tr '\0' 'a' > "$T/staged/assets/img-$k.bin"
    i=$((i + 1))
  done
  printf '#!/bin/sh\necho run\n' > "$T/staged/src/tool.sh"
  chmod 755 "$T/staged/src/tool.sh"
  ln -s ../assets/img-1.bin "$T/staged/src/img-link"
fi

python3 "$T/hash_tree.py" "$T/staged" > "$T/scan-exp.sum"
# `-y` preserves symlinks in Info-ZIP instead of dereferencing targets.
( cd "$T/staged" && zip -qry "$T/staged.zip" . )

mkdir -p "$T/repo"
cp -a "$T/staged/." "$T/repo/"
(
  cd "$T/repo"
  rm -rf .git
  git init -q
  git add -A
  git -c user.email=t@t -c user.name=t commit -qm base
)

mkdir -p by-zip by-git
unzip -q "$T/staged.zip" -d by-zip
git clone -q "$T/repo" by-git

# Independent child-process scan of both imported trees.
sh -c '
  set -eu
  T=$1
  fail=0
  exp=$(wc -l < "$T/scan-exp.sum")
  for d in by-zip by-git; do
    python3 "$T/hash_tree.py" "$d" > "$T/scan-$d.sum"
    got=$(wc -l < "$T/scan-$d.sum")
    [ "$got" -eq "$exp" ] || { echo "COUNT-MISMATCH $d got=$got exp=$exp"; fail=1; }
    cmp -s "$T/scan-$d.sum" "$T/scan-exp.sum" || { echo "DIGEST-MISMATCH $d"; fail=1; }
  done
  [ ! -e "$T/staged/src/tool.sh" ]       || [ -x by-zip/src/tool.sh ]                                             || { echo "EXEC-BIT-LOST"; fail=1; }
  [ ! -L "$T/staged/src/img-link" ]      || [ "$(readlink by-zip/src/img-link)" = "$(readlink "$T/staged/src/img-link")" ] || { echo "LINK-TARGET-LOST"; fail=1; }
  [ ! -e "$T/staged/pages/页面-1.html" ] || [ -e "by-zip/pages/页面-1.html" ]                                     || { echo "CJK-NAME-LOST"; fail=1; }
  [ ! -e "$T/staged/pages/note 1.md" ]   || [ -e "by-zip/pages/note 1.md" ]                                       || { echo "SPACE-NAME-LOST"; fail=1; }
  [ ! -e "$T/staged/src/.hidden-1" ]     || [ -e by-zip/src/.hidden-1 ]                                           || { echo "HIDDEN-LOST"; fail=1; }
  [ ! -e "$T/staged/src/empty-1" ]       || [ -e by-zip/src/empty-1 ]                                             || { echo "EMPTY-LOST"; fail=1; }
  [ "$fail" -eq 0 ]
' -- "$T"

sample after-import
drain --mode require-ok --timeout 180s
sample remote-final --as remote

check manifest-equals --this control --against @after-import \
    --ignore 'by-git/.git/**,.sim/**' --req 'scenario1-check1'
check manifest-equals --this @remote-final --against @after-import \
    --ignore 'by-git/.git/**,.sim/**' --req 'scenario1-check4'
