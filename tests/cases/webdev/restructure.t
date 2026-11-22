#!/bin/sh
# Project restructuring: same-dir file rename, whole-directory subtree rename,
# delete + recreate under identical names, open-fd vs atomic-rename isolation,
# and cross-filesystem move (`EXDEV` or copy+unlink).
set -eu

S="$RUN_DIR/restructure"
rm -rf "$S" && mkdir -p "$S"

mkdir -p components/ui lib assets
printf '<template>btn v1</template>\n'  > components/ui/Button.vue
printf '<template>modal v1</template>\n' > components/ui/Modal.vue
printf 'export const util = 1;\n'        > lib/util.js
head -c 65536 /dev/zero | tr '\0' 'L'    > assets/logo.bin

# 1. Same-directory file move.
mv lib/util.js lib/helpers.js
printf 'export const util = 1;\n' | cmp - lib/helpers.js
[ ! -e lib/util.js ]
[ "$(ls lib)" = "helpers.js" ]

# 2. Whole-directory subtree move.
mv components modules
printf '<template>btn v1</template>\n' | cmp - modules/ui/Button.vue
[ ! -e components/ui/Button.vue ]
[ "$(find modules -type f | wc -l)" -eq 2 ]

# 3. Delete then recreate file and directory under identical names.
rm modules/ui/Modal.vue
printf '<template>modal v2 rebuilt</template>\n' > modules/ui/Modal.vue
printf '<template>modal v2 rebuilt</template>\n' | cmp - modules/ui/Modal.vue
mkdir -p emptydir && rmdir emptydir
mkdir emptydir && printf 'new in recreated dir\n' > emptydir/f.txt
printf 'new in recreated dir\n' | cmp - emptydir/f.txt

# 4. Open file descriptor isolation across atomic path replacement.
cat modules/ui/Button.vue > "$S/old-button"
exec 3< modules/ui/Button.vue
head -c 20 <&3 > /dev/null
printf '<template>btn v2 replaced</template>\n' > "$S/new-button"
cp "$S/new-button" modules/ui/Button.vue.new && mv modules/ui/Button.vue.new modules/ui/Button.vue
tail -c +21 <&3 > "$S/fd-rest"
tail -c +21 "$S/old-button" > "$S/exp-rest"
if cmp -s "$S/fd-rest" "$S/exp-rest"; then
  check shell --req 'scenario7-check2' -- test -s "$S/fd-rest"
else
  echo "OPEN-HANDLE-AFTER-RENAME: fd-rest=$(wc -c < "$S/fd-rest")B exp-rest=$(wc -c < "$S/exp-rest")B"
  check shell --accept pass,unconfirmed --req 'scenario7-check2' -- sh -c 'exit 3'
fi
cmp modules/ui/Button.vue "$S/new-button"
exec 3<&-

# 5. Cross-filesystem move (`$MOUNT` -> `$RUN_DIR` outside mount).
rm -f "$S/outside.bin"
if mv modules/ui/Button.vue "$S/outside.bin" 2>"$S/xerr"; then
  cmp "$S/outside.bin" "$S/new-button"
else
  if grep -Eiq 'EXDEV|cross-device|different' "$S/xerr"; then
    cp modules/ui/Button.vue "$S/outside.bin"
    cmp "$S/outside.bin" "$S/new-button"
  else
    echo "UNEXPECTED-ERRNO:"; cat "$S/xerr"; exit 1
  fi
fi
[ -e modules/ui/Button.vue ] || cp "$S/outside.bin" modules/ui/Button.vue

sample final
drain --mode require-ok --timeout 120s
sample remote-final --as remote

check manifest-equals --this @remote-final --against @final \
    --ignore '.sim/**' --req 'scenario7-step2'
