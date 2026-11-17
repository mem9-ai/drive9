# Release signing (self-update)

`drive9 update` installs a binary it downloads from a release root. The checksums and the
binary come from that same root, so a checksum match proves the bytes arrived intact — not
that drive9 published them. Anyone who can serve that root (a compromised bucket or CDN, an
open redirect, or a `--base-url` the user was talked into) could otherwise serve a matching
pair and get code execution in the user's account.

The updater therefore authenticates the release with ed25519 signatures and refuses to
install anything it cannot verify.

## What is signed

Two files, each with a detached signature (`base64` of the raw 64-byte ed25519 signature):

| file | signature | contents |
| --- | --- | --- |
| `checksums.txt` | `checksums.txt.sig` | `<sha256>  <artifact>` lines — the format **every** drive9 CLI has always parsed |
| `release.txt` | `release.txt.sig` | `version <v>` and `checksums-sha256 <hex digest of checksums.txt>` |

`release.txt` is what binds a version to one exact manifest: an origin cannot pair an older
signed binary with a newer version string, and cannot mix one release's signed version with
another release's signed manifest.

`checksums.txt` deliberately stays in the plain, pre-signing format — no comments, no
headers, no version line. A CLI built before any of this exists parses it unchanged, so the
**first signed release is still installable by every already-deployed CLI** (they simply do
not verify signatures). Putting the version inside `checksums.txt` would have broken exactly
those clients out of self-update.

The build pins a **set** of public keys (`cmd/drive9/cli/update_key.txt`, one per line or
comma-separated — both work, in the file and on the command line). A release whose files
verify under any pinned key installs, which is what lets a rotation overlap instead of
switching in one release.

## Verifying a release by hand

The updater checks three things: both signatures, the digest that binds the version to the
manifest, and then the artifact bytes. The manual routes below check the same three, in the
same order — a genuine `release.txt` from one release combined with a genuine `checksums.txt`
from another fails, exactly as it does in `drive9 update`.

Fetch one platform artifact, not every artifact:

```bash
set -euo pipefail
KEYS='<comma-separated base64 public keys>'
for f in checksums.txt checksums.txt.sig release.txt release.txt.sig drive9-linux-amd64; do
  curl -fsSLO "https://drive9.ai/releases/$f"
done
```

With the repository's own verifier (the same parsers and checks the updater runs):

```bash
set -euo pipefail
DRIVE9_RELEASE_PUBLIC_KEY="$KEYS" go run ./cmd/drive9-release-sign -verify-release checksums.txt
grep '  drive9-linux-amd64$' checksums.txt | sha256sum --check -
```

`-verify-release` verifies both signatures, parses `release.txt`, and fails unless its
`checksums-sha256` matches the downloaded `checksums.txt` before it prints the version. The
checksum line then proves the artifact bytes.

With OpenSSL 3.x, no Go needed. The pinned keys are raw 32-byte ed25519 public keys, so wrap
one in the fixed `SubjectPublicKeyInfo` prefix first, verify both signatures, and compare the
digest yourself:

```bash
set -euo pipefail
printf '%s' '<one base64 public key>' | base64 -d > drive9-release.raw
printf '\x30\x2a\x30\x05\x06\x03\x2b\x65\x70\x03\x21\x00' > drive9-release.der
cat drive9-release.raw >> drive9-release.der
openssl pkey -pubin -inform DER -in drive9-release.der -out drive9-release.pem

verify_sig() { base64 -d "$1.sig" > "$1.sig.bin"
  openssl pkeyutl -verify -pubin -inkey drive9-release.pem -rawin -in "$1" -sigfile "$1.sig.bin"; }
verify_sig checksums.txt
verify_sig release.txt

expected="$(awk '$1 == "checksums-sha256" { print $2 }' release.txt)"
actual="$(sha256sum checksums.txt | cut -d' ' -f1)"
[ -n "$expected" ] && [ "$expected" = "$actual" ] || { echo "release.txt does not describe this checksums.txt" >&2; exit 1; }

grep '  drive9-linux-amd64$' checksums.txt | sha256sum --check -
```

Each step is `set -e`-equivalent by construction: a failed signature, a digest mismatch, or a
checksum mismatch stops the script before the artifact is used.

## Signing a release

The private key lives only in the `DRIVE9_RELEASE_SIGNING_KEY` repository secret (base64 of
either the 32-byte ed25519 seed or the 64-byte private key). The release workflow signs both
files, verifies both against the keys the release binary embeds, and fails closed when the
secret is missing — an unsigned release is a release no updated CLI can install.

To sign locally:

```bash
DRIVE9_RELEASE_SIGNING_KEY=<base64 seed> go run ./cmd/drive9-release-sign dist/checksums.txt dist/release.txt
```

The signer parses each file the way the updater will (a manifest with any non-checksum line,
a versionless description, or a description whose digest does not match the manifest is
refused before anything is signed).

## Rotating the key

The updater trusts the key set embedded in the binary it runs, and that set may hold more than
one key, so a rotation overlaps instead of switching in a single release:

1. Generate a new pair. Add its public key to `cmd/drive9/cli/update_key.txt` **next to the
   current key** (its own line, or comma-separated) and ship that release with
   `DRIVE9_RELEASE_SIGNING_KEY` still holding the **current** private key. Installed clients
   accept it (they trust the old key), and the binary they install now trusts both.
2. Keep publishing releases signed with the old key for at least one cycle. `drive9 update`
   and `drive9 update --check` work on pre-change clients (old key), on overlap clients
   (either key) and on new clients (both keys).
3. Switch `DRIVE9_RELEASE_SIGNING_KEY` to the new private key. Releases from then on are
   signed by the new key and verify on every client that installed the overlap release; the
   release job's `-verify-release` step validates the whole bundle — both signatures, the
   version-to-manifest digest, and the manifest structure — against the embedded set before
   upload.
4. Once no client can still be running a pre-overlap binary, remove the old public key from
   `update_key.txt`. Removing it earlier strands late updaters; keeping it longer costs
   nothing but leaves the retired key trusted.
5. A client that missed the overlap window still trusts only the old key and rejects new-key
   manifests: recover it with a manual install of the target artifact (download the file for
   its platform and place it on `PATH`). `/latest/` is overwritten in place by each release,
   so the overlap is the supported path — do not depend on old artifacts staying available.

There is no revocation list: a compromised key is removed from `update_key.txt` in a release
signed by the remaining keys (or a support-assisted manual install if the compromised key is
the only one).

## Custom or self-hosted releases

A fork can point the updater at its own key set at link time:

```bash
go build -ldflags "-X github.com/mem9-ai/drive9/cmd/drive9/cli.updateManifestPublicKey=<key>[,<key>]" \
  -o bin/drive9 ./cmd/drive9
```

The variable is initialized from a constant sentinel on purpose: `-X` only applies to a string
variable initialized with a constant expression, so an initializer that referenced the
embedded key file would silently ignore the override. An explicit empty override (`-X ...=`)
means "trust no keys" and refuses to update.

Sign your manifests with `drive9-release-sign`. A build with no key refuses to self-update
rather than falling back to trusting the origin.

## Residual risks

* A **stolen release key** is equivalent to a malicious release while it stays pinned. That is
  inherent to any signing scheme; rotate as above, and remove the key from `update_key.txt`
  once the overlap release has been adopted.
* **Pre-signing clients** do not verify anything; they will install the first signed release
  (the manifest format is unchanged) and remain exposed to an origin compromise until they
  install a verifying build.
* The **agent archive** channel (`scripts/publish-agent-archive.sh`) has its own checksums and
  is not signed; it is not a `drive9 update` source.
