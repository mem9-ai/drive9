# Release signing (self-update)

`drive9 update` installs a binary it downloads from a release root. The checksums and the
binary come from that same root, so a checksum match proves the bytes arrived intact — not
that drive9 published them. Anyone who can serve that root (a compromised bucket or CDN, an
open redirect, or a `--base-url` the user was talked into) could otherwise serve a matching
pair and get code execution in the user's account.

The updater therefore authenticates the release manifest with an ed25519 signature and
refuses to install anything it cannot verify.

## What is signed

`checksums.txt`, exactly as published:

```
# drive9-release-version <version>
<sha256>  drive9-linux-amd64
<sha256>  drive9-darwin-arm64
...
```

The version line is part of the signed bytes, so an origin cannot pair an older signed
binary with a newer version string to make a downgrade look like an upgrade. The sibling
`version` file still exists for older CLIs that read it; this updater does not.

The detached signature is published next to it as `checksums.txt.sig`: base64 of the raw
64-byte ed25519 signature.

## Verifying a release by hand

Two independent ways, both over the same bytes the CLI checks.

With the repository's own verifier (same code the CLI embeds):

```bash
curl -fsSLO https://drive9.ai/releases/checksums.txt
curl -fsSLO https://drive9.ai/releases/checksums.txt.sig
DRIVE9_RELEASE_PUBLIC_KEY="<base64 public key>" \
  go run ./cmd/drive9-release-sign -verify checksums.txt \
  && sha256sum --check checksums.txt   # artifact bytes, only after the signature
```

`drive9-release-sign -verify` exits non-zero when the signature does not verify, so the
checksum step cannot run on an unauthenticated manifest.

With OpenSSL 3.x, no Go needed. The embedded key is a raw 32-byte ed25519 public key, so
wrap it in the fixed `SubjectPublicKeyInfo` prefix first:

```bash
curl -fsSLO https://drive9.ai/releases/checksums.txt
curl -fsSLO https://drive9.ai/releases/checksums.txt.sig
printf '%s' '<base64 public key>' | base64 -d > drive9-release.raw
printf '\x30\x2a\x30\x05\x06\x03\x2b\x65\x70\x03\x21\x00' > drive9-release.der
cat drive9-release.raw >> drive9-release.der
openssl pkey -pubin -inform DER -in drive9-release.der -out drive9-release.pem
base64 -d checksums.txt.sig > checksums.sig
openssl pkeyutl -verify -pubin -inkey drive9-release.pem -rawin \
  -in checksums.txt -sigfile checksums.sig \
  && sha256sum --check checksums.txt
```

`pkeyutl -verify` prints `Signature Verified Successfully` and exits 0; `sha256sum --check`
runs only after that succeeds.

## Signing a release

The private key lives only in the `DRIVE9_RELEASE_SIGNING_KEY` repository secret (base64 of
either the 32-byte ed25519 seed or the 64-byte private key). The release workflow signs
`dist/checksums.txt` and fails closed when the secret is missing — an unsigned manifest is a
release that no updated CLI can install.

To sign locally:

```bash
DRIVE9_RELEASE_SIGNING_KEY=<base64 seed> go run ./cmd/drive9-release-sign dist/checksums.txt
```

## Rotating the key

Installed CLIs only trust the key embedded in the binary they are running, so a rotation is a
two-release dance: the transition release that carries the new key must itself be signed with
the **current** key.

1. Generate a new pair. Keep its private key at hand, but do **not** put it in
   `DRIVE9_RELEASE_SIGNING_KEY` yet: the release job signs with whatever is in that secret,
   and the next release must be signed with the current key.
2. Land a release that embeds the **new** public key in
   `cmd/drive9/cli/update_signing.go`, with `DRIVE9_RELEASE_SIGNING_KEY` still holding the
   **current** private key. That is the transition release.
3. Confirm an **existing** client — one whose binary predates the transition release —
   installs it: `drive9 update` succeeds and the reported version is the transition version.
   Do not test the other direction yet: the transition binary trusts the new key, and the
   manifest it came from is still signed with the old one, so updating *from* the transition
   binary fails verification until step 4.
4. Replace `DRIVE9_RELEASE_SIGNING_KEY` with the new private key and publish the next
   release. Confirm a **transition client** (installed at step 3) installs it: that update
   crosses the trust change and is the proof the rotation finished. A client still running a
   pre-transition binary cannot verify this manifest — old-key binaries reject new-key
   signatures, which is step 5's problem, not a step 4 failure.
5. Keep the transition release's artifacts published until the fleet has moved. A client that
   missed it still trusts the old key and rejects every new-key-signed manifest; recover it
   with a manual install of the target release (download the artifact for its platform and
   place it on `PATH`, or reinstall from the transition release while it is still served).

There is no revocation list: a compromised key is rotated by the same steps, starting from a
release signed with the compromised key (or a support-assisted manual install if the key is
already in an attacker's hands).

## Custom or self-hosted releases

A fork can point the updater at its own key at link time:

```bash
go build -ldflags "-X github.com/mem9-ai/drive9/cmd/drive9/cli.updateManifestPublicKey=<base64 public key>" \
  -o bin/drive9 ./cmd/drive9
```

and sign its own manifest with `drive9-release-sign`. A build with no key (empty value)
refuses to self-update rather than falling back to trusting the origin.

## Residual risks

* A **stolen release key** is equivalent to a malicious release. That is inherent to any
  signing scheme; rotate as above.
* **Old clients** (built before this change) do not verify signatures and remain exposed to
  an origin-level compromise. Only updating them fixes that.
* The **agent archive** channel (`scripts/publish-agent-archive.sh`) has its own checksums
  and is not yet signed; it is not a `drive9 update` source.
