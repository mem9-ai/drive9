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
  go run ./cmd/drive9-release-sign -verify checksums.txt
sha256sum --check checksums.txt   # proves the artifact bytes too
```

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

Installed CLIs only trust the key embedded in the build they are running, so a rotation is a
two-release dance. Publish the transition release **with the current key**, then switch:

1. Generate a new pair (`openssl genpkey -algorithm ed25519` or a small Go program). Put the
   new private key in `DRIVE9_RELEASE_SIGNING_KEY` **only at step 4**; the release job signs
   with whichever secret is configured, and step 3 must still be signed by the key that
   installed CLIs trust.
2. Land a release that embeds the **new** public key in
   `cmd/drive9/cli/update_signing.go` while `DRIVE9_RELEASE_SIGNING_KEY` still holds the
   **current** key. This is the transition release: installed CLIs accept it because it is
   signed by the key they already have, and the binary it installs carries the new key.
3. Confirm the transition release published and that `drive9 update` on it works.
4. Replace `DRIVE9_RELEASE_SIGNING_KEY` with the new private key and publish normally from
   then on; clients that installed the transition release verify the new key.

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
