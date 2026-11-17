package releasesign

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
)

func testKey(seed byte) (ed25519.PrivateKey, string) {
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	return private, base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey))
}

func TestSignVerifyRoundTrip(t *testing.T) {
	private, public := testKey(0x11)
	manifest := []byte(VersionPrefix + "v1.2.3\n" + strings.Repeat("a", 64) + "  drive9-linux-amd64\n")
	signature, err := Sign(manifest, private)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := Verify(manifest, signature, public); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	// The signature covers the exact bytes: any change breaks it.
	tampered := append([]byte(nil), manifest...)
	tampered[len(tampered)-1] = '\n'
	tampered = append(tampered, '#')
	if err := Verify(tampered, signature, public); err == nil {
		t.Fatal("Verify accepted a manifest that was not the signed one")
	}
}

func TestVerifyRejectsBadInputs(t *testing.T) {
	private, public := testKey(0x22)
	_, otherPublic := testKey(0x33)
	manifest := []byte(VersionPrefix + "v1.0.0\n")
	signature, err := Sign(manifest, private)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	short := []byte(base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize-1)))

	for _, tc := range []struct {
		name      string
		manifest  []byte
		signature []byte
		public    string
		want      string
	}{
		{name: "no build key", manifest: manifest, signature: signature, public: "", want: "no release-signing public key configured"},
		{name: "malformed build key", manifest: manifest, signature: signature, public: "not-base64", want: "public key is invalid"},
		{name: "short build key", manifest: manifest, signature: signature, public: base64.StdEncoding.EncodeToString([]byte("short")), want: "public key is invalid"},
		{name: "missing signature", manifest: manifest, signature: []byte("  \n"), public: public, want: "missing or empty"},
		{name: "non-base64 signature", manifest: manifest, signature: []byte("!!!"), public: public, want: "signature is malformed"},
		{name: "short signature", manifest: manifest, signature: short, public: public, want: "want 64"},
		{name: "foreign key", manifest: manifest, signature: signature, public: otherPublic, want: "does not verify"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Verify(tc.manifest, tc.signature, tc.public)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Verify error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParsePrivateKeyAcceptsSeedAndExpandedForms(t *testing.T) {
	private, _ := testKey(0x44)
	for name, encoded := range map[string]string{
		"seed":     base64.StdEncoding.EncodeToString(private.Seed()),
		"expanded": base64.StdEncoding.EncodeToString(private),
	} {
		t.Run(name, func(t *testing.T) {
			parsed, err := ParsePrivateKey(encoded)
			if err != nil {
				t.Fatalf("ParsePrivateKey: %v", err)
			}
			if !parsed.Equal(private) {
				t.Fatal("parsed key differs from the generated one")
			}
		})
	}
	for name, tc := range map[string]struct{ encoded, want string }{
		"empty":     {"", "private key is invalid: empty"},
		"not b64":   {"!!!", "private key is invalid: not base64"},
		"wrong len": {base64.StdEncoding.EncodeToString(make([]byte, 16)), "want 32"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParsePrivateKey(tc.encoded); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParsePrivateKey error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSignRejectsWrongLengthKey(t *testing.T) {
	if _, err := Sign([]byte("manifest"), ed25519.PrivateKey([]byte("short"))); err == nil || !strings.Contains(err.Error(), "want 64") {
		t.Fatalf("Sign error = %v, want a key-length error", err)
	}
}

// A rotation can overlap: the build pins old,new while releases are still signed with the
// old key, so clients keep updating across the switch. Every key in the set is validated
// before any is trusted.
func TestVerifyAcceptsAnyTrustedKey(t *testing.T) {
	oldKey, oldPublic := testKey(0x71)
	_, newPublic := testKey(0x72)
	_, otherPublic := testKey(0x73)
	manifest := []byte(VersionPrefix + "v2.0.0\n" + strings.Repeat("b", 64) + "  drive9-linux-amd64\n")
	signature, err := Sign(manifest, oldKey)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	for _, tc := range []struct {
		name    string
		public  string
		wantErr string
	}{
		{name: "single key", public: oldPublic},
		{name: "old first", public: newPublic + "," + oldPublic},
		{name: "new first", public: oldPublic + "," + newPublic},
		{name: "no match", public: newPublic + "," + otherPublic, wantErr: "does not verify"},
		{name: "empty element", public: oldPublic + ",", wantErr: "empty entry in the key list"},
		{name: "malformed element", public: oldPublic + ",not-base64", wantErr: "is not a base64"},
		{name: "empty set", public: " , ", wantErr: "empty entry in the key list"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Verify(manifest, signature, tc.public)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Verify: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Verify error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseManifestShapes(t *testing.T) {
	sumA := strings.Repeat("a", 64)
	sumB := strings.Repeat("b", 64)
	valid := []byte(VersionPrefix + "v3.1.4\n" + sumA + "  drive9-linux-amd64\n" + sumB + " *drive9-darwin-arm64\n")
	manifest, err := ParseManifest(valid)
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if manifest.Version != "v3.1.4" {
		t.Fatalf("version = %q, want v3.1.4", manifest.Version)
	}
	if manifest.Checksums["drive9-linux-amd64"] != sumA || manifest.Checksums["drive9-darwin-arm64"] != sumB {
		t.Fatalf("checksums = %v, want both artifacts (binary marker accepted)", manifest.Checksums)
	}

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "missing version", body: sumA + "  drive9-linux-amd64\n", want: "no \"# drive9-release-version\" line"},
		{name: "empty version", body: VersionPrefix + "\n" + sumA + "  drive9-linux-amd64\n", want: "empty version"},
		{name: "duplicate version", body: VersionPrefix + "v1\n" + VersionPrefix + "v2\n" + sumA + "  a\n", want: "more than one version"},
		{name: "no checksums", body: VersionPrefix + "v1.0.0\n", want: "empty checksum list"},
		{name: "short checksum", body: VersionPrefix + "v1.0.0\nabc  drive9-linux-amd64\n", want: "invalid checksum"},
		{name: "path in name", body: VersionPrefix + "v1.0.0\n" + sumA + "  releases/drive9-linux-amd64\n", want: "must be a release artifact filename"},
		{name: "lookalike version line", body: "# drive9-release-versionX v1\n" + sumA + "  a\n", want: "no \"# drive9-release-version\" line"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseManifest([]byte(tc.body)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseManifest error = %v, want %q", err, tc.want)
			}
		})
	}
}

// The 64-byte private key form must be seed||publicKey, or signing silently produces
// signatures no key can verify.
func TestParsePrivateKeyRejectsMismatchedExpandedForm(t *testing.T) {
	private, _ := testKey(0x81)
	_, otherPublic := testKey(0x82)
	otherRaw, err := base64.StdEncoding.DecodeString(otherPublic)
	if err != nil || len(otherRaw) != ed25519.PublicKeySize {
		t.Fatalf("decode other public key: %v", err)
	}
	mismatched := append(append([]byte(nil), private.Seed()...), otherRaw...)
	if _, err := ParsePrivateKey(base64.StdEncoding.EncodeToString(mismatched)); err == nil || !strings.Contains(err.Error(), "seed||publicKey") {
		t.Fatalf("ParsePrivateKey error = %v, want the seed||publicKey refusal", err)
	}
	// The consistent form still parses.
	if parsed, err := ParsePrivateKey(base64.StdEncoding.EncodeToString(private)); err != nil || !parsed.Equal(private) {
		t.Fatalf("ParsePrivateKey(consistent expanded form) = %v, %v", parsed, err)
	}
}
