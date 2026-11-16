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
		{name: "no build key", manifest: manifest, signature: signature, public: "", want: "carries no release-signing key"},
		{name: "malformed build key", manifest: manifest, signature: signature, public: "not-base64", want: "invalid release-signing key"},
		{name: "short build key", manifest: manifest, signature: signature, public: base64.StdEncoding.EncodeToString([]byte("short")), want: "invalid release-signing key"},
		{name: "missing signature", manifest: manifest, signature: []byte("  \n"), public: public, want: "missing or empty"},
		{name: "non-base64 signature", manifest: manifest, signature: []byte("!!!"), public: public, want: "not valid base64"},
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
		"empty":     {"", "is empty"},
		"not b64":   {"!!!", "not valid base64"},
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
