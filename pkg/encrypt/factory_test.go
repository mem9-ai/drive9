package encrypt

import (
	"context"
	"testing"
)

func TestFactoryLocalAESRoundTrip(t *testing.T) {
	enc, err := New(context.Background(), Config{Type: TypeLocalAES, Key: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"})
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := enc.Encrypt(context.Background(), []byte("secret-value"))
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := enc.Decrypt(context.Background(), ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "secret-value" {
		t.Fatalf("unexpected plaintext: %q", string(plaintext))
	}
}

func TestFactoryRejectsUnsupportedType(t *testing.T) {
	_, err := New(context.Background(), Config{Type: Type("unknown"), Key: "abc"})
	if err == nil {
		t.Fatal("expected unsupported type error")
	}
}

func TestFactoryKMSEncryptorRequiresKey(t *testing.T) {
	_, err := New(context.Background(), Config{Type: TypeKMS, Region: "us-east-1"})
	if err == nil {
		t.Fatal("expected kms key validation error")
	}
}
