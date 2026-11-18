package encrypt

import (
	"bytes"
	"context"
	"testing"
)

func TestLocalAESRejectsInvalidKeyLength(t *testing.T) {
	if _, err := NewLocalAESEncryptor([]byte("short")); err == nil {
		t.Fatal("expected key length error")
	}
}

func TestLocalAESDecryptRejectsTamperedCiphertext(t *testing.T) {
	enc, err := NewLocalAESEncryptor([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := enc.Encrypt(context.Background(), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	ciphertext[len(ciphertext)-1] ^= 0x01
	if _, err := enc.Decrypt(context.Background(), ciphertext); err == nil {
		t.Fatal("expected decrypt failure for tampered ciphertext")
	}
}

// Rotation: a retired key must still decrypt what it wrapped, while new writes
// only the current key can read.
func TestLocalAESEncryptorRotation(t *testing.T) {
	ctx := context.Background()
	oldKey := bytes.Repeat([]byte{0x11}, 32)
	newKey := bytes.Repeat([]byte{0x22}, 32)

	oldEnc, err := NewLocalAESEncryptor(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := oldEnc.Encrypt(ctx, []byte("wrapped before rotation"))
	if err != nil {
		t.Fatal(err)
	}

	rotating, err := NewLocalAESEncryptorWithPrevious(newKey, [][]byte{oldKey})
	if err != nil {
		t.Fatal(err)
	}
	got, err := rotating.Decrypt(ctx, legacy)
	if err != nil {
		t.Fatalf("decrypt legacy value: %v", err)
	}
	if string(got) != "wrapped before rotation" {
		t.Fatalf("plaintext = %q", got)
	}

	fresh, err := rotating.Encrypt(ctx, []byte("wrapped after rotation"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldEnc.Decrypt(ctx, fresh); err == nil {
		t.Fatal("retired key decrypted a value written by the new key")
	}
	newOnly, err := NewLocalAESEncryptor(newKey)
	if err != nil {
		t.Fatal(err)
	}
	if plain, err := newOnly.Decrypt(ctx, fresh); err != nil || string(plain) != "wrapped after rotation" {
		t.Fatalf("new key failed on new value: %q %v", plain, err)
	}

	// A ciphertext no key in the ring matches errors instead of returning junk.
	if _, err := rotating.Decrypt(ctx, append(legacy, 0x00)); err == nil {
		t.Fatal("tampered ciphertext decrypted")
	}
}
