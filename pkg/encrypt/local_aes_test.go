package encrypt

import (
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
