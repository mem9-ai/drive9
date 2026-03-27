package tenant

import (
	"crypto/rand"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := "my-secret-password"
	ct, err := enc.Encrypt([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}

	got, err := enc.Decrypt(ct)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != plaintext {
		t.Errorf("got %q, want %q", got, plaintext)
	}
}

func TestEncryptorBadKeySize(t *testing.T) {
	_, err := NewEncryptor([]byte("too-short"))
	if err == nil {
		t.Error("expected error for short key")
	}
}

func TestDecryptBadCiphertext(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	_, err := enc.Decrypt([]byte("short"))
	if err == nil {
		t.Error("expected error for short ciphertext")
	}
}

func TestDecryptWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	enc1, _ := NewEncryptor(key1)
	enc2, _ := NewEncryptor(key2)

	ct, _ := enc1.Encrypt([]byte("secret"))
	_, err := enc2.Decrypt(ct)
	if err == nil {
		t.Error("expected error decrypting with wrong key")
	}
}
