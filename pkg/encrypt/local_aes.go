package encrypt

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

type LocalAESEncryptor struct {
	gcm cipher.AEAD
	// previous holds retired master keys. Decryption tries the primary key and
	// then each retired key, so a rotation (new DRIVE9_MASTER_KEY plus the old
	// key in DRIVE9_MASTER_KEY_PREVIOUS) reads existing ciphertext while every
	// new write uses the primary. Without this there was no way to retire a
	// leaked master key without losing the data it protects.
	previous []cipher.AEAD
}

func NewLocalAESEncryptor(masterKey []byte) (*LocalAESEncryptor, error) {
	if len(masterKey) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes")
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &LocalAESEncryptor{gcm: gcm}, nil
}

func (e *LocalAESEncryptor) Encrypt(_ context.Context, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return e.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func (e *LocalAESEncryptor) Decrypt(_ context.Context, ciphertext []byte) ([]byte, error) {
	ns := e.gcm.NonceSize()
	if len(ciphertext) < ns {
		return nil, fmt.Errorf("ciphertext too short")
	}
	if plain, err := e.gcm.Open(nil, ciphertext[:ns], ciphertext[ns:], nil); err == nil {
		return plain, nil
	}
	for _, gcm := range e.previous {
		if plain, err := gcm.Open(nil, ciphertext[:ns], ciphertext[ns:], nil); err == nil {
			return plain, nil
		}
	}
	return nil, fmt.Errorf("decrypt: no master key matches this ciphertext")
}

// NewLocalAESEncryptorWithPrevious creates a rotating encryptor: writes use
// primary, reads also try previous (retired) keys. Rotation is then a two-step
// deployment — add the old key to the previous list, then drop it once every
// stored value has been rewritten.
func NewLocalAESEncryptorWithPrevious(primary []byte, previous [][]byte) (*LocalAESEncryptor, error) {
	enc, err := NewLocalAESEncryptor(primary)
	if err != nil {
		return nil, err
	}
	for _, key := range previous {
		if len(key) != 32 {
			return nil, fmt.Errorf("previous master key must be 32 bytes, got %d", len(key))
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, fmt.Errorf("previous master key: %w", err)
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("previous master key: %w", err)
		}
		enc.previous = append(enc.previous, gcm)
	}
	return enc, nil
}
