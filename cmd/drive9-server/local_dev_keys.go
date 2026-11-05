package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// EnvLocalDevKeyFile overrides where the local provider keeps its generated
// development keys. The default is <user config dir>/drive9/local-dev-keys.json.
const EnvLocalDevKeyFile = "DRIVE9_LOCAL_DEV_KEY_FILE"

// envLocalDevKeysInUse records that DRIVE9_MASTER_KEY or DRIVE9_TOKEN_SIGNING_KEY
// was filled from the generated dev-key file rather than the environment. The
// non-loopback guard keys off this instead of comparing against a literal, so
// there is no fixed key material anywhere in the binary.
const envLocalDevKeysInUse = "DRIVE9_LOCAL_DEV_KEYS_IN_USE"

// localDevMasterKeyLen is the AES-256 key size the local encryptor expects.
const localDevMasterKeyLen = 32

// localDevKeys is the persisted key material for a
// DRIVE9_TENANT_PROVIDER=local installation.
type localDevKeys struct {
	MasterKeyHex       string `json:"master_key_hex"`
	TokenSigningKeyHex string `json:"token_signing_key_hex"`
}

// localDevKeyFilePath resolves the key file location.
func localDevKeyFilePath() (string, error) {
	if v := strings.TrimSpace(os.Getenv(EnvLocalDevKeyFile)); v != "" {
		return v, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		// Containers often run without HOME. Fall back to a per-uid file in the
		// temp dir: still 0600 and still not shared between users.
		return filepath.Join(os.TempDir(), fmt.Sprintf("drive9-local-dev-keys-%d.json", os.Getuid())), nil
	}
	return filepath.Join(dir, "drive9", "local-dev-keys.json"), nil
}

// loadOrCreateLocalDevKeys returns this installation's local-provider keys,
// generating and persisting them on first use.
//
// No key material lives in source: each role gets a fresh 32-byte random key,
// written once with mode 0600 and reused afterwards, so API keys and encrypted
// tenant data survive a restart. Rotation is deleting the file (or setting
// DRIVE9_MASTER_KEY / DRIVE9_TOKEN_SIGNING_KEY, which take precedence): the next
// start writes a new pair. An unreadable or malformed file fails closed instead
// of silently regenerating, because regenerating would strand existing vault
// data behind a key nobody holds.
func loadOrCreateLocalDevKeys() (localDevKeys, string, error) {
	path, err := localDevKeyFilePath()
	if err != nil {
		return localDevKeys{}, "", err
	}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		var keys localDevKeys
		if unmarshalErr := json.Unmarshal(raw, &keys); unmarshalErr != nil {
			return localDevKeys{}, path, fmt.Errorf(
				"local dev key file %s is malformed (%v); delete it to generate a new pair, or set DRIVE9_MASTER_KEY and DRIVE9_TOKEN_SIGNING_KEY",
				path, unmarshalErr)
		}
		if validateErr := keys.validate(); validateErr != nil {
			return localDevKeys{}, path, fmt.Errorf(
				"local dev key file %s is unusable (%v); delete it to generate a new pair, or set DRIVE9_MASTER_KEY and DRIVE9_TOKEN_SIGNING_KEY",
				path, validateErr)
		}
		return keys, path, nil
	case !errors.Is(err, os.ErrNotExist):
		return localDevKeys{}, path, fmt.Errorf("read local dev key file %s: %w", path, err)
	}

	keys, err := newLocalDevKeys()
	if err != nil {
		return localDevKeys{}, path, err
	}
	if err := writeLocalDevKeys(path, keys); err != nil {
		return localDevKeys{}, path, err
	}
	return keys, path, nil
}

// newLocalDevKeys draws one random key per role.
func newLocalDevKeys() (localDevKeys, error) {
	master, err := randomKeyHex()
	if err != nil {
		return localDevKeys{}, fmt.Errorf("generate local dev master key: %w", err)
	}
	token, err := randomKeyHex()
	if err != nil {
		return localDevKeys{}, fmt.Errorf("generate local dev token signing key: %w", err)
	}
	return localDevKeys{MasterKeyHex: master, TokenSigningKeyHex: token}, nil
}

func randomKeyHex() (string, error) {
	buf := make([]byte, localDevMasterKeyLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// writeLocalDevKeys persists keys with 0600 permissions, creating the directory
// 0700 and replacing the file atomically.
func writeLocalDevKeys(path string, keys localDevKeys) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create local dev key dir %s: %w", dir, err)
		}
	}
	raw, err := json.MarshalIndent(keys, "", "  ")
	if err != nil {
		return fmt.Errorf("encode local dev keys: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".local-dev-keys-*.tmp")
	if err != nil {
		return fmt.Errorf("create local dev key file in %s: %w", filepath.Dir(path), err)
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("set local dev key permissions: %w", err)
	}
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write local dev keys: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close local dev keys: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("install local dev key file %s: %w", path, err)
	}
	tmpName = ""
	return nil
}

func (k localDevKeys) validate() error {
	if err := validateKeyHex("master_key_hex", k.MasterKeyHex); err != nil {
		return err
	}
	if err := validateKeyHex("token_signing_key_hex", k.TokenSigningKeyHex); err != nil {
		return err
	}
	if strings.EqualFold(k.MasterKeyHex, k.TokenSigningKeyHex) {
		return errors.New("master and token signing keys are identical")
	}
	return nil
}

func validateKeyHex(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s is empty", field)
	}
	if len(value) != localDevMasterKeyLen*2 {
		return fmt.Errorf("%s must be %d hex characters, got %d", field, localDevMasterKeyLen*2, len(value))
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s is not hex: %w", field, err)
	}
	return nil
}
