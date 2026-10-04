package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const runtimeReceiptVersion = 1

var runtimeReceiptDirectory = func() string {
	dir := configDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "runtime-receipts")
}

type runtimeReceipt struct {
	Version        int    `json:"version"`
	Endpoint       string `json:"endpoint"`
	IdempotencyKey string `json:"idempotency_key"`
	RequestSHA256  string `json:"request_sha256"`
}

type runtimeReceiptState struct {
	path     string
	receipt  runtimeReceipt
	existing bool
}

func prepareRuntimeReceiptKey(explicitKey, receiptPath string) (runtimeReceiptState, error) {
	if strings.TrimSpace(explicitKey) != "" && strings.TrimSpace(receiptPath) != "" {
		return runtimeReceiptState{}, fmt.Errorf("--idempotency-key and --idempotency-receipt are mutually exclusive")
	}
	if strings.TrimSpace(receiptPath) != "" {
		receipt, err := loadRuntimeReceipt(receiptPath)
		if err == nil {
			return runtimeReceiptState{path: receiptPath, receipt: receipt, existing: true}, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return runtimeReceiptState{}, err
		}
		key, keyErr := runtimeKey("")
		if keyErr != nil {
			return runtimeReceiptState{}, keyErr
		}
		return runtimeReceiptState{path: receiptPath, receipt: runtimeReceipt{Version: runtimeReceiptVersion, IdempotencyKey: key}}, nil
	}
	key, err := runtimeKey(explicitKey)
	if err != nil {
		return runtimeReceiptState{}, err
	}
	state := runtimeReceiptState{receipt: runtimeReceipt{Version: runtimeReceiptVersion, IdempotencyKey: key}}
	if strings.TrimSpace(explicitKey) == "" {
		dir := runtimeReceiptDirectory()
		if dir == "" {
			return runtimeReceiptState{}, fmt.Errorf("cannot determine Runtime receipt directory")
		}
		state.path = filepath.Join(dir, key+".json")
	}
	return state, nil
}

func (s *runtimeReceiptState) persist(endpoint string, request any) error {
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode Runtime request receipt: %w", err)
	}
	requestHash := sha256.Sum256(requestJSON)
	wantHash := hex.EncodeToString(requestHash[:])
	if s.existing {
		if s.receipt.Version != runtimeReceiptVersion || s.receipt.Endpoint != endpoint || s.receipt.RequestSHA256 != wantHash {
			return fmt.Errorf("idempotency receipt %q does not match this Runtime request", s.path)
		}
		return nil
	}
	if s.path == "" {
		return nil
	}
	s.receipt.Endpoint = endpoint
	s.receipt.RequestSHA256 = wantHash
	return writeRuntimeReceipt(s.path, s.receipt)
}

func (s runtimeReceiptState) complete() error {
	if s.path == "" {
		return nil
	}
	return removeRuntimeReceipt(s.path)
}

func (s runtimeReceiptState) submitError(err error) error {
	if s.path == "" {
		return err
	}
	return fmt.Errorf("%w; idempotency receipt retained at %q, retry the identical request with --idempotency-receipt %q", err, s.path, s.path)
}

func loadRuntimeReceipt(path string) (runtimeReceipt, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return runtimeReceipt{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return runtimeReceipt{}, fmt.Errorf("idempotency receipt %q must be a regular owner-only file", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return runtimeReceipt{}, err
	}
	var receipt runtimeReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		return runtimeReceipt{}, fmt.Errorf("decode idempotency receipt %q: %w", path, err)
	}
	if receipt.Version != runtimeReceiptVersion || strings.TrimSpace(receipt.IdempotencyKey) == "" || strings.TrimSpace(receipt.Endpoint) == "" || len(receipt.RequestSHA256) != sha256.Size*2 {
		return runtimeReceipt{}, fmt.Errorf("idempotency receipt %q is invalid", path)
	}
	return receipt, nil
}

func writeRuntimeReceipt(path string, receipt runtimeReceipt) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create Runtime receipt directory: %w", err)
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create Runtime receipt %q: %w", path, err)
	}
	remove := true
	defer func() {
		_ = file.Close()
		if remove {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write Runtime receipt %q: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync Runtime receipt %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close Runtime receipt %q: %w", path, err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open Runtime receipt directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync Runtime receipt directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close Runtime receipt directory: %w", err)
	}
	remove = false
	return nil
}

func removeRuntimeReceipt(path string) error {
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remove accepted Runtime receipt %q: %w", path, err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open Runtime receipt directory after removing %q: %w", path, err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync Runtime receipt directory after removing %q: %w", path, err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close Runtime receipt directory after removing %q: %w", path, err)
	}
	return nil
}
