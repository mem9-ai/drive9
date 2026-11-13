package staging

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

const fixedShadowSuffix = ".fixed-upload"

// EnableFixedUploadRecovery is called before a fixed mount accepts writes.
// Ordinary main stores never enable these markers or perform their disk I/O.
func (s *ShadowStore) EnableFixedUploadRecovery() error {
	s.fixedMu.Lock()
	defer s.fixedMu.Unlock()
	if s.fixedEnabled.Load() {
		return nil
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	protected := map[string]bool{}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), fixedShadowSuffix) {
			if entry.IsDir() {
				return fmt.Errorf("invalid fixed shadow marker")
			}
			protected[strings.TrimSuffix(entry.Name(), fixedShadowSuffix)] = true
		}
	}
	s.fixedProtected = protected
	s.fixedEnabled.Store(true)
	return nil
}

// invalidateFixedShadow runs under the shadow path lock, BEFORE a mutation.
// The removal is durable before any newer bytes can use the same disk pathname.
func (s *ShadowStore) invalidateFixedShadow(path string) error {
	if !s.fixedEnabled.Load() {
		return nil
	}
	s.fixedMu.Lock()
	defer s.fixedMu.Unlock()
	hash := HashPath(path)
	if !s.fixedProtected[hash] {
		return nil
	}
	if err := os.Remove(filepath.Join(s.dir, hash+fixedShadowSuffix)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := fsyncDir(s.dir); err != nil {
		return err
	}
	delete(s.fixedProtected, hash)
	return nil
}

// ProtectFixedShadow saves a random identity for the exact uploaded source.
// Process-local content generations are checked here but are never used after
// reopening the store. Mutations invalidate this token before changing bytes.
func (s *ShadowStore) ProtectFixedShadow(path string, generation uint64) (string, error) {
	if err := s.EnableFixedUploadRecovery(); err != nil {
		return "", err
	}
	pl := s.acquirePathLock(path)
	defer s.releasePathLock(path, pl)
	s.mu.RLock()
	matches := generation != 0 && s.writeGen[path] == generation
	s.mu.RUnlock()
	if !matches {
		return "", fmt.Errorf("fixed shadow generation changed")
	}
	s.fixedMu.Lock()
	defer s.fixedMu.Unlock()
	hash := HashPath(path)
	file := filepath.Join(s.dir, hash+fixedShadowSuffix)
	token := ""
	if s.fixedProtected[hash] {
		raw, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		token = string(raw)
		if viewprotocol.ValidateOperationID(token) != nil {
			return "", fmt.Errorf("invalid fixed shadow marker")
		}
	} else {
		token = "shadow-" + rand.Text()
	}
	// Mark it before the save: a failed directory fsync can leave a renamed file.
	s.fixedProtected[hash] = true
	if err := atomicWrite(file, []byte(token)); err != nil {
		return "", err
	}
	return token, nil
}

// RetireFixedShadow checks a persisted token and durably unlinks only its
// original bytes. A newer image returns false without changing its state.
// The caller must preserve its publication record on any error.
func (s *ShadowStore) RetireFixedShadow(path, token string) (bool, error) {
	if viewprotocol.ValidateOperationID(token) != nil {
		return false, fmt.Errorf("invalid fixed shadow identity")
	}
	if err := s.EnableFixedUploadRecovery(); err != nil {
		return false, err
	}
	pl := s.acquirePathLock(path)
	defer s.releasePathLock(path, pl)
	s.fixedMu.Lock()
	defer s.fixedMu.Unlock()
	hash := HashPath(path)
	marker := filepath.Join(s.dir, hash+fixedShadowSuffix)
	raw, err := os.ReadFile(marker)
	if errors.Is(err, os.ErrNotExist) {
		if _, statErr := os.Stat(s.shadowPath(path)); errors.Is(statErr, os.ErrNotExist) {
			return true, fsyncDir(s.dir)
		} else if statErr != nil {
			return false, statErr
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if viewprotocol.ValidateOperationID(string(raw)) != nil {
		return false, fmt.Errorf("invalid fixed shadow marker")
	}
	if string(raw) != token {
		return false, nil
	}
	if err := os.Remove(s.shadowPath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := fsyncDir(s.dir); err != nil {
		return false, err
	}
	// Pinned fds remain available in memory after unlink; a restart has no pins.
	s.mu.Lock()
	if recovered := s.files["__hash:"+hash]; recovered != nil {
		if current := s.files[path]; current == nil {
			s.files[path] = recovered
		} else if current != recovered {
			_ = recovered.fd.Close()
		}
		delete(s.files, "__hash:"+hash)
	}
	cleanup, _ := s.removeCoreLocked(path, 0, shadowRetiredInvalidated)
	s.mu.Unlock()
	// The disk name is already durably absent; only retire bookkeeping remains.
	cleanup.immediatePath = ""
	if cleanup.retired != nil {
		cleanup.retired.diskPath = ""
		close(cleanup.retired.cleanupDone)
	}
	if cleanup.removedSize > 0 {
		s.pendingBytes.Add(-cleanup.removedSize)
	}
	if err := os.Remove(marker); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := fsyncDir(s.dir); err != nil {
		return false, err
	}
	delete(s.fixedProtected, hash)
	return true, nil
}

// ReleaseFixedShadowAttempt allows a new initiation only after confirmed remote
// cancellation. A token belonging to newer bytes is left untouched.
func (s *ShadowStore) ReleaseFixedShadowAttempt(path, token string) error {
	if viewprotocol.ValidateOperationID(token) != nil {
		return fmt.Errorf("invalid fixed shadow identity")
	}
	if err := s.EnableFixedUploadRecovery(); err != nil {
		return err
	}
	pl := s.acquirePathLock(path)
	defer s.releasePathLock(path, pl)
	s.fixedMu.Lock()
	defer s.fixedMu.Unlock()
	hash := HashPath(path)
	file := filepath.Join(s.dir, hash+fixedShadowSuffix)
	raw, err := os.ReadFile(file)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(raw) != token {
		return nil
	}
	if err := os.Remove(file); err != nil {
		return err
	}
	if err := fsyncDir(s.dir); err != nil {
		return err
	}
	delete(s.fixedProtected, hash)
	return nil
}
