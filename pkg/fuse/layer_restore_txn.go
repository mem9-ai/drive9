package fuse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	layerRestoreTxnPrefix    = ".layer-restore-txn-"
	layerRestoreTxnSuffix    = ".json"
	layerRestoreBackupMarker = ".layer-restore-backup-"
)

var nextLayerRestoreTxnID atomic.Uint64

var (
	errLayerRestoreStateUncertain = errors.New("layer restore local state is uncertain")
	errLayerRestoreRecoveryFailed = errors.New("layer restore recovery failed")
)

type layerRestoreFileSnapshot struct {
	Path       string `json:"path"`
	BackupPath string `json:"backup_path,omitempty"`
	Existed    bool   `json:"existed"`
}

type layerRestoreTxnRecord struct {
	Version int                        `json:"version"`
	Files   []layerRestoreFileSnapshot `json:"files"`
}

// layerRestoreTxn is a durable rollback record for the two-file layer cache
// commit (shadow content plus pending metadata). Layer data is authoritative on
// the server, but the local pair must still be old-or-new across an I/O error or
// process crash: a mixed pair can be mistaken for an unfinished local write.
//
// The marker is the commit record. While it exists, startup always restores the
// snapshots. A successful caller fsyncs both new files before removing the
// marker, making marker removal the final commit point. Backup files are then
// best-effort garbage; the normal commit/rollback paths remove them.
type layerRestoreTxn struct {
	mu         sync.Mutex
	markerPath string
	record     layerRestoreTxnRecord
	finished   bool
	// removeMarker is a per-transaction test seam. Production transactions
	// leave it nil and use removeLayerRestoreMarker. Keeping the seam on the
	// transaction (rather than package-global) makes failure injection safe for
	// parallel tests and lets us exercise both remove and directory-fsync
	// failures at the actual commit point.
	removeMarker func(string) error
}

func beginLayerRestoreTxn(pendingDir string, paths ...string) (*layerRestoreTxn, error) {
	uniq := make(map[string]struct{}, len(paths))
	cleaned := make([]string, 0, len(paths))
	for _, p := range paths {
		p = filepath.Clean(strings.TrimSpace(p))
		if p == "." || p == "" {
			continue
		}
		if _, ok := uniq[p]; ok {
			continue
		}
		uniq[p] = struct{}{}
		cleaned = append(cleaned, p)
	}
	sort.Strings(cleaned)
	id := fmt.Sprintf("%d-%d", os.Getpid(), nextLayerRestoreTxnID.Add(1))
	record := layerRestoreTxnRecord{Version: 1, Files: make([]layerRestoreFileSnapshot, 0, len(cleaned))}
	for i, p := range cleaned {
		snap := layerRestoreFileSnapshot{Path: p}
		st, err := os.Stat(p)
		switch {
		case err == nil:
			if !st.Mode().IsRegular() {
				cleanupLayerRestoreBackups(record.Files)
				return nil, fmt.Errorf("layer restore snapshot %s: not a regular file", p)
			}
			snap.Existed = true
			snap.BackupPath = fmt.Sprintf("%s%s%s-%d", p, layerRestoreBackupMarker, id, i)
			if err := snapshotLayerRestoreFile(p, snap.BackupPath, st.Mode().Perm()); err != nil {
				cleanupLayerRestoreBackups(record.Files)
				return nil, fmt.Errorf("layer restore snapshot %s: %w", p, err)
			}
		case errors.Is(err, os.ErrNotExist):
			// Absence is part of the old state and must be restored by removal.
		default:
			cleanupLayerRestoreBackups(record.Files)
			return nil, fmt.Errorf("layer restore stat %s: %w", p, err)
		}
		record.Files = append(record.Files, snap)
	}
	markerPath := filepath.Join(pendingDir, layerRestoreTxnPrefix+id+layerRestoreTxnSuffix)
	body, err := json.Marshal(record)
	if err != nil {
		cleanupLayerRestoreBackups(record.Files)
		return nil, fmt.Errorf("layer restore transaction marshal: %w", err)
	}
	if err := atomicWrite(markerPath, body); err != nil {
		// atomicWrite may have installed the marker and only then failed its
		// directory fsync. In that ambiguous case the backups must remain: a
		// later startup can safely roll back the still-unchanged old files.
		if _, statErr := os.Stat(markerPath); errors.Is(statErr, os.ErrNotExist) {
			cleanupLayerRestoreBackups(record.Files)
			return nil, fmt.Errorf("layer restore transaction persist: %w", err)
		}
		// atomicWrite renamed the marker but could not prove the parent
		// directory durable. That marker may survive and roll back a successor
		// writer after restart, so the live mount must enter the same fail-closed
		// state as a failed transaction rollback.
		return nil, errors.Join(
			fmt.Errorf("layer restore transaction persist: %w", err),
			errLayerRestoreStateUncertain,
		)
	}
	return &layerRestoreTxn{markerPath: markerPath, record: record}, nil
}

func snapshotLayerRestoreFile(path, backupPath string, mode os.FileMode) error {
	_ = os.Remove(backupPath)
	if err := os.Link(path, backupPath); err != nil {
		if err := copyLayerRestoreFile(path, backupPath, mode); err != nil {
			return err
		}
	}
	return fsyncDir(filepath.Dir(path))
}

func copyLayerRestoreFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(dst)
		return err
	}
	return nil
}

func replaceLayerRestoreFileFromBackup(snap layerRestoreFileSnapshot) error {
	if !snap.Existed {
		if err := os.Remove(snap.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return fsyncDir(filepath.Dir(snap.Path))
	}
	if snap.BackupPath == "" {
		return errors.New("missing layer restore backup path")
	}
	st, err := os.Stat(snap.BackupPath)
	if err != nil {
		return fmt.Errorf("stat backup: %w", err)
	}
	tmp := fmt.Sprintf("%s.rollback-%d", snap.Path, nextLayerRestoreTxnID.Add(1))
	_ = os.Remove(tmp)
	if err := os.Link(snap.BackupPath, tmp); err != nil {
		if err := copyLayerRestoreFile(snap.BackupPath, tmp, st.Mode().Perm()); err != nil {
			return fmt.Errorf("copy backup: %w", err)
		}
	}
	if err := os.Rename(tmp, snap.Path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("restore backup: %w", err)
	}
	return fsyncDir(filepath.Dir(snap.Path))
}

func (tx *layerRestoreTxn) rollback() error {
	if tx == nil {
		return nil
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.finished {
		return nil
	}
	for _, snap := range tx.record.Files {
		if err := replaceLayerRestoreFileFromBackup(snap); err != nil {
			// Keep the marker and every backup. Startup will retry the rollback
			// before PendingIndex exposes any recovered metadata.
			return fmt.Errorf("layer restore rollback %s: %w", snap.Path, err)
		}
	}
	if err := tx.removeDurableMarker(); err != nil {
		return err
	}
	cleanupLayerRestoreBackups(tx.record.Files)
	tx.finished = true
	return nil
}

func (tx *layerRestoreTxn) commit() error {
	if tx == nil {
		return nil
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.finished {
		return nil
	}
	// Removing and syncing the marker is the final commit point. Backups are
	// deliberately removed afterwards so a crash can never leave a marker
	// whose rollback material has already been discarded.
	if err := tx.removeDurableMarker(); err != nil {
		return err
	}
	cleanupLayerRestoreBackups(tx.record.Files)
	tx.finished = true
	return nil
}

func (tx *layerRestoreTxn) removeDurableMarker() error {
	if tx.removeMarker != nil {
		return tx.removeMarker(tx.markerPath)
	}
	return removeLayerRestoreMarker(tx.markerPath)
}

// rollbackLayerRestoreFailure keeps the original failure while marking a
// second rollback failure as a mount-wide fail-closed condition. The caller
// still holds the pending/shadow path locks, so no successor writer can
// publish between the failed commit point and this rollback attempt.
func rollbackLayerRestoreFailure(tx *layerRestoreTxn, primary error) error {
	if rollbackErr := tx.rollback(); rollbackErr != nil {
		return errors.Join(primary, fmt.Errorf("%w: %v", errLayerRestoreStateUncertain, rollbackErr))
	}
	return primary
}

func removeLayerRestoreMarker(markerPath string) error {
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove layer restore transaction: %w", err)
	}
	if err := fsyncDir(filepath.Dir(markerPath)); err != nil {
		return fmt.Errorf("sync layer restore transaction dir: %w", err)
	}
	return nil
}

func cleanupLayerRestoreBackups(files []layerRestoreFileSnapshot) {
	dirs := make(map[string]struct{})
	for _, snap := range files {
		if snap.BackupPath == "" {
			continue
		}
		_ = os.Remove(snap.BackupPath)
		dirs[filepath.Dir(snap.BackupPath)] = struct{}{}
	}
	for dir := range dirs {
		_ = fsyncDir(dir)
	}
}

func recoverLayerRestoreTransactions(pendingDir string) error {
	entries, err := os.ReadDir(pendingDir)
	if err != nil {
		return err
	}
	txns := make([]*layerRestoreTxn, 0)
	owners := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, layerRestoreTxnPrefix) || !strings.HasSuffix(name, layerRestoreTxnSuffix) {
			continue
		}
		markerPath := filepath.Join(pendingDir, name)
		raw, err := os.ReadFile(markerPath)
		if err != nil {
			return fmt.Errorf("read layer restore transaction %s: %w", markerPath, err)
		}
		var record layerRestoreTxnRecord
		if err := json.Unmarshal(raw, &record); err != nil || record.Version != 1 {
			if err == nil {
				err = fmt.Errorf("unsupported version %d", record.Version)
			}
			return fmt.Errorf("decode layer restore transaction %s: %w", markerPath, err)
		}
		for _, snap := range record.Files {
			cleanPath := filepath.Clean(snap.Path)
			if owner, exists := owners[cleanPath]; exists {
				// One current implementation can never start a second transaction
				// for a path while the first still owns its path lock. Overlap means
				// an older/crashed implementation left ambiguous rollback order;
				// applying either snapshot first could destroy the other generation.
				return fmt.Errorf("overlapping layer restore transactions %s and %s for %s", owner, markerPath, cleanPath)
			}
			owners[cleanPath] = markerPath
		}
		txns = append(txns, &layerRestoreTxn{markerPath: markerPath, record: record})
	}
	// Decode and conflict-check every marker before modifying any file. This
	// makes malformed/multi-marker startup deterministic and fail-closed rather
	// than partially rolling back one generation before discovering another.
	for _, tx := range txns {
		if err := tx.rollback(); err != nil {
			return fmt.Errorf("recover %s: %w", tx.markerPath, err)
		}
	}
	return nil
}
