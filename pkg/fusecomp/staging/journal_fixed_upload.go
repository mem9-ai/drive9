package staging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// PrepareFixedUpload durably records an immutable completion request BEFORE
// publication. payload belongs to the uploader; it must include the original
// work binding and local generation. The caller owns globally unique operation
// IDs and must never reuse one, even after settlement. The WAL detects conflicts
// only while their records remain; compaction discards settled identity history.
// This record survives later path writes, unlink markers and WAL compaction.
func (j *Journal) PrepareFixedUpload(operationID, path string, payload []byte) error {
	if viewprotocol.ValidateOperationID(operationID) != nil || path == "" || len(payload) == 0 || len(payload) > 64<<10 || !json.Valid(payload) {
		return fmt.Errorf("invalid fixed upload recovery record")
	}
	j.fixedMu.Lock()
	defer j.fixedMu.Unlock()
	pending, settled, err := j.fixedUploadState()
	if err != nil {
		return err
	}
	if settled[operationID] {
		return fmt.Errorf("fixed upload recovery identity already settled")
	}
	for _, previous := range pending {
		if previous.OperationID != operationID {
			continue
		}
		if previous.Path != path || !bytes.Equal(previous.Meta, payload) {
			return fmt.Errorf("fixed upload recovery identity reused")
		}
		if err := j.FsyncShared(); err != nil {
			return err
		}
		return fsyncDir(filepath.Dir(j.path))
	}
	if err := j.Append(JournalEntry{Op: JournalFixedUpload, Path: path, OperationID: operationID, Meta: payload}); err != nil {
		return err
	}
	if err := j.FsyncShared(); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(j.path))
}

// SettleFixedUpload retires recovery only after the caller has durably settled
// the exact local generation. A remote success alone is not enough: cleanup may
// fail, in which case the original completion must remain available for replay.
func (j *Journal) SettleFixedUpload(operationID string) error {
	if viewprotocol.ValidateOperationID(operationID) != nil {
		return fmt.Errorf("invalid fixed upload operation ID")
	}
	j.fixedMu.Lock()
	defer j.fixedMu.Unlock()
	pending, err := j.PendingFixedUploads()
	if err != nil {
		return err
	}
	for _, entry := range pending {
		if entry.OperationID != operationID {
			continue
		}
		if err := j.Append(JournalEntry{Op: JournalFixedUploadDone, Path: entry.Path, OperationID: operationID}); err != nil {
			return err
		}
		return j.FsyncShared()
	}
	// An earlier settlement may have reached the WAL but failed its fsync.
	return j.FsyncShared()
}

// PendingFixedUploads returns unresolved requests in journal order, independent
// of the current pending-file index. The returned bytes are caller-owned.
// Damaged or incomplete frames fail closed and retain the WAL for repair; this
// API does not guess whether a truncated tail was acknowledged before a crash.
func (j *Journal) PendingFixedUploads() ([]JournalEntry, error) {
	entries, _, err := j.fixedUploadState()
	return entries, err
}

func (j *Journal) fixedUploadState() ([]JournalEntry, map[string]bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, nil, os.ErrClosed
	}
	data, err := os.ReadFile(j.path)
	if err != nil {
		return nil, nil, err
	}
	return pendingFixedUploads(data)
}

func pendingFixedUploads(data []byte) ([]JournalEntry, map[string]bool, error) {
	pending := make(map[string]JournalEntry)
	settled := make(map[string]bool)
	var recordErr error
	corrupt := scanJournalFrames(data, func(entry JournalEntry, _ []byte) {
		if recordErr != nil || entry.Op != JournalFixedUpload && entry.Op != JournalFixedUploadDone {
			return
		}
		if viewprotocol.ValidateOperationID(entry.OperationID) != nil || entry.Path == "" {
			recordErr = fmt.Errorf("invalid fixed upload journal identity")
			return
		}
		previous, exists := pending[entry.OperationID]
		if entry.Op == JournalFixedUploadDone {
			if exists && previous.Path != entry.Path {
				recordErr = fmt.Errorf("fixed upload settlement path mismatch")
				return
			}
			delete(pending, entry.OperationID)
			settled[entry.OperationID] = true
			return
		}
		if settled[entry.OperationID] || len(entry.Meta) == 0 || len(entry.Meta) > 64<<10 || !json.Valid(entry.Meta) || exists && (previous.Path != entry.Path || !bytes.Equal(previous.Meta, entry.Meta)) {
			recordErr = fmt.Errorf("invalid or changed fixed upload recovery request")
			return
		}
		pending[entry.OperationID] = entry
	})
	if corrupt {
		return nil, nil, fmt.Errorf("corrupt fixed upload journal")
	}
	if recordErr != nil {
		return nil, nil, recordErr
	}
	entries := make([]JournalEntry, 0, len(pending))
	for _, entry := range pending {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, k int) bool { return entries[i].Seq < entries[k].Seq })
	return entries, settled, nil
}
