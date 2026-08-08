package mountstate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ExitReason records why a mount worker last exited.
type ExitReason struct {
	Reason       string    `json:"reason"`
	Detail       string    `json:"detail,omitempty"`
	Code         int       `json:"code"`
	PID          int       `json:"pid,omitempty"`
	PendingFiles int       `json:"pending_files,omitempty"`
	PendingBytes int64     `json:"pending_bytes,omitempty"`
	At           time.Time `json:"at"`
}

func ExitReasonPath(mountPoint string) string {
	canonical := canonicalMountPoint(mountPoint)
	return filepath.Join(os.TempDir(), "drive9-mount-"+hash8(canonical)+".exit.json")
}

func WriteExitReason(mountPoint string, rec ExitReason) error {
	if rec.At.IsZero() {
		rec.At = time.Now().UTC()
	} else {
		rec.At = rec.At.UTC()
	}
	path := ExitReasonPath(mountPoint)
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal exit reason: %w", err)
	}
	return writeFileAtomic(path, append(data, '\n'), 0o600)
}

func ReadExitReason(mountPoint string) (ExitReason, string, error) {
	path := ExitReasonPath(mountPoint)
	data, err := os.ReadFile(path)
	if err != nil {
		return ExitReason{}, path, err
	}
	var rec ExitReason
	if err := json.Unmarshal(data, &rec); err != nil {
		return ExitReason{}, path, fmt.Errorf("read exit reason %s: %w", path, err)
	}
	return rec, path, nil
}

func ClearExitReason(mountPoint string) error {
	path := ExitReasonPath(mountPoint)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
