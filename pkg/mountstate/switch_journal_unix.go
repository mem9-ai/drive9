//go:build !windows

package mountstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pingcap/failpoint"
	"golang.org/x/sys/unix"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

const maxSwitchRecordLen = 64 << 10

type switchJournalRecord struct {
	Type           string                     `json:"type"`
	MountID        string                     `json:"mount_id,omitempty"`
	Scope          string                     `json:"scope,omitempty"`
	OperationID    string                     `json:"operation_id,omitempty"`
	FromGeneration uint64                     `json:"from_generation,omitempty"`
	Generation     uint64                     `json:"generation,omitempty"`
	From           viewprotocol.SwitchBinding `json:"from,omitempty"`
	Target         viewprotocol.SwitchBinding `json:"target,omitempty"`
	Unbind         *PendingUnbind             `json:"unbind,omitempty"`
}

// SwitchJournal persists local mount view bindings, pending switch decisions,
// and bounded pending_unbinds records with fail-closed fsync semantics.
type SwitchJournal struct {
	mu             sync.Mutex
	file           *os.File
	mountID        string
	scope          string
	generation     uint64
	binding        viewprotocol.SwitchBinding
	pending        *SwitchReceipt
	pendingUnbinds []PendingUnbind
	history        map[string]SwitchReceipt
	abortedPending bool
	uncertain      bool
}

func OpenSwitchJournal(path, mountID, scope string, initial viewprotocol.SwitchBinding) (*SwitchJournal, error) {
	if !filepath.IsAbs(path) || scope == "" {
		return nil, fmt.Errorf("%w: invalid journal path or scope", ErrSwitchInvalid)
	}
	if err := viewprotocol.ValidateOperationID(mountID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSwitchInvalid, err)
	}
	if err := initial.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSwitchInvalid, err)
	}
	return openSwitchJournalFile(path, mountID, scope, initial, true)
}

func OpenExistingSwitchJournal(path string) (*SwitchJournal, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("%w: path must be absolute", ErrSwitchInvalid)
	}
	return openSwitchJournalFile(path, "", "", viewprotocol.SwitchBinding{}, false)
}

func openSwitchJournalFile(path, mountID, scope string, initial viewprotocol.SwitchBinding, create bool) (*SwitchJournal, error) {
	flags := os.O_RDWR | unix.O_NOFOLLOW
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
		}
	}()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock switch journal: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%w: unsafe switch journal mode %v", ErrSwitchCorrupt, st.Mode())
	}
	j := &SwitchJournal{
		file:    f,
		history: make(map[string]SwitchReceipt),
	}
	if st.Size() == 0 {
		if !create {
			return nil, fmt.Errorf("%w: empty switch journal", ErrSwitchCorrupt)
		}
		if err := j.appendAndApplyLocked(switchJournalRecord{
			Type:       "init",
			MountID:    mountID,
			Scope:      scope,
			Generation: 1,
			Target:     initial,
		}); err != nil {
			return nil, err
		}
		ok = true
		return j, nil
	}
	if err := j.replayFileLocked(); err != nil {
		return nil, err
	}
	if mountID != "" && j.mountID != mountID {
		return nil, fmt.Errorf("%w: journal mount_id %q != %q", ErrSwitchConflict, j.mountID, mountID)
	}
	if scope != "" && j.scope != scope {
		return nil, fmt.Errorf("%w: journal scope mismatch", ErrSwitchConflict)
	}
	ok = true
	return j, nil
}

func (j *SwitchJournal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.file == nil {
		return nil
	}
	err := j.file.Close()
	j.file = nil
	return err
}

func (j *SwitchJournal) appendAndApplyLocked(rec switchJournalRecord) error {
	if j.uncertain {
		return ErrSwitchUncertain
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > maxSwitchRecordLen {
		return fmt.Errorf("%w: record size %d out of bounds", ErrSwitchInvalid, len(payload))
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	sum := sha256.Sum256(payload)
	frame := make([]byte, 0, 4+len(payload)+sha256.Size)
	frame = append(frame, hdr[:]...)
	frame = append(frame, payload...)
	frame = append(frame, sum[:]...)

	var fpErr error
	failpoint.InjectCall("switchJournalBeforeSync", &fpErr)
	if fpErr != nil {
		j.uncertain = true
		return fmt.Errorf("%w: %v", ErrSwitchUncertain, fpErr)
	}
	if _, err := j.file.Write(frame); err != nil {
		j.uncertain = true
		return fmt.Errorf("%w: write: %v", ErrSwitchUncertain, err)
	}
	if err := j.file.Sync(); err != nil {
		j.uncertain = true
		return fmt.Errorf("%w: sync: %v", ErrSwitchUncertain, err)
	}
	return j.applyRecordLocked(rec, j.generation == 0)
}

func (j *SwitchJournal) replayFileLocked() error {
	if _, err := j.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	raw, err := io.ReadAll(j.file)
	if err != nil {
		return err
	}
	r := bytes.NewReader(raw)
	count := 0
	for r.Len() > 0 {
		if r.Len() < 4+sha256.Size {
			return fmt.Errorf("%w: truncated frame header", ErrSwitchCorrupt)
		}
		var hdr [4]byte
		_, _ = io.ReadFull(r, hdr[:])
		n := int(binary.BigEndian.Uint32(hdr[:]))
		if n <= 0 || n > maxSwitchRecordLen || r.Len() < n+sha256.Size {
			return fmt.Errorf("%w: invalid frame length %d", ErrSwitchCorrupt, n)
		}
		payload := make([]byte, n)
		_, _ = io.ReadFull(r, payload)
		var gotSum [sha256.Size]byte
		_, _ = io.ReadFull(r, gotSum[:])
		if sha256.Sum256(payload) != gotSum {
			return fmt.Errorf("%w: frame checksum mismatch", ErrSwitchCorrupt)
		}
		var rec switchJournalRecord
		dec := json.NewDecoder(bytes.NewReader(payload))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rec); err != nil {
			return fmt.Errorf("%w: %v", ErrSwitchCorrupt, err)
		}
		if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: trailing JSON in frame", ErrSwitchCorrupt)
		}
		if err := j.applyRecordLocked(rec, count == 0); err != nil {
			return fmt.Errorf("%w: %v", ErrSwitchCorrupt, err)
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("%w: missing init frame", ErrSwitchCorrupt)
	}
	return nil
}

// DrainPriorScopeJournals scans stateDir() for unlocked prior journal files
// ("drive9-view-*.log") belonging to the same mount scope (excluding skipPath),
// queues any active or uncommitted lease unbinds under the prior incarnation's
// MountID, drains pending_unbinds via unbindFn, and unlinks each fully-drained
// stale journal while holding its exclusive flock.
func DrainPriorScopeJournals(
	ctx context.Context,
	skipPath, scope string,
	unbindFn func(context.Context, PendingUnbind) error,
) (bool, error) {
	if scope == "" {
		return false, fmt.Errorf("%w: empty scope", ErrSwitchInvalid)
	}
	dir := stateDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	cleanSkip := filepath.Clean(skipPath)
	drainedAny := false
	for _, ent := range entries {
		name := ent.Name()
		if !strings.HasPrefix(name, "drive9-view-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		candPath := filepath.Join(dir, name)
		if filepath.Clean(candPath) == cleanSkip {
			continue
		}
		f, oerr := os.OpenFile(candPath, os.O_RDWR|unix.O_NOFOLLOW, 0o600)
		if oerr != nil {
			if os.IsNotExist(oerr) {
				continue
			}
			return drainedAny, oerr
		}
		if lerr := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); lerr != nil {
			_ = f.Close()
			if errors.Is(lerr, unix.EWOULDBLOCK) || errors.Is(lerr, unix.EAGAIN) {
				// Owned by another live mount process.
				continue
			}
			return drainedAny, fmt.Errorf("lock prior switch journal %s: %w", candPath, lerr)
		}
		st, serr := f.Stat()
		if serr != nil {
			_ = f.Close()
			return drainedAny, serr
		}
		if !st.Mode().IsRegular() || st.Mode().Perm()&0o077 != 0 {
			_ = f.Close()
			continue
		}
		if st.Size() == 0 {
			_ = os.Remove(candPath)
			_ = f.Close()
			continue
		}
		j := &SwitchJournal{
			file:    f,
			history: make(map[string]SwitchReceipt),
		}
		if rerr := j.replayFileLocked(); rerr != nil {
			_ = j.Close()
			return drainedAny, fmt.Errorf("replay prior switch journal %s: %w", candPath, rerr)
		}
		if j.scope != scope {
			_ = j.Close()
			continue
		}
		if qerr := j.QueueCurrentUnbindForUnmount(); qerr != nil {
			_ = j.Close()
			return drainedAny, qerr
		}
		hadUnbinds := len(j.Snapshot().PendingUnbinds) > 0
		if derr := j.DrainPendingUnbinds(ctx, unbindFn); derr != nil {
			_ = j.Close()
			return drainedAny, derr
		}
		if hadUnbinds {
			drainedAny = true
		}
		if !j.Uncertain() && len(j.Snapshot().PendingUnbinds) == 0 {
			_ = os.Remove(candPath)
		}
		_ = j.Close()
	}
	return drainedAny, nil
}
