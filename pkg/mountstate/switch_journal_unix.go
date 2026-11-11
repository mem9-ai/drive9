//go:build !windows

package mountstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/failpoint"
	"golang.org/x/sys/unix"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

var (
	ErrSwitchConflict       = errors.New("mount switch identity or generation conflict")
	ErrSwitchPending        = errors.New("mount switch needs recovery")
	ErrSwitchUncertain      = errors.New("mount switch journal outcome unknown")
	ErrSwitchJournalInvalid = errors.New("invalid mount switch journal")
	ErrSwitchJournalLimit   = errors.New("mount switch journal size limit")
)

const (
	maxSwitchRecordBytes  = 4096
	maxSwitchJournalBytes = 32 << 20
)

type switchRecord struct {
	Version int                  `json:"version"`
	Kind    string               `json:"kind"`
	MountID string               `json:"mount_id,omitempty"`
	Scope   string               `json:"scope,omitempty"`
	Initial viewprotocol.Binding `json:"initial,omitempty"`
	Receipt *SwitchReceipt       `json:"receipt,omitempty"`
}

type switchJournalWriter interface {
	Write([]byte) (int, error)
	Sync() error
}

// SwitchJournal has one process owner (an exclusive file lock) and serializes
// callers. It stores binding decisions and their completion, not credentials,
// permission grants, work callbacks or FUSE file protections.
type SwitchJournal struct {
	mu         sync.Mutex
	file       *os.File
	writer     switchJournalWriter
	failed     bool
	bytes      int64
	scope      string
	mountID    string
	initial    viewprotocol.Binding
	generation uint64
	binding    viewprotocol.Binding
	pending    string
	receipts   map[string]SwitchReceipt
}

// CreateSwitchJournal creates the durable identity for a new mount incarnation.
// scope is the authenticated filesystem/server/root/mountpoint digest.
// An existing pathname is never overwritten, including an empty/corrupt file.
func CreateSwitchJournal(path, mountID, scope string, initial viewprotocol.Binding) (*SwitchJournal, error) {
	return openSwitchJournal(path, mountID, scope, initial, true, syncSwitchDirectory)
}

// OpenSwitchJournal reopens an existing mount incarnation. A missing journal
// is an error, not permission to initialize main. path must be in a private,
// current-user-owned directory. Existing complete records are synced again
// before recovery is reported. Torn/corrupt tails fail closed.
func OpenSwitchJournal(path, mountID, scope string, initial viewprotocol.Binding) (*SwitchJournal, error) {
	return openSwitchJournal(path, mountID, scope, initial, false, syncSwitchDirectory)
}

func openSwitchJournal(path, mountID, scope string, initial viewprotocol.Binding, create bool, syncDir func(string) error) (*SwitchJournal, error) {
	if viewprotocol.ValidateOperationID(mountID) != nil || viewprotocol.ValidateOperationID(scope) != nil || initial.Validate() != nil || !filepath.IsAbs(path) {
		return nil, ErrSwitchJournalInvalid
	}
	dir := filepath.Dir(path)
	if err := validateStateDir(dir); err != nil {
		return nil, err
	}
	flags := unix.O_RDWR | unix.O_APPEND | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if create {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	fd, err := unix.Open(path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
		}
	}()
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, err
	}
	if create {
		if err := f.Chmod(0o600); err != nil {
			return nil, err
		}
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if err := checkTrustedFileInfo(path, info); err != nil {
		return nil, err
	}
	if info.Mode().Perm() != 0o600 {
		return nil, ErrSwitchJournalInvalid
	}
	if info.Size() > maxSwitchJournalBytes {
		return nil, ErrSwitchJournalLimit
	}
	j := &SwitchJournal{file: f, writer: f, mountID: mountID, scope: scope, initial: initial, receipts: make(map[string]SwitchReceipt)}
	if create {
		rec := switchRecord{Version: 1, Kind: "init", MountID: mountID, Scope: scope, Initial: initial}
		if err := j.append(rec); err != nil {
			return nil, err
		}
		j.generation, j.binding = 1, initial
	} else {
		if err := j.load(); err != nil {
			return nil, err
		}
		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrSwitchUncertain, err)
		}
	}
	if err := syncDir(dir); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSwitchUncertain, err)
	}
	ok = true
	return j, nil
}

func syncSwitchDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
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

func (j *SwitchJournal) ready() error {
	if j.file == nil {
		return os.ErrClosed
	}
	if j.failed {
		return ErrSwitchUncertain
	}
	return nil
}

func (j *SwitchJournal) Snapshot() (SwitchSnapshot, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return SwitchSnapshot{}, err
	}
	out := SwitchSnapshot{MountID: j.mountID, Generation: j.generation, Binding: j.binding}
	if j.pending != "" {
		receipt := j.receipts[j.pending]
		out.Pending = &receipt
	}
	return out, nil
}

func (j *SwitchJournal) Lookup(operationID string) (SwitchReceipt, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return SwitchReceipt{}, false, err
	}
	if viewprotocol.ValidateOperationID(operationID) != nil {
		return SwitchReceipt{}, false, viewprotocol.ErrOperationID
	}
	r, ok := j.receipts[operationID]
	return r, ok, nil
}

// Decide is called only after authorization, admission closure and draining.
// A successful call is the point after which the caller cannot report that the
// old binding remains selected. Any uncertain error requires recovery with new
// I/O still closed. Retrying an old identity returns its original result even
// after subsequent switches; it never switches back.
func (j *SwitchJournal) Decide(operationID string, expectedGeneration uint64, target viewprotocol.Binding) (SwitchReceipt, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return SwitchReceipt{}, err
	}
	if viewprotocol.ValidateOperationID(operationID) != nil || expectedGeneration == 0 || target.Validate() != nil {
		return SwitchReceipt{}, ErrSwitchJournalInvalid
	}
	if old, ok := j.receipts[operationID]; ok {
		if old.FromGeneration != expectedGeneration || old.Target != target {
			return SwitchReceipt{}, ErrSwitchConflict
		}
		return old, nil
	}
	if j.pending != "" {
		return SwitchReceipt{}, ErrSwitchPending
	}
	if expectedGeneration != j.generation || j.generation == math.MaxUint64 {
		return SwitchReceipt{}, ErrSwitchConflict
	}
	r := SwitchReceipt{OperationID: operationID, FromGeneration: j.generation, Generation: j.generation + 1, From: j.binding, Target: target, State: SwitchRecovering}
	// Always reserve enough journal space for this decision's completion.
	if j.bytes+2*(maxSwitchRecordBytes+4+sha256.Size) > maxSwitchJournalBytes {
		return SwitchReceipt{}, ErrSwitchJournalLimit
	}
	if err := j.append(switchRecord{Version: 1, Kind: "decide", Receipt: &r}); err != nil {
		return SwitchReceipt{}, err
	}
	j.apply(r)
	return r, nil
}

// Complete persists the caller's successful target installation and cache
// invalidation. It grants no server permission and releases no old protections.
func (j *SwitchJournal) Complete(operationID string) (SwitchReceipt, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return SwitchReceipt{}, err
	}
	r, ok := j.receipts[operationID]
	if !ok {
		return SwitchReceipt{}, ErrSwitchConflict
	}
	if r.State == SwitchDone {
		return r, nil
	}
	if j.pending != operationID {
		return SwitchReceipt{}, ErrSwitchConflict
	}
	r.State = SwitchDone
	if err := j.append(switchRecord{Version: 1, Kind: "complete", Receipt: &r}); err != nil {
		return SwitchReceipt{}, err
	}
	j.apply(r)
	return r, nil
}

func (j *SwitchJournal) apply(r SwitchReceipt) {
	j.receipts[r.OperationID] = r
	j.generation, j.binding = r.Generation, r.Target
	j.pending = ""
	if r.State == SwitchRecovering {
		j.pending = r.OperationID
	}
}

func (j *SwitchJournal) append(rec switchRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if len(data) > maxSwitchRecordBytes {
		return ErrSwitchJournalLimit
	}
	sum := sha256.Sum256(data)
	frame := make([]byte, 4, 4+len(data)+len(sum))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	frame = append(frame, data...)
	frame = append(frame, sum[:]...)
	if j.bytes+int64(len(frame)) > maxSwitchJournalBytes {
		return ErrSwitchJournalLimit
	}
	n, err := j.writer.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err == nil {
		// Exercise cancellation and uncertain durability at the actual sync
		// boundary without changing the failpoint-off write path.
		failpoint.InjectCall("switchJournalBeforeSync", &err)
		if err == nil {
			err = j.writer.Sync()
		}
	}
	if err != nil {
		j.failed = true
		return fmt.Errorf("%w: %w", ErrSwitchUncertain, err)
	}
	j.bytes += int64(len(frame))
	return nil
}

func (j *SwitchJournal) load() error {
	for {
		var header [4]byte
		_, err := io.ReadFull(j.file, header[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return ErrSwitchJournalInvalid
		}
		n := binary.BigEndian.Uint32(header[:])
		if n == 0 || n > maxSwitchRecordBytes {
			return ErrSwitchJournalInvalid
		}
		data := make([]byte, int(n)+sha256.Size)
		if _, err := io.ReadFull(j.file, data); err != nil {
			return ErrSwitchJournalInvalid
		}
		j.bytes += int64(4 + len(data))
		if j.bytes > maxSwitchJournalBytes {
			return ErrSwitchJournalLimit
		}
		sum := sha256.Sum256(data[:n])
		if !bytes.Equal(sum[:], data[n:]) {
			return ErrSwitchJournalInvalid
		}
		var rec switchRecord
		dec := json.NewDecoder(bytes.NewReader(data[:n]))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rec); err != nil {
			return ErrSwitchJournalInvalid
		}
		if dec.Decode(new(any)) != io.EOF {
			return ErrSwitchJournalInvalid
		}
		if err := j.replay(rec); err != nil {
			return err
		}
	}
	if j.generation == 0 {
		return ErrSwitchJournalInvalid
	}
	return nil
}

func (j *SwitchJournal) replay(rec switchRecord) error {
	if rec.Version != 1 {
		return ErrSwitchJournalInvalid
	}
	if rec.Kind == "init" {
		if j.generation != 0 || rec.Receipt != nil || rec.MountID != j.mountID || rec.Scope != j.scope || rec.Initial != j.initial || rec.Initial.Validate() != nil {
			return ErrSwitchJournalInvalid
		}
		j.generation, j.binding = 1, rec.Initial
		return nil
	}
	if j.generation == 0 || rec.MountID != "" || rec.Scope != "" || rec.Initial != (viewprotocol.Binding{}) || rec.Receipt == nil {
		return ErrSwitchJournalInvalid
	}
	r := *rec.Receipt
	if viewprotocol.ValidateOperationID(r.OperationID) != nil || r.Target.Validate() != nil || r.From.Validate() != nil {
		return ErrSwitchJournalInvalid
	}
	switch rec.Kind {
	case "decide":
		if _, ok := j.receipts[r.OperationID]; ok {
			return ErrSwitchJournalInvalid
		}
		if j.pending != "" || j.generation == math.MaxUint64 || r.FromGeneration != j.generation || r.Generation != j.generation+1 || r.From != j.binding || r.State != SwitchRecovering {
			return ErrSwitchJournalInvalid
		}
	case "complete":
		old, ok := j.receipts[r.OperationID]
		if !ok || j.pending != r.OperationID || r.State != SwitchDone {
			return ErrSwitchJournalInvalid
		}
		old.State = SwitchDone
		if r != old {
			return ErrSwitchJournalInvalid
		}
	default:
		return ErrSwitchJournalInvalid
	}
	j.apply(r)
	return nil
}
