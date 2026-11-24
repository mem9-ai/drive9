//go:build !windows

package mountstate

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	for attempt := 0; attempt < 4; attempt++ {
		f, err := os.OpenFile(path, os.O_RDWR|unix.O_NOFOLLOW, 0o600)
		if err != nil {
			if os.IsNotExist(err) && create {
				created, existed, cerr := createAndInitSwitchJournalAtomic(path, mountID, scope, initial)
				if cerr != nil {
					return nil, cerr
				}
				if !existed {
					return created, nil
				}
				continue
			}
			return nil, err
		}
		ok := false
		closeIfFailed := func() {
			if !ok {
				_ = f.Close()
			}
		}
		if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			closeIfFailed()
			return nil, fmt.Errorf("lock switch journal: %w", err)
		}
		st, err := f.Stat()
		if err != nil {
			closeIfFailed()
			return nil, err
		}
		if pathSt, lerr := os.Lstat(path); lerr != nil || !os.SameFile(st, pathSt) {
			closeIfFailed()
			if create && (os.IsNotExist(lerr) || lerr == nil) {
				continue
			}
			if lerr != nil {
				return nil, lerr
			}
			return nil, fmt.Errorf("%w: switch journal replaced before lock", ErrSwitchConflict)
		}
		if !st.Mode().IsRegular() || st.Mode().Perm()&0o077 != 0 {
			closeIfFailed()
			return nil, fmt.Errorf("%w: unsafe switch journal mode %v", ErrSwitchCorrupt, st.Mode())
		}
		j := &SwitchJournal{
			file:    f,
			history: make(map[string]SwitchReceipt),
		}
		if st.Size() == 0 {
			if !create {
				closeIfFailed()
				return nil, fmt.Errorf("%w: empty switch journal", ErrSwitchCorrupt)
			}
			if err := j.appendAndApplyLocked(switchJournalRecord{
				Type:       "init",
				MountID:    mountID,
				Scope:      scope,
				Generation: 1,
				Target:     initial,
			}); err != nil {
				closeIfFailed()
				return nil, err
			}
			if err := syncJournalParentDir(path); err != nil {
				closeIfFailed()
				return nil, err
			}
			ok = true
			return j, nil
		}
		if err := j.replayFileLocked(); err != nil {
			closeIfFailed()
			return nil, err
		}
		if mountID != "" && j.mountID != mountID {
			closeIfFailed()
			return nil, fmt.Errorf("%w: journal mount_id %q != %q", ErrSwitchConflict, j.mountID, mountID)
		}
		if scope != "" && j.scope != scope {
			closeIfFailed()
			return nil, fmt.Errorf("%w: journal scope mismatch", ErrSwitchConflict)
		}
		ok = true
		return j, nil
	}
	return nil, fmt.Errorf("%w: concurrent journal creation contention at %s", ErrSwitchConflict, path)
}

func createAndInitSwitchJournalAtomic(path, mountID, scope string, initial viewprotocol.SwitchBinding) (*SwitchJournal, bool, error) {
	var rnd [8]byte
	_, _ = rand.Read(rnd[:])
	tmpPath := filepath.Join(filepath.Dir(path), fmt.Sprintf(".drive9-view-init-%d-%s.tmp", os.Getpid(), hex.EncodeToString(rnd[:])))
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, false, err
	}
	ok := false
	tmpRemoved := false
	defer func() {
		if !tmpRemoved {
			_ = os.Remove(tmpPath)
		}
		if !ok {
			_ = f.Close()
		}
	}()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, false, fmt.Errorf("lock new switch journal: %w", err)
	}
	j := &SwitchJournal{
		file:    f,
		history: make(map[string]SwitchReceipt),
	}
	if err := j.appendAndApplyLocked(switchJournalRecord{
		Type:       "init",
		MountID:    mountID,
		Scope:      scope,
		Generation: 1,
		Target:     initial,
	}); err != nil {
		return nil, false, err
	}
	if lerr := os.Link(tmpPath, path); lerr != nil {
		if os.IsExist(lerr) {
			return nil, true, nil
		}
		return nil, false, lerr
	}
	if rerr := os.Remove(tmpPath); rerr != nil && !os.IsNotExist(rerr) {
		_ = os.Remove(path)
		return nil, false, rerr
	}
	tmpRemoved = true
	if serr := syncJournalParentDir(path); serr != nil {
		_ = os.Remove(path)
		return nil, false, serr
	}
	ok = true
	return j, false, nil
}

func syncJournalParentDir(filePath string) error {
	var fpErr error
	failpoint.InjectCall("switchJournalBeforeDirSync", &fpErr)
	if fpErr != nil {
		return fmt.Errorf("sync switch journal parent dir: %w", fpErr)
	}
	dirPath := filepath.Dir(filePath)
	df, err := os.Open(dirPath)
	if err != nil {
		return fmt.Errorf("open switch journal parent dir %s: %w", dirPath, err)
	}
	syncErr := df.Sync()
	closeErr := df.Close()
	if syncErr != nil {
		return fmt.Errorf("sync switch journal parent dir %s: %w", dirPath, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close switch journal parent dir %s: %w", dirPath, closeErr)
	}
	return nil
}

// RemoveSwitchJournal unlinks path and fsyncs its parent directory so the
// directory entry removal is durable across power loss.
func RemoveSwitchJournal(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return syncJournalParentDir(path)
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
	if j.file == nil {
		return ErrSwitchClosed
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

func (j *SwitchJournal) truncateTornTailLocked(validEnd int) error {
	if err := j.file.Truncate(int64(validEnd)); err != nil {
		return err
	}
	if _, err := j.file.Seek(int64(validEnd), io.SeekStart); err != nil {
		return err
	}
	return j.file.Sync()
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
	validEnd := 0
	for r.Len() > 0 {
		frameStart := len(raw) - r.Len()
		if r.Len() < 4+sha256.Size {
			if count > 0 {
				return j.truncateTornTailLocked(validEnd)
			}
			return fmt.Errorf("%w: truncated frame header", ErrSwitchCorrupt)
		}
		if count > 0 && len(bytes.TrimRight(raw[frameStart:], "\x00")) == 0 {
			return j.truncateTornTailLocked(validEnd)
		}
		var hdr [4]byte
		_, _ = io.ReadFull(r, hdr[:])
		n := int(binary.BigEndian.Uint32(hdr[:]))
		if n <= 0 || n > maxSwitchRecordLen {
			return fmt.Errorf("%w: invalid frame length %d", ErrSwitchCorrupt, n)
		}
		if r.Len() < n+sha256.Size {
			if count > 0 {
				return j.truncateTornTailLocked(validEnd)
			}
			return fmt.Errorf("%w: truncated frame body %d", ErrSwitchCorrupt, n)
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
		validEnd = len(raw) - r.Len()
		count++
	}
	if count == 0 {
		return fmt.Errorf("%w: missing init frame", ErrSwitchCorrupt)
	}
	return nil
}

func journalFilenameScopePrefix(name string) (string, bool) {
	rest := strings.TrimPrefix(name, "drive9-view-")
	rest = strings.TrimSuffix(rest, ".log")
	if len(rest) < 18 || rest[16] != '-' {
		return "", false
	}
	prefix := rest[:16]
	for i := 0; i < 16; i++ {
		switch c := prefix[i]; {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return "", false
		}
	}
	return prefix, true
}

func peekInitScopeLocked(f *os.File) (string, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	var hdr [4]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return "", fmt.Errorf("%w: truncated init header", ErrSwitchCorrupt)
	}
	n := int(binary.BigEndian.Uint32(hdr[:]))
	if n <= 0 || n > maxSwitchRecordLen {
		return "", fmt.Errorf("%w: invalid init frame length %d", ErrSwitchCorrupt, n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(f, payload); err != nil {
		return "", fmt.Errorf("%w: truncated init payload", ErrSwitchCorrupt)
	}
	var gotSum [sha256.Size]byte
	if _, err := io.ReadFull(f, gotSum[:]); err != nil {
		return "", fmt.Errorf("%w: truncated init checksum", ErrSwitchCorrupt)
	}
	if sha256.Sum256(payload) != gotSum {
		return "", fmt.Errorf("%w: init checksum mismatch", ErrSwitchCorrupt)
	}
	var rec switchJournalRecord
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return "", fmt.Errorf("%w: %v", ErrSwitchCorrupt, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("%w: trailing JSON in init frame", ErrSwitchCorrupt)
	}
	if rec.Type != "init" || rec.Scope == "" {
		return "", fmt.Errorf("%w: invalid init frame", ErrSwitchCorrupt)
	}
	return rec.Scope, nil
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
	if len(scope) < 16 {
		return false, fmt.Errorf("%w: invalid scope", ErrSwitchInvalid)
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
		fileScopePrefix, hasScopePrefix := journalFilenameScopePrefix(name)
		if hasScopePrefix && fileScopePrefix != scope[:16] {
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
		pathSt, lserr := os.Lstat(candPath)
		if lserr != nil || !os.SameFile(st, pathSt) {
			_ = f.Close()
			continue
		}
		if !st.Mode().IsRegular() || st.Mode().Perm()&0o077 != 0 {
			_ = f.Close()
			continue
		}
		if st.Size() == 0 {
			// Never unlink a zero-byte unknown-scope file; an external/older
			// opener could be between O_CREATE and Flock, and a zero-byte file
			// carries zero server lease intents anyway.
			_ = f.Close()
			continue
		}
		initScope, perr := peekInitScopeLocked(f)
		if perr != nil {
			_ = f.Close()
			if hasScopePrefix && fileScopePrefix == scope[:16] {
				return drainedAny, fmt.Errorf("replay prior switch journal %s: %w", candPath, perr)
			}
			// Unscoped corrupt file cannot be attributed to this mount scope;
			// skip so an unrelated corrupt artifact never blocks startup.
			continue
		}
		if initScope != scope {
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
			_ = RemoveSwitchJournal(candPath)
		}
		_ = j.Close()
	}
	return drainedAny, nil
}
