package fuse

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// JournalOp identifies the type of operation recorded in a journal entry.
type JournalOp int

const (
	JournalWrite       JournalOp = iota // Write data to a file
	JournalTruncate                     // Truncate a file
	JournalRename                       // Rename a file
	JournalUnlink                       // Delete a file
	JournalMkdir                        // Create a directory
	JournalRmdir                        // Remove a directory
	JournalFsync                        // Fsync a file (local durability marker)
	JournalCommit                       // Remote commit completed (can be compacted)
	JournalPendingMeta                  // Durable pending-index record (Meta carries the serialized WriteBackMeta)
)

// JournalEntry represents a single operation in the WAL.
type JournalEntry struct {
	Seq uint64    `json:"seq"`
	Op  JournalOp `json:"op"`
	// Generation binds a completion marker to the exact pending-index
	// generation it uploaded. Zero preserves the legacy path-scoped semantics
	// used by unlink/cancel and old journals.
	Generation uint64 `json:"generation,omitempty"`
	Inode      uint64 `json:"inode,omitempty"`
	Path       string `json:"path"`
	NewPath    string `json:"new_path,omitempty"` // for Rename
	Offset     int64  `json:"offset,omitempty"`   // for Write
	Length     int64  `json:"length,omitempty"`   // for Write
	BaseRev    int64  `json:"base_rev,omitempty"`
	Timestamp  int64  `json:"timestamp,omitempty"`
	// ShadowSpill records that the fsync'd data lives in a streaming shadow
	// file, so crash recovery must rebuild the pending entry in spill mode
	// (streamed upload) instead of full-memory ReadAll.
	ShadowSpill bool `json:"shadow_spill,omitempty"`
	// Meta is the serialized WriteBackMeta for JournalPendingMeta frames:
	// the durable pending-index record published through the WAL instead of
	// a per-file atomicWrite (issue #959/#960 follow-up — one group-committed
	// fsync replaces two per-close fsyncs).
	Meta []byte `json:"meta,omitempty"`
}

type journalDoneState struct {
	path       map[string]uint64
	generation map[string]map[uint64]uint64
}

func newJournalDoneState() *journalDoneState {
	return &journalDoneState{
		path:       make(map[string]uint64),
		generation: make(map[string]map[uint64]uint64),
	}
}

func journalEntryGeneration(entry JournalEntry) uint64 {
	if entry.Generation != 0 {
		return entry.Generation
	}
	if entry.Op != JournalPendingMeta || len(entry.Meta) == 0 {
		return 0
	}
	var meta WriteBackMeta
	if json.Unmarshal(entry.Meta, &meta) != nil {
		return 0
	}
	return meta.Generation
}

func (state *journalDoneState) observe(entry JournalEntry) {
	if entry.Op != JournalCommit && entry.Op != JournalUnlink {
		return
	}
	gen := entry.Generation
	if entry.Op == JournalUnlink || gen == 0 {
		if entry.Seq > state.path[entry.Path] {
			state.path[entry.Path] = entry.Seq
		}
		return
	}
	byGeneration := state.generation[entry.Path]
	if byGeneration == nil {
		byGeneration = make(map[uint64]uint64)
		state.generation[entry.Path] = byGeneration
	}
	if entry.Seq > byGeneration[gen] {
		byGeneration[gen] = entry.Seq
	}
}

func (state *journalDoneState) supersedes(entry JournalEntry) bool {
	if state.path[entry.Path] > entry.Seq {
		return true
	}
	gen := journalEntryGeneration(entry)
	return gen != 0 && state.generation[entry.Path][gen] > entry.Seq
}

func (state *journalDoneState) coversForCompaction(entry JournalEntry) bool {
	if state.path[entry.Path] >= entry.Seq {
		return true
	}
	gen := journalEntryGeneration(entry)
	return gen != 0 && state.generation[entry.Path][gen] >= entry.Seq
}

// Journal is an append-only WAL for crash recovery. Each entry is
// length-prefixed + CRC32 for integrity verification.
//
// Wire format per entry:
//
//	[4 bytes: payload length (little-endian uint32)]
//	[N bytes: JSON payload]
//	[4 bytes: CRC32 of payload (little-endian uint32)]
type Journal struct {
	mu   sync.Mutex
	fd   *os.File
	seq  atomic.Uint64
	path string

	// syncMu serializes actual fd.Sync calls so concurrent FsyncShared
	// waiters coalesce into one fsync (group commit). doneGen (guarded by
	// mu) is the highest seq known durable; it only advances on success.
	syncMu  sync.Mutex
	doneGen uint64
}

// NewJournal opens or creates a journal WAL file at the given path. The
// sequence counter resumes from the highest Seq already on disk so that
// appends before any Replay never reuse sequence numbers.
func NewJournal(path string) (*Journal, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("journal open: %w", err)
	}
	j := &Journal{
		fd:   fd,
		path: path,
	}
	if data, err := os.ReadFile(path); err == nil {
		var maxSeq uint64
		scanJournalFrames(data, func(entry JournalEntry, _ []byte) {
			if entry.Seq > maxSeq {
				maxSeq = entry.Seq
			}
		})
		j.seq.Store(maxSeq)
	}
	return j, nil
}

// scanJournalFrames walks the wire format, skipping corrupt or truncated
// frames, and calls fn with each decoded entry and its raw frame bytes.
func scanJournalFrames(data []byte, fn func(entry JournalEntry, frame []byte)) {
	pos := 0
	for pos+8 <= len(data) { // need at least 4 (len) + 4 (crc)
		payloadLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		frameEnd := pos + 4 + int(payloadLen) + 4
		if frameEnd > len(data) {
			break // truncated entry
		}
		payload := data[pos+4 : pos+4+int(payloadLen)]
		storedCRC := binary.LittleEndian.Uint32(data[pos+4+int(payloadLen):])
		if storedCRC != crc32.ChecksumIEEE(payload) {
			// Corrupt entry — skip to next possible entry boundary.
			pos++
			continue
		}
		var entry JournalEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			pos = frameEnd
			continue
		}
		fn(entry, data[pos:frameEnd])
		pos = frameEnd
	}
}

// Append writes a journal entry to the WAL. The entry is length-prefixed
// and CRC32-checksummed for integrity.
func (j *Journal) Append(entry JournalEntry) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Seq must be assigned under the same lock that orders the writes:
	// recovery and compaction treat higher Seq as "later on disk", so a
	// frame must never be written before another frame with a lower Seq.
	entry.Seq = j.seq.Add(1)

	payload, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("journal marshal: %w", err)
	}

	// Build wire frame: [len:4][payload:N][crc:4]
	frame := make([]byte, 4+len(payload)+4)
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload)))
	copy(frame[4:4+len(payload)], payload)
	checksum := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(frame[4+len(payload):], checksum)

	_, err = j.fd.Write(frame)
	if err != nil {
		return fmt.Errorf("journal write: %w", err)
	}
	return nil
}

// Fsync ensures all journal entries are durable on disk.
func (j *Journal) Fsync() error {
	return j.FsyncShared()
}

// FsyncShared makes every entry appended before the call durable, coalescing
// concurrent waiters into a single fsync: the first caller becomes the sync
// leader and everyone who appended before its snapshot returns with it.
// Callers whose entries were already covered by a successful sync return
// immediately. A failed sync is returned to the leader and to waiters that
// queued behind it; doneGen never advances on failure, so later callers retry.
func (j *Journal) FsyncShared() error {
	j.mu.Lock()
	target := j.seq.Load()
	if j.doneGen >= target {
		j.mu.Unlock()
		return nil
	}
	j.mu.Unlock()

	j.syncMu.Lock()
	defer j.syncMu.Unlock()

	// Another leader may have covered our target while we queued on syncMu.
	j.mu.Lock()
	if j.doneGen >= target {
		j.mu.Unlock()
		return nil
	}
	j.mu.Unlock()

	// Snapshot everything appended so far; the sync covers up to here.
	j.mu.Lock()
	snapshot := j.seq.Load()
	j.mu.Unlock()
	if err := j.fd.Sync(); err != nil {
		return err
	}
	j.mu.Lock()
	if snapshot > j.doneGen {
		j.doneGen = snapshot
	}
	j.mu.Unlock()
	return nil
}

// SyncLoop periodically calls FsyncShared until the channel closes, bounding
// the power-loss exposure of un-fsynced appends (issue #964). It stops on
// channel close or context cancellation and returns.
func (j *Journal) SyncLoop(ctx context.Context, interval time.Duration) {
	if j == nil || interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = j.FsyncShared()
		}
	}
}

// DurableSeq reports the highest journal sequence known to be on stable
// storage (advanced only by successful fsyncs). Exposed for tests and
// diagnostics.
func (j *Journal) DurableSeq() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.doneGen
}

// Replay reads all entries from the journal file and calls fn for each
// valid entry. Corrupt entries (bad CRC or truncated) are skipped, and
// replay continues from the next valid entry boundary.
func (j *Journal) Replay(fn func(JournalEntry)) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	data, err := os.ReadFile(j.path)
	if err != nil {
		return err
	}

	maxSeq := j.seq.Load()
	scanJournalFrames(data, func(entry JournalEntry, _ []byte) {
		fn(entry)
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}
	})
	j.seq.Store(maxSeq)
	return nil
}

// Compact removes all committed entries from the journal by rewriting
// the file with only uncommitted entries.
func (j *Journal) Compact() error {
	// syncMu before mu matches FsyncShared's order and keeps the fd swap
	// below from racing an in-flight Sync.
	j.syncMu.Lock()
	defer j.syncMu.Unlock()
	j.mu.Lock()
	defer j.mu.Unlock()

	data, err := os.ReadFile(j.path)
	if err != nil {
		return err
	}

	// First pass: find path-scoped legacy boundaries and exact-generation
	// completion boundaries. A completion for generation N must never discard a
	// newer generation N+1 that happened to reach the WAL first.
	done := newJournalDoneState()
	scanJournalFrames(data, func(entry JournalEntry, _ []byte) {
		if entry.Op == JournalCommit {
			done.observe(entry)
		}
	})

	if len(done.path) == 0 && len(done.generation) == 0 {
		return nil // nothing to compact
	}

	// Second pass: keep frames that are newer than the commit boundary
	// for their path, and all frames for paths without a commit marker.
	var kept []byte
	scanJournalFrames(data, func(entry JournalEntry, frame []byte) {
		if !done.coversForCompaction(entry) {
			kept = append(kept, frame...)
		}
	})

	// Rewrite journal file.
	tmpPath := j.path + ".compact"
	if err := atomicWrite(tmpPath, kept); err != nil {
		return fmt.Errorf("journal compact write: %w", err)
	}
	if err := os.Rename(tmpPath, j.path); err != nil {
		return fmt.Errorf("journal compact rename: %w", err)
	}

	// Reopen.
	_ = j.fd.Close()
	fd, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("journal reopen: %w", err)
	}
	j.fd = fd
	return nil
}

// Close closes the journal file.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.fd.Close()
}

// replayJournalIntoPending rebuilds pending-index entries from the WAL for
// crash recovery. A path is resurrected only when its latest data entry
// (fsync) is NOT superseded by a later JournalCommit/JournalUnlink marker;
// otherwise every historically committed path would re-upload on each mount,
// and a stale WAL entry could pair with a newer session's shadow file and
// upload torn content under old metadata.
//
// Entries whose .meta survived (already present in the pending index) are
// left untouched by legacy fsync frames because the file can carry metadata
// absent from the WAL. Full pending-meta frames can replace an older .meta
// left by best-effort snapshotting; a strictly newer generation is also
// authoritative when a layer mount edits a retained committed overlay.
func replayJournalIntoPending(j *Journal, idx *PendingIndex, shadows *ShadowStore) error {
	if j == nil || idx == nil {
		return nil
	}

	latestData := make(map[string]JournalEntry) // path → latest fsync entry
	latestMeta := make(map[string]JournalEntry) // path → latest pending-meta entry
	done := newJournalDoneState()
	if err := j.Replay(func(e JournalEntry) {
		switch e.Op {
		case JournalCommit, JournalUnlink:
			done.observe(e)
		case JournalFsync:
			// Only fsync frames assert "shadow data is durable"; other ops
			// (write/rename/mkdir/...) must not resurrect pending uploads.
			if prev, ok := latestData[e.Path]; !ok || e.Seq > prev.Seq {
				latestData[e.Path] = e
			}
		case JournalPendingMeta:
			if prev, ok := latestMeta[e.Path]; !ok || e.Seq > prev.Seq {
				latestMeta[e.Path] = e
			}
		}
	}); err != nil {
		return err
	}

	// A path can have both frame kinds (close published a meta frame, a later
	// fsync(2) added a data frame, or vice versa); the newest frame wins.
	ressurrect := func(path string, e JournalEntry, fromMeta bool) error {
		if done.supersedes(e) {
			return nil // committed or unlinked after the last local record
		}
		var meta WriteBackMeta
		if fromMeta {
			if err := json.Unmarshal(e.Meta, &meta); err != nil {
				return fmt.Errorf("journal replay resurrect %s: %w", path, err)
			}
		}
		if current, ok := idx.GetMeta(path); ok {
			// .meta and .dat may be an older best-effort snapshot even though
			// a later Flush durably published shadow + full WAL metadata.
			// Legacy fsync frames have no full publication timestamp, so they
			// cannot displace a surviving metadata record on that evidence.
			if !fromMeta || !current.Mtime.Before(meta.Mtime) {
				return nil
			}
		}
		if fromMeta {
			// Issue #964: close staging is un-fsynced, so the WAL meta frame
			// may reach disk while the shadow content did not. Reject a frame
			// with missing or short content rather than uploading torn bytes.
			// The guard must cover non-spill frames too: both
			// kinds upload from the shadow file, and a lazy overwrite of a
			// pre-existing file would otherwise CAS-commit truncated content
			// over a good remote revision.
			if shadows != nil {
				if size, ok := shadows.ContentSize(path); !ok || size < meta.Size {
					shadows.Remove(path)
					// This frame never became the selected publication. Preserve
					// any older .meta/.dat snapshot so migration can recover it
					// with its own original CAS revision.
					if fallback, ok := idx.GetMeta(path); ok {
						// Supersede the rejected frame durably BEFORE migration
						// installs fallback bytes. Otherwise another crash could
						// pair those bytes with this rejected frame's newer base.
						raw, err := json.Marshal(fallback)
						if err != nil {
							return fmt.Errorf("journal recovery fallback %s: %w", path, err)
						}
						if err := j.Append(JournalEntry{Op: JournalPendingMeta, Path: path, Meta: raw}); err != nil {
							return fmt.Errorf("journal recovery fallback %s: %w", path, err)
						}
						if err := j.FsyncShared(); err != nil {
							return fmt.Errorf("journal recovery fallback sync %s: %w", path, err)
						}
					}
					return nil
				}
			}
			if err := idx.publishRecoveredMeta(e.Meta); err != nil {
				return fmt.Errorf("journal replay resurrect %s: %w", path, err)
			}
			return nil
		}
		if idx.HasPending(path) {
			return nil
		}
		kind := PendingOverwrite
		if e.BaseRev == 0 {
			kind = PendingNew
		}
		var err error
		if e.ShadowSpill {
			_, err = idx.PutShadowSpill(path, e.Length, kind, e.BaseRev)
		} else {
			_, err = idx.PutWithBaseRev(path, e.Length, kind, e.BaseRev)
		}
		if err != nil {
			return fmt.Errorf("journal replay resurrect %s: %w", path, err)
		}
		return nil
	}
	for path, e := range latestData {
		if m, ok := latestMeta[path]; ok && m.Seq > e.Seq {
			continue // the meta frame is newer and carries full fidelity
		}
		if err := ressurrect(path, e, false); err != nil {
			return err
		}
	}
	for path, e := range latestMeta {
		if d, ok := latestData[path]; ok && d.Seq > e.Seq {
			continue // the fsync frame is newer
		}
		if err := ressurrect(path, e, true); err != nil {
			return err
		}
	}
	return nil
}
