// Package staging implements the write plane's local staging ledger: the
// shadow store (staged content images, generation-fenced and quota-bound),
// the pending index (metadata records with compare-and-set generations
// and request fencing), and the durable write-ahead journal, plus the
// write-back cache of .dat/.meta pairs that backs them. Staging bundles
// the trio under one directory and implements fs.StagingLedger: the
// buffered write path stages through it, commit machinery drains it, and
// mount-time recovery replays the journal into the index.
package staging

import (
	"fmt"
	"path/filepath"
)

// Staging composes the local staging ledger — shadow bytes, pending
// metadata, and the durable journal — under one directory. The writeback
// extension stages through it and its commit queue drains it; recovery
// replays it at mount. It implements fs.StagingLedger.
type Staging struct {
	Shadow  *ShadowStore
	Pending *PendingIndex
	Journal *Journal
}

// NewStaging builds the ledger with the default quota.
func NewStaging(dir string) (*Staging, error) {
	return NewStagingWithQuota(dir, 0, 0)
}

// NewStagingWithQuota builds the ledger under dir (the cache plugin's
// subtree): <dir>/shadow, <dir>/pending, <dir>/journal.wal.
// freeRatio/sizeMB carry the write-cache quota (defaults 0.10 / 1024 when
// zero): the shadow store refuses new writes with ENOSPC past the budget.
func NewStagingWithQuota(dir string, freeRatio float64, sizeMB int64) (*Staging, error) {
	shadow, err := NewShadowStoreWithQuota(filepath.Join(dir, "shadow"), freeRatio, sizeMB)
	if err != nil {
		return nil, fmt.Errorf("staging shadow: %w", err)
	}
	pending, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		shadow.Close()
		return nil, fmt.Errorf("staging pending: %w", err)
	}
	pending.SetShadowStore(shadow)

	journal, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		shadow.Close()
		return nil, fmt.Errorf("staging journal: %w", err)
	}
	pending.SetJournal(journal)
	return &Staging{Shadow: shadow, Pending: pending, Journal: journal}, nil
}

// Recover replays the journal into the pending index. Call once at mount,
// before any plugin stages.
func (s *Staging) Recover() error {
	if s == nil || s.Journal == nil || s.Pending == nil {
		return nil
	}
	return ReplayJournalIntoPending(s.Journal, s.Pending, s.Shadow)
}

// Compact drops journal frames already covered by commit markers.
func (s *Staging) Compact() error {
	if s == nil || s.Journal == nil {
		return nil
	}
	return s.Journal.Compact()
}

// Close releases the ledger's resources.
func (s *Staging) Close() {
	if s == nil {
		return
	}
	if s.Journal != nil {
		_ = s.Journal.Close()
	}
	if s.Shadow != nil {
		s.Shadow.Close()
	}
}
