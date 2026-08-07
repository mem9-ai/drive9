package migration

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrIncompleteRound = errors.New("cannot publish incomplete migration round")
	ErrStaleRound      = errors.New("migration round is not current")
)

type EntryKind string

const (
	EntryRegular   EntryKind = "regular"
	EntryDirectory EntryKind = "directory"
	EntrySymlink   EntryKind = "symlink"
	EntrySpecial   EntryKind = "special"
)

// SourceVersion is the comparable lstat identity used for stability checks.
type SourceVersion struct {
	Device, Inode    uint64
	Kind             EntryKind
	Size             int64
	MtimeNS, CtimeNS int64
	Mode             uint32
}

type SourceEntry struct {
	Path            string
	LocalPath       string
	Kind            EntryKind
	Version         SourceVersion
	Mode            uint32
	ChecksumSHA256  string
	LinkTarget      string
	HardlinkKey     string
	HardlinkPrimary bool
}

type TargetEntry struct {
	Path           string
	Kind           EntryKind
	Size           int64
	Mode           uint32
	HasMode        bool
	Revision       int64
	ResourceID     string
	Nlink          uint32
	ChecksumSHA256 string
}

type FindingKind string
type Severity string

const (
	FindingSourceOnly     FindingKind = "source_only"
	FindingTargetOnly     FindingKind = "target_only"
	FindingContent        FindingKind = "content"
	FindingMetadata       FindingKind = "metadata"
	FindingType           FindingKind = "type"
	FindingIdentity       FindingKind = "identity"
	FindingRevision       FindingKind = "revision"
	FindingInvalidUTF8    FindingKind = "invalid_utf8"
	FindingNFCCollision   FindingKind = "nfc_collision"
	FindingSpecialFile    FindingKind = "special_file"
	FindingNestedMount    FindingKind = "nested_mount"
	FindingSourceIdentity FindingKind = "source_identity"
	FindingSparseFile     FindingKind = "sparse_file"
	FindingSymlinkTarget  FindingKind = "symlink_target"
	FindingModeBits       FindingKind = "unsupported_mode_bits"

	SeverityWarning Severity = "warning"
	SeverityBlocker Severity = "blocker"
)

type Finding struct {
	Path     string      `json:"path"`
	Kind     FindingKind `json:"type"`
	Severity Severity    `json:"severity"`
}

type RoundMode string

const (
	RoundModeDeep         RoundMode = "deep_recovery"
	RoundModeFull         RoundMode = "full"
	RoundModeFast         RoundMode = "fast"
	RoundModeVerification RoundMode = "verification"
)

type Round struct {
	ID           string
	Mode         RoundMode
	StartedAt    time.Time
	CompletedAt  time.Time
	ScanComplete bool
	Converged    bool
	Source       map[string]SourceEntry
	Target       map[string]TargetEntry
	Findings     []Finding
}

type RoundStatus struct {
	ID           string    `json:"id,omitempty"`
	Mode         RoundMode `json:"mode,omitempty"`
	StartedAt    time.Time `json:"started_at,omitempty"`
	CompletedAt  time.Time `json:"completed_at,omitempty"`
	ScanComplete bool      `json:"scan_complete"`
	Converged    bool      `json:"round_converged"`
	FailureClass string    `json:"failure_class,omitempty"`
}

type Conditions struct {
	ReadyForRollout  bool `json:"ready_for_rollout"`
	CurrentConverged bool `json:"current_converged"`
	Attention        bool `json:"attention"`
}

type GraceCandidate struct {
	Path, Token string
	FirstSeen   time.Time
}

type RetryItem struct {
	Path, Token, ErrorClass string
	NextAttempt             time.Time
}

type VerificationState struct {
	Status        string    `json:"status,omitempty"`
	RequestedAt   time.Time `json:"requested_at,omitempty"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	SourceCount   int64     `json:"source_count"`
	MismatchCount int64     `json:"mismatch_count"`
}

type CandidateCounts struct {
	Mtime              int `json:"mtime"`
	SourceTokenChanged int `json:"source_token_changed"`
	NewPath            int `json:"new_path"`
	Filtered           int `json:"filtered"`
}

type StateSnapshot struct {
	Phase               Phase
	RecoveryComplete    bool
	InitialCopyComplete bool
	Current             RoundStatus
	LastComplete        *Round
	Conditions          Conditions
	Grace               map[string]GraceCandidate
	Retry               map[string]RetryItem
	Observed            map[string]SourceVersion
	Reconciled          map[string]SourceVersion
	CandidateCounts     CandidateCounts
	PendingRepairs      int
	ActiveOperations    int
	RepairMtimeFloor    *time.Time
	Verification        VerificationState
}

// State owns all process-local mutable facts for one Job.
type State struct {
	mu                  sync.RWMutex
	phase               Phase
	recoveryComplete    bool
	initialCopyComplete bool
	attention           bool
	conditions          Conditions
	current             RoundStatus
	lastComplete        *Round
	grace               map[string]GraceCandidate
	retry               map[string]RetryItem
	observed            map[string]SourceVersion
	reconciled          map[string]SourceVersion
	candidateCounts     CandidateCounts
	pendingRepairs      int
	activeOperations    int
	repairMtimeFloor    *time.Time
	verification        VerificationState
}

func NewState(phase Phase) *State {
	return &State{
		phase:      phase,
		grace:      make(map[string]GraceCandidate),
		retry:      make(map[string]RetryItem),
		observed:   make(map[string]SourceVersion),
		reconciled: make(map[string]SourceVersion),
	}
}

func (s *State) BeginRound(id string, mode RoundMode, startedAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current = RoundStatus{ID: id, Mode: mode, StartedAt: startedAt}
	s.candidateCounts = CandidateCounts{}
	s.pendingRepairs = 0
	s.recomputeLocked()
}

func (s *State) PublishRound(round Round) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if round.ID == "" || round.ID != s.current.ID {
		return ErrStaleRound
	}
	if !round.ScanComplete {
		s.current.ScanComplete = false
		s.current.Converged = false
		s.recomputeLocked()
		return ErrIncompleteRound
	}
	copy := cloneRound(round)
	s.observed = make(map[string]SourceVersion, len(copy.Source))
	for path, entry := range copy.Source {
		s.observed[path] = entry.Version
		if s.reconciled[path] != entry.Version {
			copy.Converged = false
		}
	}
	for path := range s.reconciled {
		if _, exists := copy.Source[path]; !exists {
			delete(s.reconciled, path)
		}
	}
	s.lastComplete = &copy
	s.current = RoundStatus{
		ID: round.ID, Mode: round.Mode, StartedAt: round.StartedAt,
		CompletedAt: round.CompletedAt, ScanComplete: true, Converged: copy.Converged,
	}
	s.recomputeLocked()
	return nil
}

// MarkReconciled advances one token only after successful deep work.
func (s *State) MarkReconciled(path string, version SourceVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconciled[path] = version
	s.clearPathLocked(path)
	s.recomputeLocked()
}

func (s *State) setRepairMtimeFloor(floor time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repairMtimeFloor = &floor
	s.recomputeLocked()
}

func sourceVersionToken(version SourceVersion) string {
	return fmt.Sprintf("%d:%d:%s:%d:%d:%d:%d", version.Device, version.Inode, version.Kind, version.Size, version.MtimeNS, version.CtimeNS, version.Mode)
}

type casEventContext struct {
	Phase     Phase
	RoundID   string
	Candidate GraceCandidate
}

func (s *State) eventContextForCAS(path, token string) (casEventContext, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	candidate, ok := s.grace[path+"\x00"+token]
	return casEventContext{Phase: s.phase, RoundID: s.current.ID, Candidate: candidate}, ok
}

func (s *State) trackGrace(path string, version SourceVersion, now time.Time) GraceCandidate {
	s.mu.Lock()
	defer s.mu.Unlock()
	token := sourceVersionToken(version)
	key := path + "\x00" + token
	for prior, candidate := range s.grace {
		if candidate.Path == path && prior != key {
			delete(s.grace, prior)
		}
	}
	for prior, retry := range s.retry {
		if retry.Path == path && retry.Token != token {
			delete(s.retry, prior)
		}
	}
	candidate, exists := s.grace[key]
	if !exists {
		candidate = GraceCandidate{Path: path, Token: token, FirstSeen: now}
		s.grace[key] = candidate
	}
	s.recomputeLocked()
	return candidate
}

func (s *State) queueRetry(path string, version SourceVersion, class string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token := sourceVersionToken(version)
	for key, retry := range s.retry {
		if retry.Path == path && retry.Token != token {
			delete(s.retry, key)
		}
	}
	s.retry[path+"\x00"+token] = RetryItem{Path: path, Token: token, ErrorClass: class, NextAttempt: now}
	s.recomputeLocked()
}

func (s *State) clearAbsentWork(source map[string]SourceEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, candidate := range s.grace {
		if _, exists := source[candidate.Path]; !exists {
			delete(s.grace, key)
		}
	}
	for key, retry := range s.retry {
		if _, exists := source[retry.Path]; !exists {
			delete(s.retry, key)
		}
	}
	s.recomputeLocked()
}

func (s *State) clearPathLocked(path string) {
	for key, candidate := range s.grace {
		if candidate.Path == path {
			delete(s.grace, key)
		}
	}
	for key, retry := range s.retry {
		if retry.Path == path {
			delete(s.retry, key)
		}
	}
}

func (s *State) FailRound(id, failureClass string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current.ID == id {
		s.current.ScanComplete = false
		s.current.Converged = false
		s.current.FailureClass = failureClass
	}
	s.recomputeLocked()
}

func (s *State) SetRecoveryComplete(complete bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recoveryComplete = complete
	s.recomputeLocked()
}

func (s *State) SetInitialCopyComplete(complete bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initialCopyComplete = complete
	s.recomputeLocked()
}

func (s *State) SetAttention(attention bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attention = attention
	s.recomputeLocked()
}

func (s *State) setCandidateCounts(counts CandidateCounts) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candidateCounts = counts
}

func (s *State) setPendingRepairs(pending int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingRepairs = max(0, pending)
	s.recomputeLocked()
}

func (s *State) beginOperation() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeOperations++
	s.recomputeLocked()
}

func (s *State) endOperation() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeOperations > 0 {
		s.activeOperations--
	}
	s.recomputeLocked()
}

func (s *State) recomputeLocked() {
	latestConverged := s.lastComplete != nil && s.current.ID == s.lastComplete.ID && s.current.ScanComplete && s.current.Converged
	syncReady := s.phase == PhaseSyncing && s.recoveryComplete && s.initialCopyComplete && latestConverged && !s.attention
	dualReady := s.phase == PhaseDualWriteRepairing && s.recoveryComplete && latestConverged && len(s.grace) == 0 && len(s.retry) == 0 && s.pendingRepairs == 0 && s.activeOperations == 0 && !s.attention
	// Conditions are derived rather than independently mutable.
	s.conditions = Conditions{ReadyForRollout: syncReady, CurrentConverged: dualReady, Attention: s.attention}
}

func (s *State) Snapshot() StateSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var last *Round
	if s.lastComplete != nil {
		copy := cloneRound(*s.lastComplete)
		last = &copy
	}
	return StateSnapshot{
		Phase: s.phase, RecoveryComplete: s.recoveryComplete, InitialCopyComplete: s.initialCopyComplete,
		Current: s.current, LastComplete: last, Conditions: s.conditions,
		Grace: cloneMap(s.grace), Retry: cloneMap(s.retry), Observed: cloneMap(s.observed), Reconciled: cloneMap(s.reconciled),
		CandidateCounts: s.candidateCounts, PendingRepairs: s.pendingRepairs, ActiveOperations: s.activeOperations,
		RepairMtimeFloor: cloneTime(s.repairMtimeFloor), Verification: s.verification,
	}
}

func cloneRound(round Round) Round {
	round.Source = cloneMap(round.Source)
	round.Target = cloneMap(round.Target)
	round.Findings = append([]Finding(nil), round.Findings...)
	return round
}

func cloneMap[K comparable, V any](input map[K]V) map[K]V {
	result := make(map[K]V, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
