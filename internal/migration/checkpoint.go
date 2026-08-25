package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	CheckpointVersion  = "v2"
	maxCheckpointBytes = 64 << 10
)

var (
	ErrCheckpointMismatch = errors.New("migration checkpoint mismatch")
	ErrCheckpointConflict = errors.New("migration checkpoint ownership conflict")
)

// Checkpoint is the complete durable Worker schema; all per-file facts stay in memory.
type Checkpoint struct {
	Version       string `json:"version"`
	JobID         string `json:"job_id"`
	ConfigHash    string `json:"config_hash"`
	VolumeID      string `json:"volume_id"`
	EBSRoot       string `json:"ebs_root"`
	SourceSubpath string `json:"source_subpath"`
	SourceRoot    string `json:"source_root"`
	Endpoint      string `json:"endpoint"`
	SpaceRef      string `json:"space_ref"`
	Prefix        string `json:"prefix"`
	CredentialRef string `json:"credential_ref"`
	HighestPhase  Phase  `json:"highest_phase"`
	FenceIntent   bool   `json:"fence_intent"`
	FenceComplete bool   `json:"fence_complete"`
}

type CheckpointRecord struct {
	Checkpoint Checkpoint
	Revision   int64
}

type Recovery struct {
	State                *State
	Record               CheckpointRecord
	WritesAllowed        bool
	DeepRecoveryRequired bool
	FenceRecoveryOnly    bool
}

type CheckpointStore struct {
	client      *client.Client
	beforeWrite func(Checkpoint) error
	afterWrite  func(Checkpoint) error
}

func NewCheckpointStore(api *client.Client) *CheckpointStore {
	return &CheckpointStore{client: api}
}

func CheckpointPath(jobID string) (string, error) {
	if err := validateJobID(jobID); err != nil {
		return "", fmt.Errorf("invalid checkpoint job ID")
	}
	return ControlPrefix + "/jobs/" + jobID + "/checkpoint.json", nil
}

func checkpointFromStartup(startup *Startup) Checkpoint {
	return Checkpoint{
		Version: CheckpointVersion, JobID: startup.Job.JobID,
		ConfigHash: startup.ConfigHash, VolumeID: startup.Job.VolumeID,
		EBSRoot: startup.Job.EBSRoot, SourceSubpath: startup.Job.Subpath,
		SourceRoot: startup.Job.Source.Root, Endpoint: startup.Config.Drive9.Endpoint,
		SpaceRef: startup.Job.Target.SpaceRef, Prefix: startup.Job.Target.Prefix,
		CredentialRef: startup.Space.CredentialRef, HighestPhase: startup.Phase,
	}
}

// Load reads one stable checkpoint revision; absence returns a zero record.
func (s *CheckpointStore) Load(ctx context.Context, jobID string) (CheckpointRecord, error) {
	checkpointPath, err := CheckpointPath(jobID)
	if err != nil {
		return CheckpointRecord{}, err
	}
	before, err := s.client.StatCtx(ctx, checkpointPath)
	if client.IsNotFound(err) {
		return CheckpointRecord{}, nil
	}
	if err != nil {
		return CheckpointRecord{}, fmt.Errorf("stat checkpoint: %w", err)
	}
	if before.IsDir || before.Revision <= 0 {
		return CheckpointRecord{}, fmt.Errorf("%w: checkpoint type or revision", ErrCheckpointMismatch)
	}
	if before.Size > maxCheckpointBytes {
		return CheckpointRecord{}, fmt.Errorf("%w: checkpoint exceeds size limit", ErrCheckpointMismatch)
	}
	stream, err := s.client.ReadStreamRange(ctx, checkpointPath, 0, maxCheckpointBytes+1)
	if err != nil {
		return CheckpointRecord{}, fmt.Errorf("read checkpoint: %w", err)
	}
	body, readErr := io.ReadAll(stream)
	closeErr := stream.Close()
	if readErr != nil {
		return CheckpointRecord{}, fmt.Errorf("read checkpoint: %w", readErr)
	}
	if closeErr != nil {
		return CheckpointRecord{}, fmt.Errorf("close checkpoint: %w", closeErr)
	}
	if len(body) > maxCheckpointBytes {
		return CheckpointRecord{}, fmt.Errorf("%w: checkpoint exceeds size limit", ErrCheckpointMismatch)
	}
	after, err := s.client.StatCtx(ctx, checkpointPath)
	if err != nil || after.Revision != before.Revision {
		return CheckpointRecord{}, fmt.Errorf("%w: checkpoint changed during read", ErrCheckpointConflict)
	}
	checkpoint, err := decodeCheckpoint(body)
	if err != nil {
		return CheckpointRecord{}, err
	}
	if checkpoint.JobID != jobID {
		return CheckpointRecord{}, fmt.Errorf("%w: checkpoint job ID", ErrCheckpointMismatch)
	}
	return CheckpointRecord{Checkpoint: checkpoint, Revision: before.Revision}, nil
}

func decodeCheckpoint(body []byte) (Checkpoint, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var checkpoint Checkpoint
	if err := decoder.Decode(&checkpoint); err != nil {
		return Checkpoint{}, fmt.Errorf("%w: decode: %v", ErrCheckpointMismatch, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Checkpoint{}, fmt.Errorf("%w: trailing checkpoint data", ErrCheckpointMismatch)
	}
	if err := validateCheckpoint(checkpoint); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}

func validateCheckpoint(checkpoint Checkpoint) error {
	if checkpoint.Version != CheckpointVersion || validateJobID(checkpoint.JobID) != nil || !volumeIDPattern.MatchString(checkpoint.VolumeID) || checkpoint.ConfigHash == "" || checkpoint.EBSRoot == "" || checkpoint.SourceSubpath == "" || checkpoint.SourceRoot == "" || checkpoint.Endpoint == "" || checkpoint.SpaceRef == "" || checkpoint.Prefix == "" || checkpoint.CredentialRef == "" {
		return fmt.Errorf("%w: incomplete immutable identity", ErrCheckpointMismatch)
	}
	if validateSourceSubpath(checkpoint.SourceSubpath) != nil || !filepath.IsAbs(checkpoint.EBSRoot) || filepath.Clean(checkpoint.EBSRoot) != checkpoint.EBSRoot || checkpoint.SourceRoot != effectiveSourceRoot(checkpoint.EBSRoot, checkpoint.SourceSubpath) {
		return fmt.Errorf("%w: invalid source identity", ErrCheckpointMismatch)
	}
	if phaseRank(checkpoint.HighestPhase) == 0 || (checkpoint.FenceIntent && phaseRank(checkpoint.HighestPhase) < phaseRank(PhaseDualWriteRepairing)) || (checkpoint.HighestPhase == PhaseCutoverReady && !checkpoint.FenceComplete) || (checkpoint.FenceComplete && (!checkpoint.FenceIntent || checkpoint.HighestPhase != PhaseCutoverReady)) {
		return fmt.Errorf("%w: invalid phase or fence state", ErrCheckpointMismatch)
	}
	return nil
}

func sameCheckpointIdentity(left, right Checkpoint) bool {
	return left.Version == right.Version && left.JobID == right.JobID && left.ConfigHash == right.ConfigHash && left.VolumeID == right.VolumeID && left.EBSRoot == right.EBSRoot && left.SourceSubpath == right.SourceSubpath && left.SourceRoot == right.SourceRoot && left.Endpoint == right.Endpoint && left.SpaceRef == right.SpaceRef && left.Prefix == right.Prefix && left.CredentialRef == right.CredentialRef
}

func validateCheckpointTransition(current, next Checkpoint, creating bool) error {
	if err := validateCheckpoint(next); err != nil {
		return err
	}
	if creating {
		return nil
	}
	if !sameCheckpointIdentity(current, next) || phaseRank(next.HighestPhase) < phaseRank(current.HighestPhase) || (current.FenceIntent && !next.FenceIntent) || (current.FenceComplete && !next.FenceComplete) {
		return fmt.Errorf("%w: regressive checkpoint transition", ErrCheckpointMismatch)
	}
	return nil
}

// Update writes a complete checkpoint with exact Revision CAS and verifies it.
func (s *CheckpointStore) Update(ctx context.Context, current CheckpointRecord, next Checkpoint) (CheckpointRecord, error) {
	if err := validateCheckpointTransition(current.Checkpoint, next, current.Revision == 0); err != nil {
		return CheckpointRecord{}, err
	}
	checkpointPath, err := CheckpointPath(next.JobID)
	if err != nil {
		return CheckpointRecord{}, err
	}
	if current.Revision == 0 {
		if err := s.ensureDirectories(ctx, next.JobID); err != nil {
			return CheckpointRecord{}, err
		}
	}
	if s.beforeWrite != nil {
		if err := s.beforeWrite(next); err != nil {
			return CheckpointRecord{}, err
		}
	}
	body, err := json.Marshal(next)
	if err != nil {
		return CheckpointRecord{}, err
	}
	_, err = s.client.WriteCtxConditionalWithRevision(ctx, checkpointPath, body, current.Revision)
	if err != nil {
		if errors.Is(err, client.ErrConflict) {
			return CheckpointRecord{}, fmt.Errorf("%w: %w", ErrCheckpointConflict, err)
		}
		return CheckpointRecord{}, fmt.Errorf("write checkpoint: %w", err)
	}
	if s.afterWrite != nil {
		if err := s.afterWrite(next); err != nil {
			return CheckpointRecord{}, err
		}
	}
	written, err := s.Load(ctx, next.JobID)
	if err != nil {
		return CheckpointRecord{}, err
	}
	if written.Revision <= current.Revision || written.Checkpoint != next {
		return CheckpointRecord{}, fmt.Errorf("%w: post-CAS checkpoint mismatch", ErrCheckpointConflict)
	}
	return written, nil
}

func (s *CheckpointStore) ensureDirectories(ctx context.Context, jobID string) error {
	for _, directory := range []string{ControlPrefix + "/", ControlPrefix + "/jobs/", ControlPrefix + "/jobs/" + jobID + "/"} {
		if err := s.client.MkdirCtx(ctx, directory, 0o700); err != nil {
			if !errors.Is(err, client.ErrConflict) {
				return fmt.Errorf("create checkpoint directory: %w", err)
			}
			stat, statErr := s.client.StatCtx(ctx, directory)
			if statErr != nil || !stat.IsDir {
				return fmt.Errorf("checkpoint directory conflict: %w", err)
			}
		}
	}
	return nil
}

// Recover restores only monotonic control state and returns a fresh empty State.
func (s *CheckpointStore) Recover(ctx context.Context, startup *Startup) (*Recovery, error) {
	if startup == nil || startup.Config == nil {
		return nil, fmt.Errorf("%w: missing startup", ErrCheckpointMismatch)
	}
	desired := checkpointFromStartup(startup)
	record, err := s.Load(ctx, desired.JobID)
	if err != nil {
		return nil, err
	}
	cutoverRequested := startup.Phase == PhaseCutoverReady
	if cutoverRequested {
		if record.Revision == 0 {
			return nil, fmt.Errorf("%w: %w: CUTOVER_READY requires an existing DUAL_WRITE_REPAIRING checkpoint", ErrInvalidPhase, ErrCheckpointMismatch)
		}
		if !sameCheckpointIdentity(record.Checkpoint, desired) {
			return nil, fmt.Errorf("%w: immutable job identity", ErrCheckpointMismatch)
		}
		if phaseRank(record.Checkpoint.HighestPhase) < phaseRank(PhaseDualWriteRepairing) {
			return nil, fmt.Errorf("%w: %w: CUTOVER_READY requires an existing DUAL_WRITE_REPAIRING checkpoint", ErrInvalidPhase, ErrCheckpointMismatch)
		}
	} else if record.Revision == 0 {
		record, err = s.Update(ctx, CheckpointRecord{}, desired)
		if err != nil {
			return nil, err
		}
	} else {
		if !sameCheckpointIdentity(record.Checkpoint, desired) {
			return nil, fmt.Errorf("%w: immutable job identity", ErrCheckpointMismatch)
		}
		if !record.Checkpoint.FenceIntent && phaseRank(startup.Phase) < phaseRank(record.Checkpoint.HighestPhase) {
			return nil, fmt.Errorf("%w: requested phase rollback", ErrCheckpointMismatch)
		}
		if phaseRank(startup.Phase) > phaseRank(record.Checkpoint.HighestPhase) {
			next := record.Checkpoint
			next.HighestPhase = startup.Phase
			record, err = s.Update(ctx, record, next)
			if err != nil {
				return nil, err
			}
		}
	}
	checkpoint := record.Checkpoint
	fenceRecovery := checkpoint.FenceIntent && !checkpoint.FenceComplete
	writesAllowed := !checkpoint.FenceIntent && checkpoint.HighestPhase != PhaseCutoverReady
	return &Recovery{
		State: NewState(checkpoint.HighestPhase), Record: record,
		WritesAllowed: writesAllowed, DeepRecoveryRequired: writesAllowed,
		FenceRecoveryOnly: fenceRecovery,
	}, nil
}
