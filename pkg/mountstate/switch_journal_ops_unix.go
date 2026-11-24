//go:build !windows

package mountstate

import (
	"context"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func (j *SwitchJournal) AppendBindIntent(item PendingUnbind) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.uncertain {
		return ErrSwitchUncertain
	}
	if j.pending != nil {
		return ErrSwitchPending
	}
	if err := item.Validate(); err != nil {
		return err
	}
	if item.Kind != UnbindKindOp || item.MountID != j.mountID {
		return fmt.Errorf("%w: bind_intent requires kind=op and matching mount_id", ErrSwitchInvalid)
	}
	for _, existing := range j.pendingUnbinds {
		if existing == item {
			return nil
		}
	}
	if len(j.pendingUnbinds) >= MaxPendingUnbinds {
		return fmt.Errorf("%w: pending_unbinds full", ErrSwitchUnbindPending)
	}
	cp := item
	return j.appendAndApplyLocked(switchJournalRecord{Type: "bind_intent", Unbind: &cp})
}

func (j *SwitchJournal) AppendUnbindDone(item PendingUnbind) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.uncertain {
		return ErrSwitchUncertain
	}
	found := false
	for _, existing := range j.pendingUnbinds {
		if existing == item {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	cp := item
	return j.appendAndApplyLocked(switchJournalRecord{Type: "unbind_done", Unbind: &cp})
}

func (j *SwitchJournal) BindInitial(target viewprotocol.SwitchBinding, operationID string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.uncertain {
		return ErrSwitchUncertain
	}
	if err := target.Validate(); err != nil || target.LayerID == "" {
		return fmt.Errorf("%w: invalid initial layer target", ErrSwitchInvalid)
	}
	if !target.ReadOnly {
		if err := viewprotocol.ValidateOperationID(operationID); err != nil {
			return fmt.Errorf("%w: %v", ErrSwitchInvalid, err)
		}
	}
	return j.appendAndApplyLocked(switchJournalRecord{
		Type:        "bind_initial",
		OperationID: operationID,
		Target:      target,
	})
}

func (j *SwitchJournal) Decide(operationID string, expectedGeneration uint64, target viewprotocol.SwitchBinding) (SwitchReceipt, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.uncertain {
		return SwitchReceipt{}, false, ErrSwitchUncertain
	}
	if err := viewprotocol.ValidateOperationID(operationID); err != nil {
		return SwitchReceipt{}, false, fmt.Errorf("%w: %v", ErrSwitchInvalid, err)
	}
	if err := target.Validate(); err != nil {
		return SwitchReceipt{}, false, fmt.Errorf("%w: %v", ErrSwitchInvalid, err)
	}
	if prior, ok := j.history[operationID]; ok {
		if prior.FromGeneration == expectedGeneration && prior.Target.LayerID == target.LayerID && prior.Target.ReadOnly == target.ReadOnly {
			return prior, true, nil
		}
		return SwitchReceipt{}, false, fmt.Errorf("%w: operation_id %q already bound to different switch", ErrSwitchConflict, operationID)
	}
	if j.pending != nil {
		return SwitchReceipt{}, false, ErrSwitchPending
	}
	if expectedGeneration != j.generation {
		return SwitchReceipt{}, false, fmt.Errorf("%w: expected_generation %d != current %d", ErrSwitchConflict, expectedGeneration, j.generation)
	}
	if !j.abortedPending && target.LayerID == j.binding.LayerID {
		return SwitchReceipt{}, false, fmt.Errorf("%w: target matches current active view", ErrSwitchConflict)
	}
	hasTargetIntent := false
	extraUnbinds := 0
	for _, u := range j.pendingUnbinds {
		if !target.ReadOnly && target.LayerID != "" && u.LayerID == target.LayerID && u.MountID == j.mountID && u.Kind == UnbindKindOp && u.OperationID == operationID {
			hasTargetIntent = true
			continue
		}
		extraUnbinds++
	}
	if extraUnbinds > 0 {
		return SwitchReceipt{}, false, ErrSwitchUnbindPending
	}
	if target.LayerID != "" && !target.ReadOnly && !hasTargetIntent {
		return SwitchReceipt{}, false, fmt.Errorf("%w: missing persisted bind_intent for %s", ErrSwitchInvalid, target.LayerID)
	}
	if err := j.appendAndApplyLocked(switchJournalRecord{
		Type:           "decide",
		OperationID:    operationID,
		FromGeneration: expectedGeneration,
		Generation:     expectedGeneration + 1,
		From:           j.binding,
		Target:         target,
	}); err != nil {
		return SwitchReceipt{}, false, err
	}
	return *j.pending, false, nil
}

func (j *SwitchJournal) MarkTargetRevoked(operationID string) (SwitchReceipt, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.uncertain {
		return SwitchReceipt{}, ErrSwitchUncertain
	}
	if j.pending == nil || j.pending.OperationID != operationID {
		return SwitchReceipt{}, fmt.Errorf("%w: operation %q is not pending", ErrSwitchConflict, operationID)
	}
	if j.pending.State == SwitchTargetRevoked {
		return *j.pending, nil
	}
	if err := j.appendAndApplyLocked(switchJournalRecord{Type: "target_revoked", OperationID: operationID}); err != nil {
		return SwitchReceipt{}, err
	}
	return *j.pending, nil
}

func (j *SwitchJournal) Complete(operationID string) (SwitchReceipt, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.uncertain {
		return SwitchReceipt{}, ErrSwitchUncertain
	}
	if prior, ok := j.history[operationID]; ok && prior.State == SwitchDone {
		return prior, nil
	}
	if j.pending == nil || j.pending.OperationID != operationID || j.pending.State != SwitchRecovering {
		return SwitchReceipt{}, fmt.Errorf("%w: operation %q cannot complete", ErrSwitchConflict, operationID)
	}
	if err := j.appendAndApplyLocked(switchJournalRecord{Type: "complete", OperationID: operationID}); err != nil {
		return SwitchReceipt{}, err
	}
	return j.history[operationID], nil
}

func (j *SwitchJournal) AbortPendingForRecover(operationID string) (SwitchReceipt, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.uncertain {
		return SwitchReceipt{}, ErrSwitchUncertain
	}
	if j.pending == nil || j.pending.OperationID != operationID {
		return SwitchReceipt{}, fmt.Errorf("%w: operation %q is not pending", ErrSwitchConflict, operationID)
	}
	if j.pending.State != SwitchTargetRevoked {
		return SwitchReceipt{}, ErrSwitchNotRevoked
	}
	if err := j.appendAndApplyLocked(switchJournalRecord{Type: "abort_pending", OperationID: operationID}); err != nil {
		return SwitchReceipt{}, err
	}
	return j.history[operationID], nil
}

func (j *SwitchJournal) AbortPendingForUnmount() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.uncertain {
		return ErrSwitchUncertain
	}
	if j.pending == nil {
		return nil
	}
	return j.appendAndApplyLocked(switchJournalRecord{Type: "abort_pending", OperationID: j.pending.OperationID})
}

func (j *SwitchJournal) QueueCurrentUnbindForUnmount() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.uncertain {
		return ErrSwitchUncertain
	}
	if j.file == nil {
		return ErrSwitchClosed
	}
	if j.pending != nil {
		return j.appendAndApplyLocked(switchJournalRecord{Type: "abort_pending", OperationID: j.pending.OperationID})
	}
	if j.binding.LayerID == "" || j.binding.ReadOnly {
		return nil
	}
	return j.appendAndApplyLocked(switchJournalRecord{Type: "queue_unbind"})
}

func (j *SwitchJournal) DrainPendingUnbinds(ctx context.Context, unbindFn func(context.Context, PendingUnbind) error) error {
	snap := j.Snapshot()
	if j.Uncertain() {
		return ErrSwitchUncertain
	}
	var firstErr error
	for _, item := range snap.PendingUnbinds {
		if err := unbindFn(ctx, item); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := j.AppendUnbindDone(item); err != nil {
			return err
		}
	}
	return firstErr
}
