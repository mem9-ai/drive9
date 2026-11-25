//go:build !windows

package mountstate

import (
	"fmt"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func (j *SwitchJournal) Uncertain() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.uncertain
}

func (j *SwitchJournal) Snapshot() SwitchSnapshot {
	j.mu.Lock()
	defer j.mu.Unlock()
	var pend *SwitchReceipt
	if j.pending != nil {
		cp := *j.pending
		pend = &cp
	}
	unbinds := append([]PendingUnbind(nil), j.pendingUnbinds...)
	return SwitchSnapshot{
		MountID:        j.mountID,
		Generation:     j.generation,
		Binding:        j.binding,
		Pending:        pend,
		PendingUnbinds: unbinds,
	}
}

func (j *SwitchJournal) Operation(operationID string) (SwitchReceipt, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	rec, ok := j.history[operationID]
	return rec, ok
}

func addPendingUnbindBounded(list []PendingUnbind, item PendingUnbind) ([]PendingUnbind, error) {
	if err := item.Validate(); err != nil {
		return nil, err
	}
	for _, existing := range list {
		if existing == item {
			return list, nil
		}
	}
	if len(list) >= MaxPendingUnbinds {
		return nil, fmt.Errorf("%w: pending_unbinds full (%d)", ErrSwitchUnbindPending, len(list))
	}
	out := make([]PendingUnbind, len(list)+1)
	copy(out, list)
	out[len(list)] = item
	return out, nil
}

func removePendingUnbind(list []PendingUnbind, item PendingUnbind) ([]PendingUnbind, bool) {
	out := make([]PendingUnbind, 0, len(list))
	removed := false
	for _, existing := range list {
		if existing == item {
			removed = true
			continue
		}
		out = append(out, existing)
	}
	return out, removed
}

func (j *SwitchJournal) applyRecordLocked(rec switchJournalRecord, first bool) error {
	if first && rec.Type != "init" {
		return fmt.Errorf("expected init record, got %q", rec.Type)
	}
	if !first && rec.Type == "init" {
		return fmt.Errorf("duplicate init record")
	}
	switch rec.Type {
	case "init":
		if err := viewprotocol.ValidateOperationID(rec.MountID); err != nil {
			return err
		}
		if rec.Scope == "" || rec.Generation != 1 {
			return fmt.Errorf("invalid init metadata")
		}
		if err := rec.Target.Validate(); err != nil {
			return err
		}
		j.mountID = rec.MountID
		j.scope = rec.Scope
		j.generation = 1
		j.binding = rec.Target
		return nil
	case "bind_intent":
		if rec.Unbind == nil || rec.Unbind.Kind != UnbindKindOp || rec.Unbind.MountID != j.mountID {
			return fmt.Errorf("invalid bind_intent record")
		}
		next, err := addPendingUnbindBounded(j.pendingUnbinds, *rec.Unbind)
		if err != nil {
			return err
		}
		j.pendingUnbinds = next
		return nil
	case "bind_initial":
		if j.generation != 1 || j.pending != nil || j.binding.LayerID != "" {
			return fmt.Errorf("invalid bind_initial state")
		}
		if err := rec.Target.Validate(); err != nil || rec.Target.LayerID == "" {
			return fmt.Errorf("invalid bind_initial target")
		}
		if !rec.Target.ReadOnly {
			next := make([]PendingUnbind, 0, len(j.pendingUnbinds))
			matched := false
			for _, u := range j.pendingUnbinds {
				if u.LayerID == rec.Target.LayerID && u.MountID == j.mountID && u.Kind == UnbindKindOp && u.OperationID == rec.OperationID {
					matched = true
					continue
				}
				next = append(next, u)
			}
			if !matched {
				return fmt.Errorf("bind_initial missing matching bind_intent")
			}
			j.pendingUnbinds = next
		}
		j.binding = rec.Target
		return nil
	case "unbind_done":
		if rec.Unbind == nil {
			return fmt.Errorf("missing unbind payload")
		}
		next, _ := removePendingUnbind(j.pendingUnbinds, *rec.Unbind)
		j.pendingUnbinds = next
		return nil
	case "decide":
		if j.pending != nil || rec.FromGeneration != j.generation || rec.Generation != j.generation+1 {
			return fmt.Errorf("invalid decide generation transition")
		}
		if err := viewprotocol.ValidateOperationID(rec.OperationID); err != nil {
			return err
		}
		if err := rec.Target.Validate(); err != nil {
			return err
		}
		if err := validateDecisionLayerRef(rec.Target.LayerID, rec.RequestedLayerRef); err != nil {
			return err
		}
		nextUnbinds := append([]PendingUnbind(nil), j.pendingUnbinds...)
		if rec.Target.LayerID != "" && !rec.Target.ReadOnly {
			filtered := make([]PendingUnbind, 0, len(nextUnbinds))
			matched := false
			for _, u := range nextUnbinds {
				if u.LayerID == rec.Target.LayerID && u.MountID == j.mountID && u.Kind == UnbindKindOp && u.OperationID == rec.OperationID {
					matched = true
					continue
				}
				filtered = append(filtered, u)
			}
			if !matched {
				return fmt.Errorf("decide missing bind_intent for %s op %s", rec.Target.LayerID, rec.OperationID)
			}
			nextUnbinds = filtered
		}
		if len(nextUnbinds) != 0 {
			return fmt.Errorf("decide while pending_unbinds non-empty")
		}
		rcpt := SwitchReceipt{
			Recovery:          j.abortedPending,
			RequestedLayerRef: rec.RequestedLayerRef,
			OperationID:       rec.OperationID,
			FromGeneration:    rec.FromGeneration,
			Generation:        rec.Generation,
			From:              j.binding,
			Target:            rec.Target,
			State:             SwitchRecovering,
		}
		j.pendingUnbinds = nil
		j.generation = rec.Generation
		j.binding = rec.Target
		j.pending = &rcpt
		j.abortedPending = false
		j.history[rec.OperationID] = rcpt
		return nil
	case "target_revoked":
		if j.pending == nil || j.pending.OperationID != rec.OperationID {
			return fmt.Errorf("target_revoked without matching pending op")
		}
		j.pending.State = SwitchTargetRevoked
		j.history[rec.OperationID] = *j.pending
		return nil
	case "complete":
		if j.pending == nil || j.pending.OperationID != rec.OperationID || j.pending.State != SwitchRecovering {
			return fmt.Errorf("complete without matching RECOVERING op")
		}
		done := *j.pending
		done.State = SwitchDone
		nextUnbinds := append([]PendingUnbind(nil), j.pendingUnbinds...)
		if done.From.LayerID != "" && !done.From.ReadOnly {
			var err error
			nextUnbinds, err = addPendingUnbindBounded(nextUnbinds, PendingUnbind{
				LayerID: done.From.LayerID,
				MountID: j.mountID,
				Kind:    UnbindKindFence,
				Fence:   done.From.Fence,
			})
			if err != nil {
				return err
			}
		}
		j.pendingUnbinds = nextUnbinds
		j.pending = nil
		j.history[done.OperationID] = done
		return nil
	case "abort_pending":
		if j.pending == nil || j.pending.OperationID != rec.OperationID {
			return fmt.Errorf("abort_pending without matching pending op")
		}
		aborted := *j.pending
		aborted.State = SwitchAborted
		nextUnbinds := append([]PendingUnbind(nil), j.pendingUnbinds...)
		for _, b := range [2]viewprotocol.SwitchBinding{aborted.From, aborted.Target} {
			if b.LayerID != "" && !b.ReadOnly {
				var err error
				nextUnbinds, err = addPendingUnbindBounded(nextUnbinds, PendingUnbind{
					LayerID: b.LayerID,
					MountID: j.mountID,
					Kind:    UnbindKindFence,
					Fence:   b.Fence,
				})
				if err != nil {
					return err
				}
			}
		}
		j.pendingUnbinds = nextUnbinds
		j.pending = nil
		j.abortedPending = true
		j.binding = viewprotocol.SwitchBinding{ReadOnly: aborted.From.ReadOnly && aborted.Target.ReadOnly}
		j.history[aborted.OperationID] = aborted
		return nil
	case "queue_unbind":
		if j.pending != nil {
			return fmt.Errorf("queue_unbind while decision pending")
		}
		if j.binding.LayerID != "" && !j.binding.ReadOnly {
			next, err := addPendingUnbindBounded(j.pendingUnbinds, PendingUnbind{
				LayerID: j.binding.LayerID,
				MountID: j.mountID,
				Kind:    UnbindKindFence,
				Fence:   j.binding.Fence,
			})
			if err != nil {
				return err
			}
			j.pendingUnbinds = next
		}
		j.binding = viewprotocol.SwitchBinding{ReadOnly: j.binding.ReadOnly}
		return nil
	default:
		return fmt.Errorf("unknown switch journal record %q", rec.Type)
	}
}

func validateDecisionLayerRef(layerID, ref string) error {
	if ref == "" {
		return nil
	} // Older records only contain the resolved layer ID.
	norm, err := viewprotocol.NormalizeLayerRef(ref)
	if err != nil || norm != ref || layerID == "" || (viewprotocol.IsCanonicalLayerID(ref) && ref != layerID) {
		return fmt.Errorf("%w: invalid requested layer reference", ErrSwitchInvalid)
	}
	return nil
}
