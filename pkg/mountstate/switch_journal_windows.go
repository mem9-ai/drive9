//go:build windows

package mountstate

import (
	"context"
	"errors"
)

var errSwitchJournalUnsupported = errors.New("mount switch journal is unsupported on windows")

type SwitchJournal struct{}

func OpenSwitchJournal(string, string, string, SwitchBinding) (*SwitchJournal, error) {
	return nil, errSwitchJournalUnsupported
}

func OpenExistingSwitchJournal(string) (*SwitchJournal, error) {
	return nil, errSwitchJournalUnsupported
}

func (j *SwitchJournal) Close() error                           { return nil }
func (j *SwitchJournal) Uncertain() bool                        { return false }
func (j *SwitchJournal) Snapshot() SwitchSnapshot               { return SwitchSnapshot{} }
func (j *SwitchJournal) Operation(string) (SwitchReceipt, bool) { return SwitchReceipt{}, false }
func (j *SwitchJournal) AppendBindIntent(PendingUnbind) error   { return errSwitchJournalUnsupported }
func (j *SwitchJournal) AppendUnbindDone(PendingUnbind) error   { return errSwitchJournalUnsupported }
func (j *SwitchJournal) BindInitial(SwitchBinding, string) error {
	return errSwitchJournalUnsupported
}
func (j *SwitchJournal) Decide(string, uint64, SwitchBinding) (SwitchReceipt, bool, error) {
	return SwitchReceipt{}, false, errSwitchJournalUnsupported
}
func (j *SwitchJournal) DecideWithLayerRef(string, uint64, SwitchBinding, string) (SwitchReceipt, bool, error) {
	return SwitchReceipt{}, false, errSwitchJournalUnsupported
}
func (j *SwitchJournal) MarkTargetRevoked(string) (SwitchReceipt, error) {
	return SwitchReceipt{}, errSwitchJournalUnsupported
}
func (j *SwitchJournal) Complete(string) (SwitchReceipt, error) {
	return SwitchReceipt{}, errSwitchJournalUnsupported
}
func (j *SwitchJournal) AbortPendingForRecover(string) (SwitchReceipt, error) {
	return SwitchReceipt{}, errSwitchJournalUnsupported
}
func (j *SwitchJournal) AbortPendingForUnmount() error { return errSwitchJournalUnsupported }
func (j *SwitchJournal) QueueCurrentUnbindForUnmount() error {
	return errSwitchJournalUnsupported
}
func (j *SwitchJournal) DrainPendingUnbinds(context.Context, func(context.Context, PendingUnbind) error) error {
	return errSwitchJournalUnsupported
}

func DrainPriorScopeJournals(context.Context, string, string, func(context.Context, PendingUnbind) error) (bool, error) {
	return false, errSwitchJournalUnsupported
}

func RemoveSwitchJournal(string) error {
	return errSwitchJournalUnsupported
}
