//go:build windows

package mountstate

import (
	"context"
	"errors"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

var errSwitchJournalUnsupported = errors.New("mount switch journal is unsupported on windows")

type SwitchJournal struct{}

func OpenSwitchJournal(string, string, string, viewprotocol.SwitchBinding) (*SwitchJournal, error) {
	return nil, errSwitchJournalUnsupported
}

func OpenExistingSwitchJournal(string) (*SwitchJournal, error) {
	return nil, errSwitchJournalUnsupported
}

func (j *SwitchJournal) Close() error                                         { return nil }
func (j *SwitchJournal) Uncertain() bool                                      { return false }
func (j *SwitchJournal) Snapshot() SwitchSnapshot                             { return SwitchSnapshot{} }
func (j *SwitchJournal) Operation(string) (SwitchReceipt, bool)               { return SwitchReceipt{}, false }
func (j *SwitchJournal) AppendBindIntent(PendingUnbind) error                 { return errSwitchJournalUnsupported }
func (j *SwitchJournal) AppendUnbindDone(PendingUnbind) error                 { return errSwitchJournalUnsupported }
func (j *SwitchJournal) BindInitial(viewprotocol.SwitchBinding, string) error { return errSwitchJournalUnsupported }
func (j *SwitchJournal) Decide(string, uint64, viewprotocol.SwitchBinding) (SwitchReceipt, bool, error) {
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
