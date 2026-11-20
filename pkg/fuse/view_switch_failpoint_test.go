//go:build failpoint && !windows

package fuse

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestViewSwitchDecisionCancellationAndUnknownDurability(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "unknown"}[unknown], func(t *testing.T) {
			s, _, req, events := viewSwitchFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			name := "github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeSync"
			if err := failpoint.EnableCall(name, func(err *error) {
				cancel()
				if unknown {
					*err = syscall.EIO
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(name) })
			resp := s.apply(ctx, req)
			if unknown {
				if resp.ErrorKind != "UNKNOWN" || resp.Receipt != nil {
					t.Fatalf("uncertain decision: %+v", resp)
				}
				if _, err := s.journal.Snapshot(); !errors.Is(err, mountstate.ErrSwitchUncertain) {
					t.Fatalf("journal: %v", err)
				}
			} else if resp.ErrorKind != "RECOVERING" || resp.Receipt == nil || resp.Receipt.Target != req.Target {
				t.Fatalf("durable decision: %+v", resp)
			}
			if _, err := s.adapter.admission.enter(); !errors.Is(err, errViewAdmissionPaused) {
				t.Fatalf("reopened admission: %v", err)
			}
			for _, event := range *events {
				if event == "stop" || event == "build" {
					t.Fatalf("advanced canceled switch: %v", *events)
				}
			}
			if err := failpoint.Disable(name); err != nil {
				t.Fatal(err)
			}
			if !unknown {
				if got := s.apply(t.Context(), req); got.Error != "" || got.Receipt.State != mountstate.SwitchDone {
					t.Fatal(got)
				}
				return
			}
			// An unusable journal cannot grant any further decisions. Real
			// reopen/crash persistence is covered by mountstate tests.
			if err := s.journal.Close(); err != nil {
				t.Fatal(err)
			}
			if got := s.apply(t.Context(), req); got.ErrorKind != "UNKNOWN" || got.Receipt != nil {
				t.Fatal(got)
			}
		})
	}
}
