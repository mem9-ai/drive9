package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mem9-ai/drive9/pkg/backend"
)

// TestWriteErrorTimingResultClassification locks the write-error classification
// used by handleWrite / batch_write for logs, metrics, and server_write_timing:
// a client-cancelled write and a deadline-exceeded write are classified OUT of
// the generic "error" bucket (so they stop polluting real storage-error
// alerts), and they are classified DISTINCTLY from each other. Everything else
// stays "error". (task #198 gate: adversary-1 pts 1/3/7, adversary-2 gate test.)
func TestWriteErrorTimingResultClassification(t *testing.T) {
	bg := context.Background()

	// A context that is itself cancelled (models the client closing the
	// connection so the inbound request ctx is Done) even when the returned err
	// is a wrapped/opaque cancellation.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want string
	}{
		{"plain canceled", bg, context.Canceled, writeResultClientCanceled},
		{"wrapped canceled", bg, fmt.Errorf("backend put: %w", context.Canceled), writeResultClientCanceled},
		{"canceled via ctx only", canceledCtx, errors.New("connection reset"), writeResultClientCanceled},
		{"plain deadline", bg, context.DeadlineExceeded, writeResultDeadlineExceeded},
		{"wrapped deadline", bg, fmt.Errorf("backend put: %w", context.DeadlineExceeded), writeResultDeadlineExceeded},
		{"real storage error", bg, errors.New("s3: internal error"), "error"},
		{"sentinel upload too large", bg, backend.ErrUploadTooLarge, "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := writeErrorTimingResult(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("writeErrorTimingResult = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestWriteCanceledIsNotErrorBucket enforces adversary-1 pt 7: a client-cancelled
// write must NOT be classified as the generic "error" (Error-level write_failed)
// bucket, and cancellation must be classified separately from deadline.
func TestWriteCanceledIsNotErrorBucket(t *testing.T) {
	if writeResultClientCanceled == "error" {
		t.Fatal("client-canceled must not reuse the generic 'error' result (would keep polluting Error-level write_failed alerts)")
	}
	if writeResultClientCanceled == writeResultDeadlineExceeded {
		t.Fatal("client-canceled and deadline-exceeded must be classified distinctly")
	}
}

// TestWriteCanceledResultIsNeedsReviewNotBenign is the safety gate (adversary-2):
// the client-canceled classification MUST NOT be downgraded to a benign/ok/info
// label. A same-path supersede cancel can mask a lost update until durability is
// verified, so it must remain a needs-review signal. This test fails loudly if a
// future change silences it.
func TestWriteCanceledResultIsNeedsReviewNotBenign(t *testing.T) {
	forbidden := map[string]bool{"ok": true, "benign": true, "info": true, "success": true, "": true}
	if forbidden[writeResultClientCanceled] {
		t.Fatalf("writeResultClientCanceled = %q must express needs-review, not a benign/ok/info label", writeResultClientCanceled)
	}
	if forbidden[writeResultDeadlineExceeded] {
		t.Fatalf("writeResultDeadlineExceeded = %q must not be a benign/ok/info label", writeResultDeadlineExceeded)
	}
}
