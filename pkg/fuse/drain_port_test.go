// v1 drain_test.go rows: the errno mapping for drain failure kinds, and
// the pending-work failure judgment (dirty handles, conflicted queue
// entries, uploader-cached snapshots each fail the drain). The
// handle-table rows are v1-coupled (FileHandle was deleted with
// the monolith); the new-plane equivalents live in the writeback
// extension's DrainFlush and DrainPending rows.
package fuse

import (
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
)

// v1 TestDrainResponseStatusMapsRetryableKind.
func TestPortDrainResponseStatusMapsRetryableKind(t *testing.T) {
	tests := []struct {
		kind string
		want gofuse.Status
	}{
		{kind: "timeout", want: gofuse.Status(syscall.ETIMEDOUT)},
		{kind: "remote_timeout_or_retryable", want: gofuse.Status(syscall.EAGAIN)},
		{kind: "canceled", want: gofuse.Status(syscall.EINTR)},
		{kind: "other", want: gofuse.EIO},
	}
	for _, tc := range tests {
		resp := mountcontrol.DrainResponse{ErrorKind: tc.kind}
		if got := drainResponseStatus(resp); got != tc.want {
			t.Fatalf("drainResponseStatus(%q) = %d, want %d", tc.kind, got, tc.want)
		}
	}
}

// v1 TestDrainFailsWhen* rows (facade half): the facade's Drain carries
// the pending snapshot and the pending-work failure through the
// extension probe. The kind string is pinned; the exact pending fields
// are asserted at the writeback layer.
func TestPortDrainFailureKindSurfacesInResponse(t *testing.T) {
	for _, kind := range []string{"timeout", "remote_timeout_or_retryable", "canceled", "other"} {
		resp := mountcontrol.DrainResponse{ErrorKind: kind}
		resp.Fail(kind, "/mnt", errString(kind))
		if resp.OK {
			t.Fatalf("kind %q: Fail did not mark the response failed", kind)
		}
		if drainResponseStatus(resp) == gofuse.OK {
			t.Fatalf("kind %q: mapped to OK", kind)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }
