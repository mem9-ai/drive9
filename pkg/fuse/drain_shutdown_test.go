package fuse

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func drainFailedResponse(mp string) mountcontrol.DrainResponse {
	resp := mountcontrol.NewDrainResponse(mp, time.Now())
	resp.Fail("pending_work_remaining", "/big.bin", errors.New("3 entries remain"))
	resp.Pending.DirtyHandles = 2
	resp.Pending.CommitQueuePending = 1
	resp.Pending.CommitQueueBytes = 4096
	resp.Finish(time.Now())
	return resp
}

// TestRecordDrainFailurePersistsExitReason locks the contract `drive9 umount`
// relies on: a shutdown drain that left work behind is recorded as
// drain_failed with the pending counts, and the worker exits with the
// drain-failed code.
func TestRecordDrainFailurePersistsExitReason(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	mp := t.TempDir()
	err := recordDrainFailure(MountConfig{MountPoint: mp}, drainFailedResponse(mp))
	if err == nil {
		t.Fatal("recordDrainFailure returned nil")
	}
	var exitErr *MountExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error %T = %v, want *MountExitError", err, err)
	}
	if exitErr.Code != ExitDrainFailed {
		t.Fatalf("exit code = %d, want %d", exitErr.Code, ExitDrainFailed)
	}
	if !strings.Contains(exitErr.Detail, "pending_work_remaining") {
		t.Fatalf("detail %q missing the drain error kind", exitErr.Detail)
	}

	rec, _, rerr := mountstate.ReadExitReason(mp)
	if rerr != nil {
		t.Fatalf("read exit reason: %v", rerr)
	}
	if rec.Code != ExitDrainFailed || rec.Reason != string(ExitReasonDrainFailed) {
		t.Fatalf("record = %+v, want code=%d reason=drain_failed", rec, ExitDrainFailed)
	}
	if rec.PID != os.Getpid() {
		t.Fatalf("record pid = %d, want %d", rec.PID, os.Getpid())
	}
	if rec.PendingFiles != 3 {
		t.Fatalf("pending files = %d, want 3", rec.PendingFiles)
	}
	if rec.PendingBytes != 4096 {
		t.Fatalf("pending bytes = %d, want 4096", rec.PendingBytes)
	}
}

func TestRecordDrainFailureRedactsCredentials(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	mp := t.TempDir()
	cfg := MountConfig{MountPoint: mp, Token: "secret-token-value", OmitProcessStateCredentials: true}
	resp := drainFailedResponse(mp)
	resp.Error = "upload rejected for secret-token-value"
	// recordDrainFailure always returns the worker's exit error; the record
	// it persists is what this test inspects.
	_ = recordDrainFailure(cfg, resp)
	rec, _, rerr := mountstate.ReadExitReason(mp)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if strings.Contains(rec.Detail, "secret-token-value") {
		t.Fatalf("detail leaked the credential: %s", rec.Detail)
	}
	if !strings.Contains(rec.Detail, "<redacted>") {
		t.Fatalf("detail %q not redacted", rec.Detail)
	}
}

func TestShutdownResultReportsFailure(t *testing.T) {
	var r shutdownResult
	if err := r.err(); err != nil {
		t.Fatalf("empty result = %v", err)
	}
	r.set(nil)
	if err := r.err(); err != nil {
		t.Fatalf("nil set changed the result: %v", err)
	}
	r.set(errors.New("first"))
	r.set(errors.New("second"))
	err := r.err()
	if err == nil || !strings.Contains(err.Error(), "first") || !strings.Contains(err.Error(), "second") {
		t.Fatalf("joined result = %v", err)
	}
}

// TestCleanShutdownDrainKeepsExitClean pins the other half: a drain that
// commits everything reports no error, so a plain unmount stays successful.
func TestCleanShutdownDrainKeepsExitClean(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	mp := t.TempDir()
	resp := mountcontrol.NewDrainResponse(mp, time.Now())
	resp.Finish(time.Now())
	if !resp.OK {
		t.Fatal("fresh drain response is not OK")
	}
	var r shutdownResult
	if !resp.OK {
		r.set(errors.New("drain failed"))
	}
	if err := r.err(); err != nil {
		t.Fatalf("clean drain set an error: %v", err)
	}
	if _, _, err := mountstate.ReadExitReason(mp); err == nil {
		t.Fatal("clean drain wrote an exit reason")
	}
}
