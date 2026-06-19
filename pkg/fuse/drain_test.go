package fuse

import (
	"context"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/dat9/pkg/mountcontrol"
)

func newTestDrainFS() *Dat9FS {
	return &Dat9FS{
		fileHandles: NewHandleTable[*FileHandle](),
		opts:        &MountOptions{MountPoint: "/mnt/drive9"},
	}
}

func TestDrainResponseStatusMapsRetryableKind(t *testing.T) {
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

func TestDrainAllowsCleanOpenHandles(t *testing.T) {
	fs := newTestDrainFS()
	fs.fileHandles.Allocate(&FileHandle{Path: "/clean.txt"})

	resp := fs.Drain(context.Background())
	if !resp.OK {
		t.Fatalf("Drain OK = false, error_kind=%q error=%q pending=%+v", resp.ErrorKind, resp.Error, resp.Pending)
	}
	if resp.Pending.OpenHandles != 1 || resp.Pending.DirtyHandles != 0 {
		t.Fatalf("pending = %+v, want one clean open handle", resp.Pending)
	}
}

func TestDrainFailsWhenFinalDirtyHandleRemains(t *testing.T) {
	fs := newTestDrainFS()
	fs.fileHandles.Allocate(&FileHandle{Path: "/dirty.txt", DirtySeq: 1})

	resp := fs.Drain(context.Background())
	if resp.OK {
		t.Fatalf("Drain OK = true with pending=%+v", resp.Pending)
	}
	if resp.ErrorKind != drainPendingWorkKind {
		t.Fatalf("ErrorKind = %q, want %q", resp.ErrorKind, drainPendingWorkKind)
	}
	if resp.Pending.DirtyHandles != 1 {
		t.Fatalf("DirtyHandles = %d, want 1", resp.Pending.DirtyHandles)
	}
	if !hasDrainPhase(resp, "flush_open_handles") {
		t.Fatalf("phases = %+v, want flush_open_handles phase", resp.Phases)
	}
}

func TestDrainFailsWhenCommitConflictRemains(t *testing.T) {
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingIndex: %v", err)
	}
	if _, err := pending.PutWithBaseRev("/conflict.txt", 4, PendingOverwrite, 7); err != nil {
		t.Fatalf("PutWithBaseRev: %v", err)
	}
	if err := pending.MarkConflict("/conflict.txt"); err != nil {
		t.Fatalf("MarkConflict: %v", err)
	}
	fs := newTestDrainFS()
	fs.commitQueue = &CommitQueue{index: pending}

	resp := fs.Drain(context.Background())
	if resp.OK {
		t.Fatalf("Drain OK = true with pending=%+v", resp.Pending)
	}
	if resp.Pending.CommitQueueConflicts != 1 {
		t.Fatalf("CommitQueueConflicts = %d, want 1", resp.Pending.CommitQueueConflicts)
	}
	if resp.ErrorKind != drainPendingWorkKind {
		t.Fatalf("ErrorKind = %q, want %q", resp.ErrorKind, drainPendingWorkKind)
	}
}

func TestDrainFailsWhenUploaderCacheRemains(t *testing.T) {
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewWriteBackCache: %v", err)
	}
	if err := cache.Put("/cached.txt", []byte("data"), 4, PendingNew); err != nil {
		t.Fatalf("Put: %v", err)
	}
	fs := newTestDrainFS()
	fs.uploader = &WriteBackUploader{
		cache:    cache,
		uploadCh: make(chan string),
		inflight: make(map[string]*pathState),
	}

	resp := fs.Drain(context.Background())
	if resp.OK {
		t.Fatalf("Drain OK = true with pending=%+v", resp.Pending)
	}
	if resp.Pending.UploaderCached != 1 || resp.Pending.UploaderCachedBytes != 4 {
		t.Fatalf("uploader cached pending = %+v, want one 4-byte cache entry", resp.Pending)
	}
	if resp.ErrorKind != drainPendingWorkKind {
		t.Fatalf("ErrorKind = %q, want %q", resp.ErrorKind, drainPendingWorkKind)
	}
}

func hasDrainPhase(resp mountcontrol.DrainResponse, name string) bool {
	for _, phase := range resp.Phases {
		if phase.Name == name {
			return true
		}
	}
	return false
}
