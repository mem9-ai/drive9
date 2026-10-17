package fuse

import (
	"bytes"
	"context"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

const releaseSpillTestSize = writeBackThreshold + (1 << 20)

func newFtruncateReleaseSpill(t *testing.T) (*Dat9FS, uint64, uint64, *truncateHandoffRemote) {
	t.Helper()
	const size = releaseSpillTestSize
	fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), size+4096))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	owner, _ := fs.fileHandles.Get(a)
	owner.Lock()
	// Existing-file fixture uses the same spill flag as Create's shadow path.
	owner.ShadowSpill = true
	owner.Unlock()
	truncateHandoffTruncate(t, fs, ino, a, size)
	// Supply the complete image, as the spill path for a locally written
	// file does; an untouched lazy remote prefix cannot be locally staged.
	hardlinkWrite(t, fs, ino, a, string(bytes.Repeat([]byte("p"), size)))
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatal(st)
	}
	owner.Lock()
	ready := owner.ShadowCommitReady && owner.ShadowCommitSeq == owner.DirtySeq && owner.DirtySeq != 0
	owner.Unlock()
	if !ready {
		t.Fatal("public Flush did not stage the large spill image")
	}
	fs.commitQueue.mu.Lock()
	fs.commitQueue.maxPending = 0 // deterministic queue backpressure
	fs.commitQueue.mu.Unlock()
	return fs, ino, a, remote
}

func TestFtruncateReleaseSpillFailedHandoff(t *testing.T) {
	fs, ino, a, remote := newFtruncateReleaseSpill(t)
	var reject atomic.Bool
	reject.Store(true)
	interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if reject.Load() && r.Method == http.MethodPut {
			w.WriteHeader(http.StatusForbidden)
			return true
		}
		return false
	})
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	ctx, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	if response := fs.Drain(ctx); response.OK {
		t.Fatal("failed fallback was falsely reported drained")
	}
	owner, ok := fs.fileHandles.Get(a)
	if !ok {
		t.Fatal("failed handoff lost its retry owner")
	}
	owner.Lock()
	dirty := owner.DirtySeq != 0 && owner.Dirty.HasDirtyParts()
	owner.Unlock()
	if !dirty {
		t.Fatal("rejected handoff cleared the retry obligation")
	}
	if _, revision, _ := remote.snapshot(); revision != 1 {
		t.Fatal("failure injection did not reject the upload")
	}
	reject.Store(false)
	ctx2, stop2 := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop2()
	if response := fs.Drain(ctx2); !response.OK {
		t.Fatalf("recovery=%+v", response)
	}
	if _, ok := fs.fileHandles.Get(a); ok {
		t.Fatal("successful recovery did not finalize owner")
	}
	if data, revision, _ := remote.snapshot(); len(data) != releaseSpillTestSize || revision != 2 {
		t.Fatalf("recovery content size/revision=%d/%d", len(data), revision)
	}
}
