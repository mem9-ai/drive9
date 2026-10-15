package fuse

import (
	"bytes"
	"context"
	"strings"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func issue1023ReadSized(t *testing.T, fs *Dat9FS, ino, id uint64, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	result, st := fs.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Size: uint32(size)}, buf)
	if st != gofuse.OK {
		t.Fatalf("actual Read status=%v", st)
	}
	defer result.Done()
	data, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("ReadResult status=%v", st)
	}
	return append([]byte(nil), data...)
}

func issue1023CreatedShadowFS(t *testing.T, path string, mode SyncMode, queued bool) (*Dat9FS, *casFileServer, gofuse.CreateOut) {
	t.Helper()
	server, ts := newCASFileServer(t, path, 0, nil)
	t.Cleanup(ts.Close)
	fs, _ := pr939HandleFS(t, path, "")
	fs.syncMode = mode
	fs.client = newTestClient(ts.URL)
	// The CAS fixture implements inline uploads, not multipart uploads.
	fs.client.SetSmallFileThresholdForTests(32 << 20)
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(fs.client, cache, 1)
	fs.SetWriteBack(cache, uploader)
	uploader.OnSuccess, uploader.SnapshotStagingGens = fs.onWriteBackUploadSuccess, fs.snapshotStagingGens
	t.Cleanup(uploader.DrainAll)
	if queued {
		cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
		fs.commitQueue = cq
		cq.PathLock, cq.DurableWatermark = fs.lockRemoteCommitPath, fs.latestCommittedRevision
		cq.OnSuccess, cq.OnUploaded = fs.onCommitQueueSuccess, fs.onCommitQueueUploaded
		cq.OnCleanup, cq.OnDiscard, cq.IsSuperseded = fs.onCommitQueueCleanup, fs.onCommitQueueDiscard, fs.commitEntrySuperseded
		t.Cleanup(cq.DrainAll)
	}
	var aOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1, Caller: gofuse.Caller{Pid: 101}}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND), Mode: defaultRegularFileMode}, strings.TrimPrefix(path, "/"), &aOut); st != gofuse.OK {
		t.Fatal(st)
	}
	a, _ := fs.fileHandles.Get(aOut.Fh)
	if !a.ShadowSpill || len(fs.openHandles.SnapshotInode(aOut.NodeId)) != 1 {
		t.Fatal("actual Create-only premise missing")
	}
	return fs, server, aOut
}

func issue1023DrainShadowFS(t *testing.T, fs *Dat9FS) {
	t.Helper()
	if fs.commitQueue != nil {
		fs.commitQueue.DrainAll()
	}
	fs.uploader.DrainAll()
}

func issue1023WaitShadowFS(t *testing.T, fs *Dat9FS, path string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if fs.commitQueue != nil {
		if err := fs.commitQueue.WaitIdle(ctx); err != nil {
			t.Fatal("wait for parent queue commitment", err)
		}
	}
	if err := fs.uploader.WaitIdle(ctx); err != nil {
		t.Fatal("wait for parent upload", err)
	}
	// WaitIdle's counters can both be zero after a worker receives a job
	// but before it registers the active upload. Retirement also requires
	// the success callback to remove the parent's staging generations.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap := fs.uploader.Snapshot()
		if snap.Queued == 0 && snap.InFlight == 0 && snap.Cached == 0 &&
			fs.pendingIndex.Count() == 0 && !fs.shadowStore.Has(path) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("parent staging did not retire: uploader=%+v pending=%d shadow=%t: %v", snap, fs.pendingIndex.Count(), fs.shadowStore.Has(path), ctx.Err())
		case <-ticker.C:
		}
	}
}

func issue1023OpenShadowAppender(t *testing.T, fs *Dat9FS, ino uint64) uint64 {
	t.Helper()
	var bOut gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 202}}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &bOut); st != gofuse.OK {
		t.Fatal(st)
	}
	return bOut.Fh
}

func issue1023AssertShadowDrain(t *testing.T, fs *Dat9FS, path string) {
	t.Helper()
	if fs.pendingIndex.Count() != 0 || fs.shadowStore.Has(path) {
		t.Error("drain retained pending index or shadow")
	}
	if snap := fs.uploader.Snapshot(); snap.Queued != 0 || snap.InFlight != 0 || snap.Cached != 0 || snap.CachedBytes != 0 {
		t.Errorf("uploader drain not empty: %+v", snap)
	}
	if fs.commitQueue != nil {
		if snap := fs.commitQueue.Snapshot(); snap.Pending != 0 || snap.Bytes != 0 || snap.InFlight != 0 || snap.Delayed != 0 || snap.Conflicts != 0 || snap.ConflictBytes != 0 {
			t.Errorf("commit queue drain not empty: %+v", snap)
		}
	}
}

func TestIssue1023OversizedShadowAdmissionAndRetry(t *testing.T) {
	for _, variant := range []struct {
		name        string
		mode        SyncMode
		queued      bool
		flushParent bool
	}{
		{"strict-fsync", SyncStrict, false, false},
		{"strict-flush-release", SyncStrict, false, true},
		{"interactive-fsync", SyncInteractive, true, false},
		{"interactive-flush-release", SyncInteractive, true, true},
		{"interactive-no-queue-fsync-release", SyncInteractive, false, false},
		{"interactive-no-queue-flush-release", SyncInteractive, false, true},
	} {
		t.Run(variant.name, func(t *testing.T) {
			const path = "/r8-shadow-create-only.txt"
			fs, server, aOut := issue1023CreatedShadowFS(t, path, variant.mode, variant.queued)
			a, _ := fs.fileHandles.Get(aOut.Fh)
			prefix := []byte(strings.Repeat("p", maxLandedPayloadBytes+1))
			if n, st := pr939Append(fs, aOut.NodeId, aOut.Fh, string(prefix)); int(n) != len(prefix) || st != gofuse.OK {
				t.Fatalf("prefix=%d/%v", n, st)
			}
			bID := issue1023OpenShadowAppender(t, fs, aOut.NodeId)
			b, _ := fs.fileHandles.Get(bID)
			if got := issue1023ReadSized(t, fs, aOut.NodeId, bID, len(prefix)+2); !bytes.Equal(got, prefix) || b.Dirty.Size() != int64(len(prefix)) {
				t.Fatal("actual Open/Read prefix premise missing")
			}
			issue1023RejectOversizedAppend(t, fs, aOut.NodeId, bID, "B\n", a, b)
			if data, err := fs.shadowStore.ReadAll(path); err != nil || !bytes.Equal(data, prefix) {
				t.Error("refused append changed the acknowledged creator shadow", err)
			}
			if _, data, puts := server.snapshot(); len(data) != 0 || len(puts) != 0 {
				t.Error("refused append mutated the remote image")
			}
			if t.Failed() {
				return
			}
			var st gofuse.Status
			if variant.flushParent {
				st = fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: aOut.Fh})
			} else {
				st = fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: aOut.Fh})
			}
			if st != gofuse.OK {
				t.Fatal("parent staging", st)
			}
			if variant.flushParent || !variant.queued && variant.mode == SyncInteractive {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: aOut.Fh})
				if _, live := fs.fileHandles.Get(aOut.Fh); live {
					t.Fatal("parent Release retained its file handle")
				}
			}
			issue1023WaitShadowFS(t, fs, path)
			if _, data, _ := server.snapshot(); !bytes.Equal(data, prefix) {
				t.Fatal("parent retirement did not commit the complete creator image")
			}
			issue1023AssertShadowDrain(t, fs, path)
			if n, st := pr939Append(fs, aOut.NodeId, bID, "B\n"); n != 2 || st != gofuse.OK {
				t.Fatal("retired parent did not permit full retry", n, st)
			}
			want := append(append([]byte(nil), prefix...), []byte("B\n")...)
			if got := issue1023ReadSized(t, fs, aOut.NodeId, bID, len(want)); !bytes.Equal(got, want) {
				t.Fatal("retry lost acknowledged creator bytes")
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: bID}); st != gofuse.OK {
				t.Fatal("retried child fsync", st)
			}
			for _, id := range []uint64{aOut.Fh, bID} {
				if _, live := fs.fileHandles.Get(id); !live {
					continue
				}
				if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: id}); st != gofuse.OK {
					t.Fatal("terminal flush", st)
				}
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: id})
			}
			issue1023DrainShadowFS(t, fs)
			if _, data, _ := server.snapshot(); !bytes.Equal(data, want) {
				t.Error("terminal commit lost acknowledged bytes")
			}
			if data, err := fs.client.ReadCtx(t.Context(), path); err != nil || !bytes.Equal(data, want) {
				t.Error("direct read differs from complete acknowledged image", err)
			}
			readID := issue1023OpenShadowAppender(t, fs, aOut.NodeId)
			if data := issue1023ReadSized(t, fs, aOut.NodeId, readID, len(want)); !bytes.Equal(data, want) {
				t.Error("fresh handler read differs after drain")
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: readID})
			issue1023AssertShadowDrain(t, fs, path)
		})
	}
}

func TestIssue1023OversizedShadowCumulativeAdmission(t *testing.T) {
	const path = "/r8-shadow-cumulative.txt"
	fs, server, aOut := issue1023CreatedShadowFS(t, path, SyncStrict, false)
	a, _ := fs.fileHandles.Get(aOut.Fh)
	prefix := strings.Repeat("p", maxLandedPayloadBytes-2)
	if n, st := pr939Append(fs, aOut.NodeId, aOut.Fh, prefix); int(n) != len(prefix) || st != gofuse.OK {
		t.Fatal(n, st)
	}
	bID := issue1023OpenShadowAppender(t, fs, aOut.NodeId)
	b, _ := fs.fileHandles.Get(bID)
	if n, st := pr939Append(fs, aOut.NodeId, bID, "B\n"); n != 2 || st != gofuse.OK || b.Dirty.Size() != maxLandedPayloadBytes {
		t.Fatal("bounded child did not reach exact proof limit", n, st)
	}
	issue1023RejectOversizedAppend(t, fs, aOut.NodeId, aOut.Fh, "C\n", a, b)
	if t.Failed() {
		return
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: bID}); st != gofuse.OK {
		t.Fatal("bounded child commit", st)
	}
	if n, st := pr939Append(fs, aOut.NodeId, aOut.Fh, "C\n"); n != 2 || st != gofuse.OK {
		t.Fatal("retired bounded child did not permit retry", n, st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: aOut.Fh}); st != gofuse.OK {
		t.Fatal("retried creator commit", st)
	}
	for _, id := range []uint64{aOut.Fh, bID} {
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: aOut.NodeId}, Fh: id})
	}
	issue1023DrainShadowFS(t, fs)
	if _, data, _ := server.snapshot(); string(data) != prefix+"B\nC\n" {
		t.Error("cumulative retry lost acknowledged bytes")
	}
	issue1023AssertShadowDrain(t, fs, path)
}
