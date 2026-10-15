package fuse

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// A refusal must preserve both already acknowledged images and path-owned
// staging, including an ancestor which the target already copied at Open.
func issue1023RejectOversizedAppend(t *testing.T, fs *Dat9FS, ino, id uint64, data string, owners ...*FileHandle) {
	t.Helper()
	states := make([]r6LegacyRebaseState, len(owners))
	gens := make([]StagingGens, len(owners))
	metas := make([]*WriteBackMeta, len(owners))
	cacheBytes := make([][]byte, len(owners))
	for i, owner := range owners {
		owner.Lock()
		states[i] = r6CaptureLegacyRebaseState(owner)
		gens[i] = fs.captureHandleStagingGensLocked(owner)
		owner.Unlock()
		if fs.writeBack != nil {
			if meta, ok := fs.writeBack.GetMeta(owner.Path); ok {
				metas[i] = meta
				var err error
				cacheBytes[i], err = os.ReadFile(fs.writeBack.datFile(owner.Path))
				if err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if n, st := pr939Append(fs, ino, id, data); n != 0 || st != gofuse.EAGAIN {
		t.Errorf("unsupported descendant append=%d/%v, want zero/EAGAIN", n, st)
	}
	for i, owner := range owners {
		owner.Lock()
		after, afterGens := r6CaptureLegacyRebaseState(owner), fs.captureHandleStagingGensLocked(owner)
		owner.Unlock()
		if !reflect.DeepEqual(states[i], after) || gens[i] != afterGens {
			t.Errorf("refused append changed owner %s image/identity/generations", owner.Path)
		}
		if metas[i] != nil {
			meta, ok := fs.writeBack.GetMeta(owner.Path)
			data, err := os.ReadFile(fs.writeBack.datFile(owner.Path))
			if !ok || !reflect.DeepEqual(meta, metas[i]) || err != nil || !bytes.Equal(data, cacheBytes[i]) {
				t.Errorf("refused append changed acknowledged cache %s", owner.Path)
			}
		}
	}
}

func TestIssue1023OversizedAliasAdmissionAndRetry(t *testing.T) {
	for _, variant := range []string{"retained_fsync", "retained_release", "open_preloaded", "cumulative", "landed_but_unretired", "zero_truncate"} {
		t.Run(variant, func(t *testing.T) {
			fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
			fs.commitQueue.DrainAll()
			fs.commitQueue, fs.shadowStore, fs.pendingIndex = nil, nil, nil
			fs.client.SetSmallFileThresholdForTests(32 << 20)
			cache, err := NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			uploader := NewWriteBackUploader(fs.client, cache, 2)
			fs.SetWriteBack(cache, uploader)
			uploader.OnSuccess, uploader.SnapshotStagingGens = fs.onWriteBackUploadSuccess, fs.snapshotStagingGens
			t.Cleanup(uploader.DrainAll)
			a, _ := fs.fileHandles.Get(ids[0])
			b, _ := fs.fileHandles.Get(ids[1])
			paths := []string{a.Path, b.Path}
			prefix := "base" + strings.Repeat("A", maxLandedPayloadBytes-4)
			record := "B"
			if variant == "cumulative" || variant == "landed_but_unretired" {
				prefix = prefix[:len(prefix)-1]
			}
			if variant == "zero_truncate" {
				var out gofuse.AttrOut
				in := gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE | gofuse.FATTR_FH, Fh: ids[0], Size: 0}}
				if st := fs.SetAttr(nil, &in, &out); st != gofuse.OK {
					t.Fatal(st)
				}
				if a.Dirty.Size() != 0 || a.DirtySeq == 0 {
					t.Fatal("actual zero-length dirty truncate premise missing")
				}
				prefix, record = "", strings.Repeat("B", maxLandedPayloadBytes+1)
			} else {
				if n, st := pr939Append(fs, ino, ids[0], prefix[4:]); int(n) != len(prefix)-4 || st != gofuse.OK {
					t.Fatal(n, st)
				}
			}
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
				t.Fatal(st)
			}
			if variant == "open_preloaded" {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
				var out gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &out); st != gofuse.OK {
					t.Fatal(st)
				}
				b, _ = fs.fileHandles.Get(out.Fh)
				ids[1] = out.Fh
				if b.Path != paths[1] || b.Dirty.Size() != int64(len(prefix)) || b.ContentSnapshotID != a.ContentSnapshotID {
					t.Fatal("actual Open did not copy the ancestor")
				}
			}
			if variant == "cumulative" || variant == "landed_but_unretired" {
				issue1023ShadowAppend(t, fs, ino, ids[1], "B")
				if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
					t.Fatal(st)
				}
				record = "C"
			}
			if variant == "landed_but_unretired" {
				// Real B Fsync commits the bounded descendant while the source
				// owner becomes busy after admission, before success callbacks.
				server.firstPutStarted, server.releaseFirstPut = make(chan struct{}), make(chan struct{})
				releasePut := sync.OnceFunc(func() { close(server.releaseFirstPut) })
				t.Cleanup(releasePut)
				done := make(chan gofuse.Status, 1)
				go func() { done <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}) }()
				select {
				case <-server.firstPutStarted:
				case <-time.After(3 * time.Second):
					t.Fatal("descendant Fsync did not reach the actual PUT")
				}
				var st gofuse.Status
				func() {
					a.Lock()
					defer a.Unlock()
					releasePut()
					select {
					case st = <-done:
					case <-time.After(3 * time.Second):
						t.Fatal("descendant Fsync did not complete while A was busy")
					}
				}()
				if st != gofuse.OK || a.DirtySeq == 0 || a.WriteBackSeq == 0 {
					t.Fatal("actual landed but unretired ancestor premise missing", st)
				}
				if proof := fs.landedAppendCommit(b.Path); !slices.Contains(proof.ancestors, a.StagedSnapshotID) {
					t.Fatal("landed descendant does not prove A inclusion")
				}
			}
			rev, remote, puts := server.snapshot()
			issue1023RejectOversizedAppend(t, fs, ino, ids[1], record, a, b)
			if r, data, p := server.snapshot(); r != rev || !bytes.Equal(data, remote) || len(p) != len(puts) {
				t.Error("refused append mutated remote data")
			}
			if t.Failed() {
				return
			}
			if variant == "retained_release" {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
			} else {
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
					t.Fatal("retire parent Fsync", st)
				}
			}
			if variant == "retained_release" {
				if _, live := fs.fileHandles.Get(ids[0]); live {
					t.Fatal("Release kept the parent owner live")
				}
				if _, pending := cache.GetMeta(a.Path); pending {
					t.Fatal("Release did not commit and retire the parent cache")
				}
			} else if a.DirtySeq != 0 || a.WriteBackSeq != 0 {
				t.Fatal("parent owner was not retired")
			}
			if n, st := pr939Append(fs, ino, ids[1], record); int(n) != len(record) || st != gofuse.OK {
				t.Fatal("retry append", n, st)
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
				t.Fatal("retried descendant Fsync", st)
			}
			for _, id := range ids {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			}
			uploader.DrainAll()
			want := prefix + record
			if variant == "cumulative" || variant == "landed_but_unretired" {
				want = prefix + "B" + record
			}
			if _, body, _ := server.snapshot(); string(body) != want {
				t.Errorf("remote bytes=%d, want complete acknowledged bytes=%d", len(body), len(want))
			}
			for _, path := range paths {
				data, err := fs.client.ReadCtx(t.Context(), path)
				if err != nil || string(data) != want {
					t.Errorf("direct alias %s bytes=%d want=%d error=%v", path, len(data), len(want), err)
				}
				if _, ok := cache.GetMeta(path); ok {
					t.Errorf("drain left pending %s", path)
				}
			}
			if snap := uploader.Snapshot(); snap.Queued != 0 || snap.InFlight != 0 || snap.Cached != 0 || snap.CachedBytes != 0 {
				t.Errorf("drain not empty: %+v", snap)
			}
		})
	}
}

func TestIssue1023R8NoQueuePreflightErrno(t *testing.T) {
	for _, operation := range []string{"fsync", "flush-handle"} {
		for _, identity := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/identity=%t", operation, identity), func(t *testing.T) {
				fs, ino, server, ids := r6LinkedHandlerFixture(t, identity, nil, nil)
				fs.commitQueue.DrainAll()
				fs.commitQueue = nil
				fs.syncMode = SyncStrict
				issue1023ShadowAppend(t, fs, ino, ids[0], "A")
				issue1023ShadowAppend(t, fs, ino, ids[1], "B")
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
					t.Fatal(st)
				}
				peer, _ := fs.fileHandles.Get(ids[1])
				peer.Lock()
				if !identity {
					peer.LineageTrusted = false
				}
				buffer, seq, version, base := peer.Dirty, peer.DirtySeq, peer.Dirty.contentVersion, peer.BaseRev
				before := string(peer.Dirty.Bytes())
				contentID := peer.ContentSnapshotID
				ancestors := append([]string(nil), peer.contentAncestors...)
				gens := fs.captureHandleStagingGensLocked(peer)
				identityErr := fs.refreshUnidentifiedAppendAliasesLocked(t.Context(), peer)
				lineageErr := fs.adoptLandedAppendSnapshotLocked(t.Context(), peer)
				t.Logf("raw preflight identity=%v lineage=%v mapper=%v base=%d floor=%d size=%d", identityErr, lineageErr, httpToFuseStatus(lineageErr), peer.BaseRev, fs.latestCommittedRevision(peer.Path), peer.Dirty.Size())
				peer.Unlock()
				if !errors.Is(lineageErr, syscall.EAGAIN) || identity && !errors.Is(identityErr, syscall.EAGAIN) {
					t.Fatal("actual raw EAGAIN preflight premise missing")
				}
				rev, body, puts := server.snapshot()
				var st gofuse.Status
				if operation == "fsync" {
					st = fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
				} else {
					peer.Lock()
					st = fs.flushHandle(t.Context(), peer)
					peer.Unlock()
				}
				if st != gofuse.EAGAIN {
					t.Errorf("preflight=%v wantEAGAIN", st)
				}
				peer.Lock()
				if peer.Dirty != buffer || peer.DirtySeq != seq || peer.Dirty.contentVersion != version || peer.BaseRev != base || string(peer.Dirty.Bytes()) != before || peer.ContentSnapshotID != contentID || !slices.Equal(peer.contentAncestors, ancestors) || fs.captureHandleStagingGensLocked(peer) != gens {
					t.Error("preflight changed acknowledged image or ownership")
				}
				peer.Unlock()
				if r, b, p := server.snapshot(); r != rev || !bytes.Equal(b, body) || len(p) != len(puts) {
					t.Error("failed preflight attempted remote mutation")
				}
			})
		}
	}
}
