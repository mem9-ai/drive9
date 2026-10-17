package fuse

import (
	"bytes"
	"fmt"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func addTruncateHandoffAlias(t *testing.T, fs *Dat9FS, ino uint64, remote *truncateHandoffRemote) {
	t.Helper()
	remote.mu.Lock()
	remote.nlink = 2
	remote.mu.Unlock()
	data, _, _ := remote.snapshot()
	if !fs.inodes.AddAlias(ino, "/alias.bin", "handoff-resource", 2, false, int64(len(data)), time.Now()) {
		t.Fatal("add existing hardlink")
	}
}

func TestFtruncateMaterializedRootAboveInlineThreshold(t *testing.T) {
	for _, queued := range []bool{true, false} {
		for _, materialized := range []bool{false, true} {
			t.Run(fmt.Sprintf("queue=%t/materialized=%t", queued, materialized), func(t *testing.T) {
				initial := bytes.Repeat([]byte("p"), 100_000)
				fs, ino, remote := truncateHandoffFS(t, initial)
				remote.mu.Lock()
				remote.inlineThreshold = 50_000
				remote.mu.Unlock()
				fs.client.SetSmallFileThresholdForTests(50_000)
				if !queued {
					queue := fs.commitQueue
					fs.commitQueue = nil
					t.Cleanup(func() { fs.commitQueue = queue })
				}
				a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
				addTruncateHandoffAlias(t, fs, ino, remote)
				b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
				truncateHandoffTruncate(t, fs, ino, a, len(initial))
				h, _ := fs.fileHandles.Get(a)
				if h.Dirty.CanMaterializeFull() || h.Dirty.PartSize() != DefaultPartSize || h.StorageClass != storageClassS3 {
					t.Fatal("expected actual lazy S3 buffer with default parts")
				}
				if materialized {
					got, st := truncateHandoffRead(fs, ino, a, 0, len(initial))
					if st != gofuse.OK || !bytes.Equal(got, initial) || !h.Dirty.CanMaterializeFull() {
						t.Fatal("actual source Read did not materialize the complete image")
					}
				}
				hardlinkSync(t, fs, ino, a)
				data, rev, _ := remote.snapshot()
				if !bytes.Equal(data, initial) || stateOf(fs, ino).committedRevision != rev {
					t.Fatal("committed ledger differs from actual remote image")
				}
				if proof := fs.ftruncateAliasProof(h); proof.rev != rev || proof.snapshotID == "" || proof.checksum != payloadChecksum(data) {
					t.Errorf("bounded S3 image lost its proof: %+v", proof)
				}
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
				if got, st := truncateHandoffRead(fs, ino, b, 0, len(initial)+1); st != gofuse.OK || !bytes.Equal(got, initial) {
					t.Errorf("bounded S3 alias=%d bytes/%v", len(got), st)
				}
				hardlinkWrite(t, fs, ino, b, "H")
				hardlinkSync(t, fs, ino, b)
				want := bytes.Clone(initial)
				want[0] = 'H'
				if data, _, _ := remote.snapshot(); !bytes.Equal(data, want) {
					t.Fatal("later alias commit lost accepted bytes")
				}
			})
		}
	}
}

func TestFtruncateMaterializedRootCommitProof(t *testing.T) {
	for _, queued := range []bool{true, false} {
		t.Run(fmt.Sprintf("queue=%t", queued), func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			if !queued {
				queue := fs.commitQueue
				fs.commitQueue = nil
				t.Cleanup(func() { fs.commitQueue = queue })
			}
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			ro := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
			addTruncateHandoffAlias(t, fs, ino, remote)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, 11)
			parent, _ := fs.fileHandles.Get(a)
			if parent.Dirty.CanMaterializeFull() || parent.Dirty.PartSize() != DefaultPartSize {
				t.Fatal("expected the real Open buffer to remain lazy with default parts")
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
				t.Fatalf("sync=%v", st)
			}
			data, rev, _ := remote.snapshot()
			if string(data) != "hello world" || rev != 2 {
				t.Fatalf("committed=%q/rev%d", data, rev)
			}
			proof := fs.ftruncateAliasProof(parent)
			if proof.rev != rev || proof.snapshotID == "" || proof.checksum != payloadChecksum(data) {
				t.Errorf("materialized upload lost its proof: %+v", proof)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			for _, id := range []uint64{b, ro} {
				got, st := truncateHandoffRead(fs, ino, id, 0, 64)
				if st != gofuse.OK || !bytes.Equal(got, data) {
					t.Errorf("reader%d=%q/%v, want committed image", id, got, st)
				}
			}
			if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); n != 1 || st != gofuse.OK {
				t.Fatalf("next alias write=%d/%v", n, st)
			}
			hardlinkSync(t, fs, ino, b)
			data, _, _ = remote.snapshot()
			if string(data) != "Hello world" {
				t.Fatalf("next committed image=%q", data)
			}
		})
	}
}

func TestFtruncatePathCommitReconcilesObservers(t *testing.T) {
	for _, aliases := range []bool{false, true} {
		for _, syncSibling := range []bool{false, true} {
			for _, size := range []int{3, 8} {
				t.Run(fmt.Sprintf("aliases=%t/sync=%t/size=%d", aliases, syncSibling, size), func(t *testing.T) {
					fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
					a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
					ro := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
					if aliases {
						addTruncateHandoffAlias(t, fs, ino, remote)
					}
					flags := syscall.O_RDWR
					if syncSibling {
						flags |= syscall.O_SYNC
					}
					b := truncateHandoffOpen(t, fs, ino, flags)
					truncateHandoffTruncate(t, fs, ino, a, 5)
					hardlinkSync(t, fs, ino, a)
					if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
						InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE, Size: uint64(size),
					}}, &gofuse.AttrOut{}); st != gofuse.OK {
						t.Fatal(st)
					}
					want := make([]byte, size)
					copy(want, "hello")
					data, rev, _ := remote.snapshot()
					if !bytes.Equal(data, want) || rev != 3 {
						t.Fatalf("pathname image=%q/rev%d want%q/rev3", data, rev, want)
					}
					ids := []uint64{a, b, ro}
					for _, id := range ids {
						h, _ := fs.fileHandles.Get(id)
						if h.pendingFtruncate.Load() != nil || h.DirtySeq != 0 || (h.Dirty != nil && h.Dirty.HasDirtyParts()) {
							t.Errorf("acknowledged handle%d retained an unresolved mutation", id)
						}
						got, st := truncateHandoffRead(fs, ino, id, 0, 64)
						if st != gofuse.OK || !bytes.Equal(got, want) {
							t.Errorf("existing reader%d=%q/%v want%q", id, got, st, want)
						}
					}
					fresh := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
					got, st := truncateHandoffRead(fs, ino, fresh, 0, 64)
					if st != gofuse.OK || !bytes.Equal(got, want) {
						t.Fatalf("fresh reader=%q/%v", got, st)
					}
					if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); n != 1 || st != gofuse.OK {
						t.Fatalf("post-pathname write=%d/%v", n, st)
					}
					hardlinkSync(t, fs, ino, b)
					want[0] = 'H'
					data, _, _ = remote.snapshot()
					if !bytes.Equal(data, want) {
						t.Fatalf("post-pathname committed=%q want%q", data, want)
					}
				})
			}
		}
	}
}

func TestFtruncateMaterializedRootRejectsFailedUpload(t *testing.T) {
	for _, queued := range []bool{true, false} {
		t.Run(fmt.Sprintf("queue=%t", queued), func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			if !queued {
				queue := fs.commitQueue
				fs.commitQueue = nil
				t.Cleanup(func() { fs.commitQueue = queue })
			}
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, 11)
			if _, err := newTestClient(fs.client.BaseURL()).WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte("other world"), 1); err != nil {
				t.Fatal(err)
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st == gofuse.OK {
				t.Fatal("conflicting upload succeeded")
			}
			parent, _ := fs.fileHandles.Get(a)
			if proof := fs.ftruncateAliasProof(parent); proof.rev != 0 || proof.snapshotID != "" || proof.checksum != "" {
				t.Fatalf("failed upload published proof: %+v", proof)
			}
			if state := stateOf(fs, ino); state.committedSeq != 0 || parent.DirtySeq == 0 || !parent.Dirty.HasDirtyParts() {
				t.Fatal("failed upload retired the uncommitted mutation")
			}
			data, rev, _ := remote.snapshot()
			if string(data) != "other world" || rev != 2 {
				t.Fatalf("conflict overwrote remote=%q/rev%d", data, rev)
			}
		})
	}
}

func TestFtruncatePathCommitDropsOldReadPin(t *testing.T) {
	fs, ino, _ := truncateHandoffFS(t, []byte("hello world"))
	if err := fs.shadowStore.WriteFull("/file.bin", []byte("hello world"), 1); err != nil {
		t.Fatal(err)
	}
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	ro := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
	reader, _ := fs.fileHandles.Get(ro)
	oldPin := reader.ShadowGen
	if !reader.ShadowPinned || oldPin == 0 {
		t.Fatal("expected a real Open pin of the old committed shadow")
	}
	// Unpin only deletes retired generations; an active cache may remain.
	// Retire this generation first so its last-reader cleanup is observable.
	fs.shadowStore.Remove("/file.bin")
	truncateHandoffTruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE, Size: 3,
	}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatal(st)
	}
	if reader.ShadowPinned || reader.ShadowGen != 0 || fs.shadowStore.SizeGen(oldPin) >= 0 {
		t.Fatalf("pathname acknowledgement retained old pin: pinned=%t gen=%d old-size=%d", reader.ShadowPinned, reader.ShadowGen, fs.shadowStore.SizeGen(oldPin))
	}
	if got, st := truncateHandoffRead(fs, ino, ro, 0, 64); st != gofuse.OK || string(got) != "hel" {
		t.Fatalf("read after pin retirement=%q/%v", got, st)
	}
}
