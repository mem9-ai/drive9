package fuse

import (
	"bytes"
	"fmt"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateLargeS3SameSizeCommit(t *testing.T) {
	for _, size := range []int{2 << 20, int(DefaultPartSize), int(DefaultPartSize) + 17, 2 * int(DefaultPartSize)} {
		for _, queued := range []bool{true, false} {
			for _, materialized := range []bool{false, true} {
				t.Run(fmt.Sprintf("size=%d/queue=%t/materialized=%t", size, queued, materialized), func(t *testing.T) {
					initial := make([]byte, size)
					for i := range initial {
						initial[i] = byte(i*37 + i/251)
					}
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
					observer := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
					truncateHandoffTruncate(t, fs, ino, a, size)
					source, _ := fs.fileHandles.Get(a)
					if source.Dirty.PartSize() != DefaultPartSize || source.StorageClass != storageClassS3 || len(source.Dirty.DirtyPartNumbers()) != 0 {
						t.Fatal("PREMISE expected a natural same-size S3 truncate with no dirty parts")
					}
					if materialized {
						got, st := truncateHandoffRead(fs, ino, a, 0, size)
						if st != gofuse.OK || !bytes.Equal(got, initial) || !source.Dirty.CanMaterializeFull() {
							t.Fatal("PREMISE actual source Read failed to materialize exact bytes")
						}
					}
					hardlinkSync(t, fs, ino, a)
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
					data, rev, puts := remote.snapshot()
					if rev != 2 || puts != 1 || !bytes.Equal(data, initial) {
						t.Fatalf("actual same-size S3 commit size=%d rev=%d puts=%d", len(data), rev, puts)
					}
					lastPart := (size-1)/int(DefaultPartSize) + 1
					remote.mu.Lock()
					parts := append([]int(nil), remote.patchParts...)
					remote.mu.Unlock()
					if len(parts) != 1 || parts[0] != lastPart {
						t.Fatalf("PATCH parts=%v want last retained part%d", parts, lastPart)
					}
					state := stateOf(fs, ino)
					stat, err := fs.client.StatCtx(t.Context(), fs.remotePath(source.Path))
					if err != nil || stat == nil || stat.Revision != rev || stat.Size != int64(size) || stat.ResourceID != "handoff-resource" || stat.StorageType != storageClassS3 || state.committedRevision != rev || state.committedSize != int64(size) {
						t.Fatalf("actual HEAD=%+v/%v ledger=%+v", stat, err, state)
					}
					for _, id := range []uint64{b, observer} {
						for _, offset := range []int{0, size - 8, size, size + 1} {
							got, st := truncateHandoffRead(fs, ino, id, int64(offset), 16)
							var want []byte
							if offset < size {
								want = initial[offset:min(offset+16, size)]
							}
							if st != gofuse.OK || !bytes.Equal(got, want) {
								t.Errorf("reader%d at%d=%x/%v want=%x", id, offset, got, st, want)
							}
						}
					}
				})
			}
		}
	}
}

func TestFtruncateLargeS3FailedPatchKeepsMutation(t *testing.T) {
	for _, materialized := range []bool{false, true} {
		for _, failure := range []string{"read-url", "short-read", "plan", "finalize"} {
			t.Run(fmt.Sprintf("materialized=%t/failure=%s", materialized, failure), func(t *testing.T) {
				initial := bytes.Repeat([]byte("retained"), (2<<20)/8)
				fs, ino, remote := truncateHandoffFS(t, initial)
				remote.mu.Lock()
				remote.inlineThreshold = 50_000
				remote.omitPatchRead = failure == "read-url"
				remote.shortPatchRead = failure == "short-read"
				remote.rejectPatchDone = failure == "finalize"
				remote.mu.Unlock()
				fs.client.SetSmallFileThresholdForTests(50_000)
				a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
				truncateHandoffTruncate(t, fs, ino, a, len(initial))
				source, _ := fs.fileHandles.Get(a)
				seq := source.DirtySeq
				if materialized {
					if got, st := truncateHandoffRead(fs, ino, a, 0, len(initial)); st != gofuse.OK || !bytes.Equal(got, initial) {
						t.Fatal("PREMISE actual source Read did not materialize retained content")
					}
				}
				wantRemote, wantRevision, wantPuts := initial, int64(1), 0
				if failure == "plan" {
					wantRemote = bytes.Clone(initial)
					wantRemote[0] = 'Q'
					if _, err := newTestClient(fs.client.BaseURL()).WriteCtxConditionalWithRevision(t.Context(), "/file.bin", wantRemote, 1); err != nil {
						t.Fatal(err)
					}
					wantRevision, wantPuts = 2, 1
				}
				st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
				data, rev, puts := remote.snapshot()
				if materialized && (failure == "read-url" || failure == "short-read") {
					if st != gofuse.OK || rev != 2 || puts != 1 || !bytes.Equal(data, initial) {
						t.Fatalf("captured loaded part did not commit: %v/rev%d/puts%d", st, rev, puts)
					}
					return
				}
				if st == gofuse.OK || rev != wantRevision || puts != wantPuts || !bytes.Equal(data, wantRemote) {
					t.Fatalf("failed PATCH acknowledged content: %v/rev%d/puts%d", st, rev, puts)
				}
				if source.DirtySeq != seq || !source.Dirty.HasDirtyParts() || stateOf(fs, ino).committedSeq >= seq || fs.latestCommittedRevision(source.Path) > rev {
					t.Fatal("failed PATCH discarded or falsely acknowledged the pending truncate")
				}
			})
		}
	}
}
