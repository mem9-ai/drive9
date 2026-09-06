package fuse

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestSQLiteSidecarTruncateFollowsLinkedFile(t *testing.T) {
	for _, sidecar := range []struct {
		name      string
		suffix    string
		appendLog bool
	}{
		{"wal", "-wal", false},
		{"wal-append-log", "-wal", true},
		{"journal", "-journal", false},
	} {
		for _, truncateMode := range []string{"other-handle", "other-handle-shorter", "other-handle-writes-shorter", "path", "same-handle", "nonzero-fd"} {
			for _, readerKind := range []string{"read-only", "clean-writable", "shadow-spill", "read-only-no-shadow", "clean-writable-no-shadow"} {
				readOnly := strings.HasPrefix(readerKind, "read-only")
				nonzeroShrink := truncateMode == "nonzero-fd"
				if truncateMode == "same-handle" && readOnly || nonzeroShrink && sidecar.suffix != "-wal" {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", sidecar.name, truncateMode, readerKind), func(t *testing.T) {
					filePath := "/workload.db" + sidecar.suffix
					a := makeSQLiteTruncateWALForTest(t, 1, 2, 'A')
					b := makeSQLiteTruncateWALForTest(t, 3, 4, 'B')
					if strings.HasSuffix(truncateMode, "shorter") {
						a = append(a, cloneBytes(a[sqliteWALHeaderSize:])...)
					}
					if nonzeroShrink {
						// journal_size_limit=0 preserves the new generation's
						// committed frame, removing only the old generation's tail.
						a = append(cloneBytes(b), a[sqliteWALHeaderSize:]...)
						if len(b) != 4152 {
							t.Fatalf("WAL prefix size = %d, want 32 + 24 + 4096", len(b))
						}
					}
					remote := &casFileServer{t: t, path: filePath, revision: 7, body: cloneBytes(a)}
					var appends int
					ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/v1/status" {
							_ = json.NewEncoder(w).Encode(map[string]any{"storage_capabilities": map[string]bool{"append_log_v1": true}})
							return
						}
						if sidecar.appendLog {
							w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
						}
						// Ordinary path truncate uses an unconditional PUT.
						if r.Method == http.MethodPut && r.Header.Get("X-Dat9-Expected-Revision") == "" {
							remote.mu.Lock()
							r.Header.Set("X-Dat9-Expected-Revision", fmt.Sprint(remote.revision))
							remote.mu.Unlock()
						}
						if r.Method == http.MethodPost && r.URL.Query().Has("append-log") {
							body, err := io.ReadAll(r.Body)
							if err != nil {
								t.Error(err)
								w.WriteHeader(http.StatusInternalServerError)
								return
							}
							remote.mu.Lock()
							defer remote.mu.Unlock()
							if r.Header.Get("X-Dat9-Expected-Revision") != fmt.Sprint(remote.revision) || r.Header.Get("X-Dat9-Expected-Size") != fmt.Sprint(len(remote.body)) {
								http.Error(w, "append baseline mismatch", http.StatusConflict)
								return
							}
							appends++
							remote.body = append(remote.body, body...)
							remote.revision++
							_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: remote.revision, Size: int64(len(remote.body))})
							return
						}
						remote.serveHTTP(w, r)
					}))
					t.Cleanup(ts.Close)
					c := newTestClient(ts.URL)
					c.Warm(context.Background())
					opts := &MountOptions{CacheDir: t.TempDir(), SyncMode: SyncInteractive, GVisorCompat: true}
					if sidecar.appendLog {
						opts.AppendLogPatterns = []string{"**/workload.db-wal"}
					}
					opts.setDefaults()
					fs := NewDat9FS(c, opts)
					shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(shadow.Close)
					if !strings.HasSuffix(readerKind, "no-shadow") {
						fs.shadowStore = shadow
					}
					fs.pendingIndex, err = NewPendingIndex(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					if err := shadow.WriteFull(filePath, a, 7); err != nil {
						t.Fatal(err)
					}
					ino := fs.inodes.Lookup(filePath, false, int64(len(a)), time.Now())
					fs.inodes.UpdateRevision(ino, 7)
					fs.recordCommittedRevisionWithSize(filePath, 7, int64(len(a)))
					fs.readCache.Put(filePath, a, 7)
					open := func(flags uint32) (uint64, *FileHandle) {
						t.Helper()
						var out gofuse.OpenOut
						if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: flags}, &out); st != gofuse.OK {
							t.Fatalf("Open: %v", st)
						}
						fh, _ := fs.fileHandles.Get(out.Fh)
						t.Cleanup(func() { fs.deleteFileHandle(out.Fh, fh) })
						return out.Fh, fh
					}
					flags := uint32(syscall.O_RDONLY)
					if !readOnly {
						flags = syscall.O_RDWR
					}
					readerID, reader := open(flags)
					if readerKind == "shadow-spill" {
						reader.ShadowReady = true
						reader.ShadowSpill = true
					}
					if !readOnly && (reader.Dirty == nil || reader.DirtySeq != 0 || reader.Dirty.HasDirtyParts()) {
						t.Fatal("original WAL I/O handle must have a clean writable buffer")
					}
					read := func(phase string, want []byte) {
						t.Helper()
						size := len(want)
						if size == 0 {
							size = len(a)
						}
						got, st, err := readDat9FSTestRange(fs, ino, readerID, 0, size)
						if err != nil || st != gofuse.OK || !bytes.Equal(got, want) {
							t.Errorf("%s: read status=%v err=%v bytes=%d, want %d matching bytes (old A=%t)", phase, st, err, len(got), len(want), bytes.Equal(got, a))
						}
					}
					read("before truncate", a)
					truncateID, _ := open(syscall.O_RDWR)
					if truncateMode == "same-handle" || truncateMode == "nonzero-fd" && !readOnly {
						truncateID = readerID
					}
					input := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
						InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE, Size: 0,
					}}
					if nonzeroShrink {
						input.Size = uint64(len(b))
					}
					if truncateMode != "path" {
						input.Valid |= gofuse.FATTR_FH
						input.Fh = truncateID
					}
					var out gofuse.AttrOut
					if st := fs.SetAttr(nil, input, &out); st != gofuse.OK {
						t.Fatalf("truncate: %v", st)
					}
					if out.Size != input.Size {
						t.Errorf("truncate size = %d, want %d", out.Size, input.Size)
					}
					for _, fh := range fs.fileHandlesForInode(ino) {
						fh.Lock()
						snapshot := fh.UnlinkedData != nil || fh.UnlinkedSnapshot || fh.Unlinked
						fh.Unlock()
						if snapshot {
							t.Error("truncate created an unlink snapshot on a linked handle")
						}
					}
					if nonzeroShrink {
						read("valid frame after nonzero shrink", b)
						if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: truncateID}); st != gofuse.OK {
							t.Fatalf("nonzero shrink Fsync: %v", st)
						}
						read("valid frame after shrink fsync", b)
						got, st, err := readDat9FSTestRange(fs, ino, readerID, int64(len(b)), 1)
						if err != nil || st != gofuse.OK || len(got) != 0 {
							t.Errorf("old tail after shrink fsync: status=%v err=%v bytes=%d, want EOF", st, err, len(got))
						}
						_, body, puts := remote.snapshot()
						if !bytes.Equal(body, b) {
							t.Errorf("remote WAL after nonzero shrink differs from valid prefix: size=%d", len(body))
						}
						if len(puts) == 0 {
							t.Error("nonzero shrink was not persisted")
						}
						for _, put := range puts {
							if !bytes.Equal(put.body, b) {
								t.Errorf("nonzero shrink PUT discarded or changed valid frames: size=%d", len(put.body))
							}
						}
						return
					}
					read("after truncate", nil)
					// The original writable WAL handle also writes the new generation,
					// as gVisor retains its I/O FD across the separate truncate FD.
					writerID := readerID
					if readOnly || truncateMode == "other-handle-writes-shorter" {
						writerID = truncateID
					}
					for _, chunk := range []struct{ offset, end int }{{0, 32}, {32, len(b)}} {
						data := b[chunk.offset:chunk.end]
						if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writerID, Offset: uint64(chunk.offset)}, data); st != gofuse.OK || int(n) != len(data) {
							t.Fatalf("Write: %d/%v", n, st)
						}
						if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writerID}); st != gofuse.OK {
							t.Fatalf("Fsync: %v", st)
						}
					}
					read("after generation B fsync", b)
					got, st, err := readDat9FSTestRange(fs, ino, readerID, int64(len(b)), 1)
					if err != nil || st != gofuse.OK || len(got) != 0 {
						t.Errorf("after generation B fsync: status=%v err=%v bytes=%d beyond EOF", st, err, len(got))
					}
					_, body, puts := remote.snapshot()
					if !bytes.Equal(body, b) {
						t.Errorf("remote bytes differ from B: size=%d", len(body))
					}
					if sidecar.appendLog {
						remote.mu.Lock()
						defer remote.mu.Unlock()
						if appends == 0 {
							t.Error("optimized WAL never used append-log")
						}
						for _, put := range puts {
							if len(put.body) > sqliteWALHeaderSize {
								t.Errorf("full WAL rewrite: PUT %d bytes", len(put.body))
							}
						}
					}
				})
			}
		}
	}
}

func TestSQLiteZeroTruncateBusySibling(t *testing.T) {
	for _, gvisor := range []bool{false, true} {
		for _, phase := range []string{"write", "newer-write", "newer-commit"} {
			t.Run(fmt.Sprintf("gvisor=%t/%s", gvisor, phase), func(t *testing.T) {
				const filePath = "/busy.db-wal"
				oldData := []byte("old WAL bytes that must not survive")
				newData := []byte("new")
				remote, ts := newCASFileServer(t, filePath, 7, oldData)
				t.Cleanup(ts.Close)
				opts := &MountOptions{GVisorCompat: gvisor}
				opts.setDefaults()
				fs := NewDat9FS(newTestClient(ts.URL), opts)
				ino := fs.inodes.Lookup(filePath, false, int64(len(oldData)), time.Now())
				fs.inodes.UpdateRevision(ino, 7)
				fs.recordCommittedRevisionWithSize(filePath, 7, int64(len(oldData)))
				open := func() (uint64, *FileHandle) {
					fh := &FileHandle{Ino: ino, Path: filePath, Flags: syscall.O_RDWR, BaseRev: 7, OrigSize: int64(len(oldData)),
						Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0)}
					if _, err := fh.Dirty.Write(0, oldData); err != nil {
						t.Fatal(err)
					}
					fh.Dirty.ClearDirty()
					id := fs.allocateFileHandle(fh)
					t.Cleanup(func() { fs.deleteFileHandle(id, fh) })
					return id, fh
				}
				originalID, original := open()
				truncateID, _ := open()
				original.Lock()
				unlockOriginal := sync.OnceFunc(original.Unlock)
				t.Cleanup(unlockOriginal)
				done := make(chan gofuse.Status, 1)
				go func() {
					var out gofuse.AttrOut
					done <- fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
						InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE | gofuse.FATTR_FH, Fh: truncateID,
					}}, &out)
				}()
				select {
				case st := <-done:
					if st != gofuse.OK {
						t.Fatalf("truncate: %v", st)
					}
				case <-time.After(time.Second):
					t.Fatal("truncate blocked on a busy sibling")
				}
				write := func(id uint64, offset uint64, data []byte, durable bool) {
					t.Helper()
					if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Offset: offset}, data); st != gofuse.OK || int(n) != len(data) {
						t.Fatalf("Write = %d/%v", n, st)
					}
					if durable {
						if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
							t.Fatalf("Fsync = %v", st)
						}
					}
				}
				if phase != "write" {
					write(truncateID, 0, newData, phase == "newer-commit")
				}
				unlockOriginal()
				if phase != "write" {
					got, st, err := readDat9FSTestRange(fs, ino, originalID, 0, len(newData))
					if err != nil || st != gofuse.OK || !bytes.Equal(got, newData) {
						t.Errorf("late truncate hid newer bytes: %q/%v/%v", got, st, err)
					}
					if phase == "newer-write" {
						if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: truncateID}); st != gofuse.OK {
							t.Fatalf("newer write Fsync = %v", st)
						}
					}
					write(originalID, uint64(len(newData)), []byte("!"), true)
					newData = append(newData, '!')
				} else {
					write(originalID, 0, newData, true)
				}
				_, body, _ := remote.snapshot()
				if !bytes.Equal(body, newData) {
					t.Fatalf("remote bytes = %q, want %q", body, newData)
				}
			})
		}
	}
}

func TestSQLiteReplacementOldHandleTruncateDoesNotPublishEOF(t *testing.T) {
	for _, suffix := range []string{"-wal", "-journal"} {
		t.Run(suffix, func(t *testing.T) {
			const oldPath = "/replacement.tmp"
			newPath := "/workload.db" + suffix
			oldData := []byte("old detached inode")
			newData := []byte("new linked inode contents")
			var renamed atomic.Bool
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+newPath:
					if renamed.Load() {
						_, _ = w.Write(newData)
					} else {
						_, _ = w.Write(oldData)
					}
				case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newPath && r.URL.Query().Has("rename"):
					renamed.Store(true)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			t.Cleanup(ts.Close)
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			oldIno := fs.inodes.Lookup(newPath, false, int64(len(oldData)), time.Now())
			newIno := fs.inodes.Lookup(oldPath, false, int64(len(newData)), time.Now())
			old := &FileHandle{Ino: oldIno, Path: newPath, BaseRev: 7, OrigSize: int64(len(oldData)), Flags: syscall.O_RDWR,
				Dirty: fs.newWriteBuffer(newPath, maxPreloadSize, 0)}
			if _, err := old.Dirty.Write(0, oldData); err != nil {
				t.Fatal(err)
			}
			old.Dirty.ClearDirty()
			oldID := fs.allocateFileHandle(old)
			t.Cleanup(func() { fs.deleteFileHandle(oldID, old) })
			reader := &FileHandle{Ino: newIno, Path: oldPath, BaseRev: 8, OrigSize: int64(len(newData))}
			readerID := fs.allocateFileHandle(reader)
			t.Cleanup(func() { fs.deleteFileHandle(readerID, reader) })
			if st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1},
				strings.TrimPrefix(oldPath, "/"), strings.TrimPrefix(newPath, "/")); st != gofuse.OK {
				t.Fatalf("Rename: %v", st)
			}
			if !bytes.Equal(old.UnlinkedData, oldData) {
				t.Fatal("rename did not preserve the old inode snapshot")
			}
			var out gofuse.AttrOut
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				InHeader: gofuse.InHeader{NodeId: oldIno}, Valid: gofuse.FATTR_SIZE | gofuse.FATTR_FH, Fh: oldID,
			}}, &out); st != gofuse.OK {
				t.Fatalf("old handle truncate: %v", st)
			}
			got, st, err := readDat9FSTestRange(fs, newIno, readerID, 0, len(newData))
			if err != nil || st != gofuse.OK || !bytes.Equal(got, newData) {
				t.Fatalf("replacement read after old handle truncate = %q/%v/%v, want %q", got, st, err, newData)
			}
		})
	}
}

// makeSQLiteTruncateWALForTest builds a committed WAL frame around synthetic
// page bytes. Frame numbers, salts and cumulative checksums follow WAL format;
// the payload is for byte-preservation assertions, not a SQLite database image.
func makeSQLiteTruncateWALForTest(t *testing.T, salt1, salt2 uint32, fill byte) []byte {
	t.Helper()
	header := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, salt1, salt2)
	frame := make([]byte, 24+4096)
	binary.BigEndian.PutUint32(frame[0:4], 2) // database page number
	binary.BigEndian.PutUint32(frame[4:8], 2) // database size; marks a commit
	copy(frame[8:16], header[16:24])
	copy(frame[24:], bytes.Repeat([]byte{fill}, 4096))
	checksummed := append(cloneBytes(header[:24]), frame[:8]...)
	checksummed = append(checksummed, frame[24:]...)
	first, second := sqliteWALChecksumForTest(checksummed, false)
	binary.BigEndian.PutUint32(frame[16:20], first)
	binary.BigEndian.PutUint32(frame[20:24], second)
	return append(header, frame...)
}
