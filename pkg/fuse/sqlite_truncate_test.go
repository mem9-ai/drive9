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
		for _, truncateMode := range []string{"other-handle", "path", "same-handle", "nonzero-fd"} {
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
					if readOnly {
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
