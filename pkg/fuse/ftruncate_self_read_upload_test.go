package fuse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateInheritedSelfReadMemoryUploadCommit(t *testing.T) {
	for _, mode := range []string{"retained", "lazy-rebind", "lazy-load-failure"} {
		t.Run(mode, func(t *testing.T) {
			runFtruncateSelfReadMemoryUploadCommit(t, nil, mode)
		})
	}
}

func runFtruncateSelfReadMemoryUploadCommit(t *testing.T, beforeLargeCommit func(*Dat9FS, uint64, uint64), mode string) {
	t.Helper()
	rebind := mode != "retained"
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	for _, id := range []uint64{a, b} {
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
	}
	originalQueue, originalShadow := fs.commitQueue, fs.shadowStore
	t.Cleanup(func() { fs.commitQueue, fs.shadowStore = originalQueue, originalShadow })
	backend := fs.client.BaseURL()
	var mu sync.Mutex
	var expected, totalSize int64
	parts := make(map[int][]byte)
	var completed int
	var failTailLoad atomic.Bool
	var failedLoads atomic.Int32
	interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/fs/file.bin" {
			if failTailLoad.Load() && strings.HasPrefix(r.Header.Get("Range"), "bytes=8388608-") {
				failedLoads.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			http.ServeContent(w, r, "file.bin", time.Time{}, strings.NewReader(remote()))
			return true
		}
		if !strings.HasPrefix(r.URL.Path, "/v2/uploads/") && !strings.HasPrefix(r.URL.Path, "/probe-part/") {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/v2/uploads/initiate":
			var body struct {
				Path      string `json:"path"`
				TotalSize int64  `json:"total_size"`
				Expected  *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Expected == nil || body.Path != "/file.bin" {
				t.Errorf("initiate decode=%v expected=%v", err, body.Expected)
				w.WriteHeader(400)
				return true
			}
			expected = *body.Expected
			totalSize = body.TotalSize
			parts = make(map[int][]byte)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": "probe", "key": "probe", "part_size": 8 << 20, "total_parts": (body.TotalSize + (8 << 20) - 1) / (8 << 20)})
		case strings.HasSuffix(r.URL.Path, "/presign"):
			var body struct {
				Number int `json:"part_number"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return true
			}
			size := min(int64(8<<20), totalSize-int64(body.Number-1)*(8<<20))
			_ = json.NewEncoder(w).Encode(map[string]any{"number": body.Number, "url": fs.client.BaseURL() + "/probe-part/" + strconv.Itoa(body.Number), "size": size})
		case strings.HasSuffix(r.URL.Path, "/presign-batch"):
			var body struct {
				Parts []struct {
					Number int `json:"part_number"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return true
			}
			urls := make([]map[string]any, 0, len(body.Parts))
			for _, part := range body.Parts {
				size := min(int64(8<<20), totalSize-int64(part.Number-1)*(8<<20))
				urls = append(urls, map[string]any{"number": part.Number, "url": fs.client.BaseURL() + "/probe-part/" + strconv.Itoa(part.Number), "size": size})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": urls})
		case strings.HasPrefix(r.URL.Path, "/probe-part/"):
			number, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/probe-part/"))
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return true
			}
			parts[number], err = io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return true
			}
			w.Header().Set("ETag", fmt.Sprintf("part-%d", number))
		case strings.HasSuffix(r.URL.Path, "/complete"):
			var payload []byte
			for number := 1; number <= len(parts); number++ {
				payload = append(payload, parts[number]...)
			}
			if int64(len(payload)) != totalSize {
				t.Errorf("multipart payload size = %d, want %d", len(payload), totalSize)
				w.WriteHeader(http.StatusBadRequest)
				return true
			}
			request, err := http.NewRequest(http.MethodPut, backend+"/v1/fs/file.bin", bytes.NewReader(payload))
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return true
			}
			request.Header.Set("X-Dat9-Expected-Revision", strconv.FormatInt(expected, 10))
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return true
			}
			defer func() { _ = response.Body.Close() }()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusInternalServerError)
				return true
			}
			for name, values := range response.Header {
				for _, value := range values {
					w.Header().Add(name, value)
				}
			}
			w.WriteHeader(response.StatusCode)
			_, _ = w.Write(body)
			if response.StatusCode == http.StatusOK {
				completed++
			}
		case strings.HasSuffix(r.URL.Path, "/abort"):
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected upload request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
		return true
	})
	fs.commitQueue, fs.shadowStore = nil, nil
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	uploader := NewWriteBackUploader(fs.client, fs.writeBack, 1)
	t.Cleanup(uploader.DrainAll)
	uploader.OnSuccess = fs.onWriteBackUploadSuccess
	uploader.OnDataCommitted = fs.onWriteBackDataCommitted
	uploader.SnapshotStagingGens = fs.snapshotStagingGens
	fs.SetWriteBack(fs.writeBack, uploader)
	var opened gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC)}, &opened); st != gofuse.OK {
		t.Fatal(st)
	}
	id := opened.Fh
	hardlinkWrite(t, fs, ino, id, "hello world")
	hardlinkSync(t, fs, ino, id)
	reviewFtruncate(t, fs, ino, id, 5)
	var sibling uint64
	if rebind {
		var out gofuse.OpenOut
		if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &out); st != gofuse.OK {
			t.Fatal(st)
		}
		sibling = out.Fh
	}
	hardlinkSync(t, fs, ino, id)
	hardlinkWrite(t, fs, ino, id, "H")
	hardlinkSync(t, fs, ino, id)
	h, _ := fs.fileHandles.Get(id)
	if h.Streamer == nil {
		t.Fatal("actual Open handle did not retain its streamer")
	}
	payload := []byte(strings.Repeat("Q", 9<<20))
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Offset: 5}, payload); n != uint32(len(payload)) || st != gofuse.OK {
		t.Fatal(n, st)
	}
	if h.Streamer.Started() {
		t.Fatal("fixture reached streaming instead of UploadAll")
	}
	if beforeLargeCommit != nil {
		beforeLargeCommit(fs, ino, id)
	}
	hardlinkSync(t, fs, ino, id)
	mu.Lock()
	uploads := completed
	mu.Unlock()
	if uploads != 1 || remote() != "Hello"+string(payload) {
		t.Fatalf("multipart commits=%d remote-size=%d", uploads, len(remote()))
	}
	want := "Xello" + string(payload)
	if rebind {
		// Growing B avoids patch mode and commits an exact new tail byte.
		// The real clean-sibling refresh must then rebind A's retained
		// uploader to a lazy image; no buffer or lineage is fabricated.
		if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sibling, Offset: uint64(5 + len(payload))}, []byte("Y")); n != 1 || st != gofuse.OK {
			t.Fatal(n, st)
		}
		hardlinkSync(t, fs, ino, sibling)
		if remote() != "Hello"+string(payload)+"Y" {
			t.Fatal("sibling did not commit the exact new image")
		}
		if h.Streamer == nil || h.Dirty.PartSize() != 8<<20 || h.Dirty.IsPartLoaded(0) || h.Dirty.IsPartLoaded(1) {
			t.Fatal("sibling commit did not leave the retained uploader with a lazy buffer")
		}
		want += "Y"
	}
	fs.dirtyMu.Lock()
	mutation := fs.mutationInodes[ino]
	fs.dirtyMu.Unlock()
	t.Logf("after UploadAll base=%d inode=%d mutation=%+v", h.BaseRev, fs.ftruncateAliasRevision(h), mutation)
	hardlinkWrite(t, fs, ino, id, "X")
	readFtruncateSelfWant(t, fs, ino, id, 0, 5, "Xello")
	readFtruncateSelfWant(t, fs, ino, id, 0, 4096, "Xello"+strings.Repeat("Q", 4096-5))
	if rebind && h.Dirty.IsPartLoaded(1) {
		t.Fatal("prefix read unexpectedly loaded the untouched tail")
	}
	if mode == "lazy-load-failure" {
		seq := h.DirtySeq
		failTailLoad.Store(true)
		st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
		failTailLoad.Store(false)
		if st == gofuse.OK || failedLoads.Load() == 0 || h.DirtySeq != seq || !h.Dirty.HasDirtyParts() {
			t.Fatalf("failed materialization = %v loads=%d seq=%d want=%d dirty=%t", st, failedLoads.Load(), h.DirtySeq, seq, h.Dirty.HasDirtyParts())
		}
		if remote() != "Hello"+string(payload)+"Y" {
			t.Fatal("failed materialization changed the committed image")
		}
		readFtruncateSelfWant(t, fs, ino, id, 0, 5, "Xello")
	}
	hardlinkSync(t, fs, ino, id)
	if got := remote(); got != want {
		t.Fatalf("remote after self-read and next commit: size=%d want=%d tail=%x want=%x", len(got), len(want), []byte(got[max(0, len(got)-8):]), []byte(want[len(want)-8:]))
	}
}
