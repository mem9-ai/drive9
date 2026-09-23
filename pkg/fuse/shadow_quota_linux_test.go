//go:build linux

package fuse

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// A file-size limit produces a real partial pwrite followed by EFBIG. Isolate
// the limit and signal disposition in a subprocess rather than changing the
// environment of other tests or adding a production-only I/O injection hook.
func TestShadowRuntimeQuotaPartialWriteAccounting(t *testing.T) {
	const childEnv = "DRIVE9_TEST_SHADOW_PARTIAL_WRITE"
	if os.Getenv(childEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestShadowRuntimeQuotaPartialWriteAccounting$")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("partial-write subprocess: %v\n%s", err, output)
		}
		return
	}
	var original syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
		t.Fatal(err)
	}
	signal.Ignore(syscall.SIGXFSZ)
	limit := original
	limit.Cur = 7
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
			t.Error(err)
		}
	})
	for _, method := range []string{"at", "full", "extents"} {
		t.Run(method, func(t *testing.T) {
			s := newRuntimeQuotaStore(t, 100)
			if err := s.Ensure("/file", 0, 1); err != nil {
				t.Fatal(err)
			}
			before := s.ActiveGeneration("/file")
			var err error
			switch method {
			case "at":
				var n int
				n, err = s.WriteAt("/file", 0, []byte("0123456789"), 2)
				if n != 7 {
					t.Fatalf("partial write n=%d, want 7", n)
				}
			case "full":
				err = s.WriteFull("/file", []byte("0123456789"), 2)
			case "extents":
				wb := NewWriteBuffer("/file", 0, 0)
				if _, err := wb.Write(0, []byte("0123456789")); err != nil {
					t.Fatal(err)
				}
				err = s.WriteExtents("/file", wb, 2)
			}
			if !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("partial write err=%v, want EFBIG", err)
			}
			data, readErr := s.ReadAll("/file")
			if readErr != nil || string(data) != "0123456" || s.Size("/file") != 7 {
				t.Fatalf("partial content=%q size=%d err=%v", data, s.Size("/file"), readErr)
			}
			if s.quotaBytes.Load() != 7 || s.PendingBytes() != 7 || s.ActiveGeneration("/file") == before || s.BaseRev("/file") != 2 {
				t.Fatalf("partial mutation not accounted: quota=%d logical=%d gen=%d rev=%d", s.quotaBytes.Load(), s.PendingBytes(), s.ActiveGeneration("/file"), s.BaseRev("/file"))
			}
			s.Remove("/file")
			if s.quotaBytes.Load() != 0 || s.PendingBytes() != 0 {
				t.Fatal("partial write charge not released")
			}
		})
	}
	for _, spill := range []bool{false, true} {
		name := "write-through"
		if spill {
			name = "shadow-spill"
		}
		t.Run(name, func(t *testing.T) {
			s := newRuntimeQuotaStore(t, 100)
			const path = "/partial"
			if err := s.Ensure(path, 0, 1); err != nil {
				t.Fatal(err)
			}
			before := s.ActiveGeneration(path)
			opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
			fs.shadowStore = s
			ino := fs.inodes.Lookup(path, false, 0, time.Now())
			fh := &FileHandle{Ino: ino, Path: path, BaseRev: 1, IsNew: true,
				ShadowReady: true, ShadowSpill: spill, Dirty: fs.newWriteBuffer(path, streamingWriteMaxSize, 0),
				WritePolicy: WritePolicyWriteBack}
			id := fs.allocateFileHandle(fh)
			n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}, []byte("0123456789"))
			if n != 7 || st != gofuse.OK {
				t.Fatalf("partial FUSE write: n=%d status=%v, want 7/OK", n, st)
			}
			shadow, err := s.ReadAll(path)
			if err != nil || string(shadow) != "0123456" || string(fh.Dirty.bytesView()) != "0123456" {
				t.Fatalf("shadow/Dirty mismatch: shadow=%q dirty=%q err=%v", shadow, fh.Dirty.bytesView(), err)
			}
			if fh.DirtySeq == 0 || fh.ShadowStageGen != s.ActiveGeneration(path) || fh.ShadowStageGen == before || s.quotaBytes.Load() != 7 {
				t.Fatalf("partial FUSE state: seq=%d stage=%d active=%d quota=%d", fh.DirtySeq, fh.ShadowStageGen, s.ActiveGeneration(path), s.quotaBytes.Load())
			}
			result, readStatus := fs.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Size: 10}, make([]byte, 10))
			if readStatus != gofuse.OK {
				t.Fatal(readStatus)
			}
			got, readStatus := result.Bytes(make([]byte, 10))
			result.Done()
			if readStatus != gofuse.OK || string(got) != "0123456" {
				t.Fatalf("partial read=%q status=%v", got, readStatus)
			}
		})
	}
}
