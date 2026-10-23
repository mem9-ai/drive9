//go:build linux

package staging

import (
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"

	"errors"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
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
			// v1 drove this through a Dat9FS FUSE Write; the partial pwrite and
			// its accounting live in the store's WriteExtents, exercised here
			// directly (FUSE-level coverage is ported to pkg/extensions/writeback).
			s := newRuntimeQuotaStore(t, 100)
			const path = "/partial"
			if err := s.Ensure(path, 0, 1); err != nil {
				t.Fatal(err)
			}
			before := s.ActiveGeneration(path)
			wb := writebuffer.NewWriteBuffer(path, 1<<20, 0)
			if _, err := wb.Write(0, []byte("0123456789")); err != nil {
				t.Fatal(err)
			}
			// RLIMIT_FSIZE makes the pwrite stop at 7 bytes; the store surfaces
			// EFBIG (v1's FUSE layer treated the same partial write as
			// n=7/OK). The accounting must still charge exactly the 7 bytes
			// that landed.
			if err := s.WriteExtents(path, wb, int64(before)); err == nil {
				t.Fatal("RLIMIT_FSIZE write unexpectedly succeeded")
			} else if !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("partial WriteExtents: %v", err)
			}
			if s.quotaBytes.Load() != 7 {
				t.Fatalf("partial write charged %d, want 7", s.quotaBytes.Load())
			}
		})
	}
}
