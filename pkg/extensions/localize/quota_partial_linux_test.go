//go:build linux

package localize

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// TestOverlayQuotaPartialPathWrite pins the truncating path write's accounting
// against a REAL partial write, the case the byte quota exists for: a file-size
// limit makes the write stop short while the old image is already gone, so the
// name must end up charged exactly the bytes on disk — not the pre-write size
// and not the requested size.
//
// The limit and the SIGXFSZ disposition are process-global, so the body runs in
// a subprocess rather than perturbing the other tests.
func TestOverlayQuotaPartialPathWrite(t *testing.T) {
	const childEnv = "DRIVE9_TEST_LOCALIZE_PARTIAL_WRITE"
	if os.Getenv(childEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestOverlayQuotaPartialPathWrite$")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("partial-write subprocess: %v\n%s", err, out)
		}
		return
	}
	signal.Ignore(syscall.SIGXFSZ)
	var original syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
			t.Error(err)
		}
	})
	limit := func(n int64) {
		t.Helper()
		next := original
		next.Cur = uint64(n)
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &next); err != nil {
			t.Fatalf("set RLIMIT_FSIZE %d: %v", n, err)
		}
	}
	restore := func() {
		t.Helper()
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
			t.Fatalf("restore RLIMIT_FSIZE: %v", err)
		}
	}

	t.Run("grow", func(t *testing.T) {
		// 1000 committed bytes replaced by a 4000-byte write that lands 1500.
		f := newQuotaRouter(t, 10000)
		q := f.overlay.Quota()
		const view = "/repo/node_modules/partial-grow.bin"
		if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 1000)}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if got := q.Used(); got != 1000 {
			t.Fatalf("used after the seed = %d, want 1000", got)
		}
		limit(1500)
		_, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 4000)})
		restore()
		if !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("partial write = %v, want EFBIG", err)
		}
		// The phantom is the pre-write 1000 bytes: charging the delta and
		// resolving it with zero-based `written` used to strand them.
		if got := q.Used(); got != 1500 {
			t.Fatalf("used after the partial grow = %d, want 1500 (bytes on disk)", got)
		}
	})

	t.Run("shrink", func(t *testing.T) {
		// 1000 committed bytes replaced by a 400-byte write that lands 300.
		f := newQuotaRouter(t, 10000)
		q := f.overlay.Quota()
		const view = "/repo/node_modules/partial-shrink.bin"
		if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 1000)}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		limit(300)
		_, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 400)})
		restore()
		if !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("partial write = %v, want EFBIG", err)
		}
		if got := q.Used(); got != 300 {
			t.Fatalf("used after the partial shrink = %d, want 300 (bytes on disk)", got)
		}
	})
}
