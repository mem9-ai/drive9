package client

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"
)

// benchReaderAt serves a deterministic multi-GiB image without materializing
// it: every 4 MiB part is the sha256 of its part number, repeated.
type benchReaderAt struct {
	size     int64
	partSize int64
}

func (b benchReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n := 0
	for n < len(p) {
		part := (off + int64(n)) / b.partSize
		within := (off + int64(n)) % b.partSize
		sum := sha256.Sum256([]byte(fmt.Sprintf("part-%d", part)))
		if within%32 == 0 {
			n += copy(p[n:], sum[:])
		} else {
			end := n + int(32-within%32)
			if end > len(p) {
				end = len(p)
			}
			for i := n; i < end; i++ {
				p[i] = 0
			}
			n = end
		}
	}
	return len(p), nil
}

// TestExtentSequentialUpload6GiBBench measures the client's full extent
// sequential upload (initiate → presign batches → PUTs → complete) for a
// 6 GiB image against DRIVE9_EXTENT_JOINT_BASE. Not a gate: it prints the
// wall time (and per-phase counts via the progress callback) so server-side
// presign/verification costs are visible end to end.
//
// DRIVE9_EXTENT_UPLOAD_BENCH_SIZE overrides the size (bytes).
func TestExtentSequentialUpload6GiBBench(t *testing.T) {
	base := os.Getenv("DRIVE9_EXTENT_JOINT_BASE")
	if base == "" {
		t.Skip("DRIVE9_EXTENT_JOINT_BASE not set")
	}
	size := int64(6 << 30)
	if v := os.Getenv("DRIVE9_EXTENT_UPLOAD_BENCH_SIZE"); v != "" {
		fmt.Sscanf(v, "%d", &size)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	apiKey := os.Getenv("DRIVE9_EXTENT_JOINT_KEY")
	if apiKey == "" {
		apiKey = provisionJointTenant(t, ctx, base)
	}
	cl := New(base, apiKey)
	for i := 0; i < 60; i++ {
		if _, err := cl.StatCtx(ctx, "/"); err == nil {
			break
		} else if i == 59 {
			t.Fatalf("tenant never became active: %v", err)
		}
		time.Sleep(time.Second)
	}

	path := fmt.Sprintf("/bench-upload-%d/seq-6g.bin", time.Now().UnixNano())
	if err := cl.MkdirCtx(ctx, parentOfBench(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	ra := benchReaderAt{size: size, partSize: 4 << 20}
	var done int
	progress := func(part, total int, bytes int64) {
		done++
		if done%384 == 0 {
			t.Logf("progress %d/%d parts", part, total)
		}
	}
	start := time.Now()
	if err := cl.WriteExtentStreamConditional(ctx, path, ra, size, progress, -1); err != nil {
		t.Fatalf("upload: %v", err)
	}
	dur := time.Since(start)
	st, err := cl.StatCtx(ctx, path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	t.Logf("UPLOAD BENCH size=%d layout=%q wall=%s (%.1f MiB/s)", size, st.ContentLayout, dur, float64(size)/dur.Seconds()/(1<<20))
}

func parentOfBench(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i+1]
		}
	}
	return "/"
}
