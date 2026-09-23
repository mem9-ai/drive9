package fuse

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newRuntimeQuotaStore(t *testing.T, limit int64) *ShadowStore {
	t.Helper()
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestShadowRuntimeQuotaSequentialAndOverlaps(t *testing.T) {
	s := newRuntimeQuotaStore(t, 100)
	for _, step := range []struct {
		offset int64
		size   int
		used   int64
		reject bool
	}{
		{0, 60, 60, false},
		{60, 60, 60, true},
		{30, 80, 60, true},
		{30, 60, 90, false},
		{90, 10, 100, false},
		{0, 100, 100, false},
		{100, 1, 100, true},
	} {
		before, _ := s.ReadAll("/file")
		gen := s.ActiveGeneration("/file")
		n, err := s.WriteAt("/file", step.offset, bytes.Repeat([]byte("x"), step.size), 1)
		if step.reject {
			if n != 0 || !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("offset=%d size=%d: n=%d err=%v, want ENOSPC", step.offset, step.size, n, err)
			}
			after, readErr := s.ReadAll("/file")
			if readErr != nil || !bytes.Equal(before, after) || s.ActiveGeneration("/file") != gen {
				t.Fatalf("rejected write changed shadow: err=%v", readErr)
			}
		} else if err != nil || n != step.size {
			t.Fatalf("offset=%d size=%d: n=%d err=%v", step.offset, step.size, n, err)
		}
		if got := s.quotaBytes.Load(); got != step.used {
			t.Fatalf("used=%d, want %d", got, step.used)
		}
	}
}

func TestShadowRuntimeQuotaSparseAndMultipleFiles(t *testing.T) {
	s := newRuntimeQuotaStore(t, 100)
	if _, err := s.WriteAt("/sparse", 1<<20, []byte("ab"), 1); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteFull("/other", make([]byte, 90), 1); err != nil {
		t.Fatalf("sparse EOF poisoned another file: %v", err)
	}
	if _, err := s.WriteAt("/sparse", 100, make([]byte, 8), 1); err != nil {
		t.Fatalf("fill hole within quota: %v", err)
	}
	for _, offset := range []int64{108, (1 << 20) + 2} {
		if _, err := s.WriteAt("/sparse", offset, []byte("x"), 1); !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("fill/append offset=%d: err=%v, want ENOSPC", offset, err)
		}
	}
	if _, err := s.WriteAt("/sparse", 1<<20, []byte("cd"), 1); err != nil {
		t.Fatalf("overwrite at quota: %v", err)
	}
	s.RecoverPendingBytes()
	if s.quotaBytes.Load() != 100 || s.PendingBytes() != (1<<20)+2+90 {
		t.Fatalf("recovery mixed counters: runtime=%d logical=%d", s.quotaBytes.Load(), s.PendingBytes())
	}
	s.Remove("/other")
	if _, err := s.WriteAt("/sparse", (1<<20)+2, make([]byte, 90), 1); err != nil {
		t.Fatalf("append after releasing other file: %v", err)
	}
}

func TestShadowRuntimeQuotaMergesOutOfOrderRanges(t *testing.T) {
	s := newRuntimeQuotaStore(t, 30)
	for _, step := range []struct{ offset, length, used int64 }{
		{30, 10, 10}, {10, 10, 20}, {15, 20, 30}, {10, 30, 30},
	} {
		if _, err := s.WriteAt("/file", step.offset, make([]byte, step.length), 1); err != nil {
			t.Fatal(err)
		}
		if got := s.quotaBytes.Load(); got != step.used {
			t.Fatalf("write [%d,%d): charge=%d want=%d", step.offset, step.offset+step.length, got, step.used)
		}
	}
	if _, err := s.WriteAt("/file", 9, []byte("x"), 1); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("write outside union at quota: %v", err)
	}
}

func TestShadowWrittenRangesFragmented(t *testing.T) {
	const count = 1 << 16
	var ranges shadowWrittenRanges
	for i := 0; i < count; i++ {
		ranges.add(int64(i)<<13, 1<<12)
	}
	if want := int64(count) << 12; ranges.total != want {
		t.Fatalf("fragmented total=%d, want %d", ranges.total, want)
	}
	if got := ranges.addedBytes(int64(count)<<13, 1<<12); got != 1<<12 {
		t.Fatalf("new sparse page adds %d bytes", got)
	}
	if got := ranges.addedBytes(0, int64(count)<<13); got != int64(count)<<12 {
		t.Fatalf("all holes add %d bytes", got)
	}
	copy := ranges.clone()
	copy.add(1<<12, 1<<12) // Bridge the first two pages.
	if copy.total != ranges.total+(1<<12) || ranges.total != int64(count)<<12 {
		t.Fatal("clone mutation changed original coverage")
	}
	cut := int64(count/2)<<13 | 1<<11
	if want := int64(count/2)<<12 | 1<<11; ranges.truncate(cut) != want {
		t.Fatalf("truncated total=%d, want %d", ranges.total, want)
	}
	var reverse shadowWrittenRanges
	for i := count - 1; i >= 0; i-- {
		reverse.add(int64(i)<<13, 1<<12)
	}
	if reverse.total != int64(count)<<12 {
		t.Fatalf("reverse insertion total=%d", reverse.total)
	}
}

func TestShadowWrittenRangesAgainstByteMap(t *testing.T) {
	const size = 2048
	rng := rand.New(rand.NewSource(973))
	covered := make([]bool, size)
	var ranges shadowWrittenRanges
	for step := 0; step < 5000; step++ {
		if step%17 == 0 {
			end := rng.Intn(size + 1)
			for i := end; i < size; i++ {
				covered[i] = false
			}
			var want int64
			for _, yes := range covered {
				if yes {
					want++
				}
			}
			if got := ranges.truncate(int64(end)); got != want {
				t.Fatalf("step %d truncate to %d: got %d, want %d", step, end, got, want)
			}
			continue
		}
		start := rng.Intn(size)
		end := start + 1 + rng.Intn(size-start)
		var wantAdded int64
		for i := start; i < end; i++ {
			if !covered[i] {
				wantAdded++
			}
		}
		if got := ranges.addedBytes(int64(start), int64(end-start)); got != wantAdded {
			t.Fatalf("step %d add [%d,%d): got %d, want %d", step, start, end, got, wantAdded)
		}
		ranges.add(int64(start), int64(end-start))
		for i := start; i < end; i++ {
			covered[i] = true
		}
		var wantTotal int64
		for _, yes := range covered {
			if yes {
				wantTotal++
			}
		}
		if ranges.total != wantTotal {
			t.Fatalf("step %d total=%d, want %d", step, ranges.total, wantTotal)
		}
	}
}

func BenchmarkShadowWrittenRangesFragmentedAppend(b *testing.B) {
	for _, existing := range []int{0, 1 << 18} {
		b.Run(fmt.Sprintf("existing=%d", existing), func(b *testing.B) {
			var ranges shadowWrittenRanges
			for i := 0; i < existing; i++ {
				ranges.add(int64(i)<<13, 1<<12)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ranges.add(int64(existing+i)<<13, 1<<12)
			}
		})
	}
}

func TestShadowRuntimeQuotaFreeRatioGuardsHoleFills(t *testing.T) {
	s := newRuntimeQuotaStore(t, 0)
	if err := s.Ensure("/file", 100, 1); err != nil {
		t.Fatal(err)
	}
	gen := s.ActiveGeneration("/file")
	// A positive write cannot leave the partition 100% free.
	s.writeCacheFreeRatio = 1
	if _, err := s.WriteAt("/file", 10, []byte("x"), 1); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("hole fill bypassed free-ratio guard: %v", err)
	}
	if s.quotaBytes.Load() != 0 || s.PendingBytes() != 100 || s.ActiveGeneration("/file") != gen {
		t.Fatal("free-ratio rejection changed accounting or generation")
	}
}

func TestShadowRuntimeQuotaLifecycle(t *testing.T) {
	s := newRuntimeQuotaStore(t, 100)
	if err := s.Ensure("/file", 1<<20, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteAt("/file", 50, make([]byte, 60), 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Truncate("/file", 80, 1); err != nil {
		t.Fatal(err)
	}
	if s.quotaBytes.Load() != 30 {
		t.Fatalf("truncate charged %d, want 30", s.quotaBytes.Load())
	}
	if err := s.Ensure("/file", 60, 1); err != nil {
		t.Fatal(err)
	}
	if s.quotaBytes.Load() != 10 {
		t.Fatalf("Ensure shrink charged %d, want 10", s.quotaBytes.Load())
	}
	if err := s.Truncate("/file", 1<<20, 1); err != nil {
		t.Fatal(err)
	}
	if s.quotaBytes.Load() != 10 {
		t.Fatal("hole extension was charged")
	}
	if err := s.WriteFull("/file", make([]byte, 40), 1); err != nil {
		t.Fatal(err)
	}
	if !s.Rename("/file", "/file") || s.quotaBytes.Load() != 40 {
		t.Fatal("same-path rename released live coverage")
	}
	if err := s.WriteFull("/target", make([]byte, 60), 1); err != nil {
		t.Fatal(err)
	}
	targetPin := s.Pin("/target")
	if !s.Rename("/file", "/target") || s.quotaBytes.Load() != 40 {
		t.Fatalf("rename replacement charge=%d, want 40", s.quotaBytes.Load())
	}
	s.Unpin(targetPin)
	pin := s.Pin("/target")
	s.Remove("/target")
	if s.quotaBytes.Load() != 0 {
		t.Fatal("retired shadow still charged")
	}
	if err := s.WriteFull("/target", make([]byte, 100), 2); err != nil {
		t.Fatal(err)
	}
	s.Unpin(pin)
	if s.quotaBytes.Load() != 100 {
		t.Fatal("Unpin subtracted retired charge twice")
	}
	s.Remove("/target")
	if s.quotaBytes.Load() != 0 {
		t.Fatal("remove did not release replacement charge")
	}
}

func TestShadowRuntimeQuotaMixedWriters(t *testing.T) {
	s := newRuntimeQuotaStore(t, 100)
	if _, err := s.WriteAt("/file", 1<<20, []byte("x"), 1); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteFull("/file", make([]byte, 101), 1); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("full replacement of sparse shadow: %v", err)
	}
	if _, err := s.WriteStream("/file", bytes.NewReader(make([]byte, 100)), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteStream("/file", bytes.NewReader(make([]byte, 100)), 1); err != nil {
		t.Fatalf("same-size stream at limit: %v", err)
	}
	wb := NewWriteBuffer("/file", 1<<20, 10)
	if _, err := wb.Write(20, []byte("xy")); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteExtents("/file", wb, 1); err != nil {
		t.Fatal(err)
	}
	if s.quotaBytes.Load() != 22 || s.PendingBytes() != 22 {
		t.Fatalf("extent shrink: runtime=%d logical=%d", s.quotaBytes.Load(), s.PendingBytes())
	}
	// WriteExtents on a fresh sparse target writes only the two data bytes.
	if err := s.WriteExtents("/sparse", wb, 1); err != nil {
		t.Fatal(err)
	}
	if s.quotaBytes.Load() != 24 {
		t.Fatalf("sparse extent charge=%d, want 24", s.quotaBytes.Load())
	}
}

func TestShadowRuntimeQuotaRestartStartsEmpty(t *testing.T) {
	dir := t.TempDir()
	s, err := NewShadowStoreWithQuota(dir, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteAt("/old", 1<<20, make([]byte, 100), 1); err != nil {
		s.Close()
		t.Fatal(err)
	}
	s.Close()
	s, err = NewShadowStoreWithQuota(dir, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	s.RecoverPendingBytes()
	if s.quotaBytes.Load() != 0 {
		t.Fatal("recovered logical bytes charged to new process")
	}
	if _, err := s.WriteAt("/old", 1<<20, make([]byte, 60), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteAt("/old", 1<<20, make([]byte, 60), 1); err != nil {
		t.Fatal(err)
	}
	if s.quotaBytes.Load() != 60 {
		t.Fatalf("recovered overwrite charged %d, want 60", s.quotaBytes.Load())
	}
	s.Remove("/old")
	if s.quotaBytes.Load() != 0 {
		t.Fatal("removing recovered file subtracted uncharged old bytes")
	}
}

func TestFUSEWriteQuotaRejectionPreservesDirtyAndShadow(t *testing.T) {
	for _, spill := range []bool{false, true} {
		t.Run(fmt.Sprintf("spill=%t", spill), func(t *testing.T) {
			s := newRuntimeQuotaStore(t, 100)
			opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
			fs.shadowStore = s
			const path = "/quota"
			ino := fs.inodes.Lookup(path, false, 0, time.Now())
			if err := s.Ensure(path, 0, 0); err != nil {
				t.Fatal(err)
			}
			fh := &FileHandle{Ino: ino, Path: path, IsNew: true, ShadowReady: true, ShadowSpill: spill,
				Dirty: fs.newWriteBuffer(path, streamingWriteMaxSize, 0), WritePolicy: WritePolicyWriteBack}
			id := fs.allocateFileHandle(fh)
			in := gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}
			first := bytes.Repeat([]byte("a"), 60)
			if n, st := fs.Write(nil, &in, first); n != 60 || st != gofuse.OK {
				t.Fatalf("initial write: n=%d status=%v", n, st)
			}
			seq, gen := fh.DirtySeq, s.ActiveGeneration(path)
			for _, offset := range []uint64{60, 50} {
				in.Offset = offset
				if n, st := fs.Write(nil, &in, bytes.Repeat([]byte("b"), 60)); n != 0 || st != gofuse.Status(syscall.ENOSPC) {
					t.Fatalf("quota write: n=%d status=%v", n, st)
				}
				shadow, err := s.ReadAll(path)
				if err != nil || !bytes.Equal(shadow, first) || !bytes.Equal(fh.Dirty.bytesView(), first) {
					t.Fatalf("rejected write changed shadow or Dirty: err=%v", err)
				}
				if fh.DirtySeq != seq || s.ActiveGeneration(path) != gen || s.quotaBytes.Load() != 60 {
					t.Fatal("rejected write changed generation or charge")
				}
				result, st := fs.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Size: 120}, make([]byte, 120))
				if st != gofuse.OK {
					t.Fatalf("read after rejected write: %v", st)
				}
				got, readStatus := result.Bytes(make([]byte, 120))
				result.Done()
				if readStatus != gofuse.OK || !bytes.Equal(got, first) {
					t.Fatalf("read exposed rejected data: %q status=%v", got, readStatus)
				}
			}
		})
	}
}
