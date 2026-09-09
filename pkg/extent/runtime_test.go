package extent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
)

func TestJuiceFuseOptsWriteback(t *testing.T) {
	opts := juiceFuseOpts()
	if opts == nil || !opts.EnableWriteback {
		t.Fatal("extent VFS FuseOpts.EnableWriteback must match JuiceFS -o writeback_cache")
	}
}

func TestJuiceMetaConfOpenCacheForHTTP(t *testing.T) {
	conf := juiceMetaConf()
	if conf.OpenCache != time.Second {
		t.Fatalf("OpenCache=%s, want 1s (JuiceFS --open-cache for high-latency meta)", conf.OpenCache)
	}
	if conf.OpenCacheLimit != 10000 {
		t.Fatalf("OpenCacheLimit=%d, want JuiceFS --open-cache-limit 10000", conf.OpenCacheLimit)
	}
}

func TestApplyChunkCacheDirJfsSubdirNoUploadDelay(t *testing.T) {
	conf := chunk.Config{BufferSize: 32 << 20, UploadDelay: time.Hour}
	applyChunkCacheDir(&conf, "/mnt/cache")
	if !strings.HasSuffix(conf.CacheDir, "/jfs") {
		t.Fatalf("CacheDir=%q, want .../jfs", conf.CacheDir)
	}
	if conf.UploadDelay != 0 {
		t.Fatalf("UploadDelay=%s, want 0 (JuiceFS --writeback default)", conf.UploadDelay)
	}
	if conf.BufferSize != juiceWritebackBufferSize {
		t.Fatalf("BufferSize=%d, want JuiceFS --buffer-size %d", conf.BufferSize, juiceWritebackBufferSize)
	}
	if conf.MaxStageWrite != 1000 {
		t.Fatalf("MaxStageWrite=%d, want JuiceFS --max-stage-write 1000", conf.MaxStageWrite)
	}
	if !conf.CacheFullBlock {
		t.Fatal("CacheFullBlock must follow JuiceFS default (not --cache-partial-only)")
	}
}

func TestRegisterJuiceMetaMsgCompactNotDelegated(t *testing.T) {
	slices, id, tier, err := juiceCompactArgs([]interface{}{[]jfsmeta.Slice{{Id: 1, Len: 1}}, uint64(2), uint8(0)})
	if err != nil {
		t.Fatal(err)
	}
	if id != 2 || tier != 0 || len(slices) != 1 {
		t.Fatalf("id=%d tier=%d n=%d", id, tier, len(slices))
	}
	if jfsmeta.ErrCompactDelegated == nil {
		t.Fatal("expected juicefs ErrCompactDelegated sentinel to still exist")
	}
}

func TestJuiceCompactArgsMountShape(t *testing.T) {
	slices := []jfsmeta.Slice{{Id: 9, Size: 4, Len: 4}}
	got, id, tier, err := juiceCompactArgs([]interface{}{slices, uint64(3), uint8(1)})
	if err != nil {
		t.Fatal(err)
	}
	if id != 3 || tier != 1 || len(got) != 1 || got[0].Id != 9 {
		t.Fatalf("id=%d tier=%d got=%+v", id, tier, got)
	}
	if _, _, _, err := juiceCompactArgs(nil); err == nil {
		t.Fatal("short args must fail")
	}
}

func TestApplyChunkCacheDirEmptyIsNoop(t *testing.T) {
	conf := chunk.Config{CacheDir: "memory", Writeback: false, UploadDelay: time.Hour}
	applyChunkCacheDir(&conf, "")
	if conf.CacheDir != "memory" || conf.Writeback || conf.UploadDelay != time.Hour {
		t.Fatalf("empty cache-dir mutated config: %+v", conf)
	}
}

func TestExtentCacheRootDefaultsUnderDrive9UserCache(t *testing.T) {
	base, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("no user cache dir: %v", err)
	}
	if got, want := extentCacheRoot(""), filepath.Join(base, "drive9"); got != want {
		t.Fatalf("extentCacheRoot(\"\")=%q, want %q", got, want)
	}
	if got := extentCacheRoot("/mnt/mount-hash"); got != "/mnt/mount-hash" {
		t.Fatalf("configured cache root mutated: %q", got)
	}
}

func TestExtentChunkCacheRootNamespacesByCacheKey(t *testing.T) {
	if got := extentChunkCacheRoot("/mnt/hash", ""); got != "/mnt/hash" {
		t.Fatalf("empty key changed the root: %q", got)
	}
	a := extentChunkCacheRoot("/mnt/hash", "aaaa")
	b := extentChunkCacheRoot("/mnt/hash", "bbbb")
	if a == b {
		t.Fatalf("two cache keys resolved to the same root %q", a)
	}
	if want := filepath.Join("/mnt/hash", "aaaa"); a != want {
		t.Fatalf("root=%q, want %q", a, want)
	}
}

func TestCacheKeyForPrefixIsTenantStable(t *testing.T) {
	if got := CacheKeyForPrefix(""); got != "" {
		t.Fatalf("empty prefix produced key %q", got)
	}
	if got := CacheKeyForPrefix("  "); got != "" {
		t.Fatalf("blank prefix produced key %q", got)
	}
	a1 := CacheKeyForPrefix("t/tenant-a/")
	a2 := CacheKeyForPrefix("t/tenant-a/")
	b := CacheKeyForPrefix("t/tenant-b/")
	if a1 == "" || a1 != a2 {
		t.Fatalf("same prefix must map to the same key: %q vs %q", a1, a2)
	}
	if a1 == b {
		t.Fatalf("different tenants share cache key %q", a1)
	}
	if len(a1) != 16 {
		t.Fatalf("cache key length=%d, want 16", len(a1))
	}
}

func TestApplyChunkCacheDirWritebackSurvivesSelfCheck(t *testing.T) {
	conf := chunk.Config{
		BlockSize:      4 << 20,
		CacheSize:      1 << 30,
		CacheDir:       "memory",
		MaxUpload:      20,
		MaxDownload:    200,
		BufferSize:     32 << 20,
		CacheFullBlock: true,
		// NewRuntime passes RuntimeConfig.Writeback straight through; FUSE
		// mounts pin it on.
		Writeback: true,
	}
	applyChunkCacheDir(&conf, t.TempDir())
	conf.SelfCheck("drive9-extent")
	if !conf.Writeback {
		t.Fatal("SelfCheck disabled JuiceFS writeback")
	}
	if conf.CacheDir == "memory" || conf.CacheDir == "" {
		t.Fatalf("CacheDir=%q, want disk path after SelfCheck", conf.CacheDir)
	}
	if conf.UploadDelay != 0 {
		t.Fatalf("UploadDelay=%s, want 0", conf.UploadDelay)
	}
	if conf.BufferSize != juiceWritebackBufferSize {
		t.Fatalf("BufferSize=%d, want %d", conf.BufferSize, juiceWritebackBufferSize)
	}
}
