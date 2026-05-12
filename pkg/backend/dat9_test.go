package backend

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

func newTestBackend(t *testing.T) *Dat9Backend {
	return newTestBackendWithOptions(t, Options{})
}

func newTestBackendWithOptions(t *testing.T, opts Options) *Dat9Backend {
	t.Helper()

	s3Dir, err := os.MkdirTemp("", "dat9-s3-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })

	initBackendSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })

	s3c, err := s3client.NewLocal(s3Dir, "/s3")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewWithS3ModeAndOptions(store, s3c, true, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

var _ filesystem.FileSystem = (*Dat9Backend)(nil)

func TestCreateAndStat(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Create("/hello.txt"); err != nil {
		t.Fatal(err)
	}
	info, err := b.Stat("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "hello.txt" || info.IsDir || info.Size != 0 {
		t.Errorf("unexpected: %+v", info)
	}
}

func TestWriteCtxIfRevisionWithTagsResult_ReturnsCommittedRevision(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	// Create: committed revision should be 1.
	n, rev, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/rev-test.txt", []byte("v1"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, "")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if n != 2 {
		t.Fatalf("create bytes = %d, want 2", n)
	}
	if rev != 1 {
		t.Fatalf("create revision = %d, want 1", rev)
	}

	// Overwrite with CAS revision 1: committed revision should be 2.
	n, rev, err = b.WriteCtxIfRevisionWithTagsResult(ctx, "/rev-test.txt", []byte("v2"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, 1, nil, "")
	if err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if n != 2 {
		t.Fatalf("overwrite bytes = %d, want 2", n)
	}
	if rev != 2 {
		t.Fatalf("overwrite revision = %d, want 2", rev)
	}
}

func TestUsesDatabaseAutoEmbeddingDefaultsToFalse(t *testing.T) {
	b := newTestBackend(t)
	if b.UsesDatabaseAutoEmbedding() {
		t.Fatal("default backend should remain app-managed")
	}
}

func TestUsesDatabaseAutoEmbeddingReflectsOption(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{DatabaseAutoEmbedding: true})
	if !b.UsesDatabaseAutoEmbedding() {
		t.Fatal("backend should expose database auto-embedding option")
	}
}

func TestAsyncImageExtractWillWireRuntime(t *testing.T) {
	if AsyncImageExtractWillWireRuntime(AsyncImageExtractOptions{}) {
		t.Fatal("disabled async image extract should not wire runtime")
	}
	if !AsyncImageExtractWillWireRuntime(AsyncImageExtractOptions{Enabled: true}) {
		t.Fatal("enabled async image extract should wire runtime (extractor may be defaulted in configureOptions)")
	}
}

func TestDat9BackendAutoSemanticTaskTypes(t *testing.T) {
	t.Run("app_managed_default", func(t *testing.T) {
		b := newTestBackend(t)
		if b.AutoSemanticTaskTypes() != nil {
			t.Fatal("expected nil when not in database auto-embedding mode")
		}
	})
	t.Run("auto_mode_without_async_runtime", func(t *testing.T) {
		b := newTestBackendWithOptions(t, Options{DatabaseAutoEmbedding: true})
		if b.AutoSemanticTaskTypes() != nil {
			t.Fatal("expected nil without async image/audio extraction runtime")
		}
	})
	t.Run("auto_mode_audio_enabled_nil_extractor", func(t *testing.T) {
		b := newTestBackendWithOptions(t, Options{
			DatabaseAutoEmbedding: true,
			AsyncAudioExtract: AsyncAudioExtractOptions{
				Enabled:   true,
				Extractor: nil,
			},
		})
		if b.AutoSemanticTaskTypes() != nil {
			t.Fatalf("got %#v, want nil when audio async enabled but extractor unset", b.AutoSemanticTaskTypes())
		}
	})
	t.Run("auto_mode_with_async_image", func(t *testing.T) {
		b := newTestBackendWithOptions(t, Options{
			DatabaseAutoEmbedding: true,
			AsyncImageExtract: AsyncImageExtractOptions{
				Enabled:   true,
				Workers:   1,
				QueueSize: 4,
				Extractor: &staticImageExtractor{text: "caption"},
			},
		})
		got := b.AutoSemanticTaskTypes()
		if len(got) != 1 || got[0] != semantic.TaskTypeImgExtractText {
			t.Fatalf("got %#v, want [img_extract_text]", got)
		}
	})
	t.Run("auto_mode_with_async_audio_only", func(t *testing.T) {
		b := newTestBackendWithOptions(t, Options{
			DatabaseAutoEmbedding: true,
			AsyncAudioExtract: AsyncAudioExtractOptions{
				Enabled:   true,
				Extractor: &staticAudioExtractor{text: "x"},
			},
		})
		got := b.AutoSemanticTaskTypes()
		if len(got) != 1 || got[0] != semantic.TaskTypeAudioExtractText {
			t.Fatalf("got %#v, want [audio_extract_text]", got)
		}
	})
	t.Run("auto_mode_with_image_and_audio", func(t *testing.T) {
		b := newTestBackendWithOptions(t, Options{
			DatabaseAutoEmbedding: true,
			AsyncImageExtract: AsyncImageExtractOptions{
				Enabled:   true,
				Workers:   1,
				QueueSize: 4,
				Extractor: &staticImageExtractor{text: "caption"},
			},
			AsyncAudioExtract: AsyncAudioExtractOptions{
				Enabled:   true,
				Extractor: &staticAudioExtractor{text: "x"},
			},
		})
		got := b.AutoSemanticTaskTypes()
		if len(got) != 2 || got[0] != semantic.TaskTypeImgExtractText || got[1] != semantic.TaskTypeAudioExtractText {
			t.Fatalf("got %#v, want [img_extract_text audio_extract_text]", got)
		}
	})
}

func TestExtractTextUsesEmbeddingSafeLimit(t *testing.T) {
	if DefaultTextExtractMaxBytes >= DefaultInlineThreshold {
		t.Fatalf("DefaultTextExtractMaxBytes=%d must stay below DefaultInlineThreshold=%d", DefaultTextExtractMaxBytes, DefaultInlineThreshold)
	}
	exact := make([]byte, DefaultTextExtractMaxBytes)
	for i := range exact {
		exact[i] = 'a'
	}
	if got := extractText(exact, "application/json", DefaultTextExtractMaxBytes); int64(len(got)) != DefaultTextExtractMaxBytes {
		t.Fatalf("exact limit extracted length=%d, want %d", len(got), DefaultTextExtractMaxBytes)
	}
	oversized := append(exact, 'a')
	if got := extractText(oversized, "application/json", DefaultTextExtractMaxBytes); got != "" {
		t.Fatalf("oversized text should not be extracted, got length %d", len(got))
	}
	if got := extractText([]byte("hello"), "text/plain", DefaultTextExtractMaxBytes); got != "hello" {
		t.Fatalf("small text extracted as %q, want hello", got)
	}
	if got := extractText([]byte{0x66, 0x6f, 0x80}, "text/plain", DefaultTextExtractMaxBytes); got != "" {
		t.Fatalf("invalid UTF-8 text should not be extracted, got %q", got)
	}
}

func TestConfigurableInlineThreshold(t *testing.T) {
	t.Run("defaults applied when options omit thresholds", func(t *testing.T) {
		b := newTestBackend(t)
		if got := b.InlineThreshold(); got != DefaultInlineThreshold {
			t.Fatalf("InlineThreshold = %d, want %d", got, DefaultInlineThreshold)
		}
		if got := b.TextExtractMaxBytes(); got != DefaultTextExtractMaxBytes {
			t.Fatalf("TextExtractMaxBytes = %d, want %d", got, DefaultTextExtractMaxBytes)
		}
		// shouldStoreInDB and IsLargeFile must agree on the default cutoff so
		// a file at exactly the threshold flips storage class.
		if !b.shouldStoreInDB(DefaultInlineThreshold - 1) {
			t.Fatal("shouldStoreInDB should be true at threshold-1")
		}
		if b.shouldStoreInDB(DefaultInlineThreshold) {
			t.Fatal("shouldStoreInDB should be false at threshold")
		}
		if b.IsLargeFile(DefaultInlineThreshold - 1) {
			t.Fatal("IsLargeFile should be false at threshold-1")
		}
		if !b.IsLargeFile(DefaultInlineThreshold) {
			t.Fatal("IsLargeFile should be true at threshold")
		}
	})

	t.Run("custom inline threshold overrides default", func(t *testing.T) {
		const custom = int64(256_000)
		b := newTestBackendWithOptions(t, Options{InlineThreshold: custom})
		if got := b.InlineThreshold(); got != custom {
			t.Fatalf("InlineThreshold = %d, want %d", got, custom)
		}
		if !b.shouldStoreInDB(custom - 1) {
			t.Fatal("file below custom threshold must store inline")
		}
		if b.shouldStoreInDB(custom) {
			t.Fatal("file at custom threshold must spill to S3")
		}
		if b.IsLargeFile(custom - 1) {
			t.Fatal("IsLargeFile must be false at custom-1")
		}
		if !b.IsLargeFile(custom) {
			t.Fatal("IsLargeFile must be true at custom")
		}
	})

	t.Run("custom text extract max overrides default", func(t *testing.T) {
		const custom = int64(64_000)
		b := newTestBackendWithOptions(t, Options{TextExtractMaxBytes: custom})
		if got := b.TextExtractMaxBytes(); got != custom {
			t.Fatalf("TextExtractMaxBytes = %d, want %d", got, custom)
		}
		// Right at the cap is still extractable.
		atCap := make([]byte, custom)
		for i := range atCap {
			atCap[i] = 'a'
		}
		if got := extractText(atCap, "text/plain", b.TextExtractMaxBytes()); int64(len(got)) != custom {
			t.Fatalf("extracted len = %d, want %d", len(got), custom)
		}
		// Just over the cap must drop the text entirely (not truncate).
		over := append(atCap, 'a')
		if got := extractText(over, "text/plain", b.TextExtractMaxBytes()); got != "" {
			t.Fatalf("extracted len = %d, want 0 (dropped over cap)", len(got))
		}
	})

	t.Run("zero or negative options fall back to defaults", func(t *testing.T) {
		b := newTestBackendWithOptions(t, Options{InlineThreshold: 0, TextExtractMaxBytes: -1})
		if got := b.InlineThreshold(); got != DefaultInlineThreshold {
			t.Fatalf("zero InlineThreshold did not fall back: got %d", got)
		}
		if got := b.TextExtractMaxBytes(); got != DefaultTextExtractMaxBytes {
			t.Fatalf("negative TextExtractMaxBytes did not fall back: got %d", got)
		}
	})
}

func TestDetectContentTypeRequiresValidUTF8ForText(t *testing.T) {
	data := []byte{0x78, 0x01, 0x95, 0x90, 0xbd, 0x6a}
	if got := detectContentType("/objects/61/31ee8937c5f0aff1268064d5f2218d7d240056", data); got != "application/octet-stream" {
		t.Fatalf("content type=%q, want application/octet-stream", got)
	}
	gbkCSV := []byte{0xb0, 0xa1, ',', 'x', '\n'}
	if got := detectContentType("/data/gbk.csv", gbkCSV); got != "application/octet-stream" {
		t.Fatalf("content type=%q, want application/octet-stream for invalid UTF-8 text extension", got)
	}
	if got := detectContentType("/config.json", gbkCSV); got != "application/octet-stream" {
		t.Fatalf("content type=%q, want application/octet-stream for invalid UTF-8 JSON extension", got)
	}
}

func TestWriteAndRead(t *testing.T) {
	b := newTestBackend(t)
	n, err := b.Write("/data/file.txt", []byte("hello world"), 0, filesystem.WriteFlagCreate)
	if err != nil {
		t.Fatal(err)
	}
	if n != 11 {
		t.Errorf("expected 11 bytes, got %d", n)
	}
	data, err := b.Read("/data/file.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("got %q", data)
	}
}

func TestWriteOverwrite(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/f.txt", []byte("old"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/f.txt", []byte("new"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	data, _ := b.Read("/f.txt", 0, -1)
	if string(data) != "new" {
		t.Errorf("got %q", data)
	}
}

func TestWriteAppend(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/f.txt", []byte("hello"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/f.txt", []byte(" world"), 0, filesystem.WriteFlagAppend); err != nil {
		t.Fatal(err)
	}
	data, _ := b.Read("/f.txt", 0, -1)
	if string(data) != "hello world" {
		t.Errorf("got %q", data)
	}
}

func TestWriteRejectedWhenTenantStorageQuotaExceeded(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{MaxTenantStorageBytes: 10})
	_, err := b.Write("/quota.txt", []byte("12345678901"), 0, filesystem.WriteFlagCreate)
	if !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("expected ErrStorageQuotaExceeded, got %v", err)
	}
}

func TestWriteOverwriteReusesExistingQuota(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{MaxTenantStorageBytes: 10})
	if _, err := b.Write("/quota.txt", []byte("1234567890"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/quota.txt", []byte("abcdefghij"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("same-size overwrite should succeed: %v", err)
	}
	if _, err := b.Write("/quota.txt", []byte("abcdefghijk"), 0, filesystem.WriteFlagTruncate); !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("expected ErrStorageQuotaExceeded on growth, got %v", err)
	}
}

func TestReadWithOffset(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/f.txt", []byte("hello world"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	data, _ := b.Read("/f.txt", 6, 5)
	if string(data) != "world" {
		t.Errorf("got %q", data)
	}
}

func TestMkdirAndReadDir(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Mkdir("/data", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/data/a.txt", []byte("a"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/data/b.txt", []byte("bb"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	entries, err := b.ReadDir("/data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
	if entries[0].Name != "a.txt" || entries[1].Name != "b.txt" {
		t.Errorf("unexpected: %+v", entries)
	}
}

func TestRemove(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/f.txt", []byte("data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.Remove("/f.txt"); err != nil {
		t.Fatal(err)
	}
	_, err := b.Stat("/f.txt")
	if err != datastore.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRemoveFileAndDirCtxUseTypedPaths(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	if _, err := b.Write("/f.txt", []byte("data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.Mkdir("/empty", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := b.Mkdir("/dir", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/g.txt", []byte("data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	if err := b.RemoveFileCtx(ctx, "/dir/"); err != datastore.ErrNotFound {
		t.Fatalf("RemoveFileCtx dir error = %v, want ErrNotFound", err)
	}
	if err := b.RemoveDirCtx(ctx, "/g.txt"); err != datastore.ErrNotFound {
		t.Fatalf("RemoveDirCtx file error = %v, want ErrNotFound", err)
	}
	if err := b.RemoveFileCtx(ctx, "/f.txt"); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveDirCtx(ctx, "/empty"); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveAll(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Mkdir("/data", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/data/a.txt", []byte("a"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/data/b.txt", []byte("b"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveAll("/data/"); err != nil {
		t.Fatal(err)
	}
	_, err := b.Stat("/data/")
	if err != datastore.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStatDirWithoutTrailingSlash(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Mkdir("/data", 0o755); err != nil {
		t.Fatal(err)
	}
	info, err := b.Stat("/data")
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir {
		t.Fatal("expected /data to resolve as a directory without trailing slash")
	}
}

func TestRemoveDirWithoutTrailingSlash(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Mkdir("/data", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := b.Remove("/data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stat("/data/"); err != datastore.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRemoveAllDirWithoutTrailingSlash(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Mkdir("/data", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/data/a.txt", []byte("a"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveAll("/data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stat("/data/"); err != datastore.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRename(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/old.txt", []byte("data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stat("/old.txt"); err != datastore.ErrNotFound {
		t.Error("old path should be gone")
	}
	data, _ := b.Read("/new.txt", 0, -1)
	if string(data) != "data" {
		t.Errorf("got %q", data)
	}
}

func TestRenameReplacesExistingFile(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/config", []byte("old"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/config.lock", []byte("new config"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.Rename("/config.lock", "/config"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stat("/config.lock"); err != datastore.ErrNotFound {
		t.Fatalf("old path err = %v, want ErrNotFound", err)
	}
	data, err := b.Read("/config", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new config" {
		t.Fatalf("config = %q, want new config", data)
	}
}

func TestZeroCopyCp(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/a.txt", []byte("shared"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.CopyFile("/a.txt", "/b.txt"); err != nil {
		t.Fatal(err)
	}
	dataA, _ := b.Read("/a.txt", 0, -1)
	dataB, _ := b.Read("/b.txt", 0, -1)
	if string(dataA) != string(dataB) {
		t.Error("content mismatch")
	}
	// Delete one, other survives
	if err := b.Remove("/a.txt"); err != nil {
		t.Fatal(err)
	}
	dataB, err := b.Read("/b.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(dataB) != "shared" {
		t.Errorf("got %q", dataB)
	}
}

func TestAutoCreateParentDirs(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/a/b/c/file.txt", []byte("deep"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/a/", "/a/b/", "/a/b/c/"} {
		info, err := b.Stat(p)
		if err != nil {
			t.Errorf("expected dir %s: %v", p, err)
			continue
		}
		if !info.IsDir {
			t.Errorf("%s should be dir", p)
		}
	}
}

func TestEnsureParentDirsNoRootSelfInsert(t *testing.T) {
	b := newTestBackend(t)
	// Creating a file at root level should not insert "/" as a child of itself
	if _, err := b.Write("/top.txt", []byte("x"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	entries, err := b.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name == "/" || e.Name == "" {
			t.Errorf("root dir should not appear as its own child: %+v", e)
		}
	}
}

func TestOffsetWritePreservesTail(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/f.txt", []byte("ABCDEFGH"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	// Overwrite bytes 2-4 with "XY", should preserve tail "EFGH"
	if _, err := b.Write("/f.txt", []byte("XY"), 2, 0); err != nil {
		t.Fatal(err)
	}
	data, err := b.Read("/f.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "ABXYEFGH" {
		t.Errorf("expected ABXYEFGH, got %q", string(data))
	}
}

func TestRenameDirUpdatesName(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Mkdir("/alpha", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/alpha/file.txt", []byte("data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.Rename("/alpha/", "/beta/"); err != nil {
		t.Fatal(err)
	}
	info, err := b.Stat("/beta/")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "beta" {
		t.Errorf("expected name 'beta', got %q", info.Name)
	}
}

func TestRenameDirWithoutTrailingSlash(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Mkdir("/alpha", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/alpha/file.txt", []byte("data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.Rename("/alpha", "/beta"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stat("/alpha/"); err != datastore.ErrNotFound {
		t.Errorf("expected old path to be gone, got %v", err)
	}
	data, err := b.Read("/beta/file.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "data" {
		t.Errorf("got %q", data)
	}
}

func TestRenameDirEnsuresParentDirs(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Mkdir("/src", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/src/file.txt", []byte("data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	// Rename to a deeply nested path whose parents don't exist
	if err := b.Rename("/src/", "/x/y/dst/"); err != nil {
		t.Fatal(err)
	}
	// Parent dirs /x/ and /x/y/ should have been auto-created
	for _, p := range []string{"/x/", "/x/y/"} {
		info, err := b.Stat(p)
		if err != nil {
			t.Errorf("expected parent dir %s to exist: %v", p, err)
			continue
		}
		if !info.IsDir {
			t.Errorf("expected %s to be a directory", p)
		}
	}
	// The renamed dir and its contents should be accessible
	data, err := b.Read("/x/y/dst/file.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "data" {
		t.Errorf("expected 'data', got %q", string(data))
	}
}

func TestOpenAndOpenWrite(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/f.txt", []byte("content"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	rc, err := b.Open("/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if string(data) != "content" {
		t.Errorf("got %q", data)
	}

	wc, err := b.OpenWrite("/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wc.Write([]byte("new content")); err != nil {
		t.Fatal(err)
	}
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}

	readData, _ := b.Read("/f.txt", 0, -1)
	if string(readData) != "new content" {
		t.Errorf("got %q", readData)
	}
}
