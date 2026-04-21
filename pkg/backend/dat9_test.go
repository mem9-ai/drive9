package backend

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

type countingS3Client struct {
	inner     s3client.S3Client
	putObject int
}

func (c *countingS3Client) CreateMultipartUpload(ctx context.Context, key string, algo s3client.ChecksumAlgo) (*s3client.MultipartUpload, error) {
	return c.inner.CreateMultipartUpload(ctx, key, algo)
}

func (c *countingS3Client) PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int, partSize int64, algo s3client.ChecksumAlgo, checksumValue string, ttl time.Duration) (*s3client.UploadPartURL, error) {
	return c.inner.PresignUploadPart(ctx, key, uploadID, partNumber, partSize, algo, checksumValue, ttl)
}

func (c *countingS3Client) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []s3client.Part) error {
	return c.inner.CompleteMultipartUpload(ctx, key, uploadID, parts)
}

func (c *countingS3Client) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	return c.inner.AbortMultipartUpload(ctx, key, uploadID)
}

func (c *countingS3Client) ListParts(ctx context.Context, key, uploadID string) ([]s3client.Part, error) {
	return c.inner.ListParts(ctx, key, uploadID)
}

func (c *countingS3Client) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return c.inner.PresignGetObject(ctx, key, ttl)
}

func (c *countingS3Client) PutObject(ctx context.Context, key string, body io.Reader, size int64) error {
	c.putObject++
	return c.inner.PutObject(ctx, key, body, size)
}

func (c *countingS3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return c.inner.GetObject(ctx, key)
}

func (c *countingS3Client) DeleteObject(ctx context.Context, key string) error {
	return c.inner.DeleteObject(ctx, key)
}

func (c *countingS3Client) UploadPartCopy(ctx context.Context, destKey, uploadID string, partNumber int, sourceKey string, startByte, endByte int64) (string, error) {
	return c.inner.UploadPartCopy(ctx, destKey, uploadID, partNumber, sourceKey, startByte, endByte)
}

func (c *countingS3Client) PresignGetObjectRange(ctx context.Context, key string, startByte, endByte int64, ttl time.Duration) (string, error) {
	return c.inner.PresignGetObjectRange(ctx, key, startByte, endByte, ttl)
}

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

func TestCreateMetadataOnlyCtxCreatesConfirmedEmptyFile(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	result, err := b.CreateMetadataOnlyCtx(ctx, "/meta-only.txt")
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != "/meta-only.txt" {
		t.Fatalf("path=%q, want /meta-only.txt", result.Path)
	}
	if result.Revision != 1 {
		t.Fatalf("revision=%d, want 1", result.Revision)
	}
	if result.SizeBytes != 0 {
		t.Fatalf("size=%d, want 0", result.SizeBytes)
	}
	if result.Status != datastore.StatusConfirmed {
		t.Fatalf("status=%s, want %s", result.Status, datastore.StatusConfirmed)
	}

	nf, err := b.StatNodeCtx(ctx, "/meta-only.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.File == nil {
		t.Fatal("expected file entity for metadata-only create")
	}
	if nf.File.Status != datastore.StatusConfirmed || nf.File.Revision != 1 {
		t.Fatalf("unexpected file metadata: %+v", nf.File)
	}
	if nf.File.StorageType != datastore.StorageDB9 || nf.File.StorageRef != "inline" {
		t.Fatalf("storage=(%s,%s), want (db9,inline)", nf.File.StorageType, nf.File.StorageRef)
	}

	tasks := loadSemanticTasksForFile(t, b, nf.File.FileID)
	if len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0", len(tasks))
	}
}

func TestCreateMetadataOnlyCtxDoesNotWriteZeroByteObject(t *testing.T) {
	b := newTestBackendWithS3(t)
	counting := &countingS3Client{inner: b.S3()}
	b.s3 = counting
	b.smallInDB = false

	if _, err := b.CreateMetadataOnlyCtx(context.Background(), "/objectless.txt"); err != nil {
		t.Fatal(err)
	}
	if counting.putObject != 0 {
		t.Fatalf("PutObject calls=%d, want 0", counting.putObject)
	}
}

func TestCreateMetadataOnlyCtxThenConditionalWriteAdvancesRevision(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	if _, err := b.CreateMetadataOnlyCtx(ctx, "/after-create.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.WriteCtxIfRevision(ctx, "/after-create.txt", []byte("hello"), 0, filesystem.WriteFlagTruncate, 0); !errors.Is(err, datastore.ErrRevisionConflict) {
		t.Fatalf("expected revision conflict for create-if-absent write, got %v", err)
	}

	n, err := b.WriteCtxIfRevision(ctx, "/after-create.txt", []byte("hello"), 0, filesystem.WriteFlagTruncate, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("bytes written=%d, want 5", n)
	}

	nf, err := b.StatNodeCtx(ctx, "/after-create.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.File == nil {
		t.Fatal("expected file after conditional write")
	}
	if nf.File.Revision != 2 {
		t.Fatalf("revision=%d, want 2", nf.File.Revision)
	}
	if nf.File.SizeBytes != 5 {
		t.Fatalf("size=%d, want 5", nf.File.SizeBytes)
	}

	data, err := b.Read("/after-create.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("data=%q, want hello", data)
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
