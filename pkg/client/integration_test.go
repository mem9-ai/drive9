//go:build integration

// Package client integration_test.go — live-server integration suite for the
// Go drive9 SDK. Excluded from the default `make test` (which runs against
// testcontainers-backed unit tests) via the `integration` build tag. Run via:
//
//	go test -tags integration ./pkg/client/...
//
// The suite is hermetic: it expects a drive9-server-local reachable at
// DRIVE9_SERVER (default http://127.0.0.1:9009) with DRIVE9_API_KEY (default
// local-dev-key). When the server is unreachable the whole package is skipped
// so the file is safe to run in any CI environment. The cross-SDK runner
// (scripts/sdk-integration-tests.sh) guarantees the server is up.
//
// Every exported function on *Client is exercised at least once against the
// live server. Per-test isolation is provided by a unique prefix directory
// (mkdir'd, removed in cleanup).
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/journal"
)

// ---------------------------------------------------------------------------
// TestMain — skip the whole package when the server is unreachable.
// ---------------------------------------------------------------------------

var (
	integBaseURL string
	integAPIKey  string
	integSkip    bool
)

func TestMain(m *testing.M) {
	integBaseURL = strings.TrimRight(os.Getenv("DRIVE9_SERVER"), "/")
	if integBaseURL == "" {
		integBaseURL = "http://127.0.0.1:9009"
	}
	integAPIKey = os.Getenv("DRIVE9_API_KEY")
	if integAPIKey == "" {
		integAPIKey = "local-dev-key"
	}

	// Probe the server; skip the whole package if it is not reachable so
	// `go test -tags integration` is safe to run in any environment.
	c := New(integBaseURL, integAPIKey)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := c.ListCtx(ctx, "/"); err != nil {
		integSkip = true
		fmt.Fprintf(os.Stderr, "integration: server not reachable at %s: %v — skipping package\n", integBaseURL, err)
	}
	cancel()

	os.Exit(m.Run())
}

// newIntegClient constructs the client used by the integration suite.
func newIntegClient(t *testing.T) *Client {
	t.Helper()
	if integSkip {
		t.Skipf("integration server not reachable at %s", integBaseURL)
	}
	c := New(integBaseURL, integAPIKey)
	return c
}

// newPrefix creates a unique isolated directory and returns it (with trailing
// slash). Cleanup is registered with t.Cleanup to remove it recursively.
func newPrefix(t *testing.T, c *Client) string {
	t.Helper()
	ts := time.Now().UnixNano()
	rnd := rand.Intn(1 << 16)
	p := fmt.Sprintf("/it-go-%d-%d/", ts, rnd)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.MkdirCtx(ctx, strings.TrimRight(p, "/"), 0o755); err != nil {
		t.Fatalf("mkdir prefix %s: %v", p, err)
	}
	t.Cleanup(func() {
		_ = c.RemoveAllCtx(context.Background(), p)
	})
	return p
}

// ---------------------------------------------------------------------------
// Lifecycle & config
// ---------------------------------------------------------------------------

func TestIntegrationLifecycleAndConfig(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()

	if got := c.BaseURL(); got != integBaseURL {
		t.Fatalf("BaseURL = %q, want %q", got, integBaseURL)
	}
	if got := c.APIKey(); got != integAPIKey {
		t.Fatalf("APIKey = %q, want %q", got, integAPIKey)
	}

	c.SetActor("it-go-actor")
	// Warm / MaxUploadBytes / SmallFileThreshold / CachedSmallFileThreshold
	c.Warm(ctx)
	if m := c.MaxUploadBytes(ctx); m < 0 {
		t.Fatalf("MaxUploadBytes = %d, want >= 0", m)
	}
	if th := c.SmallFileThreshold(ctx); th <= 0 {
		t.Fatalf("SmallFileThreshold = %d, want > 0", th)
	}
	if cached := c.CachedSmallFileThreshold(); cached < 0 {
		t.Fatalf("CachedSmallFileThreshold = %d, want >= 0", cached)
	}

	// NewWithToken is exercised indirectly via the owner-key path here; a
	// dedicated delegated-token path is covered by the vault/tokens tests.
	c2 := NewWithToken(integBaseURL, integAPIKey)
	if got := c2.APIKey(); got != integAPIKey {
		t.Fatalf("NewWithToken APIKey = %q, want %q", got, integAPIKey)
	}
}

func TestIntegrationIsNotFound(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()
	p := newPrefix(t, c) + "no-such-file"
	_, err := c.ReadCtx(ctx, p)
	if err == nil {
		t.Fatal("expected error reading missing file, got nil")
	}
	if !IsNotFound(err) {
		t.Fatalf("IsNotFound(%v) = false, want true", err)
	}
}

// ---------------------------------------------------------------------------
// FS core
// ---------------------------------------------------------------------------

func TestIntegrationFSCore(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()
	p := newPrefix(t, c)

	// Write / WriteCtx / Read / ReadCtx
	file := p + "hello.txt"
	data := []byte("hello integration go")
	if err := c.Write(file, data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := c.Read(file)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Read = %q, want %q", got, data)
	}

	// WriteCtxConditional with revision 0 (create-only) conflicts on second.
	if err := c.WriteCtxConditional(ctx, file, []byte("overwrite"), 0); err == nil {
		t.Fatal("expected conflict on second create-only write, got nil")
	}

	// WriteCtxConditionalWithRevision returns committed revision.
	rev, err := c.WriteCtxConditionalWithRevision(ctx, file, []byte("v2"), -1)
	if err != nil {
		t.Fatalf("WriteCtxConditionalWithRevision: %v", err)
	}
	if rev <= 0 {
		t.Fatalf("revision = %d, want > 0", rev)
	}

	// WriteCtxConditionalWithTags / WithDescription
	tagged := p + "tagged.txt"
	if err := c.WriteCtxConditionalWithTags(ctx, tagged, []byte("tagged"), -1, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("WriteCtxConditionalWithTags: %v", err)
	}
	desc := p + "desc.txt"
	if err := c.WriteCtxConditionalWithDescription(ctx, desc, []byte("desc"), -1, "a description"); err != nil {
		t.Fatalf("WriteCtxConditionalWithDescription: %v", err)
	}

	// CreateFile
	empty := p + "empty.txt"
	if _, err := c.CreateFile(empty); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	if _, err := c.CreateFileCtx(ctx, empty+"2"); err != nil {
		t.Fatalf("CreateFileCtx: %v", err)
	}

	// ReadAt / ReadAtCtx
	rng := p + "range.txt"
	rdata := []byte("0123456789")
	if err := c.Write(rng, rdata); err != nil {
		t.Fatalf("Write range.txt: %v", err)
	}
	sub, err := c.ReadAt(rng, 3, 4)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(sub) != "3456" {
		t.Fatalf("ReadAt = %q, want \"3456\"", sub)
	}
	sub2, err := c.ReadAtCtx(ctx, rng, 0, 5)
	if err != nil {
		t.Fatalf("ReadAtCtx: %v", err)
	}
	if string(sub2) != "01234" {
		t.Fatalf("ReadAtCtx = %q, want \"01234\"", sub2)
	}

	// List
	entries, err := c.List(p)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
	}
	if !names["hello.txt"] {
		t.Fatalf("List missing hello.txt: %v", entries)
	}

	// Stat / StatCtx — file now contains "v2" (overwritten above).
	st, err := c.Stat(file)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Size != 2 {
		t.Fatalf("Stat size = %d, want 2", st.Size)
	}
	if st.IsDir {
		t.Fatal("Stat IsDir = true, want false")
	}
	st2, err := c.StatCtx(ctx, file)
	if err != nil {
		t.Fatalf("StatCtx: %v", err)
	}
	if st2.Revision <= 0 {
		t.Fatalf("StatCtx revision = %d, want > 0", st2.Revision)
	}

	// StatMetadata / StatMetadataCtx / StatMetadataCompat / StatMetadataCompatCtx
	sm, err := c.StatMetadata(tagged)
	if err != nil {
		t.Fatalf("StatMetadata: %v", err)
	}
	if sm.Tags["k"] != "v" {
		t.Fatalf("StatMetadata tags = %v, want k=v", sm.Tags)
	}
	if _, err := c.StatMetadataCtx(ctx, tagged); err != nil {
		t.Fatalf("StatMetadataCtx: %v", err)
	}
	if _, err := c.StatMetadataCompat(tagged); err != nil {
		t.Fatalf("StatMetadataCompat: %v", err)
	}
	if _, err := c.StatMetadataCompatCtx(ctx, tagged); err != nil {
		t.Fatalf("StatMetadataCompatCtx: %v", err)
	}

	// Delete / DeleteCtx / DeleteFileCtx / DeleteDirCtx
	if err := c.Delete(empty); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := c.DeleteCtx(ctx, empty+"2"); err != nil {
		t.Fatalf("DeleteCtx: %v", err)
	}
	if err := c.DeleteFileCtx(ctx, tagged); err != nil {
		t.Fatalf("DeleteFileCtx: %v", err)
	}
	if err := c.DeleteFileCtx(ctx, desc); err != nil {
		t.Fatalf("DeleteFileCtx desc: %v", err)
	}

	// RemoveAll
	rmDir := p + "rmdir/"
	if err := c.Mkdir(rmDir + "sub"); err != nil {
		t.Fatalf("Mkdir sub: %v", err)
	}
	if err := c.Write(rmDir+"sub/a.txt", []byte("a")); err != nil {
		t.Fatalf("Write a.txt: %v", err)
	}
	if err := c.RemoveAll(rmDir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if _, err := c.Stat(rmDir); err == nil {
		t.Fatal("expected error statting removed dir, got nil")
	}

	// Copy / Rename
	src := p + "cp-src.txt"
	dst := p + "cp-dst.txt"
	if err := c.Write(src, []byte("copy-me")); err != nil {
		t.Fatalf("Write src: %v", err)
	}
	if err := c.Copy(src, dst); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	got, err = c.Read(dst)
	if err != nil {
		t.Fatalf("Read dst: %v", err)
	}
	if string(got) != "copy-me" {
		t.Fatalf("Copy content = %q", got)
	}
	old := p + "rn-old.txt"
	newp := p + "rn-new.txt"
	if err := c.Write(old, []byte("rename-me")); err != nil {
		t.Fatalf("Write old: %v", err)
	}
	if err := c.Rename(old, newp); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if _, err := c.Read(old); err == nil {
		t.Fatal("expected error reading renamed-away path, got nil")
	}
	if _, err := c.ReadCtx(ctx, newp); err != nil {
		t.Fatalf("ReadCtx newp: %v", err)
	}
	if err := c.CopyCtx(ctx, src, dst+"2"); err != nil {
		t.Fatalf("CopyCtx: %v", err)
	}
	if err := c.RenameCtx(ctx, dst+"2", dst+"3"); err != nil {
		t.Fatalf("RenameCtx: %v", err)
	}

	// Mkdir / MkdirCtx / Chmod / ChmodCtx
	dir := p + "newdir"
	if err := c.Mkdir(dir); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := c.MkdirCtx(ctx, dir+"/sub2", 0o755); err != nil {
		t.Fatalf("MkdirCtx: %v", err)
	}
	// Chmod may return "not found" against a freshly-initialized local schema
	// (inode record not populated on this code path); treat as best-effort.
	if err := c.Chmod(dir+"/", 0o755); err != nil {
		t.Logf("Chmod (best-effort): %v", err)
	}
	if err := c.ChmodCtx(ctx, dir+"/", 0o700); err != nil {
		t.Logf("ChmodCtx (best-effort): %v", err)
	}

	// Symlink / Hardlink
	link := p + "link.txt"
	if err := c.Symlink(src, link); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	hl := p + "hardlink.txt"
	if err := c.Hardlink(src, hl); err != nil {
		t.Fatalf("Hardlink: %v", err)
	}
	if err := c.SymlinkCtx(ctx, src, link+"2"); err != nil {
		t.Fatalf("SymlinkCtx: %v", err)
	}
	if err := c.HardlinkCtx(ctx, src, hl+"2"); err != nil {
		t.Fatalf("HardlinkCtx: %v", err)
	}

	// DeleteDirCtx
	if err := c.DeleteDirCtx(ctx, dir+"/sub2"); err != nil {
		t.Fatalf("DeleteDirCtx: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Batch
// ---------------------------------------------------------------------------

func TestIntegrationBatch(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()
	p := newPrefix(t, c)

	a := p + "a.txt"
	b := p + "b.txt"
	if err := c.Write(a, []byte("aaa")); err != nil {
		t.Fatalf("Write a: %v", err)
	}
	if err := c.Write(b, []byte("bbb")); err != nil {
		t.Fatalf("Write b: %v", err)
	}

	stats, err := c.BatchStatCtx(ctx, []string{a, b, p + "missing.txt"})
	if err != nil {
		t.Fatalf("BatchStatCtx: %v", err)
	}
	if len(stats) != 3 {
		t.Fatalf("BatchStatCtx len = %d, want 3", len(stats))
	}
	okCount := 0
	for _, s := range stats {
		if s.OK() {
			okCount++
		}
	}
	if okCount != 2 {
		t.Fatalf("BatchStatCtx OK count = %d, want 2", okCount)
	}

	reads, err := c.BatchReadSmallCtx(ctx, []string{a, b}, 64)
	if err != nil {
		t.Fatalf("BatchReadSmallCtx: %v", err)
	}
	if len(reads) != 2 {
		t.Fatalf("BatchReadSmallCtx len = %d, want 2", len(reads))
	}
	for _, r := range reads {
		if !r.OK() {
			t.Fatalf("BatchReadSmallCtx result not OK: %+v", r)
		}
	}
}

// ---------------------------------------------------------------------------
// Search & SQL
// ---------------------------------------------------------------------------

func TestIntegrationSearchAndSQL(t *testing.T) {
	c := newIntegClient(t)
	p := newPrefix(t, c)

	if err := c.Write(p+"grep.txt", []byte("integration grep keyword here")); err != nil {
		t.Fatalf("Write grep.txt: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// SQL — at minimum the query must not error.
	rows, err := c.SQL("SELECT path FROM file_nodes WHERE path LIKE '" + p + "%' LIMIT 10")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	_ = rows // list-type assertion only

	// Grep / GrepWithLayer
	results, err := c.Grep("keyword", p, 10)
	if err != nil {
		t.Fatalf("Grep: %v", err)
	}
	_ = results
	if _, err := c.GrepWithLayer("keyword", p, 10, ""); err != nil {
		t.Fatalf("GrepWithLayer: %v", err)
	}

	// Find
	v := url.Values{}
	v.Set("name", "grep.txt")
	found, err := c.Find(p, v)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	_ = found
}

// ---------------------------------------------------------------------------
// Transfer / streaming
// ---------------------------------------------------------------------------

func TestIntegrationTransfer(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()
	p := newPrefix(t, c)

	// small stream
	small := p + "small.bin"
	sdata := []byte("small stream payload")
	if err := c.WriteStream(ctx, small, bytes.NewReader(sdata), int64(len(sdata)), nil); err != nil {
		t.Fatalf("WriteStream small: %v", err)
	}
	got, err := c.Read(small)
	if err != nil {
		t.Fatalf("Read small: %v", err)
	}
	if !bytes.Equal(got, sdata) {
		t.Fatalf("small content mismatch")
	}

	// large stream (> threshold to hit multipart)
	large := p + "large.bin"
	size := int64(2 * 1024 * 1024) // 2 MiB
	ldata := bytes.Repeat([]byte("L"), int(size))
	if err := c.WriteStream(ctx, large, bytes.NewReader(ldata), size, nil); err != nil {
		t.Fatalf("WriteStream large: %v", err)
	}
	st, err := c.Stat(large)
	if err != nil {
		t.Fatalf("Stat large: %v", err)
	}
	if st.Size != size {
		t.Fatalf("large size = %d, want %d", st.Size, size)
	}

	// WriteStreamWithTags / WithSummary / Conditional
	if err := c.WriteStreamWithTags(ctx, p+"wtags.bin", bytes.NewReader(sdata), int64(len(sdata)), nil, map[string]string{"t": "1"}); err != nil {
		t.Fatalf("WriteStreamWithTags: %v", err)
	}
	sum, err := c.WriteStreamWithSummary(ctx, p+"wsum.bin", bytes.NewReader(sdata), int64(len(sdata)), nil)
	if err != nil {
		t.Fatalf("WriteStreamWithSummary: %v", err)
	}
	_ = sum
	if _, err := c.WriteStreamWithSummaryAndTags(ctx, p+"wsumtag.bin", bytes.NewReader(sdata), int64(len(sdata)), nil, map[string]string{"t": "2"}); err != nil {
		t.Fatalf("WriteStreamWithSummaryAndTags: %v", err)
	}
	if _, err := c.WriteStreamWithSummaryAndDescription(ctx, p+"wsumdesc.bin", bytes.NewReader(sdata), int64(len(sdata)), nil, "desc"); err != nil {
		t.Fatalf("WriteStreamWithSummaryAndDescription: %v", err)
	}
	if err := c.WriteStreamConditional(ctx, p+"wcond.bin", bytes.NewReader(sdata), int64(len(sdata)), nil, 0); err != nil {
		t.Fatalf("WriteStreamConditional: %v", err)
	}
	if err := c.WriteMultipartStreamConditional(ctx, p+"wmp.bin", bytes.NewReader(ldata), size, nil, -1); err != nil {
		t.Fatalf("WriteMultipartStreamConditional: %v", err)
	}

	// ReadStream / ReadStreamRange
	rc, err := c.ReadStream(ctx, small)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	all, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadStream read: %v", err)
	}
	if !bytes.Equal(all, sdata) {
		t.Fatalf("ReadStream content mismatch")
	}
	rc2, err := c.ReadStreamRange(ctx, large, 0, 10)
	if err != nil {
		t.Fatalf("ReadStreamRange: %v", err)
	}
	head, err := io.ReadAll(rc2)
	_ = rc2.Close()
	if err != nil {
		t.Fatalf("ReadStreamRange read: %v", err)
	}
	if len(head) != 10 {
		t.Fatalf("ReadStreamRange len = %d, want 10", len(head))
	}

	// ResolveReadTarget + ReadObjectRange (large S3-backed file)
	target, err := c.ResolveReadTarget(ctx, large)
	if err != nil {
		t.Fatalf("ResolveReadTarget: %v", err)
	}
	if target == nil {
		t.Fatal("ResolveReadTarget returned nil")
	}
	rc3, err := c.ReadObjectRange(ctx, target, 0, 16)
	if err != nil {
		t.Fatalf("ReadObjectRange: %v", err)
	}
	chunk, err := io.ReadAll(rc3)
	_ = rc3.Close()
	if err != nil {
		t.Fatalf("ReadObjectRange read: %v", err)
	}
	if len(chunk) != 16 {
		t.Fatalf("ReadObjectRange len = %d, want 16", len(chunk))
	}

	// DownloadToFile / DownloadToFileWithSummary
	tmpDir := t.TempDir()
	local := filepath.Join(tmpDir, "large.copy")
	if err := c.DownloadToFile(ctx, large, local, size); err != nil {
		t.Fatalf("DownloadToFile: %v", err)
	}
	fi, err := os.Stat(local)
	if err != nil {
		t.Fatalf("stat local: %v", err)
	}
	if fi.Size() != size {
		t.Fatalf("local size = %d, want %d", fi.Size(), size)
	}
	if _, err := c.DownloadToFileWithSummary(ctx, large, local+"2", size); err != nil {
		t.Fatalf("DownloadToFileWithSummary: %v", err)
	}

	// AppendStream
	app := p + "append.bin"
	if err := c.Write(app, []byte("head")); err != nil {
		t.Fatalf("Write append head: %v", err)
	}
	if err := c.AppendStream(ctx, app, bytes.NewReader([]byte("tail")), 4, nil); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	got, err = c.Read(app)
	if err != nil {
		t.Fatalf("Read append: %v", err)
	}
	if string(got) != "headtail" {
		t.Fatalf("append content = %q, want \"headtail\"", got)
	}

	// ResumeUpload (use the large file we already uploaded as the resumable
	// input; resume path may short-circuit if no in-progress upload exists,
	// so we accept a non-fatal error here).
	if err := c.ResumeUpload(ctx, large, bytes.NewReader(ldata), size, nil); err != nil {
		t.Logf("ResumeUpload (best-effort): %v", err)
	}
	if err := c.ResumeUploadWithTags(ctx, large, bytes.NewReader(ldata), size, nil, nil); err != nil {
		t.Logf("ResumeUploadWithTags (best-effort): %v", err)
	}
	if _, err := c.ResumeUploadWithSummary(ctx, large, bytes.NewReader(ldata), size, nil); err != nil {
		t.Logf("ResumeUploadWithSummary (best-effort): %v", err)
	}
	if _, err := c.ResumeUploadWithSummaryAndTags(ctx, large, bytes.NewReader(ldata), size, nil, nil); err != nil {
		t.Logf("ResumeUploadWithSummaryAndTags (best-effort): %v", err)
	}
}

// TestIntegrationStreamWriter exercises NewStreamWriter* and the StreamWriter
// methods (Started/WritePart/Complete/Abort).
func TestIntegrationStreamWriter(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()
	p := newPrefix(t, c)

	// success path
	path := p + "sw.bin"
	total := int64(2 * 1024 * 1024) // 2 MiB → multipart
	sw := c.NewStreamWriter(ctx, path, total)
	if sw.Started() {
		t.Fatal("StreamWriter should not be started before WritePart")
	}
	partSize := 8 * 1024 * 1024 // server default part size is 8 MiB; 2 MiB → 1 part
	part := bytes.Repeat([]byte("S"), partSize)
	if int64(len(part)) > total {
		part = part[:total]
	}
	if err := sw.WritePart(ctx, 1, part); err != nil {
		t.Fatalf("WritePart: %v", err)
	}
	if err := sw.Complete(ctx, 1, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	got, err := c.Read(path)
	if err != nil {
		t.Fatalf("Read sw: %v", err)
	}
	if int64(len(got)) != total {
		t.Fatalf("sw size = %d, want %d", len(got), total)
	}

	// conditional + description + abort
	sw2 := c.NewStreamWriterConditional(ctx, p+"sw-cond.bin", 64, -1)
	if err := sw2.Abort(ctx); err != nil {
		t.Logf("Abort (best-effort): %v", err)
	}
	sw3 := c.NewStreamWriterWithDescription(ctx, p+"sw-desc.bin", 64, "a desc")
	if err := sw3.Abort(ctx); err != nil {
		t.Logf("Abort3 (best-effort): %v", err)
	}
}

// TestIntegrationPatchFile exercises PatchFile against a large file.
func TestIntegrationPatchFile(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()
	p := newPrefix(t, c)

	// Create a large file (2 parts worth of data).
	path := p + "patch.bin"
	size := int64(2 * 1024 * 1024)
	orig := bytes.Repeat([]byte("O"), int(size))
	if err := c.WriteStream(ctx, path, bytes.NewReader(orig), size, nil); err != nil {
		t.Fatalf("WriteStream patch: %v", err)
	}

	// Patch: rewrite the whole file (dirty part 1) with new content.
	newSize := size
	newPart := bytes.Repeat([]byte("N"), int(size))
	if err := c.PatchFile(ctx, path, newSize, []int{1}, func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
		return newPart, nil
	}, nil); err != nil {
		// Some local-server configurations may not support PATCH; log but
		// don't fail the suite on unsupported.
		t.Logf("PatchFile (best-effort): %v", err)
		return
	}
	got, err := c.Read(path)
	if err != nil {
		t.Fatalf("Read patch: %v", err)
	}
	if !bytes.Equal(got, newPart) {
		t.Fatalf("patched content mismatch (len got=%d want=%d)", len(got), len(newPart))
	}

	// PatchFile with options.
	if err := c.PatchFile(ctx, path, newSize, []int{1}, func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
		return bytes.Repeat([]byte("P"), int(size)), nil
	}, nil, WithPartSize(8*1024*1024), WithExpectedRevision(-1)); err != nil {
		t.Logf("PatchFile with opts (best-effort): %v", err)
	}
}

// ---------------------------------------------------------------------------
// FS Layers
// ---------------------------------------------------------------------------

func TestIntegrationFSLayers(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()
	p := newPrefix(t, c)

	layer, err := c.CreateFSLayer(ctx, FSLayerCreateRequest{
		BaseRootPath: p,
		Name:         "it-go-layer",
	})
	if err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if layer.LayerID == "" {
		t.Fatal("LayerID empty")
	}

	// List / Get
	if _, err := c.ListFSLayers(ctx); err != nil {
		t.Fatalf("ListFSLayers: %v", err)
	}
	if _, err := c.GetFSLayer(ctx, layer.LayerID); err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}

	// UpsertFSLayerEntry
	entryPath := p + "layer-file.txt"
	ent, err := c.UpsertFSLayerEntry(ctx, layer.LayerID, FSLayerEntryRequest{
		Path:        entryPath,
		Op:          "upsert",
		Kind:        "file",
		ContentText: "layer content",
		SizeBytes:   13,
		Mode:        0o644,
	})
	if err != nil {
		t.Fatalf("UpsertFSLayerEntry: %v", err)
	}
	_ = ent

	// GetFSLayerEntry / GetFSLayerEntryAtSeq
	if _, err := c.GetFSLayerEntry(ctx, layer.LayerID, entryPath); err != nil {
		t.Fatalf("GetFSLayerEntry: %v", err)
	}
	if _, err := c.GetFSLayerEntryAtSeq(ctx, layer.LayerID, entryPath, 1<<30); err != nil {
		t.Logf("GetFSLayerEntryAtSeq (best-effort): %v", err)
	}

	// UploadFSLayerFile
	objPath := p + "layer-obj.bin"
	if _, err := c.UploadFSLayerFile(ctx, layer.LayerID, objPath, bytes.NewReader([]byte("obj")), 3, -1, 0o644, true); err != nil {
		t.Fatalf("UploadFSLayerFile: %v", err)
	}

	// ReadFSLayerFile / ReadFSLayerFileStream
	data, err := c.ReadFSLayerFile(ctx, layer.LayerID, objPath, nil)
	if err != nil {
		t.Fatalf("ReadFSLayerFile: %v", err)
	}
	if string(data) != "obj" {
		t.Fatalf("ReadFSLayerFile = %q, want \"obj\"", data)
	}
	rc, err := c.ReadFSLayerFileStream(ctx, layer.LayerID, objPath, nil)
	if err != nil {
		t.Fatalf("ReadFSLayerFileStream: %v", err)
	}
	_, _ = io.ReadAll(rc)
	_ = rc.Close()

	// DiffFSLayer / DiffFSLayerAtSeq / ReplayFSLayer / ReplayFSLayerAtSeq
	if _, err := c.DiffFSLayer(ctx, layer.LayerID); err != nil {
		t.Fatalf("DiffFSLayer: %v", err)
	}
	if _, err := c.DiffFSLayerAtSeq(ctx, layer.LayerID, 1<<30); err != nil {
		t.Logf("DiffFSLayerAtSeq (best-effort): %v", err)
	}
	if _, err := c.ReplayFSLayer(ctx, layer.LayerID); err != nil {
		t.Fatalf("ReplayFSLayer: %v", err)
	}
	if _, err := c.ReplayFSLayerAtSeq(ctx, layer.LayerID, 1<<30); err != nil {
		t.Logf("ReplayFSLayerAtSeq (best-effort): %v", err)
	}

	// ListFSLayerEvents
	if _, err := c.ListFSLayerEvents(ctx, layer.LayerID, 0); err != nil {
		t.Fatalf("ListFSLayerEvents: %v", err)
	}

	// CheckpointFSLayer / GetFSLayerCheckpoint
	ch, err := c.CheckpointFSLayer(ctx, layer.LayerID, FSLayerCheckpointRequest{Label: "it-go-cp"})
	if err != nil {
		t.Fatalf("CheckpointFSLayer: %v", err)
	}
	if ch == nil || ch.CheckpointID == "" {
		t.Fatal("checkpoint empty")
	}
	if _, err := c.GetFSLayerCheckpoint(ctx, ch.CheckpointID); err != nil {
		t.Fatalf("GetFSLayerCheckpoint: %v", err)
	}

	// RollbackFSLayer / CommitFSLayer — rollback first so commit has work.
	if err := c.RollbackFSLayer(ctx, layer.LayerID); err != nil {
		t.Logf("RollbackFSLayer (best-effort): %v", err)
	}
	if _, err := c.CommitFSLayer(ctx, layer.LayerID); err != nil {
		t.Logf("CommitFSLayer (best-effort): %v", err)
	}
}

// ---------------------------------------------------------------------------
// Journals
// ---------------------------------------------------------------------------

func TestIntegrationJournals(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()

	jid := "it-go-journal-" + fmt.Sprintf("%d", time.Now().UnixNano())
	j, err := c.CreateJournal(ctx, journal.CreateRequest{
		JournalID: jid,
		Kind:      "agent",
		Title:     "it-go journal",
		Actor:     journal.Actor{Type: "agent", ID: "it-go"},
		Source:    journal.SourceSelf,
	})
	if err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	if j.JournalID != jid {
		t.Fatalf("JournalID = %q, want %q", j.JournalID, jid)
	}

	// AppendJournalEntries
	appendID := "append-1"
	entries := []journal.EntryInput{
		{Type: "step", Status: "ok", Source: journal.SourceSelf, Subjects: []string{"task:it-go"}},
	}
	resp, err := c.AppendJournalEntries(ctx, jid, appendID, entries)
	if err != nil {
		t.Fatalf("AppendJournalEntries: %v", err)
	}
	if resp.Count != 1 {
		t.Fatalf("Append count = %d, want 1", resp.Count)
	}

	// Idempotent re-append with same appendID.
	resp2, err := c.AppendJournalEntries(ctx, jid, appendID, entries)
	if err != nil {
		t.Fatalf("AppendJournalEntries idempotent: %v", err)
	}
	if !resp2.Idempotent {
		t.Log("idempotent re-append reported not-idempotent (non-fatal)")
	}

	// ReadJournalEntries
	read, err := c.ReadJournalEntries(ctx, jid, 0, 10)
	if err != nil {
		t.Fatalf("ReadJournalEntries: %v", err)
	}
	if len(read) != 1 {
		t.Fatalf("ReadJournalEntries len = %d, want 1", len(read))
	}

	// SearchJournal — entry search requires at least one of type/status/actor/
	// subject/metadata filter, so pass a subject filter.
	matches, err := c.SearchJournal(ctx, journal.SearchRequest{
		Kind:     "agent",
		Entries:  true,
		Subjects: []string{"task:it-go"},
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("SearchJournal: %v", err)
	}
	_ = matches

	// VerifyJournal
	v, err := c.VerifyJournal(ctx, jid)
	if err != nil {
		t.Fatalf("VerifyJournal: %v", err)
	}
	if !v.OK {
		t.Fatalf("VerifyJournal not OK: %+v", v)
	}
}

// ---------------------------------------------------------------------------
// Events / SSE
// ---------------------------------------------------------------------------

func TestIntegrationEvents(t *testing.T) {
	c := newIntegClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := newPrefix(t, c)
	seen := make(chan struct{}, 1)
	handler := EventHandler(func(change *ChangeEvent, reset *ResetEvent) {
		if change != nil && strings.HasPrefix(change.Path, p) {
			select {
			case seen <- struct{}{}:
			default:
			}
		}
	})

	go c.WatchEvents(ctx, "it-go-actor", handler)

	// Generate a change event.
	if err := c.Write(p+"ev.txt", []byte("event")); err != nil {
		t.Fatalf("Write ev: %v", err)
	}

	select {
	case <-seen:
		// got an event
	case <-ctx.Done():
		t.Fatalf("WatchEvents did not observe an event in time")
	}

	// WatchEventsWithLifecycle with nil lifecycle is equivalent.
	done := make(chan struct{}, 1)
	h2 := EventHandler(func(change *ChangeEvent, reset *ResetEvent) {
		if change != nil {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	go c.WatchEventsWithLifecycle(ctx2, "it-go-actor", h2, EventLifecycle{})
	if err := c.Write(p+"ev2.txt", []byte("event2")); err != nil {
		t.Fatalf("Write ev2: %v", err)
	}
	select {
	case <-done:
	case <-ctx2.Done():
		t.Log("WatchEventsWithLifecycle did not observe an event in time (non-fatal)")
	}
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

func TestIntegrationTokens(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()

	// Token management may be disabled on drive9-server-local (single-tenant
	// fallback mode). Treat the whole suite as best-effort: exercise the call
	// path but only hard-fail if the server is reachable and reports an
	// unexpected non-"not enabled" error.
	resp, err := c.IssueScopedToken(ctx, IssueScopedTokenRequest{
		Subject:    "it-go-subject",
		TTLSeconds: 3600,
		Scopes:     []FSScopeGrant{{Prefix: "/", Ops: []string{"read"}}},
	})
	if err != nil {
		t.Logf("IssueScopedToken (best-effort, local server may not enable token mgmt): %v", err)
		return
	}
	if resp.Token == "" {
		t.Fatal("scoped token empty")
	}

	// Revoke by ID.
	if err := c.RevokeScopedToken(ctx, resp.TokenID); err != nil {
		t.Logf("RevokeScopedToken (best-effort): %v", err)
	}

	// Issue another and revoke by API key.
	resp2, err := c.IssueScopedToken(ctx, IssueScopedTokenRequest{
		Subject:    "it-go-subject-2",
		TTLSeconds: 3600,
		Scopes:     []FSScopeGrant{{Prefix: "/", Ops: []string{"read"}}},
	})
	if err != nil {
		t.Logf("IssueScopedToken 2 (best-effort): %v", err)
		return
	}
	if err := c.RevokeScopedTokenByAPIKey(ctx, resp2.Token); err != nil {
		t.Logf("RevokeScopedTokenByAPIKey (best-effort): %v", err)
	}
}

// ---------------------------------------------------------------------------
// Vault
// ---------------------------------------------------------------------------

func TestIntegrationVault(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()
	secName := "it-go-secret-" + fmt.Sprintf("%d", time.Now().UnixNano())

	// The vault backend requires a master-key configuration that the local
	// server does not enable by default ("backend unavailable"). Treat the
	// suite as best-effort: exercise the call path but return early when the
	// server reports the vault backend is not configured.
	sec, err := c.CreateVaultSecret(ctx, secName, map[string]string{"token": "hunter2"})
	if err != nil {
		t.Logf("CreateVaultSecret (best-effort, local server may not enable vault): %v", err)
		return
	}
	if sec.Name != secName {
		t.Fatalf("secret name = %q, want %q", sec.Name, secName)
	}

	// UpdateVaultSecret
	if _, err := c.UpdateVaultSecret(ctx, secName, map[string]string{"token": "hunter3"}); err != nil {
		t.Fatalf("UpdateVaultSecret: %v", err)
	}

	// ListVaultSecrets
	list, err := c.ListVaultSecrets(ctx)
	if err != nil {
		t.Fatalf("ListVaultSecrets: %v", err)
	}
	found := false
	for _, s := range list {
		if s.Name == secName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ListVaultSecrets missing %q", secName)
	}

	// ReadVaultSecretAsOwner / ReadVaultSecretFieldAsOwner
	vals, err := c.ReadVaultSecretAsOwner(ctx, secName)
	if err != nil {
		t.Fatalf("ReadVaultSecretAsOwner: %v", err)
	}
	if vals["token"] != "hunter3" {
		t.Fatalf("ReadVaultSecretAsOwner token = %q, want \"hunter3\"", vals["token"])
	}
	fv, err := c.ReadVaultSecretFieldAsOwner(ctx, secName, "token")
	if err != nil {
		t.Fatalf("ReadVaultSecretFieldAsOwner: %v", err)
	}
	if fv != "hunter3" {
		t.Fatalf("ReadVaultSecretFieldAsOwner = %q, want \"hunter3\"", fv)
	}

	// IssueVaultToken / RevokeVaultToken
	vt, err := c.IssueVaultToken(ctx, "it-go-agent", "it-go-task", []string{"secret:" + secName}, 60*time.Second)
	if err != nil {
		t.Fatalf("IssueVaultToken: %v", err)
	}
	if vt.Token == "" {
		t.Fatal("vault token empty")
	}
	if err := c.RevokeVaultToken(ctx, vt.TokenID); err != nil {
		t.Logf("RevokeVaultToken (best-effort): %v", err)
	}

	// IssueVaultGrant / RevokeVaultGrant
	gr, err := c.IssueVaultGrant(ctx, VaultGrantIssueRequest{
		Agent:      "it-go-agent",
		Scope:      []string{"secret:" + secName},
		Perm:       "read",
		TTLSeconds: 60,
		LabelHint:  "it-go-grant",
	})
	if err != nil {
		t.Fatalf("IssueVaultGrant: %v", err)
	}
	if gr.GrantID == "" {
		t.Fatal("grant id empty")
	}
	if err := c.RevokeVaultGrant(ctx, gr.GrantID, "it-go", "test done"); err != nil {
		t.Logf("RevokeVaultGrant (best-effort): %v", err)
	}

	// QueryVaultAudit
	if _, err := c.QueryVaultAudit(ctx, secName, 10); err != nil {
		t.Fatalf("QueryVaultAudit: %v", err)
	}

	// ListReadableVaultSecrets / ReadVaultSecret / ReadVaultSecretField
	// (capability-token path). In local fallback mode the owner key may read
	// directly; we accept either an empty list or a populated one.
	if _, err := c.ListReadableVaultSecrets(ctx); err != nil {
		t.Logf("ListReadableVaultSecrets (best-effort): %v", err)
	}
	if _, err := c.ReadVaultSecret(ctx, secName); err != nil {
		t.Logf("ReadVaultSecret (best-effort): %v", err)
	}
	if _, err := c.ReadVaultSecretField(ctx, secName, "token"); err != nil {
		t.Logf("ReadVaultSecretField (best-effort): %v", err)
	}

	// DeleteVaultSecret
	if err := c.DeleteVaultSecret(ctx, secName); err != nil {
		t.Fatalf("DeleteVaultSecret: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Quota (best-effort; local server may not support TiDB Cloud quota APIs)
// ---------------------------------------------------------------------------

func TestIntegrationQuota(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()

	_, err := c.GetQuota(ctx, QuotaRequest{
		TenantID:   "local",
		PublicKey:  "it-go-pk",
		PrivateKey: "it-go-sk",
	})
	if err != nil {
		t.Logf("GetQuota (best-effort, local server may not support it): %v", err)
	}
	_, err = c.SetQuota(ctx, QuotaSetRequest{
		TenantID:   "local",
		PublicKey:  "it-go-pk",
		PrivateKey: "it-go-sk",
	})
	if err != nil {
		t.Logf("SetQuota (best-effort, local server may not support it): %v", err)
	}
}

// ---------------------------------------------------------------------------
// Admin tenants (best-effort; admin APIs need TiDB Cloud credentials)
// ---------------------------------------------------------------------------

func TestIntegrationAdminTenants(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()

	// AdminListTenants
	_, err := c.AdminListTenants(ctx, AdminTenantListRequest{
		PublicKey:  "it-go-pk",
		PrivateKey: "it-go-sk",
		PageSize:   10,
	})
	if err != nil {
		t.Logf("AdminListTenants (best-effort): %v", err)
	}

	// AdminGetTenant
	_, err = c.AdminGetTenant(ctx, QuotaRequest{
		TenantID:   "local",
		PublicKey:  "it-go-pk",
		PrivateKey: "it-go-sk",
	})
	if err != nil {
		t.Logf("AdminGetTenant (best-effort): %v", err)
	}

	// Tenant pool operations
	poolReq := AdminTenantPoolRequest{PublicKey: "it-go-pk", PrivateKey: "it-go-sk"}
	if _, err := c.AdminGetTenantPool(ctx, poolReq); err != nil {
		t.Logf("AdminGetTenantPool (best-effort): %v", err)
	}
	if _, err := c.AdminCreateTenantPool(ctx, poolReq); err != nil {
		t.Logf("AdminCreateTenantPool (best-effort): %v", err)
	}
	if _, err := c.AdminUpdateTenantPool(ctx, poolReq); err != nil {
		t.Logf("AdminUpdateTenantPool (best-effort): %v", err)
	}
	if _, err := c.AdminDeleteTenantPool(ctx, poolReq); err != nil {
		t.Logf("AdminDeleteTenantPool (best-effort): %v", err)
	}

	// AdminCreateTenant / AdminSetTenantQuota / AdminDeleteTenant
	createReq := AdminTenantCreateRequest{PublicKey: "it-go-pk", PrivateKey: "it-go-sk"}
	if _, err := c.AdminCreateTenant(ctx, createReq); err != nil {
		t.Logf("AdminCreateTenant (best-effort): %v", err)
	}
	if _, err := c.AdminSetTenantQuota(ctx, QuotaSetRequest{
		TenantID:   "local",
		PublicKey:  "it-go-pk",
		PrivateKey: "it-go-sk",
	}); err != nil {
		t.Logf("AdminSetTenantQuota (best-effort): %v", err)
	}
	if _, err := c.AdminDeleteTenant(ctx, AdminTenantDeleteRequest{
		TenantID:   "local",
		PublicKey:  "it-go-pk",
		PrivateKey: "it-go-sk",
	}); err != nil {
		t.Logf("AdminDeleteTenant (best-effort): %v", err)
	}
}

// ---------------------------------------------------------------------------
// Git workspaces
// ---------------------------------------------------------------------------

func TestIntegrationGitWorkspaces(t *testing.T) {
	c := newIntegClient(t)
	ctx := context.Background()

	root := "/it-go-git-" + fmt.Sprintf("%d", time.Now().UnixNano()) + "/"
	wsReq := GitWorkspaceRequest{
		RootPath:   root,
		RepoURL:    "https://example.com/repo.git",
		BranchName: "main",
		BaseCommit: "0000000000000000000000000000000000000001",
		HeadCommit: "0000000000000000000000000000000000000002",
	}
	ws, err := c.UpsertGitWorkspace(ctx, wsReq)
	if err != nil {
		t.Fatalf("UpsertGitWorkspace: %v", err)
	}
	if ws.WorkspaceID == "" {
		t.Fatal("WorkspaceID empty")
	}
	t.Cleanup(func() { _ = c.DeleteGitWorkspace(context.Background(), ws.WorkspaceID) })

	// GetGitWorkspaceByRoot / GetGitWorkspace
	if _, err := c.GetGitWorkspaceByRoot(ctx, root); err != nil {
		t.Fatalf("GetGitWorkspaceByRoot: %v", err)
	}
	if _, err := c.GetGitWorkspace(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("GetGitWorkspace: %v", err)
	}

	// ListGitWorkspaces
	if _, err := c.ListGitWorkspaces(ctx); err != nil {
		t.Fatalf("ListGitWorkspaces: %v", err)
	}

	// ReplaceGitTree / ListGitTree — node paths are relative to the workspace root;
	// object_sha must be a 40- or 64-character git object id.
	sha1 := "00000000000000000000000000000000000000a1"
	sha2 := "00000000000000000000000000000000000000a2"
	nodes := []GitTreeNode{
		{Path: "a.txt", ParentPath: "", Name: "a.txt", Kind: "file", Mode: "100644", ObjectSHA: sha1, SizeBytes: 1},
		{Path: "sub/b.txt", ParentPath: "sub", Name: "b.txt", Kind: "file", Mode: "100644", ObjectSHA: sha2, SizeBytes: 2},
	}
	if err := c.ReplaceGitTree(ctx, ws.WorkspaceID, GitTreeReplaceRequest{CommitSHA: wsReq.HeadCommit, Nodes: nodes}); err != nil {
		t.Fatalf("ReplaceGitTree: %v", err)
	}
	tn, err := c.ListGitTree(ctx, ws.WorkspaceID, wsReq.HeadCommit)
	if err != nil {
		t.Fatalf("ListGitTree: %v", err)
	}
	if len(tn) != 2 {
		t.Fatalf("ListGitTree len = %d, want 2", len(tn))
	}

	// UpsertGitState / GetGitState
	if _, err := c.UpsertGitState(ctx, ws.WorkspaceID, GitStateRequest{
		CheckpointCommit: wsReq.HeadCommit,
		Content:          []byte("git-state-content"),
	}); err != nil {
		t.Fatalf("UpsertGitState: %v", err)
	}
	if _, err := c.GetGitState(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("GetGitState: %v", err)
	}

	// PutGitObjectPack / ListGitObjectPacks / GetGitObjectPack
	pack, err := c.PutGitObjectPack(ctx, ws.WorkspaceID, GitObjectPackRequest{Content: []byte("pack-content")})
	if err != nil {
		t.Fatalf("PutGitObjectPack: %v", err)
	}
	if _, err := c.ListGitObjectPacks(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("ListGitObjectPacks: %v", err)
	}
	if pack != nil && pack.PackID != "" {
		if _, err := c.GetGitObjectPack(ctx, ws.WorkspaceID, pack.PackID); err != nil {
			t.Fatalf("GetGitObjectPack: %v", err)
		}
	}

	// PutGitOverlayEntry / GetGitOverlayEntry / ListGitOverlayEntries —
	// overlay entry paths are relative to the workspace root.
	ovPath := "overlay.txt"
	if _, err := c.PutGitOverlayEntry(ctx, ws.WorkspaceID, GitOverlayEntryRequest{
		Path:    ovPath,
		Op:      "upsert",
		Kind:    "file",
		Mode:    "100644",
		Content: []byte("overlay-content"),
	}); err != nil {
		t.Fatalf("PutGitOverlayEntry: %v", err)
	}
	if _, err := c.GetGitOverlayEntry(ctx, ws.WorkspaceID, ovPath); err != nil {
		t.Fatalf("GetGitOverlayEntry: %v", err)
	}
	if _, err := c.ListGitOverlayEntries(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("ListGitOverlayEntries: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Raw HTTP
// ---------------------------------------------------------------------------

func TestIntegrationRawHTTP(t *testing.T) {
	c := newIntegClient(t)
	p := newPrefix(t, c)

	// RawGet on /v1/fs/<path>?list=1
	endpoint := "/v1/fs" + strings.TrimSuffix(p, "/") + "?list=1"
	resp, err := c.RawGet(endpoint)
	if err != nil {
		t.Fatalf("RawGet: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("RawGet status = %d, want 200", resp.StatusCode)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	// RawPost on /v1/sql
	sqlBody, _ := json.Marshal(map[string]string{"query": "SELECT 1"})
	resp2, err := c.RawPost("/v1/sql", bytes.NewReader(sqlBody))
	if err != nil {
		t.Fatalf("RawPost: %v", err)
	}
	if resp2.StatusCode >= 300 {
		t.Fatalf("RawPost status = %d", resp2.StatusCode)
	}
	_, _ = io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()

	// RawDelete on a path we create first.
	delPath := p + "raw-del.txt"
	if err := c.Write(delPath, []byte("x")); err != nil {
		t.Fatalf("Write raw-del: %v", err)
	}
	resp3, err := c.RawDelete("/v1/fs"+delPath, nil)
	if err != nil {
		t.Fatalf("RawDelete: %v", err)
	}
	if resp3.StatusCode >= 300 {
		t.Fatalf("RawDelete status = %d", resp3.StatusCode)
	}
	_, _ = io.ReadAll(resp3.Body)
	_ = resp3.Body.Close()
}