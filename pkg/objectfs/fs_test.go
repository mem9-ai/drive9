package objectfs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
)

func testAdapter(t *testing.T) (*FS, string) {
	t.Helper()
	ensureRclone()
	dir := t.TempDir()
	f, err := fs.NewFs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	return &FS{
		f: f,
		loc: Location{
			Scheme: SchemeS3,
			Bucket: "bucket",
			Query:  map[string]string{},
		},
	}, dir
}

func TestAdapterRoundTrip(t *testing.T) {
	a, _ := testAdapter(t)
	ctx := context.Background()
	dst := Location{Path: "dir/hello.txt"}
	wh, err := a.OpenWrite(ctx, dst, WriteOpts{Size: 5, Overwrite: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wh.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := wh.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := a.Stat(ctx, dst)
	if err != nil {
		t.Fatal(err)
	}
	if st.IsDir || st.Size != 5 || st.Name != "hello.txt" {
		t.Fatalf("stat = %+v", st)
	}

	page, err := a.List(ctx, Location{Path: "dir"}, ListOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Name != "hello.txt" {
		t.Fatalf("list = %+v", page.Entries)
	}
	if _, err := a.List(ctx, Location{Path: "dir", Raw: "s3://bucket/dir"}, ListOpts{Cursor: "x"}); err == nil {
		t.Fatal("expected cursor error")
	}

	rc, err := a.OpenRead(ctx, dst)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(body) != "hello" {
		t.Fatalf("read %q err=%v", body, err)
	}

	copyDst := Location{Path: "dir/copy.txt"}
	if err := a.Copy(ctx, dst, copyDst); err != nil {
		t.Fatal(err)
	}
	if err := a.Rename(ctx, copyDst, Location{Path: "dir/moved.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Stat(ctx, copyDst); err == nil {
		t.Fatal("renamed source still present")
	}
	if err := a.Remove(ctx, Location{Path: "dir/moved.txt"}, false); err != nil {
		t.Fatal(err)
	}
}

func TestAdapterWriteAbort(t *testing.T) {
	a, dir := testAdapter(t)
	ctx := context.Background()
	loc := Location{Path: "abort.bin"}
	wh, err := a.OpenWrite(ctx, loc, WriteOpts{Size: -1})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = wh.Write([]byte("partial"))
	if err := wh.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "abort.bin"))
	if len(matches) != 0 {
		if _, err := os.Stat(matches[0]); err == nil {
			t.Fatalf("aborted write left %v", matches)
		}
	}
}

func TestAdapterIdentityIgnoresKey(t *testing.T) {
	a := &FS{loc: Location{
		Scheme: SchemeS3,
		Bucket: "b",
		Path:   "one",
		Query:  map[string]string{QueryRegion: "us-east-1"},
	}}
	b := &FS{loc: Location{
		Scheme: SchemeS3,
		Bucket: "b",
		Path:   "two",
		Query:  map[string]string{QueryRegion: "us-east-1"},
	}}
	if a.Identity() != b.Identity() {
		t.Fatalf("same bucket keys should share identity: %q vs %q", a.Identity(), b.Identity())
	}
	c := &FS{loc: Location{
		Scheme: SchemeS3,
		Bucket: "other",
		Query:  map[string]string{QueryRegion: "us-east-1"},
	}}
	if a.Identity() == c.Identity() {
		t.Fatal("different buckets must not share identity")
	}
}

func TestAzureIdentityIncludesAccount(t *testing.T) {
	a := &FS{loc: Location{
		Scheme: SchemeAZ,
		Bucket: "c",
		Query:  map[string]string{QueryAccount: "acct1"},
	}}
	b := &FS{loc: Location{
		Scheme: SchemeAzure,
		Bucket: "c",
		Query:  map[string]string{QueryAccount: "acct1"},
	}}
	if a.Identity() != b.Identity() {
		t.Fatalf("az/azure aliases should share identity: %q vs %q", a.Identity(), b.Identity())
	}
	other := &FS{loc: Location{
		Scheme: SchemeAZ,
		Bucket: "c",
		Query:  map[string]string{QueryAccount: "acct2"},
	}}
	if a.Identity() == other.Identity() {
		t.Fatal("different Azure accounts must not share identity")
	}
}

func TestAdapterRecursiveRemoveClearsDir(t *testing.T) {
	a, _ := testAdapter(t)
	ctx := context.Background()
	dir := Location{Path: "tree", DirHint: true, Raw: "s3://bucket/tree"}
	if err := a.Mkdir(ctx, dir); err != nil {
		t.Fatal(err)
	}
	child := Location{Path: "tree/a.txt", Raw: "s3://bucket/tree/a.txt"}
	wh, err := a.OpenWrite(ctx, child, WriteOpts{Size: 3, Overwrite: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wh.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := wh.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Remove(ctx, dir, true); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Stat(ctx, dir); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stat after rm -r: %v", err)
	}
}

func TestAdapterRecursiveRemoveFile(t *testing.T) {
	a, _ := testAdapter(t)
	ctx := context.Background()
	loc := Location{Path: "only.txt", Raw: "s3://bucket/only.txt"}
	wh, err := a.OpenWrite(ctx, loc, WriteOpts{Size: 3, Overwrite: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wh.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := wh.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Remove(ctx, loc, true); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Stat(ctx, loc); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stat after recursive rm of file: %v", err)
	}
}

func TestAdapterRenameSameRemoteIsNoop(t *testing.T) {
	a, _ := testAdapter(t)
	ctx := context.Background()
	loc := Location{Path: "keep.txt", Raw: "s3://bucket/keep.txt"}
	wh, err := a.OpenWrite(ctx, loc, WriteOpts{Size: 3, Overwrite: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wh.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := wh.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Rename(ctx, loc, loc); err != nil {
		t.Fatal(err)
	}
	st, err := a.Stat(ctx, loc)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != 3 {
		t.Fatalf("stat after same-path rename = %+v", st)
	}
}

func TestAdapterListFile(t *testing.T) {
	a, _ := testAdapter(t)
	ctx := context.Background()
	loc := Location{Path: "listed.txt", Raw: "s3://bucket/listed.txt"}
	wh, err := a.OpenWrite(ctx, loc, WriteOpts{Size: 3, Overwrite: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wh.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := wh.Close(); err != nil {
		t.Fatal(err)
	}
	page, err := a.List(ctx, loc, ListOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Name != "listed.txt" || page.Entries[0].IsDir {
		t.Fatalf("list file = %+v", page.Entries)
	}
}

func TestAdapterRejectsVersionID(t *testing.T) {
	a, _ := testAdapter(t)
	loc := Location{Path: "x", Query: map[string]string{QueryVersionID: "v1"}}
	if _, err := a.OpenRead(context.Background(), loc); err == nil {
		t.Fatal("expected versionId error")
	}
}
