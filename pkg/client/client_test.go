package client

import (
	"net/http/httptest"
	"os"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/server"
)

func newTestClient(t *testing.T) (*Client, func()) {
	t.Helper()

	s3Dir, err := os.MkdirTemp("", "dat9-client-s3-*")
	if err != nil {
		t.Fatal(err)
	}

	initClientTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())

	s3c, err := s3client.NewLocal(s3Dir, "/s3")
	if err != nil {
		t.Fatal(err)
	}
	b, err := backend.NewWithS3(store, s3c)
	if err != nil {
		t.Fatal(err)
	}

	srv := server.New(b)
	ts := httptest.NewServer(srv)

	cleanup := func() {
		ts.Close()
		_ = store.Close()
		_ = os.RemoveAll(s3Dir)
	}

	return New(ts.URL, ""), cleanup
}
func TestWriteAndRead(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/hello.txt", []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	data, err := c.Read("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("got %q", data)
	}
}

func TestCreateMetadataOnly(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	created, err := c.Create("/meta-only.txt")
	if err != nil {
		t.Fatal(err)
	}
	if created.Path != "/meta-only.txt" {
		t.Fatalf("path=%q, want /meta-only.txt", created.Path)
	}
	if created.Revision != 1 {
		t.Fatalf("revision=%d, want 1", created.Revision)
	}
	if created.Size != 0 {
		t.Fatalf("size=%d, want 0", created.Size)
	}
	if created.Status != "CONFIRMED" {
		t.Fatalf("status=%q, want CONFIRMED", created.Status)
	}

	info, err := c.Stat("/meta-only.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Revision != 1 || info.Size != 0 || info.IsDir {
		t.Fatalf("unexpected stat after create: %+v", info)
	}
}

func TestCreateMetadataOnlyConflict(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if _, err := c.Create("/dup.txt"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Create("/dup.txt")
	if err == nil {
		t.Fatal("expected conflict on duplicate metadata create")
	}
	statusErr, ok := err.(*StatusError)
	if !ok {
		t.Fatalf("error type=%T, want *StatusError", err)
	}
	if statusErr.StatusCode != 409 {
		t.Fatalf("status=%d, want 409", statusErr.StatusCode)
	}
}

func TestListDir(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/data/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/data/b.txt", []byte("bb")); err != nil {
		t.Fatal(err)
	}

	entries, err := c.List("/data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
}

func TestStat(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/test.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}
	info, err := c.Stat("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 4 || info.IsDir {
		t.Errorf("unexpected: %+v", info)
	}
}

func TestDelete(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/del.txt", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete("/del.txt"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Read("/del.txt")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestRemoveAll(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/data"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/data/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Mkdir("/data/nested"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/data/nested/b.txt", []byte("b")); err != nil {
		t.Fatal(err)
	}

	if err := c.RemoveAll("/data/"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat("/data/"); err == nil {
		t.Fatal("expected error after recursive delete")
	}
	if _, err := c.Read("/data/a.txt"); err == nil {
		t.Fatal("expected sibling file read to fail after recursive delete")
	}
	if _, err := c.Read("/data/nested/b.txt"); err == nil {
		t.Fatal("expected nested file read to fail after recursive delete")
	}
	entries, err := c.List("/")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name == "data" || entry.Name == "data/" {
			t.Fatalf("expected removed directory to be absent from root listing, got entries %+v", entries)
		}
	}
}

func TestCopy(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/src.txt", []byte("shared")); err != nil {
		t.Fatal(err)
	}
	if err := c.Copy("/src.txt", "/dst.txt"); err != nil {
		t.Fatal(err)
	}
	data, _ := c.Read("/dst.txt")
	if string(data) != "shared" {
		t.Errorf("got %q", data)
	}
}

func TestRename(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/old.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := c.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatal(err)
	}
	data, err := c.Read("/new.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "data" {
		t.Errorf("got %q", data)
	}
	_, err = c.Read("/old.txt")
	if err == nil {
		t.Error("expected error for old path")
	}
}

func TestMkdir(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/mydir"); err != nil {
		t.Fatal(err)
	}
	info, err := c.Stat("/mydir/")
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir {
		t.Error("expected directory")
	}
}
