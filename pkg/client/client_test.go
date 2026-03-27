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
	"github.com/mem9-ai/dat9/pkg/tenant"
)

func newTestClient(t *testing.T) (*Client, func()) {
	t.Helper()

	s3Dir, err := os.MkdirTemp("", "dat9-client-s3-*")
	if err != nil {
		t.Fatal(err)
	}

	if err := tenant.InitSchemaForProvider(testDSN, tenant.ProviderTiDBZero); err != nil {
		t.Fatal(err)
	}
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
