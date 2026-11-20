package cli

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedFilesystemBindingArguments(t *testing.T) {
	root := client.New("http://localhost", "test-token")
	for _, args := range [][]string{
		{"--layer", "a", "--work", "w", "--fence", "3", "/file"},
		{"/file", "--layer=a", "--work=w", "--fence=3"},
	} {
		before := append([]string(nil), args...)
		c, rest, err := BindFilesystemView(root, "cat", args)
		want := client.ViewBinding{LayerID: "a", WorkID: "w", Fence: 3}
		if err != nil || c.ViewBinding() != want || !reflect.DeepEqual(rest, []string{"/file"}) || !reflect.DeepEqual(before, args) {
			t.Fatal(c, rest, err)
		}
		b := c.ViewBinding()
		b.LayerID = "other"
		if c.ViewBinding() != want || root.ViewBinding() != (client.ViewBinding{}) {
			t.Fatal("binding mutated")
		}
	}
	for _, args := range [][]string{
		{"--layer", "a"}, {"--work", "w", "--fence", "1"}, {"--layer", ""},
		{"--layer", "main", "--validation=false"}, {"--layer", "a", "--layer", "b"},
		{"--layer", "a", "--work", "w", "--fence", "01"}, {"--layer", "a", "--work", "w", "--fence", "0"},
		{"--layer", "../a", "--work", "w", "--fence", "1"}, {"--layer", "a", "--work", "w", "--fence", "1", "--validation=no"},
	} {
		if _, _, err := BindFilesystemView(root, "cat", args); err == nil {
			t.Fatal("accepted", args)
		}
	}
	for _, command := range []string{"grep", "find", "archive", "sh", "tasks", "setmeta"} {
		if _, _, err := BindFilesystemView(root, command, []string{"--layer", "a", "--work", "w", "--fence", "1"}); err == nil {
			t.Fatal("unsupported", command)
		}
	}
	for _, args := range [][]string{{"--description", "--layer", "local", ":/remote"}, {"--", "--layer=a"}} {
		c, rest, err := BindFilesystemView(root, "cp", args)
		if err != nil || c != root || !reflect.DeepEqual(rest, args) {
			t.Fatal(c, rest, err)
		}
	}
	c, rest, err := BindFilesystemView(root, "cat", []string{"--layer=main", "/file"})
	if err != nil || c.ViewBinding() != (client.ViewBinding{}) || !reflect.DeepEqual(rest, []string{"/file"}) {
		t.Fatal(c, rest, err)
	}
}

func TestFixedFilesystemCLIUsesOrdinaryAPIsWithBinding(t *testing.T) {
	local := filepath.Join(t.TempDir(), "local.txt")
	if err := os.WriteFile(local, []byte("new bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	type command struct {
		name string
		args []string
		call func(*client.Client, []string) error
	}
	for _, tc := range []command{
		{"cat", []string{"/file"}, Cat}, {"ls", []string{"/dir"}, Ls}, {"stat", []string{"-o", "json", "/file"}, Stat},
		{"mkdir", []string{"/new"}, Mkdir}, {"rm", []string{"/file"}, Rm}, {"rm", []string{"-r", "/dir"}, Rm},
		{"chmod", []string{"640", "/file"}, Chmod}, {"mv", []string{"/file", "/target"}, Mv},
		{"hardlink", []string{"/file", "/target"}, Hardlink}, {"symlink", []string{"../file", "/target"}, Symlink},
		{"cp", []string{local, ":/new"}, Cp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls, writes atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				binding, err := viewprotocol.Parse(r.Header)
				if err != nil || binding != (client.ViewBinding{LayerID: "a", WorkID: "w", Fence: 3}) || !strings.HasPrefix(r.URL.Path, "/v1/views/a/v1/") {
					t.Errorf("wrong binding %s %+v %v", r.URL, binding, err)
					w.WriteHeader(400)
					return
				}
				if r.Method != "GET" && r.Method != "HEAD" {
					writes.Add(1)
					if _, err := viewprotocol.ParseOperationID(r.Header); err != nil {
						t.Error("missing operation", err)
					}
				}
				switch {
				case r.Method == "HEAD":
					http.NotFound(w, r)
				case r.Method == "GET" && r.URL.Query().Has("list"):
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"entries":[{"name":"file","size":3}]}`)
				case r.Method == "GET" && r.URL.Query().Has("stat"):
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"size":3,"revision":2,"resource_id":"inode"}`)
				case r.Method == "GET":
					_, _ = io.WriteString(w, "old bytes")
				case r.Method == "PUT":
					data, err := io.ReadAll(r.Body)
					if err != nil || string(data) != "new bytes" {
						t.Error(string(data), err)
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"revision":1,"resource_id":"inode","size_bytes":9}`)
				default:
					_, _ = io.WriteString(w, `{"status":"ok"}`)
				}
			}))
			defer srv.Close()
			root := client.New(srv.URL, "test-token")
			root.SetSmallFileThresholdForTests(1024)
			args := append([]string{"--layer", "a", "--work", "w", "--fence", "3"}, tc.args...)
			c, rest, err := BindFilesystemView(root, tc.name, args)
			if err != nil {
				t.Fatal(err)
			}
			_, err = captureStdoutE(t, func() error { return tc.call(c, rest) })
			if err != nil || calls.Load() == 0 {
				t.Fatal(err, calls.Load())
			}
			if tc.name != "cat" && tc.name != "ls" && tc.name != "stat" && writes.Load() == 0 {
				t.Fatal("mutation missing")
			}
		})
	}
}

func TestFixedFilesystemCannotLoseViewThroughNamedContextOrObject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected HTTP", r.URL); w.WriteHeader(500) }))
	defer srv.Close()
	c, err := client.New(srv.URL, "test-token").WithView(client.ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"other:/file", "s3://bucket/file"} {
		if _, err := fsHandleForArg(c, raw); err == nil {
			t.Fatal("accepted", raw)
		}
	}
	for _, call := range []func() error{
		func() error { return Cp(c, []string{"other:/file", ":/target"}) },
		func() error { return Mv(c, []string{"other:/file", "other:/target"}) },
		func() error { return Hardlink(c, []string{"other:/file", "other:/target"}) },
		func() error { return Symlink(c, []string{"other:/file", "other:/target"}) },
	} {
		if err := call(); err == nil {
			t.Fatal("lost binding")
		}
	}
	root := client.New(srv.URL, "test-token")
	b, _ := root.WithView(client.ViewBinding{LayerID: "b", WorkID: "w", Fence: 1})
	again, _ := root.WithView(c.ViewBinding())
	if SameIdentity(newDrive9FS(c), newDrive9FS(root)) || SameIdentity(newDrive9FS(c), newDrive9FS(b)) || !SameIdentity(newDrive9FS(c), newDrive9FS(again)) {
		t.Fatal("backend identity ignores binding")
	}
}

func TestFixedFilesystemCLIWriteFailureKeepsOperation(t *testing.T) {
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.WriteHeader(503)
		_, _ = io.WriteString(w, `{"error":"unknown result"}`)
	}))
	defer srv.Close()
	c, args, err := BindFilesystemView(client.New(srv.URL, "test-token"), "rm", []string{"--layer=a", "--work=w", "--fence=1", "-r", "/dir"})
	if err != nil {
		t.Fatal(err)
	}
	err = Rm(c, args)
	var outcome *client.ViewOperationError
	if !errors.As(err, &outcome) || outcome.LayerID != "a" || outcome.OperationID == "" || count.Load() != 1 {
		t.Fatal(err, count.Load())
	}
}

func TestBoundMutationFailureSurfacesOriginalOperation(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		call func(*client.Client, []string) error
	}{
		{"mkdir", []string{"/new"}, Mkdir}, {"rm", []string{"/file"}, Rm},
		{"chmod", []string{"640", "/file"}, Chmod}, {"mv", []string{"/file", "/target"}, Mv},
		{"hardlink", []string{"/file", "/target"}, Hardlink}, {"symlink", []string{"../file", "/target"}, Symlink},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids := make(chan string, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" || r.Method == "HEAD" {
					_, _ = io.WriteString(w, `{"size":3,"revision":2,"resource_id":"inode"}`)
					return
				}
				id, err := viewprotocol.ParseOperationID(r.Header)
				if err != nil {
					t.Error(err)
				}
				ids <- id
				// A gateway 503 does not establish whether the mutation committed.
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			c, args, err := BindFilesystemView(client.New(server.URL, "test-key"), tc.name, append([]string{"--layer=a", "--work=w", "--fence=1"}, tc.args...))
			if err != nil {
				t.Fatal(err)
			}
			_, err = captureStdoutE(t, func() error { return tc.call(c, args) })
			var outcome *client.ViewOperationError
			if !errors.As(err, &outcome) || outcome.LayerID != "a" {
				t.Fatalf("missing operation outcome: %v", err)
			}
			select {
			case id := <-ids:
				if id == "" || outcome.OperationID != id || !strings.Contains(err.Error(), id) {
					t.Fatalf("identity not printable: id=%q err=%v", id, err)
				}
			default:
				t.Fatal("no mutation sent")
			}
			if len(ids) != 0 {
				t.Fatal("automatically retried uncertain mutation")
			}
		})
	}
}
