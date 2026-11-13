package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestCreateModeUsesAtomicReceipt(t *testing.T) {
	for _, mode := range []uint32{0, 0o600, 0o755, 0o644} {
		for _, concurrentWrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode_%o/concurrent_%t", mode, concurrentWrite), func(t *testing.T) {
				var mu sync.Mutex
				var revision int64
				var storedMode uint32
				var data string
				var batchCalls, creates, chmods, stats int
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					switch {
					case r.URL.Path == "/v1/status":
						_, _ = w.Write([]byte(`{"storage_capabilities":{"batch_write_mode_v1":true}}`))
					case r.URL.Path == "/v1/fs:batch-write":
						batchCalls++
						var body struct {
							Items []client.BatchWriteItem `json:"items"`
						}
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Items) != 1 {
							t.Errorf("invalid batch: %+v %v", body, err)
							w.WriteHeader(400)
							return
						}
						item := body.Items[0]
						if item.Path != "/file" || item.ExpectedRevision != 0 || !item.HasMode || item.Mode != mode || len(item.Data) != 0 {
							t.Errorf("wrong create request: %+v", item)
						}
						revision, storedMode = 1, item.Mode
						if concurrentWrite {
							revision, data = 2, "foreign"
						}
						_, _ = w.Write([]byte(`{"results":[{"path":"/file","status":200,"revision":1}]}`))
					case r.URL.Query().Has("create"):
						creates++
						revision, storedMode = 1, 0o644
						if concurrentWrite {
							revision, data = 2, "foreign"
						}
						_, _ = w.Write([]byte(`{"revision":1}`))
					case r.URL.Query().Has("chmod"):
						chmods++
						w.WriteHeader(500)
					case r.Method == http.MethodHead:
						stats++
						w.WriteHeader(500)
					case r.Method == http.MethodPut:
						if r.Header.Get("X-Dat9-Expected-Revision") != strconv.FormatInt(revision, 10) {
							w.WriteHeader(http.StatusConflict)
							return
						}
						data, revision = "ours", revision+1
						_, _ = fmt.Fprintf(w, `{"revision":%d}`, revision)
					default:
						t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
						w.WriteHeader(500)
					}
				}))
				defer srv.Close()
				cl := client.New(srv.URL, "")
				cl.Warm(t.Context())
				c, err := New(cl, Options{})
				if err != nil {
					t.Fatal(err)
				}
				created, err := c.createRemoteFile(t.Context(), &fs.Op{Mode: mode, Flags: syscall.O_CREAT | syscall.O_EXCL}, "/file")
				if err != nil || created == nil || created.Revision != 1 || created.Mode != mode {
					t.Fatalf("creation receipt: %+v %v", created, err)
				}
				err = cl.WriteCtxConditional(t.Context(), "/file", []byte("ours"), created.Revision)
				if concurrentWrite && !errors.Is(err, client.ErrConflict) || !concurrentWrite && err != nil {
					t.Fatalf("follow-up write: %v", err)
				}
				mu.Lock()
				defer mu.Unlock()
				if storedMode != mode || chmods != 0 || stats != 0 || batchCalls+creates != 1 {
					t.Fatalf("mode=%o batch=%d create=%d chmod=%d stat=%d", storedMode, batchCalls, creates, chmods, stats)
				}
				if (mode == 0o644) != (creates == 1) {
					t.Fatal("default creation path changed")
				}
				if concurrentWrite && data != "foreign" {
					t.Fatal("overwrote concurrent writer")
				}
			})
		}
	}
}

func TestCreateModeFailureDoesNotFallback(t *testing.T) {
	for _, response := range []string{
		`{"results":[{"path":"/file","status":403,"error":"owner only"}]}`,
		`{"results":[{"path":"/file","status":409,"error":"exists"}]}`,
		`{"results":[{"path":"/file","status":500,"revision":1,"error":"settlement failed"}]}`,
		`{"results":[{"path":"/file","status":200}]}`,
		`{"results":[{"path":"/other","status":200,"revision":1}]}`,
		`{"results":`,
	} {
		t.Run(response, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					_, _ = w.Write([]byte(`{"storage_capabilities":{"batch_write_mode_v1":true}}`))
					return
				}
				calls++
				if r.URL.Path != "/v1/fs:batch-write" {
					t.Errorf("unexpected fallback: %s", r.URL.Path)
				}
				_, _ = w.Write([]byte(response))
			}))
			defer srv.Close()
			cl := client.New(srv.URL, "")
			cl.Warm(t.Context())
			c, err := New(cl, Options{})
			if err != nil {
				t.Fatal(err)
			}
			rev, applied, err := c.createEmptyRemote(t.Context(), "/file", 0)
			if err == nil || rev != 0 || applied || calls != 1 {
				t.Fatalf("rev=%d applied=%t error=%v calls=%d", rev, applied, err, calls)
			}
		})
	}
}

func TestCreateModeKeepsUnadvertisedServerPath(t *testing.T) {
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"storage_capabilities":{}}`))
		case r.URL.Query().Has("create"):
			calls = append(calls, "create")
			_, _ = w.Write([]byte(`{"revision":1}`))
		case r.URL.Query().Has("chmod"):
			calls = append(calls, "chmod")
			var body struct {
				Mode *uint32 `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Mode == nil || *body.Mode != 0 {
				t.Errorf("zero mode lost: %+v %v", body, err)
			}
		default:
			t.Errorf("unexpected request: %s", r.URL.String())
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	cl := client.New(srv.URL, "")
	cl.Warm(t.Context())
	c, err := New(cl, Options{})
	if err != nil {
		t.Fatal(err)
	}
	created, err := c.createRemoteFile(t.Context(), &fs.Op{Mode: 0}, "/file")
	if err != nil || created == nil || created.Mode != 0 || created.Revision != 1 || fmt.Sprint(calls) != "[create chmod]" {
		t.Fatalf("legacy creation: %+v %v calls=%v", created, err, calls)
	}
}
