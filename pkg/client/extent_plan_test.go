package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func writeExtentTestPlan(w http.ResponseWriter, plan extentReadPlan, etag string) {
	w.Header().Set("ETag", etag)
	w.Header().Set("X-Dat9-Content-Layout", "extent")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusPartialContent)
	_ = json.NewEncoder(w).Encode(plan)
}

func TestExtentPlanReadAcrossPagesAndRanges(t *testing.T) {
	for _, ranged := range []bool{false, true} {
		t.Run(fmt.Sprint(ranged), func(t *testing.T) {
			var objectCalls, planCalls atomic.Int32
			object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				objectCalls.Add(1)
				for _, h := range []string{"Authorization", "X-API-Key", "Cookie", "X-Dat9-Actor", "X-Drive9-Work-ID", "X-Drive9-Fence"} {
					if r.Header.Get(h) != "" {
						t.Errorf("object request leaked %s", h)
					}
				}
				if r.Header.Get("Range") != "bytes=2-3" {
					t.Errorf("object range %q", r.Header.Get("Range"))
				}
				w.Header().Set("Content-Range", "bytes 2-3/4")
				w.WriteHeader(http.StatusPartialContent)
				if r.URL.Path == "/a" {
					_, _ = io.WriteString(w, "AB")
				} else {
					_, _ = io.WriteString(w, "CD")
				}
			}))
			defer object.Close()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				planCalls.Add(1)
				if r.URL.Path != "/v1/fs/a% file" || r.Header.Get(extentReadPlanHeader) != "1" || r.Header.Get("Authorization") != "Bearer test-token" {
					t.Errorf("wrong plan request: %s", r.URL.Path)
				}
				if r.Header.Get("Range") == "bytes=2-5" {
					if !ranged && r.Header.Get("If-Match") != `"inode-rev1"` {
						t.Error("continuation not pinned to first version")
					}
					writeExtentTestPlan(w, extentReadPlan{Size: 6, Blocks: []extentPlanBlock{
						{FileOffset: 2, Length: 2, Zero: true},
						{FileOffset: 4, Length: 2, BlockOffset: 2, URL: object.URL + "/b"},
					}}, `"inode-rev1"`)
					return
				}
				writeExtentTestPlan(w, extentReadPlan{Size: 6, Blocks: []extentPlanBlock{{Length: 2, BlockOffset: 2, URL: object.URL + "/a"}}, NextOffset: 2}, `"inode-rev1"`)
			}))
			defer api.Close()
			c := New(api.URL, "test-token")
			jar, err := cookiejar.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			u, _ := url.Parse(object.URL)
			jar.SetCookies(u, []*http.Cookie{{Name: "private-session", Value: "test-cookie"}})
			c.httpClient.Jar = jar
			var data []byte
			if ranged {
				data, err = c.ReadAtCtx(context.Background(), "/a% file", 2, 4)
			} else {
				data, err = c.ReadCtx(context.Background(), "/a% file")
			}
			want, calls := "AB\x00\x00CD", int32(2)
			if ranged {
				want, calls = "\x00\x00CD", 1
			}
			if err != nil || string(data) != want || planCalls.Load() != calls || objectCalls.Load() != calls {
				t.Fatalf("data=%q err=%v plans=%d objects=%d", data, err, planCalls.Load(), objectCalls.Load())
			}
		})
	}
}

func TestExtentPlanRejectsIncompleteOrChangingResponse(t *testing.T) {
	for _, failure := range []string{"truncated", "trailing", "oversize", "weak", "missing blocks", "etag", "size", "layout", "http", "stalled"} {
		t.Run(failure, func(t *testing.T) {
			var calls atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				plan := extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 2, Zero: true}}, NextOffset: 2}
				etag := `"original"`
				if n == 1 {
					switch failure {
					case "truncated":
						w.Header().Set("Content-Length", "9999")
					case "weak":
						etag = `W/"original"`
					case "missing blocks":
						plan = extentReadPlan{}
					}
				} else {
					if r.Header.Get("If-Match") != etag || r.Header.Get("Range") != "bytes=2-3" {
						t.Error("missing continuation validator/range")
					}
					plan = extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{FileOffset: 2, Length: 2, Zero: true}}}
					switch failure {
					case "etag":
						etag = `"new"`
					case "size":
						plan.Size++
					case "layout":
						w.WriteHeader(200)
						_, _ = io.WriteString(w, "new body")
						return
					case "http":
						w.WriteHeader(412)
						return
					case "stalled":
						plan.Blocks = []extentPlanBlock{}
						plan.NextOffset = 2
					}
				}
				writeExtentTestPlan(w, plan, etag)
				if failure == "trailing" {
					_, _ = io.WriteString(w, `{}`)
				}
				if failure == "oversize" {
					_, _ = io.WriteString(w, strings.Repeat(" ", 4<<20))
				}
			}))
			defer api.Close()
			_, err := New(api.URL, "test-token").ReadCtx(context.Background(), "/a")
			if err == nil || calls.Load() > 2 {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestExtentPlanRejectsInvalidWindows(t *testing.T) {
	for _, tc := range []struct {
		name        string
		plan        extentReadPlan
		offset, end int64
	}{
		{"gap", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{FileOffset: 1, Length: 3, Zero: true}}}, 0, 4},
		{"overlap", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 2, Zero: true}, {FileOffset: 1, Length: 3, Zero: true}}}, 0, 4},
		{"too long", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 5, Zero: true}}}, 0, 4},
		{"zero length", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Zero: true}}}, 0, 4},
		{"negative offset", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 4, BlockOffset: -1, URL: "https://objects.invalid/x"}}}, 0, 4},
		{"overflow", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 4, BlockOffset: math.MaxInt64, URL: "https://objects.invalid/x"}}}, 0, 4},
		{"zero URL", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 4, Zero: true, URL: "https://objects.invalid/x"}}}, 0, 4},
		{"missing URL", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 4}}}, 0, 4},
		{"credentials", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 4, URL: "http://user:pass@objects.invalid/x"}}}, 0, 4},
		{"file URL", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 4, URL: "file:///secret"}}}, 0, 4},
		{"extra continuation", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 4, Zero: true}}, NextOffset: 4}, 0, 4},
		{"uncovered", extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 2, Zero: true}}}, 0, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateExtentPlan(tc.plan, tc.offset, tc.end); err == nil {
				t.Fatal("accepted invalid plan")
			}
		})
	}
	if err := validateExtentPlan(extentReadPlan{Size: math.MaxInt64, Blocks: []extentPlanBlock{{FileOffset: math.MaxInt64 - 1, Length: 1, Zero: true}}}, math.MaxInt64-1, math.MaxInt64); err != nil {
		t.Fatal(err)
	}
}

func TestExtentPlanObjectFailuresAndClose(t *testing.T) {
	for _, failure := range []string{"redirect", "full", "range", "short", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			started, canceled := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/block" {
					t.Error("followed object redirect")
				}
				switch failure {
				case "redirect":
					w.Header().Set("Location", "/redirected")
					w.WriteHeader(307)
					return
				case "full":
					w.WriteHeader(200)
					_, _ = io.WriteString(w, "data")
					return
				case "cancel":
					close(started)
					<-r.Context().Done()
					close(canceled)
					return
				}
				w.Header().Set("Content-Range", "bytes 0-3/4")
				if failure == "range" {
					w.Header().Set("Content-Range", "bytes 1-4/5")
				}
				w.Header().Set("Content-Length", "4")
				w.WriteHeader(206)
				_, _ = io.WriteString(w, "da")
			}))
			defer object.Close()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeExtentTestPlan(w, extentReadPlan{Size: 4, Blocks: []extentPlanBlock{{Length: 4, URL: object.URL + "/block?signature=test"}}}, `"version"`)
			}))
			defer api.Close()
			r, err := New(api.URL, "test-token").ReadStream(context.Background(), "/a")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close() }()
			if failure == "cancel" {
				select {
				case <-started:
				case <-time.After(2 * time.Second):
					t.Fatal("object fetch not started")
				}
				_ = r.Close()
				select {
				case <-canceled:
				case <-time.After(2 * time.Second):
					t.Fatal("close did not cancel object request")
				}
			} else {
				_, err = io.ReadAll(r)
				if err == nil || strings.Contains(err.Error(), "signature=") || calls.Load() != 1 {
					t.Fatalf("err=%v calls=%d", err, calls.Load())
				}
				if failure == "short" && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("short body: %v", err)
				}
			}
		})
	}
}

func TestExtentPlanKeepsFixedViewJSONAsBytes(t *testing.T) {
	body := `{"size":0,"blocks":[]}`
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(extentReadPlanHeader) != "" || r.URL.Path != "/v1/views/a/v1/fs/file" {
			t.Error("incorrect fixed route or negotiation", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Dat9-Content-Layout", "extent")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
		w.WriteHeader(206)
		_, _ = io.WriteString(w, body)
	}))
	defer api.Close()
	c, err := New(api.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadAtCtx(context.Background(), "/file", 0, int64(len(body)))
	if err != nil || string(got) != body {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestExtentPlanEmptyClampedAndLargeOffsets(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		size, offset, length int64
		want                 int
	}{
		{"empty", 0, 0, -1, 0},
		{"clamped", 6, 4, 20, 2},
		{"chunk boundary", (64 << 20) + 3, (64 << 20) - 2, 5, 5},
		{"largest offset", math.MaxInt64, math.MaxInt64 - 1, 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				blocks := []extentPlanBlock{}
				if tc.want > 0 {
					blocks = append(blocks, extentPlanBlock{FileOffset: tc.offset, Length: int64(tc.want), Zero: true})
				}
				writeExtentTestPlan(w, extentReadPlan{Size: tc.size, Blocks: blocks}, `"version"`)
			}))
			defer api.Close()
			c := New(api.URL, "test-token")
			var data []byte
			var err error
			if tc.length < 0 {
				data, err = c.ReadCtx(context.Background(), "/a")
			} else {
				data, err = c.ReadAtCtx(context.Background(), "/a", tc.offset, tc.length)
			}
			if err != nil || len(data) != tc.want {
				t.Fatalf("length=%d err=%v", len(data), err)
			}
		})
	}
}

func TestExtentPlanCancellationUnblocksZeroStream(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeExtentTestPlan(w, extentReadPlan{Size: math.MaxInt64, Blocks: []extentPlanBlock{{Length: math.MaxInt64, Zero: true}}}, `"version"`)
	}))
	defer api.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := New(api.URL, "test-token").ReadStream(ctx, "/a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	cancel()
	done := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, r); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled stream: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("context cancellation did not stop a zero stream")
	}
}

func TestExtentPlanStringInodeProbeFallsBackToStreaming(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(extentReadPlanHeader) != "" {
			t.Error("single object probe requested a plan")
		}
		w.Header().Set("X-Dat9-Content-Layout", "extent")
		w.Header().Set("X-Dat9-Extent-Inode", "classic-string-inode")
		w.WriteHeader(422)
	}))
	defer api.Close()
	if _, err := New(api.URL, "test-token").ResolveReadTarget(context.Background(), "/a"); !IsReadTargetNoRedirect(err) {
		t.Fatalf("probe: %v", err)
	}
}
