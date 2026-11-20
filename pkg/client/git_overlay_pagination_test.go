package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestGitOverlayListFollowsFullAndShortPages(t *testing.T) {
	var calls atomic.Int32
	first := make([]GitOverlayEntry, 200)
	for i := range first {
		first[i] = GitOverlayEntry{Path: fmt.Sprintf("a%03d", i)}
	}
	const opaque = "z &?# 中 "
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/git-workspaces/ws/overlay" || r.Method != http.MethodGet {
			t.Errorf("unexpected route %s %s", r.Method, r.URL.Path)
		}
		switch r.URL.Query().Get("after") {
		case "":
			_ = json.NewEncoder(w).Encode(gitOverlayPage{Entries: first, NextAfter: first[199].Path})
		case first[199].Path:
			_ = json.NewEncoder(w).Encode(gitOverlayPage{Entries: []GitOverlayEntry{{Path: opaque}}, NextAfter: opaque})
		case opaque:
			_ = json.NewEncoder(w).Encode(gitOverlayPage{Entries: []GitOverlayEntry{{Path: "zz"}}})
		default:
			t.Error("cursor was changed")
			w.WriteHeader(400)
		}
	}))
	defer ts.Close()
	entries, err := New(ts.URL, "test").ListGitOverlayEntries(context.Background(), "ws")
	if err != nil || len(entries) != 202 || calls.Load() != 3 {
		t.Fatalf("count=%d calls=%d err=%v", len(entries), calls.Load(), err)
	}
	for i := range first {
		if entries[i].Path != first[i].Path {
			t.Fatalf("lost first-page entry %d", i)
		}
	}
	if entries[200].Path != opaque || entries[201].Path != "zz" {
		t.Fatal("missing later page entries")
	}
}

func TestGitOverlayListLegacyAndEmptyResponses(t *testing.T) {
	for _, body := range []string{`{"entries":[]}`, `{"entries":[{"path":"legacy"}]}`} {
		t.Run(body, func(t *testing.T) {
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); _, _ = w.Write([]byte(body)) }))
			defer ts.Close()
			entries, err := New(ts.URL, "test").ListGitOverlayEntries(context.Background(), "ws")
			var want gitOverlayPage
			_ = json.Unmarshal([]byte(body), &want)
			if err != nil || len(entries) != len(want.Entries) || calls.Load() != 1 {
				t.Fatalf("count=%d calls=%d err=%v", len(entries), calls.Load(), err)
			}
		})
	}
}

func TestGitOverlayListDiscardsPartialResults(t *testing.T) {
	for _, mode := range []string{"status", "decode", "repeat", "backwards", "empty", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				if n == 1 {
					_ = json.NewEncoder(w).Encode(gitOverlayPage{Entries: []GitOverlayEntry{{Path: "b"}}, NextAfter: "b"})
					if mode == "cancel" {
						cancel()
					}
					return
				}
				if n > 2 {
					t.Error("pagination continued after invalid page")
					w.WriteHeader(500)
					return
				}
				switch mode {
				case "status":
					w.WriteHeader(500)
				case "decode":
					_, _ = w.Write([]byte(`{"entries":[`))
				case "repeat":
					_ = json.NewEncoder(w).Encode(gitOverlayPage{Entries: []GitOverlayEntry{{Path: "b"}}, NextAfter: "b"})
				case "backwards":
					_ = json.NewEncoder(w).Encode(gitOverlayPage{Entries: []GitOverlayEntry{{Path: "a"}}, NextAfter: "a"})
				case "empty":
					_ = json.NewEncoder(w).Encode(gitOverlayPage{NextAfter: "c"})
				default:
					t.Error("request after cancellation")
					w.WriteHeader(500)
				}
			}))
			defer ts.Close()
			entries, err := New(ts.URL, "test").ListGitOverlayEntries(ctx, "ws")
			if err == nil || entries != nil {
				t.Fatalf("partial list reported as success: count=%d err=%v", len(entries), err)
			}
		})
	}
}
