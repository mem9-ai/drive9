package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedExtentPresignBindsPartAndRejectsIncompleteResponse(t *testing.T) {
	key := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name   string
		alter  func(*PartURL)
		status int
		tail   string
		short  bool
		valid  bool
	}{
		{name: "signed", status: 200, valid: true},
		{name: "dedup", status: 200, valid: true, alter: func(p *PartURL) { p.SkipUpload = true; p.URL = ""; p.Headers = nil }},
		{name: "wrong part", status: 200, alter: func(p *PartURL) { p.Number++ }},
		{name: "wrong size", status: 200, alter: func(p *PartURL) { p.Size++ }},
		{name: "wrong digest", status: 200, alter: func(p *PartURL) { p.ChecksumSHA256 = strings.Repeat("b", 64) }},
		{name: "other algorithm", status: 200, alter: func(p *PartURL) { p.ChecksumCRC32C = "other" }},
		{name: "expired", status: 200, alter: func(p *PartURL) { p.ExpiresAt = time.Now().Add(-time.Minute).Format(time.RFC3339) }},
		{name: "missing expiry", status: 200, alter: func(p *PartURL) { p.ExpiresAt = "" }},
		{name: "missing URL", status: 200, alter: func(p *PartURL) { p.URL = "" }},
		{name: "ambiguous dedup", status: 200, alter: func(p *PartURL) { p.SkipUpload = true }},
		{name: "dedup headers", status: 200, alter: func(p *PartURL) { p.SkipUpload = true; p.URL = "" }},
		{name: "trailing", status: 200, tail: `{}`},
		{name: "oversize", status: 200, tail: strings.Repeat(" ", 4<<20)},
		{name: "truncated", status: 200, short: true},
		{name: "unknown", status: 503},
		{name: "rejected", status: 409},
		{name: "unexpected success", status: 202},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/v1/views/a/v2/uploads/u/presign" || r.Header.Get(viewprotocol.WorkHeader) != "work" || r.Header.Get(viewprotocol.FenceHeader) != "3" || r.Header.Get(viewprotocol.OperationHeader) != "" {
					t.Error("incorrect binding", r.Method, r.URL, r.Header)
				}
				var req struct {
					Number   int                               `json:"part_number"`
					Checksum struct{ Algorithm, Value string } `json:"checksum"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Number != 256 || req.Checksum.Algorithm != "sha256" || req.Checksum.Value != key {
					t.Error(req, err)
				}
				out := PartURL{Number: 256, Size: 4, ChecksumSHA256: key, URL: "https://objects.invalid/create-once", Headers: map[string]string{"If-None-Match": "*"}, ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}
				if tc.alter != nil {
					tc.alter(&out)
				}
				body, err := json.Marshal(out)
				if err != nil {
					t.Error(err)
					return
				}
				if tc.short {
					w.Header().Set("Content-Length", "4096")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write(body)
				_, _ = io.WriteString(w, tc.tail)
			}))
			defer srv.Close()
			c, err := New(srv.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 3})
			if err != nil {
				t.Fatal(err)
			}
			out, err := c.PresignFixedExtentPart(context.Background(), "u", 256, 4, strings.ToUpper(key))
			if tc.valid {
				if err != nil || out.Number != 256 || out.Size != 4 {
					t.Fatal(out, err)
				}
			} else if err == nil || out.URL != "" || out.SkipUpload {
				t.Fatal("invalid capability returned", out, err)
			}
			if calls.Load() != 1 {
				t.Fatal("request retried", calls.Load())
			}
		})
	}
}

func TestFixedExtentPresignInvalidInputAndRedirectCannotReachStorage(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/redirect-target" {
			t.Error("followed signing redirect")
		}
		w.Header().Set("Location", "/redirect-target")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	main := New(srv.URL, "test-token")
	key := strings.Repeat("a", 64)
	if _, err := main.PresignFixedExtentPart(context.Background(), "u", 1, 4, key); err == nil {
		t.Fatal("main accepted")
	}
	c, err := main.WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id     string
		number int
		size   int64
		key    string
	}{
		{"", 1, 4, key}, {"../u", 1, 4, key}, {"u", 0, 4, key}, {"u", 257, 4, key}, {"u", 1, 0, key}, {"u", 1, 64<<20 + 1, key}, {"u", 1, 4, "bad"},
	} {
		if _, err := c.PresignFixedExtentPart(context.Background(), tc.id, tc.number, tc.size, tc.key); err == nil {
			t.Fatal(tc)
		}
	}
	validation, err := main.WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1, Validation: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validation.PresignFixedExtentPart(context.Background(), "u", 1, 4, key); err == nil {
		t.Fatal("validation accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input reached API", calls.Load())
	}
	if _, err := c.PresignFixedExtentPart(context.Background(), "u", 1, 4, key); err == nil {
		t.Fatal("redirect accepted")
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}
