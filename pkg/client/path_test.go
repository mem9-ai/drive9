//go:build !integration

package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestValidateFSPath(t *testing.T) {
	valid := []string{
		"/",
		"",
		"/ok.txt",
		"/dir/sub dir/文件.txt",
		"relative/ok.txt", // server roots it; client only rejects what the server would
		"/trailing-slash-dir/",
	}
	for _, p := range valid {
		if err := validateFSPath(p); err != nil {
			t.Errorf("validateFSPath(%q) = %v, want nil", p, err)
		}
	}

	tests := []struct {
		name     string
		path     string
		wantSub  string
		wantHint string
	}{
		{name: "windows drive absolute", path: `C:\股票工具`, wantSub: "path contains backslash", wantHint: "Windows local path"},
		{name: "embedded windows drive segment", path: `/个股回测工具/C:\股票工具`, wantSub: "path contains backslash", wantHint: "Windows local path"},
		{name: "windows forward-slash drive", path: `D:/股票工具/x`, wantSub: "", wantHint: ""}, // valid POSIX path; no rejection
		{name: "bare backslash", path: `/dir/sub\name`, wantSub: "path contains backslash", wantHint: "forward slashes"},
		{name: "dot dot", path: "/dir/../escape", wantSub: `".." segment`, wantHint: ""},
		{name: "nul", path: "/dir/\x00x", wantSub: "NUL", wantHint: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFSPath(tc.path)
			if tc.wantSub == "" {
				if err != nil {
					t.Fatalf("validateFSPath(%q) = %v, want nil", tc.path, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateFSPath(%q) = nil, want error containing %q", tc.path, tc.wantSub)
			}
			if !strings.Contains(err.Error(), "invalid drive9 path") {
				t.Errorf("error = %v, want invalid drive9 path prefix", err)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error = %v, want containing %q", err, tc.wantSub)
			}
			if tc.wantHint != "" && !strings.Contains(err.Error(), tc.wantHint) {
				t.Errorf("error = %v, want hint containing %q", err, tc.wantHint)
			}
		})
	}
}

func TestFSMethodsRejectWindowsPathBeforeRequest(t *testing.T) {
	var called atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
	}))
	defer ts.Close()
	c := New(ts.URL, "")

	ctx := context.Background()
	// Shape observed in production: a Windows local absolute path joined
	// onto a drive9 directory as a path segment.
	bad := `/个股回测工具/C:\股票工具`
	checks := map[string]func() error{
		"Mkdir":       func() error { return c.MkdirCtx(ctx, bad, 0o755) },
		"Write":       func() error { return c.WriteCtx(ctx, bad, []byte("x")) },
		"Read":        func() error { _, err := c.ReadCtx(ctx, bad); return err },
		"List":        func() error { _, err := c.ListCtx(ctx, bad); return err },
		"Stat":        func() error { _, err := c.StatCtx(ctx, bad); return err },
		"Delete":      func() error { return c.DeleteCtx(ctx, bad) },
		"RenameSrc":   func() error { return c.RenameCtx(ctx, bad, "/ok") },
		"RenameDst":   func() error { return c.RenameCtx(ctx, "/ok", bad) },
		"CopySrc":     func() error { return c.CopyCtx(ctx, bad, "/ok") },
		"CopyDst":     func() error { return c.CopyCtx(ctx, "/ok", bad) },
		"HardlinkSrc": func() error { return c.HardlinkCtx(ctx, bad, "/ok") },
		"BatchStat":   func() error { _, err := c.BatchStatCtx(ctx, []string{"/ok", bad}); return err },
		"BatchReadSmall": func() error {
			_, err := c.BatchReadSmallCtx(ctx, []string{bad}, 16)
			return err
		},
		"BatchWrite": func() error {
			_, err := c.BatchWriteCtx(ctx, []BatchWriteItem{{Path: bad, Data: []byte("x")}})
			return err
		},
	}
	for name, fn := range checks {
		err := fn()
		if err == nil {
			t.Errorf("%s: error = nil, want invalid path error", name)
			continue
		}
		if !strings.Contains(err.Error(), "invalid drive9 path") {
			t.Errorf("%s: error = %v, want invalid drive9 path", name, err)
		}
		if !strings.Contains(err.Error(), "Windows local path") {
			t.Errorf("%s: error = %v, want Windows hint", name, err)
		}
	}
	if called.Load() {
		t.Fatal("server was called despite client-side path validation")
	}
}
