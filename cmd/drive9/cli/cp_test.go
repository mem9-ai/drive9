package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/logger"
)

func TestParseRemote(t *testing.T) {
	tests := []struct {
		input    string
		wantCtx  string
		wantPath string
		wantOK   bool
	}{
		{":/data/file.txt", "", "/data/file.txt", true},
		{":/", "", "/", true},
		{"test1:/TODO.md", "test1", "/TODO.md", true},
		{"mydb:/data/file.txt", "mydb", "/data/file.txt", true},
		{"/data/file.txt", "", "", false},
		{"/tmp/local.txt", "", "", false},
		{"local.txt", "", "", false},
		{"./local.txt", "", "", false},
		{"-", "", "", false},
		// Windows drive-letter paths must not be treated as remote.
		{"C:/tmp/a.txt", "", "", false},
		{"D:/Users/test", "", "", false},
		{"c:/data", "", "", false},
		// Two-char context names still work.
		{"ab:/file.txt", "ab", "/file.txt", true},
	}
	for _, tt := range tests {
		rp, ok := ParseRemote(tt.input)
		if ok != tt.wantOK {
			t.Errorf("ParseRemote(%q) ok=%v, want %v", tt.input, ok, tt.wantOK)
			continue
		}
		if ok {
			if rp.Context != tt.wantCtx {
				t.Errorf("ParseRemote(%q) context=%q, want %q", tt.input, rp.Context, tt.wantCtx)
			}
			if rp.Path != tt.wantPath {
				t.Errorf("ParseRemote(%q) path=%q, want %q", tt.input, rp.Path, tt.wantPath)
			}
		}
	}
}

func TestDownloadFileEmitsDownloadSummaryToCLILogForLargeFile(t *testing.T) {
	data := bytes.Repeat([]byte("ab"), (8<<20)/2)
	data = append(data, []byte("tail")...)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "HEAD /v1/fs/large.bin":
			w.Header().Set("Content-Length", "8388612")
			w.WriteHeader(http.StatusOK)
		case "GET /v1/fs/large.bin":
			w.Header().Set("Location", "http://"+r.Host+"/s3/presigned?token=fixed")
			w.WriteHeader(http.StatusFound)
		case "GET /s3/presigned":
			rangeHeader := r.Header.Get("Range")
			if !strings.HasPrefix(rangeHeader, "bytes=") {
				http.Error(w, "missing range", http.StatusBadRequest)
				return
			}
			var start, end int
			if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
				http.Error(w, "bad range", http.StatusBadRequest)
				return
			}
			if start < 0 || end < start || end >= len(data) {
				http.Error(w, "range outside test data", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[start : end+1])
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("DRIVE9_CLI_LOG_ENABLED", "true")
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	restoreLogger := logger.L()
	cliLogger, err := logger.NewCLILogger()
	if err != nil {
		t.Fatalf("NewCLILogger: %v", err)
	}
	logger.Set(cliLogger)
	defer logger.Set(restoreLogger)
	defer func() { _ = cliLogger.Sync() }()

	localPath := filepath.Join(t.TempDir(), "large.bin")
	c := client.New(srv.URL, "")

	if err := downloadFile(context.Background(), c, "/large.bin", localPath); err != nil {
		t.Fatalf("downloadFile: %v", err)
	}
	if err := cliLogger.Sync(); err != nil {
		t.Fatalf("Sync(cli log): %v", err)
	}

	logPath, err := logger.CLILogPath()
	if err != nil {
		t.Fatalf("CLILogPath: %v", err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile(cli log): %v", err)
	}
	if got := string(logBytes); !strings.Contains(got, "\"msg\":\"download_summary\"") || !strings.Contains(got, "\"type\":\"download_summary\"") {
		t.Fatalf("cli log missing download summary JSON: %q", got)
	}
}

func TestDownloadFileSkipsDownloadSummaryWhenCLILogDisabled(t *testing.T) {
	data := []byte("small content")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "HEAD /v1/fs/small.txt":
			w.Header().Set("Content-Length", "13")
			w.WriteHeader(http.StatusOK)
		case "GET /v1/fs/small.txt":
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("DRIVE9_CLI_LOG_ENABLED", "false")
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	restoreLogger := logger.L()
	cliLogger, err := logger.NewCLILogger()
	if err != nil {
		t.Fatalf("NewCLILogger: %v", err)
	}
	logger.Set(cliLogger)
	defer logger.Set(restoreLogger)
	defer func() { _ = cliLogger.Sync() }()

	localPath := filepath.Join(t.TempDir(), "small.txt")
	c := client.New(srv.URL, "")

	if err := downloadFile(context.Background(), c, "/small.txt", localPath); err != nil {
		t.Fatalf("downloadFile: %v", err)
	}
	if err := cliLogger.Sync(); err != nil {
		t.Fatalf("Sync(cli log): %v", err)
	}

	logPath, err := logger.CLILogPath()
	if err != nil {
		t.Fatalf("CLILogPath: %v", err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadFile(cli log): %v", err)
	}
	if strings.Contains(string(logBytes), "\"msg\":\"download_summary\"") {
		t.Fatalf("expected no download summary in cli log, got %q", string(logBytes))
	}
}
