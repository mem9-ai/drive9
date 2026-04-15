package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

func TestUploadFileEmitsUploadSummaryToCLILog(t *testing.T) {
	var uploaded bytes.Buffer

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "PUT /v1/fs/upload.txt":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			uploaded.Write(data)
			w.WriteHeader(http.StatusOK)
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

	localPath := filepath.Join(t.TempDir(), "upload.txt")
	if err := os.WriteFile(localPath, []byte("hello upload"), 0o644); err != nil {
		t.Fatalf("WriteFile(local): %v", err)
	}

	c := client.New(srv.URL, "")
	if err := uploadFile(context.Background(), c, localPath, "/upload.txt"); err != nil {
		t.Fatalf("uploadFile: %v", err)
	}
	if got := uploaded.String(); got != "hello upload" {
		t.Fatalf("uploaded body = %q, want %q", got, "hello upload")
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
	got := string(logBytes)
	if !strings.Contains(got, "\"msg\":\"upload_summary\"") || !strings.Contains(got, "\"type\":\"upload_summary\"") {
		t.Fatalf("cli log missing upload summary JSON: %q", got)
	}
	if !strings.Contains(got, "\"mode\":\"direct_put\"") {
		t.Fatalf("cli log missing upload mode: %q", got)
	}
}

func TestEmitUploadSummaryUsesContextLogger(t *testing.T) {
	t.Setenv("DRIVE9_CLI_LOG_ENABLED", "true")

	core, recorded := observer.New(zap.InfoLevel)
	ctx := logger.WithContext(context.Background(), zap.New(core))

	emitUploadSummary(ctx, &client.UploadSummary{
		Type:       "upload_summary",
		Mode:       "direct_put",
		RemotePath: "/upload.txt",
	}, "/tmp/upload.txt")

	entries := recorded.All()
	if len(entries) != 1 {
		t.Fatalf("recorded %d log entries, want 1", len(entries))
	}
	if entries[0].Message != "upload_summary" {
		t.Fatalf("message = %q, want upload_summary", entries[0].Message)
	}
	fields := entries[0].ContextMap()
	if fields["remote_path"] != "/upload.txt" {
		t.Fatalf("remote_path = %#v, want %q", fields["remote_path"], "/upload.txt")
	}
	if fields["local_path"] != "/tmp/upload.txt" {
		t.Fatalf("local_path = %#v, want %q", fields["local_path"], "/tmp/upload.txt")
	}
}
