package cli

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestCatWritesFullFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/fs/file.txt" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("hello world"))
	}))
	defer srv.Close()

	var out bytes.Buffer
	if err := catWithWriter(client.New(srv.URL, ""), []string{":/file.txt"}, &out); err != nil {
		t.Fatalf("Cat: %v", err)
	}
	if got := out.String(); got != "hello world" {
		t.Fatalf("Cat output = %q, want hello world", got)
	}
}

func TestCatRangeWritesRequestedBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/large.bin":
			w.Header().Set("Location", fmt.Sprintf("http://%s/s3/presigned", r.Host))
			w.WriteHeader(http.StatusFound)
		case "/s3/presigned":
			if got := r.Header.Get("Range"); got != "bytes=2-5" {
				http.Error(w, "wrong range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("2345"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := catWithWriter(client.New(srv.URL, ""), []string{"--offset", "2", "--length", "4", ":/large.bin"}, &out)
	if err != nil {
		t.Fatalf("Cat range: %v", err)
	}
	if got := out.String(); got != "2345" {
		t.Fatalf("Cat range output = %q, want 2345", got)
	}
}

func TestCatRangeRequiresOffsetAndLength(t *testing.T) {
	c := client.New("http://127.0.0.1", "")
	if err := catWithWriter(c, []string{"--offset", "2", ":/large.bin"}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "provided together") {
		t.Fatalf("offset-only error = %v, want paired flag error", err)
	}
	if err := catWithWriter(c, []string{"--length", "4", ":/large.bin"}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "provided together") {
		t.Fatalf("length-only error = %v, want paired flag error", err)
	}
}

func TestCatRangeRejectsNegativeInputs(t *testing.T) {
	c := client.New("http://127.0.0.1", "")
	if err := catWithWriter(c, []string{"--offset", "-1", "--length", "4", ":/large.bin"}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "--offset") {
		t.Fatalf("negative offset error = %v, want offset error", err)
	}
	if err := catWithWriter(c, []string{"--offset", "0", "--length", "-1", ":/large.bin"}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "--length") {
		t.Fatalf("negative length error = %v, want length error", err)
	}
}
