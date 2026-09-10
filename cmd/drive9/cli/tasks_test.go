package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

// newTasksTestServer returns an httptest.Server that answers the file task
// status endpoint like a current server, including the X-Dat9-Tasks marker.
func newTasksTestServer(t *testing.T, responseBody string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/doc.txt" || !r.URL.Query().Has("tasks") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Dat9-Tasks", "1")
		_, _ = io.WriteString(w, responseBody)
	}))
}

func TestTasksTextOutput(t *testing.T) {
	srv := newTasksTestServer(t, `{"path":"/doc.txt","tasks":[`+
		`{"task_type":"embed","status":"succeeded"},`+
		`{"task_type":"img_extract_text","status":"failed","last_error":"image_extract_provider_auth_rejected: authentication rejected (HTTP 401)"}]}`)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Tasks(c, []string{":/doc.txt"}) })
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	for _, want := range []string{"TASK_TYPE", "STATUS", "LAST_ERROR", "embed", "succeeded", "img_extract_text", "failed", "image_extract_provider_auth_rejected: authentication rejected (HTTP 401)"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q: %q", want, out)
		}
	}
}

func TestTasksJSONOutput(t *testing.T) {
	srv := newTasksTestServer(t, `{"path":"/doc.txt","tasks":[`+
		`{"task_type":"embed","status":"queued"},`+
		`{"task_type":"img_extract_text","status":"failed","last_error":"image_extract_provider_rate_limited: provider rate limited the request (HTTP 429)"}]}`)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Tasks(c, []string{"-o", "json", ":/doc.txt"}) })
	if err != nil {
		t.Fatalf("Tasks -o json: %v", err)
	}
	var got client.FileTasksResult
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("decode JSON output: %v\nraw: %s", err, out)
	}
	if got.Path != "/doc.txt" {
		t.Fatalf("path = %q, want /doc.txt", got.Path)
	}
	if len(got.Tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(got.Tasks))
	}
	if got.Tasks[0].TaskType != "embed" || got.Tasks[0].Status != "queued" {
		t.Fatalf("task[0] = %+v", got.Tasks[0])
	}
	if got.Tasks[1].LastError != "image_extract_provider_rate_limited: provider rate limited the request (HTTP 429)" {
		t.Fatalf("task[1].LastError = %q", got.Tasks[1].LastError)
	}
}

// An empty result still prints the header so callers can tell "no applicable
// tasks" apart from a no-op.
func TestTasksEmptyTextPrintsHeader(t *testing.T) {
	srv := newTasksTestServer(t, `{"path":"/doc.txt","tasks":[]}`)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Tasks(c, []string{":/doc.txt"}) })
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	if !strings.Contains(out, "TASK_TYPE") || !strings.Contains(out, "STATUS") || !strings.Contains(out, "LAST_ERROR") {
		t.Fatalf("empty output should still print the header, got %q", out)
	}
}

func TestTasksJSONEmptyTasksIsArray(t *testing.T) {
	srv := newTasksTestServer(t, `{"path":"/doc.txt","tasks":[]}`)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Tasks(c, []string{"-o", "json", ":/doc.txt"}) })
	if err != nil {
		t.Fatalf("Tasks -o json: %v", err)
	}
	if !strings.Contains(out, `"tasks": []`) {
		t.Fatalf("output should encode empty tasks as []: %q", out)
	}
}

// Object-store URIs are rejected before an object backend is opened, so no
// credentials are minted and no network call is made.
func TestTasksRejectsObjectURIWithoutNetwork(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "")
	err := Tasks(c, []string{"s3://bucket/key"})
	if err == nil || !strings.Contains(err.Error(), "only available on drive9 paths") {
		t.Fatalf("err = %v, want drive9-only error", err)
	}
}

func TestTasksSurfacesStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"path is a directory"}`)
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	err := Tasks(c, []string{":/dir/"})
	if err == nil || !strings.Contains(err.Error(), "path is a directory") {
		t.Fatalf("err = %v, want directory error", err)
	}
}

func TestTasksRejectsAuthFlag(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "")
	err := Tasks(c, []string{"--auth=local", ":/doc.txt"})
	if err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatalf("err = %v, want usage error for removed --auth flag", err)
	}
}

func TestTasksRejectsUnsupportedOutputFormat(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "")
	err := Tasks(c, []string{"-o", "yaml", ":/doc.txt"})
	if err == nil || !strings.Contains(err.Error(), "unsupported output format") {
		t.Fatalf("err = %v, want unsupported output format", err)
	}
}

func TestTasksRequiresPath(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "")
	err := Tasks(c, nil)
	if err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatalf("err = %v, want usage error", err)
	}
}
