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
// status endpoint with responseBody.
func newTasksTestServer(t *testing.T, responseBody string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/doc.txt" || !r.URL.Query().Has("tasks") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
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
	for _, want := range []string{"embed", "succeeded", "img_extract_text", "failed", "image_extract_provider_auth_rejected: authentication rejected (HTTP 401)"} {
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

func TestTasksEmptyTextOutputsNothing(t *testing.T) {
	srv := newTasksTestServer(t, `{"path":"/doc.txt","tasks":[]}`)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Tasks(c, []string{":/doc.txt"}) })
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	if strings.TrimSpace(out) != "" {
		t.Fatalf("output = %q, want empty", out)
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
