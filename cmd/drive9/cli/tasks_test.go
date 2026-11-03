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
		`{"task_type":"embed","status":"succeeded","usage":{"input_tokens":109,"output_tokens":51,"total_tokens":160,"llm_calls":2,"audio_duration_seconds":0}},`+
		`{"task_type":"img_extract_text","status":"failed","last_error":"image_extract_provider_auth_rejected: authentication rejected (HTTP 401)","usage":{"input_tokens":9,"output_tokens":1,"total_tokens":10,"llm_calls":1,"audio_duration_seconds":0}},`+
		`{"task_type":"audio_extract_text","status":"pending","pending_reason":"quota_exceeded","usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0,"llm_calls":0,"audio_duration_seconds":12.5}}]}`)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Tasks(c, []string{":/doc.txt"}) })
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	for _, want := range []string{
		"TASK_TYPE", "STATUS", "USAGE", "PENDING_REASON", "LAST_ERROR",
		"embed", "succeeded", "2 calls, 109/51 tok",
		"img_extract_text", "failed", "1 call, 9/1 tok",
		"image_extract_provider_auth_rejected: authentication rejected (HTTP 401)",
		"audio_extract_text", "pending", "quota_exceeded", "0 calls, 0/0 tok, 12.5s audio",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q: %q", want, out)
		}
	}
}

// A server that predates the usage projection reports no usage; the table must
// say so instead of claiming the task consumed nothing.
func TestTasksTextOutputMarksUnreportedUsage(t *testing.T) {
	srv := newTasksTestServer(t, `{"path":"/doc.txt","tasks":[{"task_type":"embed","status":"succeeded"}]}`)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Tasks(c, []string{":/doc.txt"}) })
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected header + 1 row, got %d lines: %q", len(lines), out)
	}
	if got := strings.Fields(lines[1]); len(got) < 3 || got[2] != "-" {
		t.Fatalf("usage cell = %v, want \"-\" in %q", got, lines[1])
	}
	if strings.Contains(out, "<nil>") {
		t.Fatalf("an absent optional field must render as an empty cell, got %q", out)
	}
}

func TestOptionalTaskCell(t *testing.T) {
	present := "quota_exceeded"
	empty := ""
	withControls := "quota\texceeded\nreason"
	for _, tc := range []struct {
		name string
		in   *string
		want string
	}{
		{name: "absent", in: nil, want: ""},
		{name: "present", in: &present, want: "quota_exceeded"},
		{name: "present but empty", in: &empty, want: ""},
		{name: "control characters", in: &withControls, want: "quota exceeded reason"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := optionalTaskCell(tc.in); got != tc.want {
				t.Fatalf("optionalTaskCell(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestTaskUsageCell(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   *client.FileTaskUsage
		want string
	}{
		{name: "unreported", in: nil, want: "-"},
		{name: "empty", in: &client.FileTaskUsage{}, want: "0 calls, 0/0 tok"},
		{name: "single call", in: &client.FileTaskUsage{InputTokens: 9, OutputTokens: 1, LLMCalls: 1}, want: "1 call, 9/1 tok"},
		{name: "multiple calls", in: &client.FileTaskUsage{InputTokens: 109, OutputTokens: 51, LLMCalls: 2}, want: "2 calls, 109/51 tok"},
		{name: "audio only", in: &client.FileTaskUsage{AudioDurationSeconds: 12.5}, want: "0 calls, 0/0 tok, 12.5s audio"},
		{name: "tokens and audio", in: &client.FileTaskUsage{InputTokens: 9, OutputTokens: 1, LLMCalls: 1, AudioDurationSeconds: 0.5}, want: "1 call, 9/1 tok, 0.5s audio"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := taskUsageCell(tc.in); got != tc.want {
				t.Fatalf("taskUsageCell(%+v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestTasksJSONOutput(t *testing.T) {
	srv := newTasksTestServer(t, `{"path":"/doc.txt","tasks":[`+
		`{"task_type":"embed","status":"queued","usage":{"input_tokens":109,"output_tokens":51,"total_tokens":160,"llm_calls":2,"audio_duration_seconds":0}},`+
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
	if usage := got.Tasks[0].Usage; usage == nil || usage.InputTokens != 109 || usage.OutputTokens != 51 || usage.TotalTokens != 160 || usage.LLMCalls != 2 {
		t.Fatalf("task[0].Usage = %+v", got.Tasks[0].Usage)
	}
	if got.Tasks[1].Usage != nil {
		t.Fatalf("task[1].Usage = %+v, want absent usage preserved as null", got.Tasks[1].Usage)
	}
	if got.Tasks[1].LastError != "image_extract_provider_rate_limited: provider rate limited the request (HTTP 429)" {
		t.Fatalf("task[1].LastError = %q", got.Tasks[1].LastError)
	}
}

// An empty result still prints the header so callers can tell "no applicable
// tasks" apart from a no-op.
func TestTasksEmptyTextPrintsHeader(t *testing.T) {
	// The server omits tasks. The text path renders a nil and an empty slice
	// identically, so this asserts only the header; the JSON and client tests
	// pin the nil -> [] normalization.
	srv := newTasksTestServer(t, `{"path":"/doc.txt"}`)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Tasks(c, []string{":/doc.txt"}) })
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	if !strings.Contains(out, "TASK_TYPE") || !strings.Contains(out, "STATUS") ||
		!strings.Contains(out, "USAGE") || !strings.Contains(out, "PENDING_REASON") ||
		!strings.Contains(out, "LAST_ERROR") {
		t.Fatalf("empty output should still print the header, got %q", out)
	}
}

func TestTasksJSONEmptyTasksIsArray(t *testing.T) {
	// The server omits tasks entirely; JSON output must still be [] (not null).
	srv := newTasksTestServer(t, `{"path":"/doc.txt"}`)
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

// Control characters in a server-supplied last_error must not break the table.
// tabwriter treats \t, \v, and \f specially, so all of them must be mapped.
func TestTasksTextOutputSanitizesControlCharacters(t *testing.T) {
	srv := newTasksTestServer(t, `{"path":"/doc.txt","tasks":[`+
		`{"task_type":"embed","status":"failed","last_error":"embed_text_failed\ninjected\trow\u000bcol\u000cform\u007fdel"}]}`)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Tasks(c, []string{":/doc.txt"}) })
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected header + 1 row, got %d lines: %q", len(lines), out)
	}
	if !strings.Contains(out, "embed_text_failed injected row col form del") {
		t.Fatalf("control characters not replaced with spaces: %q", out)
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
