package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func validRuntimeCapabilities() RuntimeCapabilities {
	return RuntimeCapabilities{
		Enabled:              true,
		ProtocolVersion:      RuntimeProtocolVersion,
		Durability:           runtimeDurability,
		WorkspaceConsistency: runtimeWorkspaceConsistency,
		ExecutionEffects:     runtimeExecutionEffects,
		FileActions:          []string{"read", "write", "edit", "delete", "list", "find", "grep"},
		Profiles:             []string{"go-build"},
		DefaultProfile:       "go-build",
		Limits: RuntimeLimits{
			MaxTenantQueuedOperations: 1000, MaxPrincipalQueuedOperations: 100,
			MaxTenantActiveOperations: 64, MaxPrincipalActiveOperations: 16,
			MaxWorkspaceEntries: 100_000, MaxWorkspaceBytes: 10 << 30, MaxSingleFileBytes: 1 << 30,
			MaxPathBytes: 4096, MaxPathDepth: 128, MaxManifestBytes: 32 << 20,
			MaxReadResultBytes: 4 << 20, MaxListEntries: 10_000, MaxSearchFileBytes: 16 << 20,
			MaxSearchResultBytes: 4 << 20, MaxOperationResultBytes: 8 << 20,
			MaxLogBytes: 16 << 20, MaxChangedBytes: 64 << 20, MaxExecutionSeconds: 24 * 60 * 60,
			CaptureDeadlineSeconds: 60, CheckpointDeadlineSeconds: 30,
		},
	}
}

func writeRuntimeCapabilities(t *testing.T, w http.ResponseWriter, capabilities RuntimeCapabilities) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(capabilities); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeSubmitFailsClosedWhenCapabilitiesAreAbsent(t *testing.T) {
	var submitted atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/runtime/capabilities" {
			http.NotFound(w, r)
			return
		}
		submitted.Store(true)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	_, err := New(server.URL, "owner-key").SubmitRuntimeExecution(context.Background(), "key", RuntimeExecutionRequest{
		WorkspaceRef: "runtime://workspace", Execution: RuntimeExecutionSpec{Shell: "true"},
	})
	if !errors.Is(err, ErrRuntimeUnsupported) {
		t.Fatalf("submit error = %v, want ErrRuntimeUnsupported", err)
	}
	if submitted.Load() {
		t.Fatal("submit reached the execution endpoint without a Runtime capability")
	}
}

func TestRuntimeSubmitPreservesIdempotencyKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/capabilities":
			writeRuntimeCapabilities(t, w, validRuntimeCapabilities())
		case "/v1/runtime/executions":
			if got := r.Header.Get("Idempotency-Key"); got != "stable-key" {
				t.Errorf("Idempotency-Key = %q", got)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"id":"exec_1","kind":"execution","state":"queued"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	operation, err := New(server.URL, "owner-key").SubmitRuntimeExecution(context.Background(), "stable-key", RuntimeExecutionRequest{
		WorkspaceRef: "runtime://workspace", Execution: RuntimeExecutionSpec{Shell: "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if operation.ID != "exec_1" || operation.State != "queued" {
		t.Fatalf("operation = %#v", operation)
	}
}

func TestRuntimeSubmitFailsClosedWhenCapabilitiesOmitRequiredLimits(t *testing.T) {
	var submitted atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/capabilities":
			capabilities := validRuntimeCapabilities()
			capabilities.Limits.MaxSearchResultBytes = 0
			writeRuntimeCapabilities(t, w, capabilities)
		case "/v1/runtime/executions":
			submitted.Store(true)
			http.Error(w, "unexpected submit", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	_, err := New(server.URL, "owner-key").SubmitRuntimeExecution(context.Background(), "key", RuntimeExecutionRequest{
		WorkspaceRef: "runtime://workspace", Execution: RuntimeExecutionSpec{Shell: "true"},
	})
	if !errors.Is(err, ErrRuntimeUnsupported) || !strings.Contains(err.Error(), "limits") {
		t.Fatalf("submit error = %v, want invalid-limit RuntimeUnsupported", err)
	}
	if submitted.Load() {
		t.Fatal("submit reached the execution endpoint with incomplete Runtime capabilities")
	}
}

func TestRuntimeCapabilitiesRejectUndownloadableOrUnsafeLimits(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RuntimeLimits)
	}{
		{name: "undownloadable log", mutate: func(limits *RuntimeLimits) { limits.MaxLogBytes = maxRuntimeArtifactBytes + 1 }},
		{name: "unsafe integer", mutate: func(limits *RuntimeLimits) { limits.MaxWorkspaceBytes = maxRuntimeEventCursor + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capabilities := validRuntimeCapabilities()
			test.mutate(&capabilities.Limits)
			if err := capabilities.validate(); !errors.Is(err, ErrRuntimeUnsupported) {
				t.Fatalf("validate() error = %v, want ErrRuntimeUnsupported", err)
			}
		})
	}
}

func TestRuntimeSubmitFailsClosedWhenDefaultProfileIsNotAdvertised(t *testing.T) {
	var submitted atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/capabilities":
			capabilities := validRuntimeCapabilities()
			capabilities.DefaultProfile = "other"
			writeRuntimeCapabilities(t, w, capabilities)
		case "/v1/runtime/executions":
			submitted.Store(true)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	_, err := New(server.URL, "owner-key").SubmitRuntimeExecution(context.Background(), "key", RuntimeExecutionRequest{
		WorkspaceRef: "runtime://workspace", Execution: RuntimeExecutionSpec{Shell: "true"},
	})
	if !errors.Is(err, ErrRuntimeUnsupported) || !strings.Contains(err.Error(), "profile") {
		t.Fatalf("submit error = %v", err)
	}
	if submitted.Load() {
		t.Fatal("submit reached server with invalid profile capabilities")
	}
}

func TestRuntimeSubmitFailsClosedForIncompatibleEffectBoundary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capabilities := validRuntimeCapabilities()
		capabilities.SupportsReplay = true
		writeRuntimeCapabilities(t, w, capabilities)
	}))
	defer server.Close()

	_, err := New(server.URL, "owner-key").SubmitRuntimeExecution(context.Background(), "key", RuntimeExecutionRequest{
		WorkspaceRef: "runtime://workspace", Execution: RuntimeExecutionSpec{Shell: "true"},
	})
	if !errors.Is(err, ErrRuntimeUnsupported) || !strings.Contains(err.Error(), "effect boundaries") {
		t.Fatalf("submit error = %v, want incompatible-boundary RuntimeUnsupported", err)
	}
}

func TestRuntimeFileSubmitRequiresAdvertisedAction(t *testing.T) {
	var submitted atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/capabilities":
			capabilities := validRuntimeCapabilities()
			capabilities.FileActions = []string{"read"}
			writeRuntimeCapabilities(t, w, capabilities)
		case "/v1/runtime/file-operations":
			submitted.Store(true)
			http.Error(w, "unexpected submit", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	_, err := New(server.URL, "owner-key").SubmitRuntimeFileOperation(context.Background(), "key", RuntimeFileOperationRequest{
		WorkspaceRef: "runtime://workspace", Operation: RuntimeFileOperationSpec{Action: "write", Path: "/file"},
	})
	if !errors.Is(err, ErrRuntimeUnsupported) || !strings.Contains(err.Error(), `file action "write"`) {
		t.Fatalf("submit error = %v, want unsupported action", err)
	}
	if submitted.Load() {
		t.Fatal("submit reached the file-operation endpoint without the advertised action")
	}
}

func TestWatchRuntimeEventsReconnectsFromLastCursor(t *testing.T) {
	var windows atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/executions/exec_1/events":
			window := windows.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			if window == 1 {
				_, _ = fmt.Fprint(w, "data: {\"operation_id\":\"exec_1\",\"cursor\":2,\"kind\":\"running\"}\n\n")
				return
			}
			if got := r.URL.Query().Get("after"); got != "2" {
				t.Errorf("reconnect cursor = %q, want 2", got)
			}
			_, _ = fmt.Fprint(w, "data: {\"operation_id\":\"exec_1\",\"cursor\":3,\"kind\":\"completed\"}\n\n")
		case "/v1/runtime/executions/exec_1":
			state := "running"
			if windows.Load() >= 2 {
				state = "succeeded"
			}
			_, _ = fmt.Fprintf(w, `{"id":"exec_1","kind":"execution","state":%q}`, state)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var cursors []int64
	err := New(server.URL, "owner-key").WatchRuntimeEvents(context.Background(), "executions", "exec_1", 0, func(event RuntimeEvent) error {
		cursors = append(cursors, event.Cursor)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cursors) != 2 || cursors[0] != 2 || cursors[1] != 3 {
		t.Fatalf("event cursors = %v", cursors)
	}
}

func TestWatchRuntimeEventsRetriesTransientServerFailure(t *testing.T) {
	var windows atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/executions/exec_1/events":
			if windows.Add(1) == 1 {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			_, _ = fmt.Fprint(w, "data: {\"operation_id\":\"exec_1\",\"cursor\":2,\"kind\":\"completed\"}\n\n")
		case "/v1/runtime/executions/exec_1":
			_, _ = fmt.Fprint(w, `{"id":"exec_1","kind":"execution","state":"succeeded"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := New(server.URL, "owner-key").WatchRuntimeEvents(ctx, "executions", "exec_1", 0, func(RuntimeEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if windows.Load() != 2 {
		t.Fatalf("event windows = %d", windows.Load())
	}
}

func TestWatchRuntimeEventWindowDoesNotAcknowledgeRejectedEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "data: {\"operation_id\":\"exec_1\",\"cursor\":2,\"kind\":\"running\"}\n\n")
	}))
	defer server.Close()

	wantErr := errors.New("reject event")
	last, err := New(server.URL, "owner-key").watchRuntimeEventWindow(context.Background(), "executions", "exec_1", 0, func(RuntimeEvent) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("watch error = %v, want %v", err, wantErr)
	}
	if last != 0 {
		t.Fatalf("acknowledged cursor = %d, want 0", last)
	}
}

func TestWatchRuntimeEventWindowRejectsForeignOperation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "data: {\"operation_id\":\"exec_other\",\"cursor\":2,\"kind\":\"running\"}\n\n")
	}))
	defer server.Close()

	called := false
	last, err := New(server.URL, "owner-key").watchRuntimeEventWindow(context.Background(), "executions", "exec_1", 0, func(RuntimeEvent) error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), `operation_id "exec_other" does not match "exec_1"`) {
		t.Fatalf("watch error = %v", err)
	}
	if called {
		t.Fatal("foreign operation event reached callback")
	}
	if last != 0 {
		t.Fatalf("acknowledged cursor = %d, want 0", last)
	}
}

func TestWatchRuntimeEventsRejectsUnsafeCursor(t *testing.T) {
	err := New("http://unused.invalid", "owner-key").WatchRuntimeEvents(context.Background(), "executions", "exec_1", maxRuntimeEventCursor+1, nil)
	if err == nil {
		t.Fatal("expected unsafe cursor rejection")
	}
}

func TestDownloadRuntimeArtifactRequiresMatchingChecksum(t *testing.T) {
	content := []byte("durable artifact")
	sum := sha256.Sum256(content)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-SHA256", hex.EncodeToString(sum[:]))
		_, _ = w.Write(content)
	}))
	defer server.Close()

	got, err := New(server.URL, "owner-key").DownloadRuntimeArtifact(context.Background(), "artifact_1")
	if err != nil || string(got) != string(content) {
		t.Fatalf("artifact = %q, err = %v", got, err)
	}
}

func TestDownloadRuntimeArtifactRejectsMissingChecksum(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("unverified"))
	}))
	defer server.Close()

	if _, err := New(server.URL, "owner-key").DownloadRuntimeArtifact(context.Background(), "artifact_1"); err == nil {
		t.Fatal("expected missing checksum rejection")
	}
}

func TestDownloadRuntimeArtifactRequiresContentLength(t *testing.T) {
	content := []byte("streamed")
	sum := sha256.Sum256(content)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Content-SHA256", hex.EncodeToString(sum[:]))
		w.(http.Flusher).Flush()
		_, _ = w.Write(content)
	}))
	defer server.Close()

	if _, err := New(server.URL, "owner-key").DownloadRuntimeArtifact(context.Background(), "artifact_1"); err == nil || !strings.Contains(err.Error(), "omitted Content-Length") {
		t.Fatalf("download error = %v, want missing Content-Length", err)
	}
}

func TestDownloadRuntimeArtifactRejectsContentLengthMismatch(t *testing.T) {
	content := []byte("abc")
	sum := sha256.Sum256(content)
	client := New("http://runtime.invalid", "owner-key")
	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"X-Content-Sha256": []string{hex.EncodeToString(sum[:])}},
			Body:          io.NopCloser(strings.NewReader(string(content))),
			ContentLength: int64(len(content) + 1),
		}, nil
	})}

	if _, err := client.DownloadRuntimeArtifact(context.Background(), "artifact_1"); err == nil || !strings.Contains(err.Error(), "Content-Length mismatch") {
		t.Fatalf("download error = %v, want Content-Length mismatch", err)
	}
}
