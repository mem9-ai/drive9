package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecRequestDoesNotExposeBackendProfileControls(t *testing.T) {
	raw, err := json.Marshal(ExecRequest{
		Argv: []string{"true"}, Workspace: RuntimeWorkspace{Root: "/project"}, TimeoutMS: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"execution_class", "resources"} {
		if _, ok := payload[field]; ok {
			t.Fatalf("request exposes backend-owned field %q: %s", field, raw)
		}
	}
}

func TestExecStreamsAndReturnsNonzeroExit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/runtime/exec" || r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("request = %s %s auth=%q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		}
		var request ExecRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Argv) != 2 || request.Argv[0] != "echo" {
			t.Fatalf("request = %#v, %v", request, err)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
		_, _ = fmt.Fprintln(w, `{"type":"stdout","execution_id":"rex_12345678","data_base64":"aGVsbG8="}`)
		_, _ = fmt.Fprintln(w, `{"type":"stderr","execution_id":"rex_12345678","data_base64":"d2Fybg=="}`)
		_, _ = fmt.Fprintln(w, `{"type":"exit","execution_id":"rex_12345678","exit_code":7,"duration_ms":12}`)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	started := ""
	result, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"echo", "hello"}}, ExecOptions{
		Stdout: &stdout, Stderr: &stderr,
		OnStarted: func(event ExecStarted) error { started = event.ExecutionID; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 7 || result.ExecutionID != "rex_12345678" || result.DurationMS != 12 || started != result.ExecutionID {
		t.Fatalf("result/started = %#v/%q", result, started)
	}
	if stdout.String() != "hello" || stderr.String() != "warn" {
		t.Fatalf("decoded output = %q/%q", stdout.String(), stderr.String())
	}
}

func TestExecNeverRetriesUnknownOutcome(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
	}))
	defer server.Close()

	_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"true"}}, ExecOptions{})
	if !IsRuntimeOutcomeUnknown(err) || calls.Load() != 1 {
		t.Fatalf("error/calls = %v/%d", err, calls.Load())
	}
}

func TestExecRejectsMalformedProtocolAsUnknown(t *testing.T) {
	tests := map[string]string{
		"output before start": `{"type":"stdout","execution_id":"rex_12345678","data_base64":"YQ=="}`,
		"invalid base64":      `{"type":"started","execution_id":"rex_12345678"}` + "\n" + `{"type":"stdout","execution_id":"rex_12345678","data_base64":"***"}`,
		"wrong id":            `{"type":"started","execution_id":"rex_12345678"}` + "\n" + `{"type":"exit","execution_id":"rex_other000","exit_code":0,"duration_ms":1}`,
		"missing duration":    `{"type":"started","execution_id":"rex_12345678"}` + "\n" + `{"type":"exit","execution_id":"rex_12345678","exit_code":0}`,
		"invalid exit code":   `{"type":"started","execution_id":"rex_12345678"}` + "\n" + `{"type":"exit","execution_id":"rex_12345678","exit_code":256,"duration_ms":1}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/x-ndjson")
				_, _ = fmt.Fprintln(w, body)
			}))
			defer server.Close()
			_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"true"}}, ExecOptions{})
			if !IsRuntimeOutcomeUnknown(err) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

type shortRuntimeWriter struct{}

func (shortRuntimeWriter) Write(data []byte) (int, error) { return len(data) - 1, nil }

func TestExecShortOutputWriteIsUnknownAndCancels(t *testing.T) {
	canceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/exec":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
			_, _ = fmt.Fprintln(w, `{"type":"stdout","execution_id":"rex_12345678","data_base64":"YQ=="}`)
		case "/v1/runtime/executions/rex_12345678/cancel":
			canceled <- struct{}{}
			_ = json.NewEncoder(w).Encode(map[string]bool{"canceled": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"true"}}, ExecOptions{Stdout: shortRuntimeWriter{}})
	if !IsRuntimeOutcomeUnknown(err) {
		t.Fatalf("error = %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("lost output observation did not request cleanup cancellation")
	}
}

func TestExecReturnsTypedTerminalErrors(t *testing.T) {
	tests := []struct {
		code string
		want any
	}{
		{code: "start_failed", want: (*RuntimeStartError)(nil)},
		{code: "timed_out", want: (*RuntimeTimeoutError)(nil)},
		{code: "canceled", want: (*RuntimeCanceledError)(nil)},
		{code: "outcome_unknown", want: (*RuntimeOutcomeUnknownError)(nil)},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/x-ndjson")
				_, _ = fmt.Fprintf(w, `{"type":"error","error_code":%q,"message":"failed"}`+"\n", test.code)
			}))
			defer server.Close()
			_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"true"}}, ExecOptions{})
			switch test.want.(type) {
			case *RuntimeStartError:
				var target *RuntimeStartError
				if !errors.As(err, &target) {
					t.Fatalf("error = %T %v", err, err)
				}
			case *RuntimeTimeoutError:
				var target *RuntimeTimeoutError
				if !errors.As(err, &target) {
					t.Fatalf("error = %T %v", err, err)
				}
			case *RuntimeCanceledError:
				var target *RuntimeCanceledError
				if !errors.As(err, &target) {
					t.Fatalf("error = %T %v", err, err)
				}
			case *RuntimeOutcomeUnknownError:
				if !IsRuntimeOutcomeUnknown(err) {
					t.Fatalf("error = %T %v", err, err)
				}
			}
		})
	}
}

func TestExecContextCancelRequestsAndConfirmsRemoteStop(t *testing.T) {
	serverStarted := make(chan struct{})
	clientStarted := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/exec":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
			w.(http.Flusher).Flush()
			close(serverStarted)
			<-stopped
		case "/v1/runtime/executions/rex_12345678/cancel":
			once.Do(func() { close(stopped) })
			_ = json.NewEncoder(w).Encode(map[string]bool{"canceled": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := New(server.URL, "secret").Exec(ctx, ExecRequest{Argv: []string{"true"}}, ExecOptions{OnStarted: func(ExecStarted) error {
			close(clientStarted)
			return nil
		}})
		result <- err
	}()
	<-serverStarted
	<-clientStarted
	cancel()
	err := <-result
	var canceled *RuntimeCanceledError
	if !errors.As(err, &canceled) || canceled.ExecutionID != "rex_12345678" {
		t.Fatalf("error = %T %#v", err, err)
	}
}

func TestRuntimeCancelConfirmationAndUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/runtime/executions/rex_12345678/cancel" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"canceled":true}`))
	}))
	defer server.Close()
	if err := New(server.URL, "secret").CancelRuntimeExecution(t.Context(), "rex_12345678"); err != nil {
		t.Fatal(err)
	}

	unknown := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"unconfirmed","code":"outcome_unknown"}`))
	}))
	defer unknown.Close()
	if err := New(unknown.URL, "secret").CancelRuntimeExecution(t.Context(), "rex_12345678"); !IsRuntimeOutcomeUnknown(err) {
		t.Fatalf("cancel error = %v", err)
	}
}

func TestRuntimeCapabilities(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"version":2,"provider":"daytona","persistence":"workspace","workspace_path":"/workspace","user":"agent","streaming":true,"cancel":true,"max_active_per_tenant":4,"future_field":1}`))
	}))
	defer server.Close()
	capabilities, err := New(server.URL, "secret").GetRuntimeCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Provider != "daytona" || capabilities.WorkspacePath != "/workspace" || capabilities.User != "agent" || capabilities.MaxActivePerTenant != 4 {
		t.Fatalf("capabilities = %#v", capabilities)
	}

	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"version":1,"streaming":true}`))
	}))
	defer old.Close()
	if _, err := New(old.URL, "secret").GetRuntimeCapabilities(context.Background()); err == nil {
		t.Fatal("version 1 runtime accepted")
	}

	disabled := httptest.NewServer(http.NotFoundHandler())
	defer disabled.Close()
	if _, err := New(disabled.URL, "secret").GetRuntimeCapabilities(context.Background()); !errors.Is(err, ErrRuntimeNotEnabled) {
		t.Fatalf("disabled runtime error = %v", err)
	}
}

func TestExecCommandRunsThroughShellAndIgnoresHeartbeats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		argv, _ := json.Marshal(request["argv"])
		if string(argv) != `["/bin/sh","-c","npm ci \u0026\u0026 npm test"]` {
			t.Fatalf("argv = %s", argv)
		}
		if _, ok := request["persistence"]; ok {
			t.Fatal("client sent a persistence field")
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintln(w, `{"type":"heartbeat"}`)
		_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678","cwd":"/workspace/web","sandbox":"warm"}`)
		_, _ = fmt.Fprintln(w, `{"type":"heartbeat"}`)
		_, _ = fmt.Fprintln(w, `{"type":"exit","execution_id":"rex_12345678","exit_code":0,"duration_ms":3}`)
	}))
	defer server.Close()
	var started ExecStarted
	result, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{Command: "npm ci && npm test", Cwd: "web"}, ExecOptions{
		OnStarted: func(info ExecStarted) error { started = info; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.Cwd != "/workspace/web" || started.Sandbox != RuntimeSandboxWarm || result.Sandbox != RuntimeSandboxWarm {
		t.Fatalf("started/result = %#v/%#v", started, result)
	}
	if _, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{Command: "x", Argv: []string{"y"}}, ExecOptions{}); err == nil {
		t.Fatal("Command and Argv together accepted")
	}
}

func TestExecBusyAndDisabledAreTyped(t *testing.T) {
	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"too many commands are already running (limit 4); try again shortly","code":"busy"}`))
	}))
	defer busy.Close()
	_, err := New(busy.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"true"}}, ExecOptions{})
	var busyErr *RuntimeBusyError
	if !errors.As(err, &busyErr) || busyErr.RetryAfter != 7*time.Second || !IsRuntimeBusy(err) {
		t.Fatalf("busy error = %T %v", err, err)
	}
	disabled := httptest.NewServer(http.NotFoundHandler())
	defer disabled.Close()
	if _, err := New(disabled.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"true"}}, ExecOptions{}); !errors.Is(err, ErrRuntimeNotEnabled) {
		t.Fatalf("disabled error = %v", err)
	}
}

func TestExecDoesNotFollowRedirect(t *testing.T) {
	var redirected atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Add(1)
	}))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", destination.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	_, err := New(origin.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"true"}}, ExecOptions{})
	var start *RuntimeStartError
	if !errors.As(err, &start) || redirected.Load() != 0 {
		t.Fatalf("error/redirected = %T %v/%d", err, err, redirected.Load())
	}
}

func TestExecRejectsOversizedFrame(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintf(w, `{"type":"error","error_code":"invalid_request","message":"%s"}`, strings.Repeat("x", maxRuntimeFrameBytes))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := New(server.URL, "secret").Exec(ctx, ExecRequest{Argv: []string{"true"}}, ExecOptions{})
	if !IsRuntimeOutcomeUnknown(err) {
		t.Fatalf("error = %v", err)
	}
}
