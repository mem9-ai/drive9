package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	if !IsRuntimeOutcomeUnknown(err) || errors.As(err, &start) || redirected.Load() != 0 {
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

func TestExecStartErrorAfterStartIsUnknown(t *testing.T) {
	for _, code := range []string{"invalid_request", "unavailable", "start_failed"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
			_, _ = fmt.Fprintf(w, `{"type":"error","execution_id":"rex_12345678","error_code":%q,"message":"failed"}`+"\n", code)
		}))
		_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"true"}}, ExecOptions{})
		server.Close()
		var start *RuntimeStartError
		if !IsRuntimeOutcomeUnknown(err) || errors.As(err, &start) {
			t.Fatalf("%s after start = %T %v", code, err, err)
		}
	}
}

func TestExecHTTPFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		status  int
		unknown bool
	}{
		{status: http.StatusBadRequest}, {status: http.StatusUnauthorized}, {status: http.StatusForbidden},
		{status: http.StatusMultipleChoices, unknown: true}, {status: http.StatusFound, unknown: true},
		{status: http.StatusTemporaryRedirect, unknown: true}, {status: http.StatusNotModified, unknown: true},
		{status: http.StatusRequestTimeout, unknown: true},
		{status: http.StatusInternalServerError, unknown: true}, {status: http.StatusBadGateway, unknown: true},
		{status: http.StatusServiceUnavailable, unknown: true}, {status: http.StatusGatewayTimeout, unknown: true},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
		}))
		_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{Argv: []string{"true"}}, ExecOptions{})
		server.Close()
		var start *RuntimeStartError
		if IsRuntimeOutcomeUnknown(err) != tc.unknown || errors.As(err, &start) == tc.unknown {
			t.Errorf("status %d: error = %T %v", tc.status, err, err)
		}
	}
}

// A server may remove a completed execution as soon as its stream disconnects.
// Cancellation must reach it before that disconnect, not reinterpret a 404.
func TestExecCancellationKeepsStreamUntilConfirmedStop(t *testing.T) {
	for _, tc := range []struct {
		name, terminal, ack       string
		code                      int
		wantCanceled, wantUnknown bool
	}{
		{"confirmed", `{"type":"error","execution_id":"rex_12345678","error_code":"canceled","message":"stopped"}`, `{"canceled":true}`, 200, true, false},
		{"unknown stop", `{"type":"error","execution_id":"rex_12345678","error_code":"outcome_unknown","message":"unconfirmed"}`, `{"code":"outcome_unknown"}`, 503, false, true},
		{"missing record", "", `{"error":"execution is not active"}`, 404, false, true},
		{"completed before cancel", `{"type":"exit","execution_id":"rex_12345678","exit_code":0,"duration_ms":1}`, `{"error":"execution is not active"}`, 404, false, false},
		{"corrupt terminal", `not json`, `{"canceled":true}`, 200, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			cancelRequested := make(chan struct{})
			streamEnded := make(chan struct{})
			var execCalls, cancelCalls, earlyDisconnect atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/runtime/exec":
					execCalls.Add(1)
					defer close(streamEnded)
					w.Header().Set("Content-Type", "application/x-ndjson")
					_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
					w.(http.Flusher).Flush()
					select {
					case <-cancelRequested:
					case <-r.Context().Done():
						earlyDisconnect.Add(1)
						return
					}
					if tc.terminal != "" {
						_, _ = fmt.Fprintln(w, tc.terminal)
					}
				case "/v1/runtime/executions/rex_12345678/cancel":
					cancelCalls.Add(1)
					select {
					case <-streamEnded:
						w.WriteHeader(404)
						return
					default:
					}
					close(cancelRequested)
					w.WriteHeader(tc.code)
					_, _ = fmt.Fprintln(w, tc.ack)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := New(server.URL, "secret").Exec(ctx, ExecRequest{Argv: []string{"sleep", "60"}}, ExecOptions{OnStarted: func(ExecStarted) error { close(started); return nil }})
				result <- err
			}()
			<-started
			cancel()
			err := <-result
			var canceled *RuntimeCanceledError
			if errors.As(err, &canceled) != tc.wantCanceled || IsRuntimeOutcomeUnknown(err) != tc.wantUnknown || (!tc.wantCanceled && !tc.wantUnknown && err != nil) {
				t.Fatalf("error=%T %v", err, err)
			}
			if execCalls.Load() != 1 || cancelCalls.Load() != 1 || earlyDisconnect.Load() != 0 {
				t.Fatalf("exec=%d cancel=%d premature disconnect=%d", execCalls.Load(), cancelCalls.Load(), earlyDisconnect.Load())
			}
		})
	}
}

func TestExecAlreadyCanceledDoesNotSend(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := New(server.URL, "secret").Exec(ctx, ExecRequest{Argv: []string{"true"}}, ExecOptions{})
	if !errors.Is(err, context.Canceled) || requests.Load() != 0 {
		t.Fatalf("error=%v requests=%d", err, requests.Load())
	}
}

type runtimeRoundTripFunc func(*http.Request) (*http.Response, error)

func (f runtimeRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type runtimeReadCloser struct{ read func([]byte) (int, error) }

func (r runtimeReadCloser) Read(p []byte) (int, error) { return r.read(p) }
func (runtimeReadCloser) Close() error                 { return nil }

func runtimeTestResponse(r *http.Request, body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/x-ndjson"}}, Body: body, Request: r}
}

// Return a buffered started frame only after startup cancellation has aborted
// the request. This pins the ID-publication race without scheduler sleeps.
func TestExecCancelDuringStartedHandoff(t *testing.T) {
	for _, bufferedStart := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered-start=%t", bufferedStart), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var execCalls, cancelCalls atomic.Int32
			c := New("http://runtime.test", "secret")
			c.httpClient.Transport = runtimeRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/cancel") {
					cancelCalls.Add(1)
					if r.URL.Path != "/v1/runtime/executions/rex_12345678/cancel" {
						t.Errorf("cancel path = %s", r.URL.Path)
					}
					return runtimeTestResponse(r, io.NopCloser(strings.NewReader(`{"canceled":true}`))), nil
				}
				execCalls.Add(1)
				read := false
				return runtimeTestResponse(r, runtimeReadCloser{read: func(p []byte) (int, error) {
					if read {
						return 0, r.Context().Err()
					}
					read = true
					cancel()
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
						return 0, errors.New("startup cancellation did not abort the request")
					}
					if bufferedStart {
						return copy(p, "{\"type\":\"started\",\"execution_id\":\"rex_12345678\"}\n"), nil
					}
					return 0, r.Context().Err()
				}}), nil
			})
			began := time.Now()
			_, err := c.Exec(ctx, ExecRequest{Argv: []string{"true"}}, ExecOptions{})
			wantCancel := int32(0)
			if bufferedStart {
				wantCancel = 1
			}
			var unknown *RuntimeOutcomeUnknownError
			if !errors.As(err, &unknown) || (bufferedStart && unknown.ExecutionID != "rex_12345678") {
				t.Fatalf("error = %T %v", err, err)
			}
			if execCalls.Load() != 1 || cancelCalls.Load() != wantCancel || time.Since(began) >= time.Second {
				t.Fatalf("exec=%d cancel=%d elapsed=%s", execCalls.Load(), cancelCalls.Load(), time.Since(began))
			}
		})
	}
}

// Corrupt observations and local consumer failures must let the one in-flight
// cancel finish, but must never turn its ACK into a valid execution result.
func TestExecObservationFailureDoesNotAbortCancellation(t *testing.T) {
	for _, mode := range []string{"json", "frame", "base64", "oversized", "contradictory", "callback", "output"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cancelStarted, failureRead, releaseCancel := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseCancel) }) }
			defer release()
			var execCalls, cancelCalls atomic.Int32
			cancelAborted := make(chan struct{}, 1)
			c := New("http://runtime.test", "secret")
			c.httpClient.Transport = runtimeRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/cancel") {
					if cancelCalls.Add(1) == 1 {
						close(cancelStarted)
					}
					select {
					case <-r.Context().Done():
						select {
						case cancelAborted <- struct{}{}:
						default:
						}
						return nil, r.Context().Err()
					case <-releaseCancel:
						return runtimeTestResponse(r, io.NopCloser(strings.NewReader(`{"canceled":true}`))), nil
					}
				}
				execCalls.Add(1)
				reads := 0
				return runtimeTestResponse(r, runtimeReadCloser{read: func(p []byte) (int, error) {
					reads++
					if reads == 1 {
						return copy(p, "{\"type\":\"started\",\"execution_id\":\"rex_12345678\"}\n"), nil
					}
					if mode == "oversized" {
						if reads == 2 {
							close(failureRead)
						}
						return copy(p, strings.Repeat("x", len(p))), nil
					}
					if reads > 2 {
						return 0, io.EOF
					}
					close(failureRead)
					frame := "not json\n"
					switch mode {
					case "contradictory":
						frame = "{\"type\":\"error\",\"execution_id\":\"rex_12345678\",\"error_code\":\"start_failed\",\"message\":\"failed after start\"}\n"
					case "frame":
						frame = "{\"type\":\"exit\",\"execution_id\":\"rex_other000\",\"exit_code\":0,\"duration_ms\":1}\n"
					case "base64":
						frame = "{\"type\":\"stdout\",\"execution_id\":\"rex_12345678\",\"data_base64\":\"***\"}\n"
					case "output":
						frame = "{\"type\":\"stdout\",\"execution_id\":\"rex_12345678\",\"data_base64\":\"YQ==\"}\n"
					}
					return copy(p, frame), nil
				}}), nil
			})
			result := make(chan error, 1)
			go func() {
				_, err := c.Exec(ctx, ExecRequest{Argv: []string{"true"}}, ExecOptions{
					Stdout: shortRuntimeWriter{},
					OnStarted: func(ExecStarted) error {
						cancel()
						<-cancelStarted
						if mode == "callback" {
							close(failureRead)
							return errors.New("consumer stopped")
						}
						return nil
					},
				})
				result <- err
			}()
			select {
			case <-failureRead:
			case <-time.After(time.Second):
				t.Fatal("failure path was not reached")
			}
			select {
			case <-cancelAborted:
				t.Fatal("cleanup aborted the in-flight cancel")
			case err := <-result:
				t.Fatalf("returned before cancellation completed: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			release()
			select {
			case err := <-result:
				if !IsRuntimeOutcomeUnknown(err) {
					t.Fatalf("error = %T %v", err, err)
				}
			case <-time.After(time.Second):
				t.Fatal("Exec did not finish after cancel ACK")
			}
			if execCalls.Load() != 1 || cancelCalls.Load() != 1 {
				t.Fatalf("exec=%d cancel=%d", execCalls.Load(), cancelCalls.Load())
			}
		})
	}
}

func TestExecCancellationObservationIsBounded(t *testing.T) {
	for _, acknowledge := range []bool{true, false} {
		t.Run(fmt.Sprintf("acknowledged=%t", acknowledge), func(t *testing.T) {
			t.Parallel()
			started, streamClosed, cancelClosed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var execCalls, cancelCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/runtime/exec" {
					execCalls.Add(1)
					defer close(streamClosed)
					w.Header().Set("Content-Type", "application/x-ndjson")
					_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
					w.(http.Flusher).Flush()
					<-r.Context().Done() // Never send a terminal frame.
					return
				}
				cancelCalls.Add(1)
				defer close(cancelClosed)
				if acknowledge {
					_, _ = fmt.Fprintln(w, `{"canceled":true}`)
					return
				}
				<-r.Context().Done() // Also exercise a stalled cancel RPC.
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := New(server.URL, "secret").Exec(ctx, ExecRequest{Argv: []string{"sleep", "60"}}, ExecOptions{OnStarted: func(ExecStarted) error { close(started); return nil }})
				result <- err
			}()
			<-started
			began := time.Now()
			cancel()
			select {
			case err := <-result:
				var canceled *RuntimeCanceledError
				if errors.As(err, &canceled) != acknowledge || IsRuntimeOutcomeUnknown(err) == acknowledge {
					t.Fatalf("error=%T %v", err, err)
				}
			case <-time.After(runtimeCancelTimeout + 5*time.Second):
				server.CloseClientConnections()
				t.Fatal("cancellation did not return within its observation bound")
			}
			if elapsed := time.Since(began); elapsed < runtimeCancelTimeout-time.Second {
				t.Fatalf("stream abandoned before observation deadline: %v", elapsed)
			}
			for _, closed := range []chan struct{}{streamClosed, cancelClosed} {
				select {
				case <-closed:
				case <-time.After(time.Second):
					t.Fatal("request remained active after Exec returned")
				}
			}
			if execCalls.Load() != 1 || cancelCalls.Load() != 1 {
				t.Fatalf("exec=%d cancel=%d", execCalls.Load(), cancelCalls.Load())
			}
		})
	}
}
