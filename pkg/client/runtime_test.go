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

	_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{}, ExecOptions{})
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
			_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{}, ExecOptions{})
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
	_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{}, ExecOptions{Stdout: shortRuntimeWriter{}})
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
			_, err := New(server.URL, "secret").Exec(t.Context(), ExecRequest{}, ExecOptions{})
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
		_, err := New(server.URL, "secret").Exec(ctx, ExecRequest{}, ExecOptions{OnStarted: func(ExecStarted) error {
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
		_ = json.NewEncoder(w).Encode(RuntimeCapabilities{
			Version: 1, Streaming: true, SeparateIO: true, Cancel: true, CancelScope: "active_execution",
		})
	}))
	defer server.Close()
	capabilities, err := New(server.URL, "secret").GetRuntimeCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Version != 1 || !capabilities.Streaming || !capabilities.SeparateIO || !capabilities.Cancel {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

func TestRuntimeCapabilitiesRejectDuplicates(t *testing.T) {
	err := validateRuntimeCapabilities(RuntimeCapabilities{
		Version: 1, Streaming: true, SeparateIO: true,
		Providers: []RuntimeProviderCapability{
			{Provider: "docker", Profiles: []string{"default"}},
			{Provider: "docker", Profiles: []string{"large"}},
		},
	})
	if err == nil {
		t.Fatal("duplicate provider error = nil")
	}
}

func TestRuntimeCapabilitiesAcceptsProviderAwareCandidates(t *testing.T) {
	capabilities := RuntimeCapabilities{
		Version: 1, Streaming: true, SeparateIO: true,
		Providers: []RuntimeProviderCapability{{
			Provider: "docker", Profiles: []string{"default", "workspace"},
			Candidates: []RuntimeCandidateCapability{{
				Profile: "default", ExecutionClass: "linux-full", Persistence: RuntimePersistenceFullRoot,
				RootFS: RuntimeRootFSIdentity{
					Driver: runtimeRootFSUserUnion, CapabilityVersion: runtimeRootFSCapabilityV4,
					ConfigHash: strings.Repeat("a", 64), LowerImageDigest: "sha256:" + strings.Repeat("b", 64),
				},
				Capabilities: map[string]bool{
					RuntimeCapabilityWorkspace: true, RuntimeCapabilityFullRoot: true, ExtentXattrProtocolV1: true,
				},
				ProductionEligible: true, BoundedSelectionEligible: true,
			}, {
				Profile: "workspace", ExecutionClass: "linux-full", Persistence: RuntimePersistenceWorkspace,
				RootFS: RuntimeRootFSIdentity{
					Driver: runtimeRootFSWorkspace, CapabilityVersion: runtimeWorkspaceCapability,
					ConfigHash: strings.Repeat("c", 64), LowerImageDigest: "sha256:" + strings.Repeat("d", 64),
				},
				Capabilities:       map[string]bool{RuntimeCapabilityWorkspace: true},
				ProductionEligible: false, BoundedSelectionEligible: false,
			}},
		}},
	}
	if err := validateRuntimeCapabilities(capabilities); err != nil {
		t.Fatal(err)
	}
	capabilities.Providers[0].Candidates[0].Persistence = ""
	if err := validateRuntimeCapabilities(capabilities); err == nil {
		t.Fatal("candidate without persistence error = nil")
	}
	capabilities.Providers[0].Candidates[0].Persistence = RuntimePersistenceFullRoot
	capabilities.Providers[0].Candidates[0].RootFS.CapabilityVersion = "drive9_rootfs.user_union.extent.v3"
	if err := validateRuntimeCapabilities(capabilities); err == nil {
		t.Fatal("v3 rootfs candidate error = nil")
	}
	capabilities.Providers[0].Candidates[0].RootFS.CapabilityVersion = runtimeRootFSCapabilityV4
	delete(capabilities.Providers[0].Candidates[0].Capabilities, ExtentXattrProtocolV1)
	if err := validateRuntimeCapabilities(capabilities); err == nil {
		t.Fatal("candidate without negotiated extent xattr capability error = nil")
	}
	capabilities.Providers[0].Candidates[0].Capabilities[ExtentXattrProtocolV1] = true
	capabilities.Providers[0].Candidates[0].Profile = "missing"
	if err := validateRuntimeCapabilities(capabilities); err == nil {
		t.Fatal("candidate outside provider profiles error = nil")
	}
	capabilities.Providers[0].Candidates[0].Profile = "default"
	capabilities.Providers[0].Candidates[1].Capabilities[RuntimeCapabilityFullRoot] = true
	if err := validateRuntimeCapabilities(capabilities); err == nil {
		t.Fatal("workspace-only candidate with full-root capability error = nil")
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
	_, err := New(origin.URL, "secret").Exec(t.Context(), ExecRequest{}, ExecOptions{})
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
	_, err := New(server.URL, "secret").Exec(ctx, ExecRequest{}, ExecOptions{})
	if !IsRuntimeOutcomeUnknown(err) {
		t.Fatalf("error = %v", err)
	}
}
