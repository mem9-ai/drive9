package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestExecStreamsOutputAndReturnsRemoteStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
		_, _ = fmt.Fprintln(w, `{"type":"stdout","execution_id":"rex_12345678","data_base64":"b3V0"}`)
		_, _ = fmt.Fprintln(w, `{"type":"stderr","execution_id":"rex_12345678","data_base64":"ZXJy"}`)
		_, _ = fmt.Fprintln(w, `{"type":"exit","execution_id":"rex_12345678","exit_code":9,"duration_ms":5}`)
	}))
	defer server.Close()

	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvServer, server.URL)
	t.Setenv(EnvAPIKey, "secret")
	stdout, stderr, resultErr := captureExecOutput(t, func() error {
		return Exec([]string{"--workspace=:/project", "--env", "A=B", "--", "sh", "-c", "exit 9"})
	})
	if stdout != "out" || stderr != "err" {
		t.Fatalf("stdout/stderr = %q/%q", stdout, stderr)
	}
	var status *runtimeExitError
	if !errors.As(resultErr, &status) || status.ExitCode() != 9 {
		t.Fatalf("error = %#v", resultErr)
	}
}

func captureExecOutput(t *testing.T, run func() error) (string, string, error) {
	t.Helper()
	originalStdout, originalStderr := os.Stdout, os.Stderr
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout, os.Stderr = stdoutW, stderrW
	runErr := run()
	os.Stdout, os.Stderr = originalStdout, originalStderr
	_ = stdoutW.Close()
	_ = stderrW.Close()
	stdout, _ := io.ReadAll(stdoutR)
	stderr, _ := io.ReadAll(stderrR)
	return string(stdout), string(stderr), runErr
}

func TestRuntimeEnvironmentValidation(t *testing.T) {
	if _, err := runtimeEnvironment([]string{"A=1", "A=2"}); err == nil {
		t.Fatal("duplicate env key accepted")
	}
	for _, invalid := range []string{"NO_EQUALS", "1BAD=x", "BAD-KEY=x"} {
		if _, err := runtimeEnvironment([]string{invalid}); err == nil {
			t.Fatalf("invalid env %q accepted", invalid)
		}
	}
	got, err := runtimeEnvironment([]string{"A=1", "EMPTY="})
	if err != nil || got["A"] != "1" || got["EMPTY"] != "" {
		t.Fatalf("environment/error = %#v/%v", got, err)
	}
}

func TestExecUnknownOutcomeIsExplicit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
	}))
	defer server.Close()
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvServer, server.URL)
	t.Setenv(EnvAPIKey, "secret")
	err := Exec([]string{"--", "true"})
	if err == nil || !strings.Contains(err.Error(), "may have run and was not retried") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecCancellationRequestsRemoteStop(t *testing.T) {
	stopped := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/exec":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = fmt.Fprintln(w, `{"type":"started","execution_id":"rex_12345678"}`)
			_, _ = fmt.Fprintln(w, `{"type":"stdout","execution_id":"rex_12345678","data_base64":"cmVhZHk="}`)
			w.(http.Flusher).Flush()
			<-stopped
		case "/v1/runtime/executions/rex_12345678/cancel":
			once.Do(func() { close(stopped) })
			_ = json.NewEncoder(w).Encode(map[string]bool{"canceled": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvServer, server.URL)
	t.Setenv(EnvAPIKey, "secret")
	originalStdout := os.Stdout
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = stdoutW
	t.Cleanup(func() {
		os.Stdout = originalStdout
		_ = stdoutW.Close()
		_ = stdoutR.Close()
	})

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- execWithContext(ctx, []string{"--", "sleep", "30"}) }()
	ready := make([]byte, len("ready"))
	if _, err := io.ReadFull(stdoutR, ready); err != nil || string(ready) != "ready" {
		t.Fatalf("ready output/error = %q/%v", ready, err)
	}
	cancel()
	err = <-result
	if err == nil || !strings.Contains(err.Error(), "canceled and stopped") || strings.Contains(err.Error(), "outcome is unknown") {
		t.Fatalf("error = %v", err)
	}
}

func TestRuntimeWorkspaceRoot(t *testing.T) {
	for _, invalid := range []string{"", "relative", "/a/..", "/a//b"} {
		if _, err := runtimeWorkspaceRoot(invalid); err == nil {
			t.Fatalf("workspace %q accepted", invalid)
		}
	}
	for _, valid := range []string{"/", ":/project"} {
		if _, err := runtimeWorkspaceRoot(valid); err != nil {
			t.Fatalf("workspace %q: %v", valid, err)
		}
	}
}
