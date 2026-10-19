package cli

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
	"testing"
)

type execTestRequest struct {
	Argv      []string          `json:"argv"`
	Cwd       string            `json:"cwd"`
	Env       map[string]string `json:"env"`
	TimeoutMS int64             `json:"timeout_ms"`
	Workspace struct {
		Root     string `json:"root"`
		LayerID  string `json:"layer_id"`
		ReadOnly bool   `json:"read_only"`
	} `json:"workspace"`
}

// execTestServer answers /v1/runtime/exec with the given frames and records
// the decoded request.
func execTestServer(t *testing.T, frames ...string) (*execTestRequest, *httptest.Server) {
	t.Helper()
	var request execTestRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, frame := range frames {
			_, _ = fmt.Fprintln(w, frame)
		}
	}))
	t.Cleanup(server.Close)
	useExecServer(t, server.URL)
	return &request, server
}

func useExecServer(t *testing.T, url string) {
	t.Helper()
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvServer, url)
	t.Setenv(EnvAPIKey, "secret")
}

func runExecForTest(t *testing.T, ctx context.Context, args ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := runExec(ctx, args, &execIDBox{}, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

const (
	execStarted = `{"type":"started","execution_id":"rex_12345678","cwd":"/workspace","sandbox":"new"}`
	execExit0   = `{"type":"exit","execution_id":"rex_12345678","exit_code":0,"duration_ms":5}`
)

func TestExecStreamsOutputAndPassesExitStatusQuietly(t *testing.T) {
	request, _ := execTestServer(t,
		execStarted,
		`{"type":"stdout","execution_id":"rex_12345678","data_base64":"b3V0"}`,
		`{"type":"heartbeat"}`,
		`{"type":"stderr","execution_id":"rex_12345678","data_base64":"ZXJy"}`,
		`{"type":"exit","execution_id":"rex_12345678","exit_code":9,"duration_ms":5}`,
	)
	stdout, stderr, err := runExecForTest(t, t.Context(), "-w", ":/project", "--layer=fork-1", "-C", "src", "-e", "A=B", "-t", "2m", "--", "sh", "-c", "exit 9")
	if stdout != "out" || stderr != "err" {
		t.Fatalf("stdout/stderr = %q/%q", stdout, stderr)
	}
	var status *execExitError
	if !errors.As(err, &status) || status.ExitCode() != 9 || !status.SilentExit() {
		t.Fatalf("error = %#v", err)
	}
	if strings.Join(request.Argv, " ") != "sh -c exit 9" || request.Workspace.Root != "/project" || request.Workspace.LayerID != "fork-1" ||
		request.Cwd != "src" || request.Env["A"] != "B" || request.TimeoutMS != 120000 {
		t.Fatalf("request = %#v", request)
	}
}

func TestExecWithoutDashDashAndDefaults(t *testing.T) {
	request, _ := execTestServer(t, execStarted, execExit0)
	if _, _, err := runExecForTest(t, t.Context(), "npm", "test", "--watch=false"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(request.Argv, " ") != "npm test --watch=false" || request.Workspace.Root != "/" || request.Cwd != "" {
		t.Fatalf("request = %#v", request)
	}
}

func TestExecSingleShellLineRunsThroughShell(t *testing.T) {
	request, _ := execTestServer(t, execStarted, execExit0)
	if _, _, err := runExecForTest(t, t.Context(), "npm ci && npm test"); err != nil {
		t.Fatal(err)
	}
	if len(request.Argv) != 3 || request.Argv[0] != "/bin/sh" || request.Argv[1] != "-c" || request.Argv[2] != "npm ci && npm test" {
		t.Fatalf("argv = %q", request.Argv)
	}
	request, _ = execTestServer(t, execStarted, execExit0)
	if _, _, err := runExecForTest(t, t.Context(), "make"); err != nil {
		t.Fatal(err)
	}
	if len(request.Argv) != 1 || request.Argv[0] != "make" {
		t.Fatalf("plain single word argv = %q", request.Argv)
	}
}

func TestExecEnvCopiesBareKeysFromLocalEnvironment(t *testing.T) {
	request, _ := execTestServer(t, execStarted, execExit0)
	t.Setenv("DRIVE9_EXEC_TEST_TOKEN", "local-value")
	if _, _, err := runExecForTest(t, t.Context(), "-e", "DRIVE9_EXEC_TEST_TOKEN", "--env", "EMPTY=", "true"); err != nil {
		t.Fatal(err)
	}
	if request.Env["DRIVE9_EXEC_TEST_TOKEN"] != "local-value" || request.Env["EMPTY"] != "" {
		t.Fatalf("env = %#v", request.Env)
	}
	for _, invalid := range [][]string{{"A=1", "A=2"}, {"1BAD=x"}, {"BAD-KEY=x"}, {"DRIVE9_EXEC_TEST_MISSING"}} {
		if _, err := runtimeEnvironment(invalid); err == nil {
			t.Errorf("env %q accepted", invalid)
		}
	}
}

func TestExecVerboseShowsSandbox(t *testing.T) {
	execTestServer(t, `{"type":"started","execution_id":"rex_12345678","cwd":"/workspace/src","sandbox":"warm"}`, execExit0)
	_, stderr, err := runExecForTest(t, t.Context(), "-v", "true")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr, "running in a warm sandbox at /workspace/src (rex_12345678)") || !strings.Contains(stderr, "drive9: exit 0 after") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestExecErrorsAreActionable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		handler  http.HandlerFunc
		wantCode int
		want     string
	}{
		{
			name: "outcome unknown",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/x-ndjson")
				_, _ = fmt.Fprintln(w, execStarted)
			},
			wantCode: execExitUnknown, want: "may have finished",
		},
		{
			name: "timeout",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/x-ndjson")
				_, _ = fmt.Fprintln(w, execStarted)
				_, _ = fmt.Fprintln(w, `{"type":"error","execution_id":"rex_12345678","error_code":"timed_out","message":"the command timed out and was stopped"}`)
			},
			wantCode: execExitTimedOut, want: "timed out",
		},
		{
			name: "busy",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"too many commands are already running for this workspace owner (limit 4); try again shortly","code":"busy"}`))
			},
			wantCode: 1, want: "retry in 5s",
		},
		{
			name:     "not enabled",
			handler:  http.NotFound,
			wantCode: 0, want: "remote exec is not enabled on this server",
		},
		{
			name: "rejected request",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"relative cwd must stay inside the workspace","code":"invalid_request"}`))
			},
			wantCode: 0, want: "relative cwd must stay inside the workspace",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			useExecServer(t, server.URL)
			_, _, err := runExecForTest(t, t.Context(), "true")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if tc.wantCode != 0 {
				var coded interface{ ExitCode() int }
				if !errors.As(err, &coded) || coded.ExitCode() != tc.wantCode {
					t.Fatalf("exit code for %v, want %d", err, tc.wantCode)
				}
			}
		})
	}
}

func TestExecInterruptCancelsRemoteCommand(t *testing.T) {
	stopped := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runtime/exec":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = fmt.Fprintln(w, execStarted)
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
	useExecServer(t, server.URL)
	stdoutR, stdoutW := io.Pipe()
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		result <- runExec(ctx, []string{"sleep", "30"}, &execIDBox{}, stdoutW, io.Discard)
		_ = stdoutW.Close()
	}()
	ready := make([]byte, len("ready"))
	if _, err := io.ReadFull(stdoutR, ready); err != nil || string(ready) != "ready" {
		t.Fatalf("ready output/error = %q/%v", ready, err)
	}
	cancel()
	go func() { _, _ = io.Copy(io.Discard, stdoutR) }()
	err := <-result
	var coded interface{ ExitCode() int }
	if err == nil || !strings.Contains(err.Error(), "canceled") || !errors.As(err, &coded) || coded.ExitCode() != execExitCanceled {
		t.Fatalf("error = %v", err)
	}
}

func TestExecUsageErrors(t *testing.T) {
	if _, _, err := runExecForTest(t, t.Context()); err == nil || !strings.Contains(err.Error(), "missing command") || !strings.Contains(err.Error(), "drive9 exec --help") {
		t.Fatalf("missing command error = %v", err)
	}
	var help *execExitError
	if _, _, err := runExecForTest(t, t.Context(), "--help"); !errors.As(err, &help) || help.ExitCode() != 0 {
		t.Fatalf("help error = %v", err)
	}
	for _, args := range [][]string{
		{"--persistence=full_root", "true"},
		{"--cpu-millis=1000", "true"},
		{"-w", "relative", "true"},
		{"--layer", "bad:layer", "true"},
	} {
		if _, _, err := runExecForTest(t, t.Context(), args...); err == nil {
			t.Errorf("%q accepted", args)
		}
	}
}
