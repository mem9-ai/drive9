package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

type fakeRuntimeClient struct {
	fileKey          string
	fileRequest      client.RuntimeFileOperationRequest
	executionKey     string
	executionRequest client.RuntimeExecutionRequest
	executionGets    int
	executionCancels int
	executionErr     error
	executionStates  []*client.RuntimeOperation
	events           []client.RuntimeEvent
	watchCalls       int
	artifacts        map[string][]byte
}

func (f *fakeRuntimeClient) RuntimeCapabilities(context.Context) (*client.RuntimeCapabilities, error) {
	return &client.RuntimeCapabilities{Enabled: true, ProtocolVersion: client.RuntimeProtocolVersion}, nil
}
func (f *fakeRuntimeClient) SubmitRuntimeExecution(_ context.Context, key string, request client.RuntimeExecutionRequest) (*client.RuntimeOperation, error) {
	f.executionKey, f.executionRequest = key, request
	if f.executionErr != nil {
		return nil, f.executionErr
	}
	return &client.RuntimeOperation{ID: "execution_1", Kind: "execution", State: "queued"}, nil
}

func TestResolveRuntimeWorkspaceCreatesOneShotScopeFromSubmissionKey(t *testing.T) {
	selection, err := resolveRuntimeWorkspace("", "", ":/project", "", "request-42")
	if err != nil {
		t.Fatal(err)
	}
	if selection.input == nil || selection.input.Source.Root != "/project" || selection.input.ClientScopeKey != "drive9-cli-workspace-request-42" {
		t.Fatalf("selection = %#v", selection)
	}
	if _, err := resolveRuntimeWorkspace("wr_1", "", ":/project", "", "request-42"); err == nil {
		t.Fatal("expected workspace selector conflict")
	}
}

func TestRuntimeExecForwardsOnlyExplicitEnvironment(t *testing.T) {
	fake := &fakeRuntimeClient{}
	originalFactory := runtimeClientFactory
	originalStdout := os.Stdout
	t.Cleanup(func() {
		runtimeClientFactory = originalFactory
		os.Stdout = originalStdout
	})
	runtimeClientFactory = func() runtimeAPI { return fake }
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(io.Discard, r)
		done <- struct{}{}
	}()
	err = RuntimeExec([]string{"--workspace", ":/project", "--env", "GOFLAGS=-mod=readonly", "--idempotency-key", "request-42", "--", "go", "test", "./..."})
	_ = w.Close()
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if fake.executionRequest.Workspace == nil || fake.executionRequest.Workspace.ClientScopeKey != "drive9-cli-workspace-request-42" {
		t.Fatalf("workspace = %#v", fake.executionRequest.Workspace)
	}
	if len(fake.executionRequest.Execution.Environment) != 1 || fake.executionRequest.Execution.Environment["GOFLAGS"] != "-mod=readonly" {
		t.Fatalf("environment = %#v", fake.executionRequest.Execution.Environment)
	}
	if fake.executionKey != "request-42" {
		t.Fatalf("idempotency key = %q", fake.executionKey)
	}
	if fake.executionGets != 1 {
		t.Fatalf("execution gets = %d, want default wait", fake.executionGets)
	}
}

func TestRuntimeEnvironmentRejectsNonPortableNames(t *testing.T) {
	for _, value := range []string{"1BAD=value", "BAD NAME=value", "BAD-NAME=value", "=value", "MISSING"} {
		var environment runtimeEnvironment
		if err := environment.Set(value); err == nil {
			t.Fatalf("Set(%q) error = nil", value)
		}
	}
}
func (f *fakeRuntimeClient) SubmitRuntimeFileOperation(_ context.Context, key string, request client.RuntimeFileOperationRequest) (*client.RuntimeOperation, error) {
	f.fileKey, f.fileRequest = key, request
	return &client.RuntimeOperation{ID: "file_1", Kind: "file_operation", State: "succeeded"}, nil
}
func (f *fakeRuntimeClient) GetRuntimeExecution(context.Context, string) (*client.RuntimeOperation, error) {
	f.executionGets++
	if len(f.executionStates) > 0 {
		index := f.executionGets - 1
		if index >= len(f.executionStates) {
			index = len(f.executionStates) - 1
		}
		return f.executionStates[index], nil
	}
	return &client.RuntimeOperation{ID: "execution_1", Kind: "execution", State: "succeeded"}, nil
}

func TestResolveRuntimeWorkspaceNormalizesOnlyCurrentDrive9Paths(t *testing.T) {
	for _, test := range []struct {
		name      string
		root      string
		want      string
		wantError bool
	}{
		{name: "cli path", root: ":/project", want: "/project"},
		{name: "absolute path", root: "/project", want: "/project"},
		{name: "drive9 uri", root: "drive9:///project", want: "/project"},
		{name: "named context", root: "production:/project", wantError: true},
		{name: "relative local", root: "project", wantError: true},
		{name: "object", root: "s3://bucket/project", wantError: true},
		{name: "local uri", root: "file:///tmp/project", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			selection, err := resolveRuntimeWorkspace("", test.root, "", "scope", "request")
			if (err != nil) != test.wantError {
				t.Fatalf("resolveRuntimeWorkspace error = %v", err)
			}
			if err == nil && selection.input.Source.Root != test.want {
				t.Fatalf("root = %q, want %q", selection.input.Source.Root, test.want)
			}
		})
	}
}

func TestRuntimeExecDetachSkipsWait(t *testing.T) {
	fake := &fakeRuntimeClient{}
	originalFactory := runtimeClientFactory
	originalStdout := os.Stdout
	t.Cleanup(func() {
		runtimeClientFactory = originalFactory
		os.Stdout = originalStdout
	})
	runtimeClientFactory = func() runtimeAPI { return fake }
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(io.Discard, r)
		done <- struct{}{}
	}()
	err = RuntimeExec([]string{"--workspace", ":/project", "--idempotency-key", "request-detach", "--detach", "--", "true"})
	_ = w.Close()
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if fake.executionGets != 0 {
		t.Fatalf("execution gets = %d, want detached submit", fake.executionGets)
	}
}

func TestRuntimeExecInterruptCancelsAndReportsTerminalState(t *testing.T) {
	fake := &fakeRuntimeClient{}
	originalFactory := runtimeClientFactory
	originalInterrupt := runtimeInterruptContext
	originalStdout := os.Stdout
	t.Cleanup(func() {
		runtimeClientFactory = originalFactory
		runtimeInterruptContext = originalInterrupt
		os.Stdout = originalStdout
	})
	runtimeClientFactory = func() runtimeAPI { return fake }
	runtimeInterruptContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(io.Discard, r)
		done <- struct{}{}
	}()
	err = RuntimeExec([]string{"--workspace", ":/project", "--idempotency-key", "request-interrupt", "--", "sleep", "30"})
	_ = w.Close()
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if fake.executionCancels != 1 {
		t.Fatalf("execution cancels = %d", fake.executionCancels)
	}
}

func TestRuntimeRecoveryRequiresExplicitAction(t *testing.T) {
	if err := RuntimeExecution([]string{"recover", "execution-1"}); err == nil || !strings.Contains(err.Error(), "exactly one recovery action") {
		t.Fatalf("missing action error = %v", err)
	}
	if err := RuntimeExecution([]string{"recover", "execution-1", "--reconcile", "--discard-uncommitted"}); err == nil ||
		!strings.Contains(err.Error(), "exactly one recovery action") {
		t.Fatalf("conflicting action error = %v", err)
	}
}
func (f *fakeRuntimeClient) GetRuntimeFileOperation(context.Context, string) (*client.RuntimeOperation, error) {
	panic("unexpected file get")
}
func (f *fakeRuntimeClient) CancelRuntimeExecution(context.Context, string) (*client.RuntimeOperation, error) {
	f.executionCancels++
	return &client.RuntimeOperation{ID: "execution_1", Kind: "execution", State: "canceled"}, nil
}
func (f *fakeRuntimeClient) CancelRuntimeFileOperation(context.Context, string) (*client.RuntimeOperation, error) {
	panic("unexpected file cancel")
}
func (f *fakeRuntimeClient) RecoverRuntimeExecution(context.Context, string, string, string) (*client.RuntimeRecovery, error) {
	panic("unexpected execution recovery")
}
func (f *fakeRuntimeClient) RecoverRuntimeFileOperation(context.Context, string, string, string) (*client.RuntimeRecovery, error) {
	panic("unexpected file recovery")
}
func (f *fakeRuntimeClient) GetRuntimeRecovery(context.Context, string) (*client.RuntimeRecovery, error) {
	panic("unexpected recovery get")
}
func (f *fakeRuntimeClient) WatchRuntimeEvents(_ context.Context, collection, id string, after int64, onEvent func(client.RuntimeEvent) error) error {
	f.watchCalls++
	if collection != "executions" || id == "" || after != 0 {
		return fmt.Errorf("unexpected watch request")
	}
	for _, event := range f.events {
		if err := onEvent(event); err != nil {
			return err
		}
	}
	return nil
}
func (f *fakeRuntimeClient) DownloadRuntimeArtifact(_ context.Context, id string) ([]byte, error) {
	content, ok := f.artifacts[id]
	if !ok {
		return nil, fmt.Errorf("missing artifact %s", id)
	}
	return content, nil
}

func TestRuntimeExecutionLogsReplaysTerminalArtifact(t *testing.T) {
	fake := &fakeRuntimeClient{
		executionStates: []*client.RuntimeOperation{{ID: "execution_1", State: "succeeded", LogArtifactID: "artifact_1"}},
		artifacts:       map[string][]byte{"artifact_1": []byte("complete output\n")},
	}
	var output bytes.Buffer
	if err := runtimeExecutionLogs(context.Background(), fake, "execution_1", false, &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != "complete output\n" || fake.watchCalls != 0 {
		t.Fatalf("output = %q, watch calls = %d", output.String(), fake.watchCalls)
	}
}

func TestRuntimeExecutionLogsRequiresFollowForActiveExecution(t *testing.T) {
	fake := &fakeRuntimeClient{executionStates: []*client.RuntimeOperation{{ID: "execution_1", State: "running"}}}
	err := runtimeExecutionLogs(context.Background(), fake, "execution_1", false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--follow") {
		t.Fatalf("error = %v", err)
	}
	if fake.watchCalls != 0 {
		t.Fatalf("watch calls = %d", fake.watchCalls)
	}
}

func TestRuntimeExecutionLogsFollowsEventsAndCompletesFromArtifact(t *testing.T) {
	prefix := []byte("first chunk\n")
	fake := &fakeRuntimeClient{
		executionStates: []*client.RuntimeOperation{
			{ID: "execution_1", State: "running"},
			{ID: "execution_1", State: "succeeded", LogArtifactID: "artifact_1"},
		},
		events: []client.RuntimeEvent{{Kind: "output", Payload: json.RawMessage(fmt.Sprintf(
			`{"stream":"combined","data_base64":%q,"truncated":true}`, base64.StdEncoding.EncodeToString(prefix)))}},
		artifacts: map[string][]byte{"artifact_1": append(append([]byte(nil), prefix...), []byte("remaining\n")...)},
	}
	var output bytes.Buffer
	if err := runtimeExecutionLogs(context.Background(), fake, "execution_1", true, &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != "first chunk\nremaining\n" || fake.watchCalls != 1 {
		t.Fatalf("output = %q, watch calls = %d", output.String(), fake.watchCalls)
	}
}

func TestRuntimeExecutionLogsRejectsArtifactThatDoesNotMatchReplay(t *testing.T) {
	fake := &fakeRuntimeClient{
		executionStates: []*client.RuntimeOperation{
			{ID: "execution_1", State: "running"},
			{ID: "execution_1", State: "succeeded", LogArtifactID: "artifact_1"},
		},
		events: []client.RuntimeEvent{{Kind: "output", Payload: json.RawMessage(
			`{"stream":"combined","data_base64":"cHJlZml4","truncated":false}`)}},
		artifacts: map[string][]byte{"artifact_1": []byte("different")},
	}
	err := runtimeExecutionLogs(context.Background(), fake, "execution_1", true, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("error = %v", err)
	}
}

func TestRuntimeWorkspaceRequiresExactlyOneSource(t *testing.T) {
	for _, test := range []struct {
		name               string
		ref, root, scope   string
		wantRef, wantInput bool
		wantError          bool
	}{
		{name: "ref", ref: "runtime://workspace", wantRef: true},
		{name: "input", root: "/project", scope: "agent", wantInput: true},
		{name: "missing", wantError: true},
		{name: "both", ref: "runtime://workspace", root: "/project", scope: "agent", wantError: true},
		{name: "partial input", root: "/project", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			selection, err := runtimeWorkspace(test.ref, test.root, test.scope)
			if (err != nil) != test.wantError {
				t.Fatalf("runtimeWorkspace error = %v", err)
			}
			if err == nil && (selection.ref != "") != test.wantRef {
				t.Fatalf("selection ref = %q", selection.ref)
			}
			if err == nil && (selection.input != nil) != test.wantInput {
				t.Fatalf("selection input = %#v", selection.input)
			}
		})
	}
}

func TestRuntimeExecPersistsAndReusesReceiptAfterAmbiguousSubmit(t *testing.T) {
	receiptPath := filepath.Join(t.TempDir(), "submit.json")
	fake := &fakeRuntimeClient{executionErr: errors.New("connection reset")}
	originalFactory := runtimeClientFactory
	t.Cleanup(func() { runtimeClientFactory = originalFactory })
	runtimeClientFactory = func() runtimeAPI { return fake }

	err := RuntimeExec([]string{"--workspace", ":/project", "--idempotency-receipt", receiptPath, "--detach", "--", "true"})
	if err == nil || !strings.Contains(err.Error(), receiptPath) {
		t.Fatalf("submit error = %v", err)
	}
	firstKey := fake.executionKey
	info, statErr := os.Stat(receiptPath)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("receipt mode = %o", info.Mode().Perm())
	}

	fake.executionErr = nil
	originalStdout := os.Stdout
	t.Cleanup(func() { os.Stdout = originalStdout })
	r, w, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	os.Stdout = w
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(io.Discard, r)
		done <- struct{}{}
	}()
	err = RuntimeExec([]string{"--workspace", ":/project", "--idempotency-receipt", receiptPath, "--detach", "--", "true"})
	_ = w.Close()
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if fake.executionKey != firstKey {
		t.Fatalf("retry key = %q, want %q", fake.executionKey, firstKey)
	}
	if _, err := os.Stat(receiptPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("accepted receipt still exists: %v", err)
	}
}

func TestRuntimeReceiptRejectsChangedRequestAndUnsafeFile(t *testing.T) {
	receiptPath := filepath.Join(t.TempDir(), "submit.json")
	fake := &fakeRuntimeClient{executionErr: errors.New("connection reset")}
	originalFactory := runtimeClientFactory
	t.Cleanup(func() { runtimeClientFactory = originalFactory })
	runtimeClientFactory = func() runtimeAPI { return fake }

	if err := RuntimeExec([]string{"--workspace", ":/project", "--idempotency-receipt", receiptPath, "--detach", "--", "true"}); err == nil {
		t.Fatal("expected ambiguous submit error")
	}
	fake.executionErr = nil
	if err := RuntimeExec([]string{"--workspace", ":/project", "--idempotency-receipt", receiptPath, "--detach", "--", "false"}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("changed request error = %v", err)
	}
	if err := os.Chmod(receiptPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RuntimeExec([]string{"--workspace", ":/project", "--idempotency-receipt", receiptPath, "--detach", "--", "true"}); err == nil || !strings.Contains(err.Error(), "owner-only") {
		t.Fatalf("unsafe receipt error = %v", err)
	}
}

func TestRuntimeReceiptCompletionIsIdempotent(t *testing.T) {
	receiptPath := filepath.Join(t.TempDir(), "submit.json")
	if err := os.WriteFile(receiptPath, []byte("receipt"), 0o600); err != nil {
		t.Fatal(err)
	}
	state := runtimeReceiptState{path: receiptPath}
	if err := state.complete(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(receiptPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("accepted receipt still exists: %v", err)
	}
	if err := state.complete(); err != nil {
		t.Fatalf("repeated completion: %v", err)
	}
}

func TestRuntimeExecUsesDocumentedShellAndTimeoutFlags(t *testing.T) {
	fake := &fakeRuntimeClient{}
	originalFactory := runtimeClientFactory
	originalStdout := os.Stdout
	t.Cleanup(func() {
		runtimeClientFactory = originalFactory
		os.Stdout = originalStdout
	})
	runtimeClientFactory = func() runtimeAPI { return fake }
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(io.Discard, r)
		done <- struct{}{}
	}()
	err = RuntimeExec([]string{"--workspace", ":/project", "--idempotency-key", "shell-timeout", "--timeout", "2m", "--shell-script", "go test ./..."})
	_ = w.Close()
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if fake.executionRequest.Execution.Shell != "go test ./..." || fake.executionRequest.Execution.TimeoutSeconds != 120 {
		t.Fatalf("execution = %#v", fake.executionRequest.Execution)
	}
	if err := RuntimeExec([]string{"--workspace", ":/project", "--idempotency-key", "bad-timeout", "--timeout", "1500ms", "--", "true"}); err == nil {
		t.Fatal("expected fractional-second timeout rejection")
	}
}

func TestRuntimeFSBuildsOneDurableOperation(t *testing.T) {
	fake := &fakeRuntimeClient{}
	originalFactory := runtimeClientFactory
	originalStdout := os.Stdout
	t.Cleanup(func() {
		runtimeClientFactory = originalFactory
		os.Stdout = originalStdout
	})
	runtimeClientFactory = func() runtimeAPI { return fake }
	originalReceiptDirectory := runtimeReceiptDirectory
	runtimeReceiptDirectory = func() string { return filepath.Join(t.TempDir(), "receipts") }
	t.Cleanup(func() { runtimeReceiptDirectory = originalReceiptDirectory })
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var output bytes.Buffer
		_, _ = io.Copy(&output, r)
		done <- output.String()
	}()

	err = RuntimeFS([]string{"read", "--root", "/project", "--scope-key", "agent", "/README.md"})
	_ = w.Close()
	output := <-done
	if err != nil {
		t.Fatal(err)
	}
	if fake.fileKey == "" || !strings.HasPrefix(fake.fileKey, "drive9-cli-") {
		t.Fatalf("generated idempotency key = %q", fake.fileKey)
	}
	if fake.fileRequest.Workspace == nil || fake.fileRequest.Workspace.Source.Root != "/project" || fake.fileRequest.Workspace.ClientScopeKey != "agent" {
		t.Fatalf("workspace request = %#v", fake.fileRequest)
	}
	if fake.fileRequest.Operation.Action != "read" || fake.fileRequest.Operation.Path != "/README.md" {
		t.Fatalf("file operation = %#v", fake.fileRequest.Operation)
	}
	if !strings.Contains(output, `"id":"file_1"`) {
		t.Fatalf("output = %q", output)
	}
}
