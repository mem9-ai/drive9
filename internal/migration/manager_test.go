package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func testRuntimeStartup(jobIDs ...string) *RuntimeStartup {
	configuredJobs := make([]JobConfig, 0, len(jobIDs))
	spaces := make(map[string]SpaceConfig, len(jobIDs))
	for _, jobID := range jobIDs {
		configuredJobs = append(configuredJobs, JobConfig{
			JobID: jobID, Subpath: "/" + jobID,
			Target: TargetConfig{SpaceRef: jobID, Prefix: "/"},
		})
		spaces[jobID] = SpaceConfig{CredentialRef: jobID + "-key"}
	}
	config := &Config{
		Version: ConfigVersion, Drive9: Drive9Config{Endpoint: "https://drive9.example.com"},
		JobDefaults: JobDefaults{
			Sync: SyncDefaults{GracePeriod: Duration(DefaultGracePeriod)},
			Performance: PerformanceDefaults{
				MaxBytesPerSecond: 1024, SmallFileWorkers: 1, LargeFileWorkers: 1,
			},
		},
		Spaces: spaces,
		EBSSources: []EBSSourceConfig{{
			VolumeID: "vol-001", NodeName: "node-a", Root: "/ebs", Jobs: configuredJobs,
		}},
	}
	runtime := &RuntimeStartup{
		Config: config, Source: EBSSourceConfig{VolumeID: "vol-001", NodeName: "node-a", Root: "/ebs"},
		Phase: PhaseSyncing,
	}
	for _, jobID := range jobIDs {
		runtime.Jobs = append(runtime.Jobs, &Startup{
			Config: config,
			Job: Job{
				JobID: jobID, VolumeID: "vol-001", NodeName: "node-a", EBSRoot: "/ebs",
				Subpath: "/" + jobID, Source: SourceConfig{Type: "ebs", Root: "/ebs/" + jobID},
				Target: TargetConfig{SpaceRef: jobID, Prefix: "/"},
			},
			Space: spaces[jobID],
			Phase: PhaseSyncing,
		})
	}
	return runtime
}

func TestManagerValidatesSharedMappingsBeforeJobLoops(t *testing.T) {
	runtime := testRuntimeStartup("job-a", "job-b")
	runtime.Config.EBSSources[0].Jobs[1].Target.Prefix = ControlPrefix + "/unsafe"
	preflightCalls := 0
	manager, err := newManagerWithDependencies(runtime, managerDependencies{
		preflight: func(context.Context, *Startup) (PreflightResult, error) {
			preflightCalls++
			return PreflightResult{}, nil
		},
	})
	if err == nil || manager != nil {
		t.Fatalf("invalid shared mapping manager=%+v error=%v", manager, err)
	}
	if preflightCalls != 0 {
		t.Fatalf("preflight calls=%d, want 0", preflightCalls)
	}
}

func TestManagerRetriesOneJobWithoutBlockingSibling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	calls := make(map[string]int)
	running := make(chan string, 2)
	manager, err := newManagerWithDependencies(testRuntimeStartup("job-a", "job-b"), managerDependencies{
		preflight: func(_ context.Context, startup *Startup) (PreflightResult, error) {
			mu.Lock()
			calls[startup.Job.JobID]++
			attempt := calls[startup.Job.JobID]
			mu.Unlock()
			if startup.Job.JobID == "job-a" && attempt == 1 {
				return PreflightResult{}, &client.StatusError{StatusCode: http.StatusServiceUnavailable}
			}
			return PreflightResult{}, nil
		},
		newWorker: func(_ context.Context, startup *Startup) (*Worker, error) {
			return &Worker{startup: startup}, nil
		},
		runWorker: func(ctx context.Context, worker *Worker) error {
			running <- worker.startup.Job.JobID
			<-ctx.Done()
			return nil
		},
		wait: func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case jobID := <-running:
			seen[jobID] = true
		case <-time.After(time.Second):
			t.Fatalf("running Jobs=%v", seen)
		}
	}
	mu.Lock()
	if calls["job-a"] != 2 || calls["job-b"] != 1 {
		t.Fatalf("preflight calls=%v", calls)
	}
	mu.Unlock()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestManagerPermanentFailureStopsOnlyOneJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed := make(chan struct{}, 1)
	running := make(chan struct{}, 1)
	manager, err := newManagerWithDependencies(testRuntimeStartup("job-a", "job-b"), managerDependencies{
		preflight: func(_ context.Context, startup *Startup) (PreflightResult, error) {
			if startup.Job.JobID == "job-a" {
				failed <- struct{}{}
				return PreflightResult{}, ErrCheckpointMismatch
			}
			return PreflightResult{}, nil
		},
		newWorker: func(_ context.Context, startup *Startup) (*Worker, error) {
			return &Worker{startup: startup}, nil
		},
		runWorker: func(ctx context.Context, _ *Worker) error {
			running <- struct{}{}
			<-ctx.Done()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("failing Job did not run preflight")
	}
	select {
	case <-running:
	case <-time.After(time.Second):
		t.Fatal("healthy sibling did not run")
	}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot, ok := manager.snapshot("job-a")
		if ok && snapshot.State == RuntimeStopped {
			if !strings.Contains(snapshot.Error, ErrCheckpointMismatch.Error()) {
				t.Fatalf("stopped snapshot=%+v", snapshot)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job-a snapshot=%+v", snapshot)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestManagerControlReportsStartupOnlyJobsAndRejectsUnscopedMutation(t *testing.T) {
	manager, err := NewManager(testRuntimeStartup("job-a", "job-b"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	socket := testControlSocket(t)
	server, err := startControl(ctx, socket, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	var output bytes.Buffer
	if err := Control(ctx, socket, ControlRequest{Command: "status", Output: "json"}, &output); err != nil {
		t.Fatal(err)
	}
	var status EBSStatusOutput
	if err := json.Unmarshal(output.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.VolumeID != "vol-001" || len(status.Jobs) != 2 {
		t.Fatalf("status=%+v", status)
	}
	for _, job := range status.Jobs {
		if job.RuntimeState != RuntimeInitializing || job.Phase != "" || job.StartupPhase != PhaseSyncing {
			t.Fatalf("startup-only Job=%+v", job)
		}
	}
	output.Reset()
	if err := Control(ctx, socket, ControlRequest{Command: "status", JobID: "job-a", Output: "json"}, &output); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &status); err != nil || len(status.Jobs) != 1 || status.Jobs[0].JobID != "job-a" {
		t.Fatalf("filtered status=%+v err=%v", status, err)
	}
	if err := Control(ctx, socket, ControlRequest{Command: "verify-full"}, &bytes.Buffer{}); !errors.Is(err, ErrIllegalAction) {
		t.Fatalf("unscoped mutation error=%v", err)
	}
}

func TestManagerMutationGatesAreIndependentPerJob(t *testing.T) {
	manager, err := NewManager(testRuntimeStartup("job-a", "job-b"))
	if err != nil {
		t.Fatal(err)
	}
	for _, jobID := range []string{"job-a", "job-b"} {
		slot := manager.jobs[jobID]
		slot.run(&Worker{startup: slot.startup})
	}
	releaseA, err := manager.prepareControl(context.Background(), ControlRequest{Command: "verify-full", JobID: "job-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseA()
	releaseB, err := manager.prepareControl(context.Background(), ControlRequest{Command: "verify-full", JobID: "job-b"})
	if err != nil {
		t.Fatalf("Job B was blocked by Job A: %v", err)
	}
	releaseB()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := manager.prepareControl(ctx, ControlRequest{Command: "verify-full", JobID: "job-a"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Job A mutation error=%v", err)
	}
}

func TestBoundedRuntimeErrorRedactsHTTPBodyAndPreservesUTF8(t *testing.T) {
	message := boundedRuntimeError(&client.StatusError{StatusCode: http.StatusServiceUnavailable, Message: "secret response body"})
	if message != "drive9 request failed: HTTP 503" {
		t.Fatalf("HTTP error=%q", message)
	}
	message = boundedRuntimeError(errors.New(strings.Repeat("界", maximumRuntimeErrorBytes+1)))
	if len([]rune(message)) != maximumRuntimeErrorBytes || !strings.HasSuffix(message, "界") {
		t.Fatalf("bounded UTF-8 error has %d runes", len([]rune(message)))
	}
}
