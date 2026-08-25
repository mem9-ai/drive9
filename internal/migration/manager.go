package migration

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type RuntimeState string

const (
	RuntimeInitializing RuntimeState = "INITIALIZING"
	RuntimeRetrying     RuntimeState = "RETRYING"
	RuntimeRunning      RuntimeState = "RUNNING"
	RuntimeStopped      RuntimeState = "STOPPED"
)

const maximumRuntimeErrorBytes = 512

type jobRuntimeSlot struct {
	mu       sync.RWMutex
	startup  *Startup
	state    RuntimeState
	worker   *Worker
	attempts int
	err      string
}

type jobRuntimeSnapshot struct {
	Startup  *Startup
	State    RuntimeState
	Worker   *Worker
	Attempts int
	Error    string
}

type managerDependencies struct {
	preflight func(context.Context, *Startup) (PreflightResult, error)
	newWorker func(context.Context, *Startup) (*Worker, error)
	runWorker func(context.Context, *Worker) error
	wait      func(context.Context, time.Duration) error
}

// Manager supervises independent Job Workers for one selected EBS process.
type Manager struct {
	startup *RuntimeStartup
	jobs    map[string]*jobRuntimeSlot
	order   []string
	deps    managerDependencies
}

func NewManager(startup *RuntimeStartup) (*Manager, error) {
	return newManagerWithDependencies(startup, managerDependencies{})
}

func newManagerWithDependencies(startup *RuntimeStartup, deps managerDependencies) (*Manager, error) {
	if startup == nil || startup.Config == nil || len(startup.Jobs) == 0 {
		return nil, errors.New("Manager requires runtime startup configuration")
	}
	if err := ValidateMappings(startup.Config); err != nil {
		return nil, fmt.Errorf("Manager static mapping: %w", err)
	}
	if deps.preflight == nil {
		deps.preflight = preflightJob
	}
	if deps.newWorker == nil {
		deps.newWorker = NewWorker
	}
	if deps.runWorker == nil {
		deps.runWorker = func(ctx context.Context, worker *Worker) error { return worker.Run(ctx) }
	}
	if deps.wait == nil {
		deps.wait = waitForManagerRetry
	}
	manager := &Manager{
		startup: startup, jobs: make(map[string]*jobRuntimeSlot, len(startup.Jobs)),
		order: make([]string, 0, len(startup.Jobs)), deps: deps,
	}
	for _, job := range startup.Jobs {
		if job == nil || validateJobID(job.Job.JobID) != nil {
			return nil, errors.New("Manager requires valid Job startup identity")
		}
		if _, exists := manager.jobs[job.Job.JobID]; exists {
			return nil, errors.New("Manager requires unique Job IDs")
		}
		manager.jobs[job.Job.JobID] = &jobRuntimeSlot{startup: job, state: RuntimeInitializing}
		manager.order = append(manager.order, job.Job.JobID)
	}
	return manager, nil
}

func (m *Manager) Run(ctx context.Context) error {
	var wait sync.WaitGroup
	for _, jobID := range m.order {
		slot := m.jobs[jobID]
		wait.Add(1)
		go func() {
			defer wait.Done()
			m.runJob(ctx, slot)
		}()
	}
	<-ctx.Done()
	wait.Wait()
	return nil
}

func (m *Manager) runJob(ctx context.Context, slot *jobRuntimeSlot) {
	attempt := 0
	for {
		slot.setInitializing(attempt)
		if _, err := m.deps.preflight(ctx, slot.startup); err != nil {
			if ctx.Err() != nil {
				return
			}
			if !retryableInitializationError(err) {
				slot.stop(err)
				return
			}
			slot.setRetrying(attempt+1, err)
			if err := m.deps.wait(ctx, retryDelay(attempt, maxRetryDelay)); err != nil {
				return
			}
			attempt++
			continue
		}
		worker, err := m.deps.newWorker(ctx, slot.startup)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !retryableInitializationError(err) {
				slot.stop(err)
				return
			}
			slot.setRetrying(attempt+1, err)
			if err := m.deps.wait(ctx, retryDelay(attempt, maxRetryDelay)); err != nil {
				return
			}
			attempt++
			continue
		}
		slot.run(worker)
		if err := m.deps.runWorker(ctx, worker); err != nil && ctx.Err() == nil {
			slot.stop(err)
		}
		return
	}
}

func (s *jobRuntimeSlot) setInitializing(attempts int) {
	s.mu.Lock()
	s.state, s.worker, s.attempts, s.err = RuntimeInitializing, nil, attempts, ""
	s.mu.Unlock()
}

func (s *jobRuntimeSlot) setRetrying(attempts int, err error) {
	s.mu.Lock()
	s.state, s.worker, s.attempts, s.err = RuntimeRetrying, nil, attempts, boundedRuntimeError(err)
	s.mu.Unlock()
}

func (s *jobRuntimeSlot) run(worker *Worker) {
	s.mu.Lock()
	s.state, s.worker, s.err = RuntimeRunning, worker, ""
	s.mu.Unlock()
}

func (s *jobRuntimeSlot) stop(err error) {
	s.mu.Lock()
	s.state, s.worker, s.err = RuntimeStopped, nil, boundedRuntimeError(err)
	s.mu.Unlock()
}

func (s *jobRuntimeSlot) snapshot() jobRuntimeSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return jobRuntimeSnapshot{Startup: s.startup, State: s.state, Worker: s.worker, Attempts: s.attempts, Error: s.err}
}

func (m *Manager) snapshot(jobID string) (jobRuntimeSnapshot, bool) {
	slot, ok := m.jobs[jobID]
	if !ok {
		return jobRuntimeSnapshot{}, false
	}
	return slot.snapshot(), true
}

func (m *Manager) snapshots(jobID string) ([]jobRuntimeSnapshot, error) {
	if jobID != "" {
		snapshot, ok := m.snapshot(jobID)
		if !ok {
			return nil, ErrIllegalAction
		}
		return []jobRuntimeSnapshot{snapshot}, nil
	}
	result := make([]jobRuntimeSnapshot, 0, len(m.order))
	for _, id := range m.order {
		result = append(result, m.jobs[id].snapshot())
	}
	return result, nil
}

func retryableInitializationError(err error) bool {
	if isAuthError(err) || retryableWorkerError(err) || errors.Is(err, fs.ErrPermission) {
		return true
	}
	var status *client.StatusError
	if errors.As(err, &status) {
		return status.StatusCode == http.StatusTooManyRequests || status.StatusCode >= 500
	}
	var network net.Error
	return errors.As(err, &network)
}

func waitForManagerRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func boundedRuntimeError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	var status *client.StatusError
	if errors.As(err, &status) {
		message = fmt.Sprintf("drive9 request failed: HTTP %d", status.StatusCode)
	}
	message = strings.Join(strings.Fields(message), " ")
	runes := []rune(message)
	if len(runes) <= maximumRuntimeErrorBytes {
		return message
	}
	return string(runes[:maximumRuntimeErrorBytes])
}

func RunManager(ctx context.Context, startup *RuntimeStartup) error {
	return RunManagerAt(ctx, startup, DefaultControlSocket)
}

func RunManagerAt(ctx context.Context, startup *RuntimeStartup, socket string) error {
	manager, err := NewManager(startup)
	if err != nil {
		return err
	}
	control, err := startControl(ctx, socket, manager)
	if err != nil {
		return err
	}
	defer control.close()
	return manager.Run(ctx)
}
