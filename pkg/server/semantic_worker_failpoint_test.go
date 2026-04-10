//go:build failpoint

package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

const semanticWorkerBeforeFinalizeFailpoint = "github.com/mem9-ai/dat9/pkg/server/semanticWorkerBeforeFinalize"
const semanticWorkerBeforeRenewFailpoint = "github.com/mem9-ai/dat9/pkg/server/semanticWorkerBeforeRenew"
const semanticWorkerAfterRenewFailpoint = "github.com/mem9-ai/dat9/pkg/server/semanticWorkerAfterRenew"
const semanticWorkerOnLeaseLostFailpoint = "github.com/mem9-ai/dat9/pkg/server/semanticWorkerOnLeaseLost"

type gatedPanicServerImageExtractor struct {
	started chan struct{}
	release chan struct{}
}

func (e *gatedPanicServerImageExtractor) ExtractImageText(context.Context, backend.ImageExtractRequest) (string, error) {
	if e != nil && e.started != nil {
		select {
		case e.started <- struct{}{}:
		default:
		}
	}
	if e != nil && e.release != nil {
		<-e.release
	}
	panic("panic image extractor")
}

func TestSemanticWorkerFinalizeAckSkipsWhenLeaseOwnershipLostAtBoundary(t *testing.T) {
	_, b := newTestServerWithSemanticWorker(t, staticSemanticEmbedder{vec: []float32{0.1, 0.2, 0.3}}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   5 * time.Second,
	})
	if _, err := b.Write("/docs/finalize-ack.txt", []byte("hello finalize ack"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/finalize-ack.txt")

	if err := failpoint.EnableCall(semanticWorkerBeforeFinalizeFailpoint,
		func(_ string, store *datastore.Store, task *semantic.Task, action, reason, result string) {
			if task == nil || task.ResourceID != nf.FileID || action != string(semanticTaskActionAck) || reason != "written" || result != "ok" {
				return
			}
			if _, err := store.DB().Exec(`UPDATE semantic_tasks SET receipt = ? WHERE task_id = ?`, "lost-receipt", task.TaskID); err != nil {
				panic(err)
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(semanticWorkerBeforeFinalizeFailpoint)
	})

	waitForEmbeddingRevision(t, b, "/docs/finalize-ack.txt", 1, 3*time.Second)
	waitForTaskStatusByResource(t, b, nf.FileID, string(semantic.TaskProcessing), 3*time.Second)

	task := mustGetServerSemanticTaskByResource(t, b, nf.FileID)
	if task.Status != string(semantic.TaskProcessing) {
		t.Fatalf("task status=%q, want %q", task.Status, semantic.TaskProcessing)
	}
	if task.AttemptCount != 1 {
		t.Fatalf("attempt_count=%d, want 1", task.AttemptCount)
	}
}

func TestSemanticWorkerFinalizeRetrySkipsWhenLeaseOwnershipLostAtBoundary(t *testing.T) {
	_, b := newTestServerWithSemanticWorker(t, staticSemanticEmbedder{err: errors.New("embed failed")}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   5 * time.Second,
		RetryBaseDelay:  20 * time.Millisecond,
	})
	if _, err := b.Write("/docs/finalize-retry.txt", []byte("hello finalize retry"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/finalize-retry.txt")

	if err := failpoint.EnableCall(semanticWorkerBeforeFinalizeFailpoint,
		func(_ string, store *datastore.Store, task *semantic.Task, action, reason, result string) {
			if task == nil || task.ResourceID != nf.FileID || action != string(semanticTaskActionRetry) || result != "embed_error" {
				return
			}
			if _, err := store.DB().Exec(`UPDATE semantic_tasks SET receipt = ? WHERE task_id = ?`, "lost-receipt", task.TaskID); err != nil {
				panic(err)
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(semanticWorkerBeforeFinalizeFailpoint)
	})

	waitForTaskStatusByResource(t, b, nf.FileID, string(semantic.TaskProcessing), 3*time.Second)

	task := mustGetServerSemanticTaskByResource(t, b, nf.FileID)
	if task.Status != string(semantic.TaskProcessing) {
		t.Fatalf("task status=%q, want %q", task.Status, semantic.TaskProcessing)
	}
	if task.AttemptCount != 1 {
		t.Fatalf("attempt_count=%d, want 1", task.AttemptCount)
	}
	if task.LastError != "" {
		t.Fatalf("last_error=%q, want empty because retry finalize was skipped", task.LastError)
	}
}

func TestSemanticWorkerRenewsLeaseForLongImageTaskWithFailpoint(t *testing.T) {
	extractor := &gatedServerImageExtractor{
		text:    "renewed image text",
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseExtractor := func() {
		releaseOnce.Do(func() { close(extractor.release) })
	}
	t.Cleanup(releaseExtractor)
	b := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})
	fileID := insertServerImageFileForExtractTest(t, b, "/img/renew-failpoint.png", "image/png", []byte("fake-png"))
	payload, err := json.Marshal(semantic.ImgExtractTaskPayload{Path: "/img/renew-failpoint.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := b.Store().EnqueueSemanticTask(context.Background(), &semantic.Task{
		TaskID:          "img-task-renew-failpoint",
		TaskType:        semantic.TaskTypeImgExtractText,
		ResourceID:      fileID,
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     now.Add(-time.Second),
		PayloadJSON:     payload,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatal(err)
	}

	leaseDuration := 80 * time.Millisecond
	claimed, found, err := b.Store().ClaimSemanticTask(context.Background(), time.Now().UTC(), leaseDuration, semantic.TaskTypeImgExtractText)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected image task to be claimable")
	}

	renewed := make(chan time.Time, 1)
	if err := failpoint.EnableCall(semanticWorkerAfterRenewFailpoint,
		func(_ string, _ *datastore.Store, task *semantic.Task, leaseUntil time.Time) {
			if task != nil && task.TaskID == claimed.TaskID {
				select {
				case renewed <- leaseUntil.UTC():
				default:
				}
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(semanticWorkerAfterRenewFailpoint)
	})

	m := newSemanticWorkerManager(b, nil, nil, nil, SemanticWorkerOptions{LeaseDuration: leaseDuration})
	if m == nil {
		t.Fatal("expected semantic worker manager")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.processTask(context.Background(), &semanticTarget{
			tenantID: semanticLocalTenantID,
			backend:  b,
			store:    b.Store(),
		}, claimed)
	}()

	select {
	case <-extractor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("image task did not start")
	}

	var renewedLeaseUntil time.Time
	select {
	case renewedLeaseUntil = <-renewed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected renew failpoint to observe renewed lease")
	}

	recovered, err := b.Store().RecoverExpiredSemanticTasks(context.Background(), renewedLeaseUntil.Add(-time.Millisecond), 64)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 0 {
		t.Fatalf("recovered=%d, want 0", recovered)
	}
	if claimed.LeaseUntil == nil || !renewedLeaseUntil.After(claimed.LeaseUntil.UTC()) {
		t.Fatalf("renewed lease_until=%v, want after %v", renewedLeaseUntil, claimed.LeaseUntil)
	}

	releaseExtractor()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processTask did not finish")
	}
	waitForNamedTaskStatus(t, b, claimed.TaskID, string(semantic.TaskSucceeded), 2*time.Second)
	task := mustGetServerSemanticTask(t, b, claimed.TaskID)
	if task.AttemptCount != 1 {
		t.Fatalf("attempt_count=%d, want 1", task.AttemptCount)
	}
}

func TestSemanticWorkerCancelsLeaseLostImageTaskWithFailpoint(t *testing.T) {
	core, recorded := observer.New(zap.InfoLevel)
	restoreLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(restoreLogger) })

	extractor := &gatedServerImageExtractor{
		text:     "stale image text",
		started:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		canceled: make(chan struct{}, 1),
	}
	var releaseOnce sync.Once
	releaseExtractor := func() {
		releaseOnce.Do(func() { close(extractor.release) })
	}
	t.Cleanup(releaseExtractor)
	b := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})
	fileID := insertServerImageFileForExtractTest(t, b, "/img/lease-lost-failpoint.png", "image/png", []byte("fake-png"))
	payload, err := json.Marshal(semantic.ImgExtractTaskPayload{Path: "/img/lease-lost-failpoint.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := b.Store().EnqueueSemanticTask(context.Background(), &semantic.Task{
		TaskID:          "img-task-lease-lost-failpoint",
		TaskType:        semantic.TaskTypeImgExtractText,
		ResourceID:      fileID,
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     now.Add(-time.Second),
		PayloadJSON:     payload,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatal(err)
	}

	leaseDuration := 80 * time.Millisecond
	claimed, found, err := b.Store().ClaimSemanticTask(context.Background(), time.Now().UTC(), leaseDuration, semantic.TaskTypeImgExtractText)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected image task to be claimable")
	}

	if err := failpoint.EnableCall(semanticWorkerBeforeRenewFailpoint,
		func(_ string, store *datastore.Store, task *semantic.Task) {
			if task == nil || task.TaskID != claimed.TaskID {
				return
			}
			if _, err := store.DB().Exec(`UPDATE semantic_tasks
				SET status = ?, receipt = NULL, leased_at = NULL, lease_until = NULL, available_at = ?, updated_at = ?
				WHERE task_id = ?`,
				semantic.TaskQueued, now.Add(time.Second), now.Add(time.Second), claimed.TaskID); err != nil {
				panic(err)
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(semanticWorkerBeforeRenewFailpoint)
	})

	m := newSemanticWorkerManager(b, nil, nil, nil, SemanticWorkerOptions{LeaseDuration: leaseDuration})
	if m == nil {
		t.Fatal("expected semantic worker manager")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.processTask(context.Background(), &semanticTarget{
			tenantID: semanticLocalTenantID,
			backend:  b,
			store:    b.Store(),
		}, claimed)
	}()

	select {
	case <-extractor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("image task did not start")
	}
	select {
	case <-extractor.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected lease-lost cancellation")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processTask did not finish after lease loss")
	}

	task := mustGetServerSemanticTask(t, b, claimed.TaskID)
	if task.Status != string(semantic.TaskQueued) {
		t.Fatalf("task status=%q, want %q", task.Status, semantic.TaskQueued)
	}
	if task.AttemptCount != 1 {
		t.Fatalf("attempt_count=%d, want 1", task.AttemptCount)
	}

	waitForObservedLog(t, recorded, "semantic_worker_lease_lost", 2*time.Second)
	assertNoObservedLogForTask(t, recorded, claimed.TaskID, "semantic_worker_ack_ok", "semantic_worker_ack_failed", "semantic_worker_retry_scheduled", "semantic_worker_retry_failed", "semantic_worker_dead_lettered")
}

func TestSemanticWorkerPanicStopsLeaseRenewalWithFailpoint(t *testing.T) {
	extractor := &gatedPanicServerImageExtractor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	b := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})
	fileID := insertServerImageFileForExtractTest(t, b, "/img/panic-failpoint.png", "image/png", []byte("fake-png"))
	payload, err := json.Marshal(semantic.ImgExtractTaskPayload{Path: "/img/panic-failpoint.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := b.Store().EnqueueSemanticTask(context.Background(), &semantic.Task{
		TaskID:          "img-task-panic-failpoint",
		TaskType:        semantic.TaskTypeImgExtractText,
		ResourceID:      fileID,
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     now.Add(-time.Second),
		PayloadJSON:     payload,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatal(err)
	}

	leaseDuration := 80 * time.Millisecond
	claimed, found, err := b.Store().ClaimSemanticTask(context.Background(), time.Now().UTC(), leaseDuration, semantic.TaskTypeImgExtractText)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected image task to be claimable")
	}

	var releaseOnce sync.Once
	if err := failpoint.EnableCall(semanticWorkerAfterRenewFailpoint,
		func(_ string, _ *datastore.Store, task *semantic.Task, _ time.Time) {
			if task == nil || task.TaskID != claimed.TaskID {
				return
			}
			releaseOnce.Do(func() { close(extractor.release) })
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(semanticWorkerAfterRenewFailpoint)
	})

	m := newSemanticWorkerManager(b, nil, nil, nil, SemanticWorkerOptions{LeaseDuration: leaseDuration})
	if m == nil {
		t.Fatal("expected semantic worker manager")
	}
	panicCh := make(chan any, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
		}()
		m.processTask(context.Background(), &semanticTarget{
			tenantID: semanticLocalTenantID,
			backend:  b,
			store:    b.Store(),
		}, claimed)
	}()

	select {
	case <-extractor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("image task did not start")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processTask did not panic and finish")
	}
	select {
	case <-panicCh:
	default:
		t.Fatal("expected processTask panic to be recovered in test")
	}

	recovered, err := b.Store().RecoverExpiredSemanticTasks(context.Background(), time.Now().UTC().Add(2*leaseDuration), 64)
	if err != nil {
		t.Fatal(err)
	}
	if recovered == 0 {
		t.Fatal("expected expired task to be recoverable after handler panic")
	}
	waitForNamedTaskStatus(t, b, claimed.TaskID, string(semantic.TaskQueued), time.Second)
}

func mustGetServerSemanticTaskByResource(t *testing.T, b *backend.Dat9Backend, resourceID string) serverSemanticTaskState {
	t.Helper()
	var task serverSemanticTaskState
	err := b.Store().DB().QueryRow(`SELECT task_id, task_type, resource_id, status, attempt_count, COALESCE(last_error, '')
		FROM semantic_tasks WHERE resource_id = ? AND resource_version = 1`, resourceID).Scan(
		&task.TaskID,
		&task.TaskType,
		&task.ResourceID,
		&task.Status,
		&task.AttemptCount,
		&task.LastError,
	)
	if err != nil {
		t.Fatalf("get semantic task by resource %s: %v", resourceID, err)
	}
	return task
}

func waitForTaskStatusByResource(t *testing.T, b *backend.Dat9Backend, resourceID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = 1`, resourceID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var status string
	if err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = 1`, resourceID).Scan(&status); err != nil {
		t.Fatalf("wait task status query by resource: %v", err)
	}
	t.Fatalf("resource %s status=%q, want %q", resourceID, status, want)
}
