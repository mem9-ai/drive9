package server

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

type staticSemanticEmbedder struct {
	vec []float32
	err error
}

func (e staticSemanticEmbedder) EmbedText(context.Context, string) ([]float32, error) {
	if e.err != nil {
		return nil, e.err
	}
	return append([]float32(nil), e.vec...), nil
}

func newTestBackendForSemanticWorker(t *testing.T) *backend.Dat9Backend {
	t.Helper()
	s3Dir, err := os.MkdirTemp("", "dat9-semantic-worker-s3-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })

	initServerTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })

	s3c, err := s3client.NewLocal(s3Dir, "/s3")
	if err != nil {
		t.Fatal(err)
	}
	b, err := backend.NewWithS3(store, s3c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func newTestServerWithSemanticWorker(t *testing.T, embedder staticSemanticEmbedder, workerOpts SemanticWorkerOptions) (*Server, *backend.Dat9Backend) {
	t.Helper()
	b := newTestBackendForSemanticWorker(t)
	s := NewWithConfig(Config{Backend: b, SemanticEmbedder: embedder, SemanticWorkers: workerOpts})
	t.Cleanup(func() { s.Close() })
	return s, b
}

func waitForEmbeddingRevision(t *testing.T, b *backend.Dat9Backend, path string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nf, err := b.Store().Stat(context.Background(), path)
		if err == nil && nf.File != nil && nf.File.EmbeddingRevision != nil && *nf.File.EmbeddingRevision == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	nf, err := b.Store().Stat(context.Background(), path)
	if err != nil || nf.File == nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if nf.File.EmbeddingRevision == nil {
		t.Fatalf("timed out waiting for embedding revision %d on %s", want, path)
	}
	t.Fatalf("timed out waiting for embedding revision %d on %s, got %d", want, path, *nf.File.EmbeddingRevision)
}

func waitForTaskStatus(t *testing.T, b *backend.Dat9Backend, fileID string, version int64, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = ?`, fileID, version).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var status string
	if err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = ?`, fileID, version).Scan(&status); err != nil {
		t.Fatalf("wait task status query: %v", err)
	}
	t.Fatalf("task status=%q, want %q", status, want)
}

func TestSemanticWorkerProcessesEmbedTask(t *testing.T) {
	_, b := newTestServerWithSemanticWorker(t, staticSemanticEmbedder{vec: []float32{0.1, 0.2, 0.3}}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	})
	if _, err := b.Write("/docs/a.txt", []byte("hello semantic worker"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/a.txt")
	waitForEmbeddingRevision(t, b, "/docs/a.txt", 1, 3*time.Second)
	waitForTaskStatus(t, b, nf.FileID, 1, string(semantic.TaskSucceeded), 3*time.Second)
}

func TestSemanticWorkerAcksObsoleteRevisionAndWritesLatest(t *testing.T) {
	b := newTestBackendForSemanticWorker(t)
	if _, err := b.Write("/docs/b.txt", []byte("version one"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/docs/b.txt", []byte("version two"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	s := NewWithConfig(Config{Backend: b, SemanticEmbedder: staticSemanticEmbedder{vec: []float32{0.1, 0.2}}, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })
	nf, err := b.Store().Stat(context.Background(), "/docs/b.txt")
	if err != nil || nf.File == nil {
		t.Fatalf("stat /docs/b.txt: %v", err)
	}
	waitForEmbeddingRevision(t, b, "/docs/b.txt", 2, 3*time.Second)
	waitForTaskStatus(t, b, nf.File.FileID, 1, string(semantic.TaskSucceeded), 3*time.Second)
	waitForTaskStatus(t, b, nf.File.FileID, 2, string(semantic.TaskSucceeded), 3*time.Second)
}

func TestSemanticWorkerRetriesThenDeadLetters(t *testing.T) {
	_, b := newTestServerWithSemanticWorker(t, staticSemanticEmbedder{err: errors.New("embed failed")}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 30 * time.Millisecond,
		LeaseDuration:   100 * time.Millisecond,
		RetryBaseDelay:  20 * time.Millisecond,
	})
	if _, err := b.Write("/docs/c.txt", []byte("will dead letter"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf, err := b.Store().Stat(context.Background(), "/docs/c.txt")
	if err != nil || nf.File == nil {
		t.Fatalf("stat /docs/c.txt: %v", err)
	}
	if _, err := b.Store().DB().Exec(`UPDATE semantic_tasks SET max_attempts = 2 WHERE resource_id = ? AND resource_version = 1`, nf.File.FileID); err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, b, nf.File.FileID, 1, string(semantic.TaskDeadLettered), 3*time.Second)
	if got := mustServerFile(t, b, "/docs/c.txt").EmbeddingRevision; got != nil {
		t.Fatalf("embedding revision=%d, want nil", *got)
	}
	var lastErr string
	if err := b.Store().DB().QueryRow(`SELECT last_error FROM semantic_tasks WHERE resource_id = ? AND resource_version = 1`, nf.File.FileID).Scan(&lastErr); err != nil {
		t.Fatal(err)
	}
	if lastErr == "" {
		t.Fatal("expected dead-lettered task to retain last_error")
	}
}

func TestSemanticWorkerRecoversExpiredClaim(t *testing.T) {
	b := newTestBackendForSemanticWorker(t)
	if _, err := b.Write("/docs/d.txt", []byte("recover me"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf, err := b.Store().Stat(context.Background(), "/docs/d.txt")
	if err != nil || nf.File == nil {
		t.Fatalf("stat /docs/d.txt: %v", err)
	}
	claimed, found, err := b.Store().ClaimSemanticTask(context.Background(), time.Now().UTC(), 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected claim to find embed task")
	}
	s := NewWithConfig(Config{Backend: b, SemanticEmbedder: staticSemanticEmbedder{vec: []float32{0.3, 0.2, 0.1}}, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 20 * time.Millisecond,
		LeaseDuration:   100 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })
	waitForEmbeddingRevision(t, b, "/docs/d.txt", 1, 3*time.Second)
	waitForTaskStatus(t, b, nf.File.FileID, 1, string(semantic.TaskSucceeded), 3*time.Second)
	if claimed.TaskID == "" {
		t.Fatal("expected claimed task id")
	}
}

func mustServerFile(t *testing.T, b *backend.Dat9Backend, path string) *datastore.File {
	t.Helper()
	nf, err := b.Store().Stat(context.Background(), path)
	if err != nil || nf.File == nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return nf.File
}
