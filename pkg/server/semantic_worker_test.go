package server

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/semantic"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

type gatedSemanticEmbedder struct {
	vec     []float32
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *gatedSemanticEmbedder) EmbedText(context.Context, string) ([]float32, error) {
	e.once.Do(func() {
		select {
		case e.started <- struct{}{}:
		default:
		}
		<-e.release
	})
	return append([]float32(nil), e.vec...), nil
}

type staticServerImageExtractor struct {
	text string
	err  error
}

func (e staticServerImageExtractor) ExtractImageText(_ context.Context, _ backend.ImageExtractRequest) (string, backend.ImageExtractUsage, error) {
	if e.err != nil {
		return "", backend.ImageExtractUsage{}, e.err
	}
	return e.text, backend.ImageExtractUsage{}, nil
}

type staticServerAudioExtractor struct {
	text string
	err  error
}

func (e staticServerAudioExtractor) ExtractAudioText(_ context.Context, _ backend.AudioExtractRequest) (string, backend.AudioExtractUsage, error) {
	if e.err != nil {
		return "", backend.AudioExtractUsage{}, e.err
	}
	return e.text, backend.AudioExtractUsage{}, nil
}

func newTestTenantPool(t *testing.T) *tenant.Pool {
	return newTestTenantPoolWithBackendOptions(t, backend.Options{})
}

func newTestTenantPoolWithBackendOptions(t *testing.T, opts backend.Options) *tenant.Pool {
	t.Helper()
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost", BackendOptions: opts}, enc)
	t.Cleanup(func() { pool.Close() })
	return pool
}

func newTestBackendForSemanticWorkerWithOptions(t *testing.T, opts backend.Options) *backend.Dat9Backend {
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
	b, err := backend.NewWithS3ModeAndOptions(store, s3c, true, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func newTestBackendForSemanticWorker(t *testing.T) *backend.Dat9Backend {
	return newTestBackendForSemanticWorkerWithOptions(t, backend.Options{})
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

func TestSemanticWorkerProcessesImgExtractTaskWithoutEmbedder(t *testing.T) {
	b := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: staticServerImageExtractor{text: "cat on sofa screenshot invoice"},
		},
	})
	fileID := insertServerImageFileForExtractTest(t, b, "/img/worker.png", "image/png", []byte("fake-png"))
	payload, err := json.Marshal(semantic.ImgExtractTaskPayload{Path: "/img/worker.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := b.Store().EnqueueSemanticTask(context.Background(), &semantic.Task{
		TaskID:          "img-task-1",
		TaskType:        semantic.TaskTypeImgExtractText,
		ResourceID:      fileID,
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     now,
		PayloadJSON:     payload,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatal(err)
	}

	s := NewWithConfig(Config{Backend: b, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })
	if s.semanticWorker == nil {
		t.Fatal("expected semantic worker to start for auto image tasks")
	}

	waitForContentTextOnServer(t, b, "/img/worker.png", "cat on sofa", 3*time.Second)
	waitForNamedTaskStatus(t, b, "img-task-1", string(semantic.TaskSucceeded), 3*time.Second)
	if tasks := loadSemanticTaskRowsForResource(t, b, fileID); len(tasks) != 1 || tasks[0].TaskType != string(semantic.TaskTypeImgExtractText) {
		t.Fatalf("unexpected semantic task rows: %+v", tasks)
	}
}

func TestSemanticWorkerProcessesAudioExtractTaskWithoutEmbedder(t *testing.T) {
	b := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: backend.AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: staticServerAudioExtractor{text: "hello from audio worker"},
		},
	})
	fileID := insertServerImageFileForExtractTest(t, b, "/rec/worker.mp3", "audio/mpeg", []byte{0xff, 0xf3})
	payload, err := json.Marshal(semantic.AudioExtractTaskPayload{Path: "/rec/worker.mp3", ContentType: "audio/mpeg"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := b.Store().EnqueueSemanticTask(context.Background(), &semantic.Task{
		TaskID:          "audio-task-1",
		TaskType:        semantic.TaskTypeAudioExtractText,
		ResourceID:      fileID,
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     now,
		PayloadJSON:     payload,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatal(err)
	}

	s := NewWithConfig(Config{Backend: b, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })
	if s.semanticWorker == nil {
		t.Fatal("expected semantic worker to start for auto audio tasks")
	}

	waitForContentTextOnServer(t, b, "/rec/worker.mp3", "hello from audio", 3*time.Second)
	waitForNamedTaskStatus(t, b, "audio-task-1", string(semantic.TaskSucceeded), 3*time.Second)
	if tasks := loadSemanticTaskRowsForResource(t, b, fileID); len(tasks) != 1 || tasks[0].TaskType != string(semantic.TaskTypeAudioExtractText) {
		t.Fatalf("unexpected semantic task rows: %+v", tasks)
	}
}

func TestSemanticWorkerProcessesMP4AudioExtractTaskWithoutEmbedder(t *testing.T) {
	b := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: backend.AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: staticServerAudioExtractor{text: "hello from mp4 audio worker"},
		},
	})
	fileID := insertServerImageFileForExtractTest(t, b, "/rec/worker.mp4", "video/mp4", []byte{0x00, 0x00, 0x00, 0x18})
	payload, err := json.Marshal(semantic.AudioExtractTaskPayload{Path: "/rec/worker.mp4", ContentType: "video/mp4"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := b.Store().EnqueueSemanticTask(context.Background(), &semantic.Task{
		TaskID:          "audio-mp4-task-1",
		TaskType:        semantic.TaskTypeAudioExtractText,
		ResourceID:      fileID,
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     now,
		PayloadJSON:     payload,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatal(err)
	}

	s := NewWithConfig(Config{Backend: b, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })
	if s.semanticWorker == nil {
		t.Fatal("expected semantic worker to start for auto audio tasks")
	}

	waitForContentTextOnServer(t, b, "/rec/worker.mp4", "hello from mp4 audio", 3*time.Second)
	waitForNamedTaskStatus(t, b, "audio-mp4-task-1", string(semantic.TaskSucceeded), 3*time.Second)
	if tasks := loadSemanticTaskRowsForResource(t, b, fileID); len(tasks) != 1 || tasks[0].TaskType != string(semantic.TaskTypeAudioExtractText) {
		t.Fatalf("unexpected semantic task rows: %+v", tasks)
	}
}

func TestSemanticWorkerKeepsBorrowedTenantBackendAliveDuringInvalidate(t *testing.T) {
	initServerTenantSchema(t, testDSN)
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPool(t)
	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host, port := "127.0.0.1", 3306
	if parsed.Addr != "" {
		h, p, _ := strings.Cut(parsed.Addr, ":")
		if h != "" {
			host = h
		}
		if p != "" {
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}
	passCipher, err := pool.Encrypt(context.Background(), []byte(parsed.Passwd))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tenantID := token.NewID()
	tenantMeta := &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           host,
		DBPort:           port,
		DBUser:           parsed.User,
		DBPasswordCipher: passCipher,
		DBName:           parsed.DBName,
		DBTLS:            false,
		Provider:         tenant.ProviderDB9,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), tenantMeta); err != nil {
		t.Fatal(err)
	}
	b, err := pool.Get(context.Background(), tenantMeta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/docs/pinned-worker.txt", []byte("hello borrowed backend"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/pinned-worker.txt")
	embedder := &gatedSemanticEmbedder{
		vec:     []float32{0.1, 0.2, 0.3},
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}

	s := NewWithConfig(Config{Meta: metaStore, Pool: pool, SemanticEmbedder: embedder, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })

	select {
	case <-embedder.started:
	case <-time.After(2 * time.Second):
		t.Fatal("embed task did not start")
	}
	pool.Invalidate(tenantID)
	close(embedder.release)

	probeStore, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = probeStore.Close() }()
	waitForStoreEmbeddingRevision(t, probeStore, "/docs/pinned-worker.txt", 1, 3*time.Second)
	waitForStoreTaskStatusByResource(t, probeStore, nf.FileID, 1, string(semantic.TaskSucceeded), 3*time.Second)
}

func TestSemanticWorkerImageOnlySkipsLegacyEmbedBacklog(t *testing.T) {
	b := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: staticServerImageExtractor{text: "cat on sofa screenshot invoice"},
		},
	})
	ctx := context.Background()
	fileID := insertServerImageFileForExtractTest(t, b, "/img/backlog.png", "image/png", []byte("fake-png"))
	payload, err := json.Marshal(semantic.ImgExtractTaskPayload{Path: "/img/backlog.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	if _, err := b.Store().EnqueueSemanticTask(ctx, &semantic.Task{
		TaskID:          "legacy-embed-task",
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      "legacy-embed-file",
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     base,
		CreatedAt:       base,
		UpdatedAt:       base,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Store().EnqueueSemanticTask(ctx, &semantic.Task{
		TaskID:          "img-task-typed-claim",
		TaskType:        semantic.TaskTypeImgExtractText,
		ResourceID:      fileID,
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     base.Add(time.Second),
		PayloadJSON:     payload,
		CreatedAt:       base.Add(time.Second),
		UpdatedAt:       base.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	s := NewWithConfig(Config{Backend: b, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })

	waitForContentTextOnServer(t, b, "/img/backlog.png", "cat on sofa", 3*time.Second)
	waitForNamedTaskStatus(t, b, "img-task-typed-claim", string(semantic.TaskSucceeded), 3*time.Second)
	legacy := mustGetServerSemanticTask(t, b, "legacy-embed-task")
	if legacy.Status != string(semantic.TaskQueued) || legacy.AttemptCount != 0 {
		t.Fatalf("legacy embed task=%+v, want queued with attempt_count 0", legacy)
	}
}

func TestSemanticWorkerEmbedOnlySkipsImageTasks(t *testing.T) {
	b := newTestBackendForSemanticWorker(t)
	ctx := context.Background()
	base := time.Now().UTC()
	if _, err := b.Store().EnqueueSemanticTask(ctx, &semantic.Task{
		TaskID:          "legacy-img-task",
		TaskType:        semantic.TaskTypeImgExtractText,
		ResourceID:      "legacy-image-file",
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     base,
		CreatedAt:       base,
		UpdatedAt:       base,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write("/docs/embed-only.txt", []byte("hello typed claim"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/embed-only.txt")

	s := NewWithConfig(Config{Backend: b, SemanticEmbedder: staticSemanticEmbedder{vec: []float32{0.1, 0.2, 0.3}}, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })

	waitForEmbeddingRevision(t, b, "/docs/embed-only.txt", 1, 3*time.Second)
	waitForTaskStatus(t, b, nf.FileID, 1, string(semantic.TaskSucceeded), 3*time.Second)
	legacy := mustGetServerSemanticTask(t, b, "legacy-img-task")
	if legacy.Status != string(semantic.TaskQueued) || legacy.AttemptCount != 0 {
		t.Fatalf("legacy image task=%+v, want queued with attempt_count 0", legacy)
	}
}

func TestServerDoesNotStartSemanticWorkerWithoutHandlers(t *testing.T) {
	b := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{DatabaseAutoEmbedding: true})
	s := NewWithConfig(Config{Backend: b, SemanticWorkers: SemanticWorkerOptions{Workers: 1}})
	t.Cleanup(func() { s.Close() })
	if s.semanticWorker != nil {
		t.Fatal("semantic worker should stay disabled when no handler is configured")
	}
}

func TestSemanticWorkerTaskTypesForTarget(t *testing.T) {
	bApp := newTestBackendForSemanticWorker(t)
	m := newSemanticWorkerManager(bApp, nil, nil, staticSemanticEmbedder{vec: []float32{0.1}}, SemanticWorkerOptions{})
	got := m.taskTypesForTarget(bApp)
	if len(got) != 1 || got[0] != semantic.TaskTypeEmbed {
		t.Fatalf("got %#v, want embed", got)
	}

	bAuto := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled: true, Workers: 1, QueueSize: 4, Extractor: staticServerImageExtractor{text: "ok"},
		},
	})
	mAuto := newSemanticWorkerManager(bAuto, nil, nil, nil, SemanticWorkerOptions{})
	gotAuto := mAuto.taskTypesForTarget(bAuto)
	if len(gotAuto) != 1 || gotAuto[0] != semantic.TaskTypeImgExtractText {
		t.Fatalf("got %#v, want img_extract_text", gotAuto)
	}

	bAudio := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: backend.AsyncAudioExtractOptions{
			Enabled: true, Extractor: staticServerAudioExtractor{text: "x"},
		},
	})
	mAudio := newSemanticWorkerManager(bAudio, nil, nil, nil, SemanticWorkerOptions{})
	gotAudio := mAudio.taskTypesForTarget(bAudio)
	if len(gotAudio) != 1 || gotAudio[0] != semantic.TaskTypeAudioExtractText {
		t.Fatalf("got %#v, want audio_extract_text", gotAudio)
	}

	bBoth := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled: true, Workers: 1, QueueSize: 4, Extractor: staticServerImageExtractor{text: "i"},
		},
		AsyncAudioExtract: backend.AsyncAudioExtractOptions{
			Enabled: true, Extractor: staticServerAudioExtractor{text: "a"},
		},
	})
	mBoth := newSemanticWorkerManager(bBoth, nil, nil, nil, SemanticWorkerOptions{})
	gotBoth := mBoth.taskTypesForTarget(bBoth)
	if len(gotBoth) != 2 {
		t.Fatalf("got %#v, want 2 task types", gotBoth)
	}
	for _, want := range []semantic.TaskType{semantic.TaskTypeImgExtractText, semantic.TaskTypeAudioExtractText} {
		if !slices.Contains(gotBoth, want) {
			t.Fatalf("got %#v, missing %v", gotBoth, want)
		}
	}
}

func TestSemanticWorkerTaskTypesForTargetNilWithoutEmbedder(t *testing.T) {
	b := newTestBackendForSemanticWorker(t)
	m := newSemanticWorkerManager(b, nil, nil, nil, SemanticWorkerOptions{})
	if m.taskTypesForTarget(b) != nil {
		t.Fatal("app-managed backend without embedder should yield nil task types")
	}
}

func TestSemanticWorkerManagerStartsForMultiTenantImageOnlyMode(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPoolWithBackendOptions(t, backend.Options{
		AsyncImageExtract: backend.AsyncImageExtractOptions{Enabled: true},
	})
	passCipher, err := pool.Encrypt(context.Background(), []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tenantID := token.NewID()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: passCipher,
		DBName:           "app",
		DBTLS:            false,
		Provider:         tenant.ProviderDB9,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	m := newSemanticWorkerManager(nil, metaStore, pool, nil, SemanticWorkerOptions{})
	if m == nil {
		t.Fatal("expected semantic worker manager in multi-tenant image-only mode")
	}
	_ = tenantID
}

func TestSemanticWorkerManagerStartsForMultiTenantAudioOnlyMode(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPoolWithBackendOptions(t, backend.Options{
		AsyncAudioExtract: backend.AsyncAudioExtractOptions{Enabled: true, Extractor: staticServerAudioExtractor{text: "x"}},
	})
	passCipher, err := pool.Encrypt(context.Background(), []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tenantID := token.NewID()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: passCipher,
		DBName:           "app",
		DBTLS:            false,
		Provider:         tenant.ProviderDB9,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	m := newSemanticWorkerManager(nil, metaStore, pool, nil, SemanticWorkerOptions{})
	if m == nil {
		t.Fatal("expected semantic worker manager in multi-tenant audio-only mode")
	}
	_ = tenantID
}

func TestSemanticWorkerManagerNilWhenMultiTenantOnlyFallbackHasAutoTasks(t *testing.T) {
	initServerTenantSchema(t, testDSN)
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPool(t)
	fallback := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled: true, Workers: 1, QueueSize: 4, Extractor: staticServerImageExtractor{text: "x"},
		},
	})

	m := newSemanticWorkerManager(fallback, metaStore, pool, nil, SemanticWorkerOptions{})
	if m != nil {
		t.Fatal("expected nil manager: multi-tenant listTenantRefs never schedules fallback, so fallback-only auto types are unreachable")
	}
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

func TestSemanticWorkerCollectObservationLocalFallback(t *testing.T) {
	b := newTestBackendForSemanticWorker(t)
	ctx := context.Background()
	base := time.Now().UTC()

	if _, err := b.Store().EnqueueSemanticTask(ctx, &semantic.Task{
		TaskID:          "task-processing",
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      "file-processing",
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     base.Add(-5 * time.Second),
		CreatedAt:       base,
		UpdatedAt:       base,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Store().EnqueueSemanticTask(ctx, &semantic.Task{
		TaskID:          "task-dead",
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      "file-dead",
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     1,
		AvailableAt:     base.Add(-4 * time.Second),
		CreatedAt:       base.Add(time.Second),
		UpdatedAt:       base.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	claimed, found, err := b.Store().ClaimSemanticTask(ctx, base.Add(2*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected claimed processing task")
	}
	if _, err := b.Store().EnqueueSemanticTask(ctx, &semantic.Task{
		TaskID:          "task-queued",
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      "file-queued",
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     base.Add(-3 * time.Second),
		CreatedAt:       base.Add(2 * time.Second),
		UpdatedAt:       base.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Store().EnqueueSemanticTask(ctx, &semantic.Task{
		TaskID:          "task-future",
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      "file-future",
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     base.Add(30 * time.Second),
		CreatedAt:       base.Add(3 * time.Second),
		UpdatedAt:       base.Add(3 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	deadTask, found, err := b.Store().ClaimSemanticTask(ctx, base.Add(3*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected dead-letter task to be claimed")
	}
	if err := b.Store().RetrySemanticTask(ctx, deadTask.TaskID, deadTask.Receipt, base.Add(4*time.Second), "permanent failure"); err != nil {
		t.Fatal(err)
	}

	m := newSemanticWorkerManager(b, nil, nil, staticSemanticEmbedder{vec: []float32{0.1}}, SemanticWorkerOptions{})
	if m == nil {
		t.Fatal("expected semantic worker manager")
	}
	m.markProcessingStart()
	defer m.markProcessingDone()

	snapshot := m.collectObservation(ctx, base.Add(5*time.Second))
	if snapshot.inflight != 1 {
		t.Fatalf("inflight=%d, want 1", snapshot.inflight)
	}
	if snapshot.queued != 2 {
		t.Fatalf("queued=%d, want 2", snapshot.queued)
	}
	if snapshot.processing != 1 {
		t.Fatalf("processing=%d, want 1", snapshot.processing)
	}
	if snapshot.deadLettered != 1 {
		t.Fatalf("dead_lettered=%d, want 1", snapshot.deadLettered)
	}
	if snapshot.queueLagSeconds < 7.5 || snapshot.queueLagSeconds > 8.5 {
		t.Fatalf("queue_lag_seconds=%v, want about 8", snapshot.queueLagSeconds)
	}

	waitForNamedTaskStatus(t, b, claimed.TaskID, string(semantic.TaskProcessing), time.Second)
}

func TestSemanticWorkerListTenantRefsImageOnlyIncludesAutoProviders(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPoolWithBackendOptions(t, backend.Options{
		AsyncImageExtract: backend.AsyncImageExtractOptions{Enabled: true},
	})
	passCipher, err := pool.Encrypt(context.Background(), []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	autoTenantID := token.NewID()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               autoTenantID,
		Status:           meta.TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: passCipher,
		DBName:           "app",
		DBTLS:            false,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	keepTenantID := token.NewID()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               keepTenantID,
		Status:           meta.TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: passCipher,
		DBName:           "app",
		DBTLS:            false,
		Provider:         tenant.ProviderDB9,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	orig := semanticWorkerUsesTiDBAutoEmbedding
	semanticWorkerUsesTiDBAutoEmbedding = func(provider string) bool {
		return provider == tenant.ProviderTiDBZero
	}
	defer func() {
		semanticWorkerUsesTiDBAutoEmbedding = orig
	}()

	m := newSemanticWorkerManager(nil, metaStore, pool, nil, SemanticWorkerOptions{})
	if m == nil {
		t.Fatal("expected semantic worker manager")
	}
	refs, err := m.listTenantRefs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatalf("tenant ref count=%d, want 1", len(refs))
	}
	if refs[0].id != autoTenantID {
		t.Fatalf("tenant ref id=%q, want %q", refs[0].id, autoTenantID)
	}
}

func TestSemanticWorkerListTenantRefsEmbedOnlySkipsAutoProviders(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPool(t)
	passCipher, err := pool.Encrypt(context.Background(), []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	autoTenantID := token.NewID()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               autoTenantID,
		Status:           meta.TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: passCipher,
		DBName:           "app",
		DBTLS:            false,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	keepTenantID := token.NewID()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               keepTenantID,
		Status:           meta.TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: passCipher,
		DBName:           "app",
		DBTLS:            false,
		Provider:         tenant.ProviderDB9,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	orig := semanticWorkerUsesTiDBAutoEmbedding
	semanticWorkerUsesTiDBAutoEmbedding = func(provider string) bool {
		return provider == tenant.ProviderTiDBZero
	}
	defer func() {
		semanticWorkerUsesTiDBAutoEmbedding = orig
	}()

	m := newSemanticWorkerManager(nil, metaStore, pool, staticSemanticEmbedder{vec: []float32{0.1}}, SemanticWorkerOptions{})
	if m == nil {
		t.Fatal("expected semantic worker manager")
	}
	refs, err := m.listTenantRefs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatalf("tenant ref count=%d, want 1", len(refs))
	}
	if refs[0].id != keepTenantID {
		t.Fatalf("tenant ref id=%q, want %q", refs[0].id, keepTenantID)
	}
}

func TestSemanticWorkerHTTPMultiTenantImageOnlySkipsAppTenantEmbedTasks(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPoolWithBackendOptions(t, backend.Options{
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: staticServerImageExtractor{text: "auto tenant caption"},
		},
	})

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	initServerTenantSchema(t, testDSN)
	appStore, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, appStore.DB())
	if err := appStore.Close(); err != nil {
		t.Fatal(err)
	}

	insertTenantWithToken := func(provider string, tenantDSN string) (*meta.Tenant, string) {
		t.Helper()
		parsedTenant, err := mysql.ParseDSN(tenantDSN)
		if err != nil {
			t.Fatal(err)
		}
		host, port := "127.0.0.1", 3306
		if parsedTenant.Addr != "" {
			h, p, _ := strings.Cut(parsedTenant.Addr, ":")
			if h != "" {
				host = h
			}
			if p != "" {
				_, _ = fmt.Sscanf(p, "%d", &port)
			}
		}
		passCipher, err := pool.Encrypt(context.Background(), []byte(parsedTenant.Passwd))
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		tenantMeta := &meta.Tenant{
			ID:               token.NewID(),
			Status:           meta.TenantActive,
			DBHost:           host,
			DBPort:           port,
			DBUser:           parsedTenant.User,
			DBPasswordCipher: passCipher,
			DBName:           parsedTenant.DBName,
			DBTLS:            false,
			Provider:         provider,
			SchemaVersion:    1,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		if err := metaStore.InsertTenant(context.Background(), tenantMeta); err != nil {
			t.Fatal(err)
		}
		tok, err := token.IssueToken(tokenSecret, tenantMeta.ID, 1)
		if err != nil {
			t.Fatal(err)
		}
		tokCipher, err := pool.Encrypt(context.Background(), []byte(tok))
		if err != nil {
			t.Fatal(err)
		}
		if err := metaStore.InsertAPIKey(context.Background(), &meta.APIKey{
			ID:            token.NewID(),
			TenantID:      tenantMeta.ID,
			KeyName:       provider,
			JWTCiphertext: tokCipher,
			JWTHash:       token.HashToken(tok),
			TokenVersion:  1,
			Status:        meta.APIKeyActive,
			IssuedAt:      now,
			CreatedAt:     now,
			UpdatedAt:     now,
		}); err != nil {
			t.Fatal(err)
		}
		return tenantMeta, tok
	}

	appTenant, appToken := insertTenantWithToken(tenant.ProviderDB9, testDSN)

	s := NewWithConfig(Config{Meta: metaStore, Pool: pool, TokenSecret: tokenSecret, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })
	if s.semanticWorker == nil {
		t.Fatal("expected semantic worker manager in multi-tenant image-only mode")
	}
	ts := httptest.NewServer(s)
	defer ts.Close()

	putWithToken := func(tok, path string, body []byte) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut, ts.URL+path, strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.ContentLength = int64(len(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			payload, _ := io.ReadAll(resp.Body)
			t.Fatalf("PUT %s status=%d body=%s", path, resp.StatusCode, payload)
		}
	}

	putWithToken(appToken, "/v1/fs/docs/app.txt", []byte("hello app tenant"))
	appBackend, releaseApp, err := pool.Acquire(context.Background(), appTenant)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseApp()

	appFile := mustServerFile(t, appBackend, "/docs/app.txt")
	time.Sleep(300 * time.Millisecond)
	var appTask serverSemanticTaskState
	err = appBackend.Store().DB().QueryRow(`SELECT task_id, task_type, resource_id, status, attempt_count, COALESCE(last_error, '')
		FROM semantic_tasks WHERE resource_id = ? AND resource_version = 1`, appFile.FileID).Scan(
		&appTask.TaskID,
		&appTask.TaskType,
		&appTask.ResourceID,
		&appTask.Status,
		&appTask.AttemptCount,
		&appTask.LastError,
	)
	if err != nil {
		t.Fatal(err)
	}
	if appTask.TaskType != string(semantic.TaskTypeEmbed) {
		t.Fatalf("app task_type=%q, want %q", appTask.TaskType, semantic.TaskTypeEmbed)
	}
	if appTask.Status != string(semantic.TaskQueued) || appTask.AttemptCount != 0 {
		t.Fatalf("app task=%+v, want queued with attempt_count 0", appTask)
	}
}

func TestSemanticWorkerListTenantRefsDoesNotUseFallbackImageCapabilityForPoolTenants(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPool(t)
	passCipher, err := pool.Encrypt(context.Background(), []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	autoTenantID := token.NewID()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               autoTenantID,
		Status:           meta.TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: passCipher,
		DBName:           "app",
		DBTLS:            false,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	fallback := newTestBackendForSemanticWorkerWithOptions(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 1,
			Extractor: staticServerImageExtractor{text: "fallback only"},
		},
	})

	orig := semanticWorkerUsesTiDBAutoEmbedding
	semanticWorkerUsesTiDBAutoEmbedding = func(provider string) bool {
		return provider == tenant.ProviderTiDBZero
	}
	defer func() {
		semanticWorkerUsesTiDBAutoEmbedding = orig
	}()

	// Pool has no async image extract, so TiDB-auto tenants get no task types from
	// the pool; embedder is only to satisfy worker viability (multi-tenant path does
	// not count fallback-only fbAuto — see newSemanticWorkerManager).
	m := newSemanticWorkerManager(fallback, metaStore, pool, staticSemanticEmbedder{vec: []float32{0.1}}, SemanticWorkerOptions{})
	if m == nil {
		t.Fatal("expected semantic worker manager")
	}
	refs, err := m.listTenantRefs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Fatalf("tenant ref count=%d, want 0", len(refs))
	}
}

func TestSemanticWorkerProcessEmbedTaskWithoutEmbedderDoesNotPanic(t *testing.T) {
	b := newTestBackendForSemanticWorker(t)
	if _, err := b.Write("/docs/no-embedder.txt", []byte("hello guard"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/no-embedder.txt")
	claimed, found, err := b.Store().ClaimSemanticTask(context.Background(), time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected embed task to be claimable")
	}
	var opts SemanticWorkerOptions
	opts.normalize()
	m := &semanticWorkerManager{fallback: b, opts: opts}
	m.processTask(context.Background(), &semanticTarget{
		tenantID: semanticLocalTenantID,
		store:    b.Store(),
	}, claimed)
	task := mustGetServerSemanticTask(t, b, claimed.TaskID)
	if task.Status != string(semantic.TaskQueued) {
		t.Fatalf("task status=%q, want %q", task.Status, semantic.TaskQueued)
	}
	if !strings.Contains(task.LastError, "embed handler not configured") {
		t.Fatalf("last_error=%v, want embed handler message", task.LastError)
	}
	if task.ResourceID != nf.FileID {
		t.Fatalf("task resource_id=%q, want %q", task.ResourceID, nf.FileID)
	}
}

func TestSemanticTaskLeaseExecutionStopIsIdempotent(t *testing.T) {
	e := &semanticTaskLeaseExecution{
		ctx:    context.Background(),
		cancel: func() {},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	close(e.doneCh)

	_ = e.stop()
	_ = e.stop()
}

func TestSemanticWorkerClaimAndAckLogsIncludeTaskFields(t *testing.T) {
	core, recorded := observer.New(zap.InfoLevel)
	restoreLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(restoreLogger) })

	_, b := newTestServerWithSemanticWorker(t, staticSemanticEmbedder{vec: []float32{0.1, 0.2, 0.3}}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	})
	if _, err := b.Write("/docs/logs.txt", []byte("hello semantic logs"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/logs.txt")
	waitForTaskStatus(t, b, nf.FileID, 1, string(semantic.TaskSucceeded), 3*time.Second)

	claim := waitForObservedLog(t, recorded, "semantic_worker_claim_ok", 3*time.Second)
	if claim.ContextMap()["task_type"] != string(semantic.TaskTypeEmbed) {
		t.Fatalf("claim task_type=%v, want %q", claim.ContextMap()["task_type"], semantic.TaskTypeEmbed)
	}
	if claim.ContextMap()["resource_id"] != nf.FileID {
		t.Fatalf("claim resource_id=%v, want %q", claim.ContextMap()["resource_id"], nf.FileID)
	}
	if claim.ContextMap()["result"] != "ok" {
		t.Fatalf("claim result=%v, want %q", claim.ContextMap()["result"], "ok")
	}

	ack := waitForObservedLog(t, recorded, "semantic_worker_ack_ok", 3*time.Second)
	if ack.ContextMap()["task_type"] != string(semantic.TaskTypeEmbed) {
		t.Fatalf("ack task_type=%v, want %q", ack.ContextMap()["task_type"], semantic.TaskTypeEmbed)
	}
	if ack.ContextMap()["resource_id"] != nf.FileID {
		t.Fatalf("ack resource_id=%v, want %q", ack.ContextMap()["resource_id"], nf.FileID)
	}
	if ack.ContextMap()["reason"] != "written" {
		t.Fatalf("ack reason=%v, want %q", ack.ContextMap()["reason"], "written")
	}
}

func TestSemanticWorkerDeadLetterLogIncludesTaskFields(t *testing.T) {
	core, recorded := observer.New(zap.WarnLevel)
	restoreLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(restoreLogger) })

	_, b := newTestServerWithSemanticWorker(t, staticSemanticEmbedder{err: errors.New("embed failed")}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 30 * time.Millisecond,
		LeaseDuration:   100 * time.Millisecond,
		RetryBaseDelay:  20 * time.Millisecond,
	})
	if _, err := b.Write("/docs/dead.txt", []byte("dead letter logs"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/dead.txt")
	if _, err := b.Store().DB().Exec(`UPDATE semantic_tasks SET max_attempts = 1 WHERE resource_id = ? AND resource_version = 1`, nf.FileID); err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, b, nf.FileID, 1, string(semantic.TaskDeadLettered), 3*time.Second)

	entry := waitForObservedLog(t, recorded, "semantic_worker_dead_lettered", 3*time.Second)
	if entry.ContextMap()["task_type"] != string(semantic.TaskTypeEmbed) {
		t.Fatalf("dead-letter task_type=%v, want %q", entry.ContextMap()["task_type"], semantic.TaskTypeEmbed)
	}
	if entry.ContextMap()["resource_id"] != nf.FileID {
		t.Fatalf("dead-letter resource_id=%v, want %q", entry.ContextMap()["resource_id"], nf.FileID)
	}
	if entry.ContextMap()["result"] != "dead_lettered" {
		t.Fatalf("dead-letter result=%v, want %q", entry.ContextMap()["result"], "dead_lettered")
	}
}

func TestSemanticWorkerDoesNotClaimUnsupportedTaskType(t *testing.T) {
	b := newTestBackendForSemanticWorker(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := b.Store().EnqueueSemanticTask(ctx, &semantic.Task{
		TaskID:          "task-generate-l0",
		TaskType:        semantic.TaskTypeGenerateL0,
		ResourceID:      "summary-target",
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     1,
		AvailableAt:     now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		t.Fatal(err)
	}
	s := NewWithConfig(Config{Backend: b, SemanticEmbedder: staticSemanticEmbedder{vec: []float32{0.1}}, SemanticWorkers: SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 30 * time.Millisecond,
		LeaseDuration:   100 * time.Millisecond,
		RetryBaseDelay:  20 * time.Millisecond,
	}})
	t.Cleanup(func() { s.Close() })

	time.Sleep(300 * time.Millisecond)
	task := mustGetServerSemanticTask(t, b, "task-generate-l0")
	if task.Status != string(semantic.TaskQueued) || task.AttemptCount != 0 {
		t.Fatalf("generate_l0 task=%+v, want queued with attempt_count 0", task)
	}
}

func TestSemanticWorkerRetryDelayHonorsConfiguredMax(t *testing.T) {
	m := newSemanticWorkerManager(newTestBackendForSemanticWorker(t), nil, nil, staticSemanticEmbedder{vec: []float32{0.1}}, SemanticWorkerOptions{
		RetryBaseDelay: 200 * time.Millisecond,
		RetryMaxDelay:  2 * time.Second,
	})
	if m == nil {
		t.Fatal("expected semantic worker manager")
	}
	if got := m.retryDelay(1); got != 200*time.Millisecond {
		t.Fatalf("retryDelay(1)=%v, want %v", got, 200*time.Millisecond)
	}
	if got := m.retryDelay(4); got != 1600*time.Millisecond {
		t.Fatalf("retryDelay(4)=%v, want %v", got, 1600*time.Millisecond)
	}
	if got := m.retryDelay(5); got != 2*time.Second {
		t.Fatalf("retryDelay(5)=%v, want %v", got, 2*time.Second)
	}
}

func TestSemanticWorkerNormalizeRetryMaxDelayAtLeastBase(t *testing.T) {
	var opts SemanticWorkerOptions
	opts.RetryBaseDelay = 5 * time.Second
	opts.RetryMaxDelay = time.Second
	opts.normalize()
	if opts.RetryMaxDelay != opts.RetryBaseDelay {
		t.Fatalf("RetryMaxDelay=%v, want %v", opts.RetryMaxDelay, opts.RetryBaseDelay)
	}
}

type serverSemanticTaskRow struct {
	TaskType string
	Status   string
}

type serverSemanticTaskState struct {
	TaskID       string
	TaskType     string
	ResourceID   string
	Status       string
	AttemptCount int
	LastError    string
}

func insertServerImageFileForExtractTest(t *testing.T, b *backend.Dat9Backend, path, contentType string, data []byte) string {
	t.Helper()
	fileID := mustServerImageFileID(t, b, path, contentType, data)
	return fileID
}

func mustServerImageFileID(t *testing.T, b *backend.Dat9Backend, path, contentType string, data []byte) string {
	t.Helper()
	fileID := "file-" + token.NewID()
	now := time.Now().UTC()
	err := b.Store().InTx(context.Background(), func(tx *sql.Tx) error {
		if err := b.Store().InsertFileTx(tx, &datastore.File{
			FileID:      fileID,
			StorageType: datastore.StorageDB9,
			StorageRef:  "inline",
			ContentBlob: append([]byte(nil), data...),
			ContentType: contentType,
			SizeBytes:   int64(len(data)),
			Revision:    1,
			Status:      datastore.StatusConfirmed,
			CreatedAt:   now,
			ConfirmedAt: &now,
		}); err != nil {
			return err
		}
		if err := b.Store().EnsureParentDirsTx(tx, path, func() string { return token.NewID() }); err != nil {
			return err
		}
		return b.Store().InsertNodeTx(tx, &datastore.FileNode{
			NodeID:     token.NewID(),
			Path:       path,
			ParentPath: pathutil.ParentPath(path),
			Name:       pathutil.BaseName(path),
			FileID:     fileID,
			CreatedAt:  now,
		})
	})
	if err != nil {
		t.Fatalf("insert image file %s: %v", path, err)
	}
	return fileID
}

func waitForContentTextOnServer(t *testing.T, b *backend.Dat9Backend, path, wantSubstring string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nf, err := b.Store().Stat(context.Background(), path)
		if err == nil && nf.File != nil && strings.Contains(nf.File.ContentText, wantSubstring) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	nf, err := b.Store().Stat(context.Background(), path)
	if err != nil || nf.File == nil {
		t.Fatalf("stat %s while waiting for content text: %v", path, err)
	}
	t.Fatalf("content_text=%q, want substring %q", nf.File.ContentText, wantSubstring)
}

func loadSemanticTaskRowsForResource(t *testing.T, b *backend.Dat9Backend, resourceID string) []serverSemanticTaskRow {
	t.Helper()
	rows, err := b.Store().DB().Query(`SELECT task_type, status FROM semantic_tasks WHERE resource_id = ? ORDER BY created_at, task_id`, resourceID)
	if err != nil {
		t.Fatalf("query semantic tasks for %s: %v", resourceID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []serverSemanticTaskRow
	for rows.Next() {
		var row serverSemanticTaskRow
		if err := rows.Scan(&row.TaskType, &row.Status); err != nil {
			t.Fatalf("scan semantic task for %s: %v", resourceID, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate semantic tasks for %s: %v", resourceID, err)
	}
	return out
}

func mustGetServerSemanticTask(t *testing.T, b *backend.Dat9Backend, taskID string) serverSemanticTaskState {
	t.Helper()
	var task serverSemanticTaskState
	err := b.Store().DB().QueryRow(`SELECT task_id, task_type, resource_id, status, attempt_count, COALESCE(last_error, '')
		FROM semantic_tasks WHERE task_id = ?`, taskID).Scan(
		&task.TaskID,
		&task.TaskType,
		&task.ResourceID,
		&task.Status,
		&task.AttemptCount,
		&task.LastError,
	)
	if err != nil {
		t.Fatalf("get semantic task %s: %v", taskID, err)
	}
	return task
}

func mustServerFile(t *testing.T, b *backend.Dat9Backend, path string) *datastore.File {
	t.Helper()
	nf, err := b.Store().Stat(context.Background(), path)
	if err != nil || nf.File == nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return nf.File
}

func waitForNamedTaskStatus(t *testing.T, b *backend.Dat9Backend, taskID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE task_id = ?`, taskID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var status string
	if err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE task_id = ?`, taskID).Scan(&status); err != nil {
		t.Fatalf("wait named task status query: %v", err)
	}
	t.Fatalf("task %s status=%q, want %q", taskID, status, want)
}

func waitForObservedLog(t *testing.T, recorded *observer.ObservedLogs, message string, timeout time.Duration) observer.LoggedEntry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries := recorded.FilterMessage(message).AllUntimed()
		if len(entries) > 0 {
			return entries[len(entries)-1]
		}
		time.Sleep(20 * time.Millisecond)
	}
	entries := recorded.FilterMessage(message).AllUntimed()
	if len(entries) == 0 {
		t.Fatalf("timed out waiting for log message %q", message)
	}
	return entries[len(entries)-1]
}

func waitForStoreEmbeddingRevision(t *testing.T, store *datastore.Store, path string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nf, err := store.Stat(context.Background(), path)
		if err == nil && nf.File != nil && nf.File.EmbeddingRevision != nil && *nf.File.EmbeddingRevision == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	nf, err := store.Stat(context.Background(), path)
	if err != nil || nf.File == nil {
		t.Fatalf("stat %s while waiting for embedding revision: %v", path, err)
	}
	if nf.File.EmbeddingRevision == nil {
		t.Fatalf("timed out waiting for embedding revision %d on %s", want, path)
	}
	t.Fatalf("embedding revision=%d, want %d", *nf.File.EmbeddingRevision, want)
}

func waitForStoreTaskStatusByResource(t *testing.T, store *datastore.Store, resourceID string, version int64, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := store.DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = ?`, resourceID, version).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var status string
	if err := store.DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = ?`, resourceID, version).Scan(&status); err != nil {
		t.Fatalf("wait task status by resource query: %v", err)
	}
	t.Fatalf("resource %s version %d status=%q, want %q", resourceID, version, status, want)
}
