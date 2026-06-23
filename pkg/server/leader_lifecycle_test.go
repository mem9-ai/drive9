package server

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/leader"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
	"go.uber.org/zap"
)

var leaderLifecycleMetaStoreDSN = testDSN

// newLeaderLifecycleServer builds a fully-wired server with a disabled leader
// manager (so onLead fires synchronously in NewWithConfig), an active tenant
// backend cached in the pool, and the metaStore reset to a clean state. It
// returns the server, pool, and cached tenant ID so callers can inspect worker
// state. All resources are registered with t.Cleanup.
func newLeaderLifecycleServer(t *testing.T) (*Server, *tenant.Pool, string) {
	t.Helper()
	initServerTenantSchema(t, leaderLifecycleMetaStoreDSN)

	metaStore, err := meta.Open(leaderLifecycleMetaStoreDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPoolWithBackendOptions(t, backendOptionsWithFileGC())
	pool.SetMetaStore(metaStore)
	t.Cleanup(func() { pool.Close() })

	parsed, err := mysql.ParseDSN(leaderLifecycleMetaStoreDSN)
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
			if n, err := strconv.Atoi(p); err == nil {
				port = n
			}
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

	leaderMgr := leader.NewManager(nil, leader.WithDisabled(),
		// The server overwrites these callbacks with its own onLead/onLose via
		// SetCallbacks in NewWithConfig. The disabled manager invokes onLead
		// synchronously inside Start.
		leader.WithCallbacks(func() {}, func() {}),
	)

	prov := &fakeProvisioner{provider: tenant.ProviderDB9}
	srv := NewWithConfig(Config{
		Meta:             metaStore,
		Pool:             pool,
		Provisioner:      prov,
		TokenSecret:      []byte("leader-lifecycle-test-secret"),
		SemanticEmbedder: staticSemanticEmbedder{vec: []float32{0.1, 0.2, 0.3}},
		SemanticWorkers: SemanticWorkerOptions{
			Workers:         1,
			PollInterval:    10 * time.Millisecond,
			RecoverInterval: 50 * time.Millisecond,
			LeaseDuration:   200 * time.Millisecond,
		},
		Leader: leaderMgr,
		Logger: zap.NewNop(),
	})
	t.Cleanup(func() { srv.Close() })

	// Acquire a backend so a FileGC worker can be started on it by onLead.
	if _, err := pool.Get(context.Background(), tenantMeta); err != nil {
		t.Fatal(err)
	}
	return srv, pool, tenantID
}

// TestLeaderGatedWorkersStartAndStop is a regression for the blocking review
// finding on PR #601: SetCallbacks was overwrite-only, so the server's
// onLead/onLose clobbered main.go's mutation-replay / expiry-sweep callbacks and
// those workers never started. This test proves that, with the server as the
// single callback owner, ALL leader-gated workers start on leadership gain and
// stop on leadership loss:
//   - semantic worker
//   - object GC worker
//   - central-quota mutation replay worker
//   - upload-reservation expiry sweep worker
//   - per-tenant FileGC worker
//
// It uses a disabled leader manager, whose Start invokes onLead synchronously,
// so the assertions are deterministic.
func TestLeaderGatedWorkersStartAndStop(t *testing.T) {
	srv, pool, tenantID := newLeaderLifecycleServer(t)

	// NewWithConfig already called leader.Start(), which (disabled) fired
	// onLead synchronously, so all leader-gated workers should be running.
	if srv.semanticWorker == nil || srv.semanticWorker.cancel == nil {
		t.Fatal("semantic worker should be started on leadership gain")
	}
	if srv.objectGCWorker == nil || !objectGCWorkerRunning(srv.objectGCWorker) {
		t.Fatal("object GC worker should be started on leadership gain")
	}
	if srv.replayWorker == nil {
		t.Fatal("mutation replay worker should be started on leadership gain")
	}
	if srv.expirySweepWorker == nil {
		t.Fatal("expiry sweep worker should be started on leadership gain")
	}
	if !srv.leaderWorkersStarted {
		t.Fatal("leaderWorkersStarted flag should be true after onLead")
	}
	// Per-tenant FileGC: the cached backend should have a running FileGC worker.
	b := pool.S3Backend(tenantID)
	if b == nil {
		t.Fatal("cached backend should exist for the tenant")
	}
	if !b.FileGCWorkerRunning() {
		t.Fatal("per-tenant FileGC worker should be started on leadership gain")
	}

	// Simulate leadership loss. onLose should stop ALL leader-gated workers.
	srv.onLose()

	if srv.leaderWorkersStarted {
		t.Fatal("leaderWorkersStarted flag should be false after onLose")
	}
	if srv.replayWorker != nil {
		t.Fatal("mutation replay worker should be nil after onLose")
	}
	if srv.expirySweepWorker != nil {
		t.Fatal("expiry sweep worker should be nil after onLose")
	}
	if srv.semanticWorker != nil && srv.semanticWorker.cancel != nil {
		t.Fatal("semantic worker should be stopped after onLose")
	}
	if b.FileGCWorkerRunning() {
		t.Fatal("per-tenant FileGC worker should be stopped after onLose")
	}
}

func objectGCWorkerRunning(w *objectGCWorker) bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stop != nil
}

func backendOptionsWithFileGC() backend.Options {
	return backend.Options{AppSemanticTasksEnabled: true}
}

// TestLeaderGatedWorkersCloseRacesOnLead is a regression for qiffang R3: the
// leader worker start/stop path must be truly non-overlapping so that Close()
// (or onLose) racing with a concurrent onLead cannot leave orphan workers
// running (e.g. start assigns replay/expiry/FileGC/semantic/objectGC after a
// stop already ran). Because startLeaderWorkers/stopLeaderWorkers now hold
// leaderWorkerMu for the entire transition, the two are serialized; this test
// races them in a loop (run with -race to catch the data race on
// s.replayWorker / s.expirySweepWorker) and asserts that after Close() no
// leader-gated worker remains running.
func TestLeaderGatedWorkersCloseRacesOnLead(t *testing.T) {
	for i := range 50 {
		srv, pool, tenantID := newLeaderLifecycleServer(t)
		b := pool.S3Backend(tenantID)
		if b == nil {
			t.Fatalf("iter %d: cached backend missing", i)
		}

		// Race onLead (re-start path) against Close (which stops the leader
		// manager first, then stopLeaderWorkers). Whichever ordering wins the
		// leaderWorkerMu, Close must end with everything stopped.
		done := make(chan struct{})
		go func() {
			defer close(done)
			srv.onLead() // guarded by leaderWorkersStarted → no-op if already started
		}()
		srv.Close()
		<-done

		if srv.leaderWorkersStarted {
			t.Fatalf("iter %d: leaderWorkersStarted should be false after Close", i)
		}
		if srv.replayWorker != nil || srv.expirySweepWorker != nil {
			t.Fatalf("iter %d: central-quota worker should be nil after Close", i)
		}
		if objectGCWorkerRunning(srv.objectGCWorker) {
			t.Fatalf("iter %d: object GC worker should be stopped after Close", i)
		}
		if srv.semanticWorker != nil && srv.semanticWorker.cancel != nil {
			t.Fatalf("iter %d: semantic worker should be stopped after Close", i)
		}
		if b.FileGCWorkerRunning() {
			t.Fatalf("iter %d: per-tenant FileGC worker should be stopped after Close", i)
		}
	}
}

// TestLeaderGatedWorkersOnLoseRacesOnLead races onLead against onLose directly
// (no Close) to confirm the serialized state machine never leaves workers
// running after a loss, regardless of start/stop interleaving. After the
// racing pair settles, a final onLose guarantees the stopped state.
func TestLeaderGatedWorkersOnLoseRacesOnLead(t *testing.T) {
	for i := range 50 {
		srv, pool, tenantID := newLeaderLifecycleServer(t)
		b := pool.S3Backend(tenantID)
		if b == nil {
			t.Fatalf("iter %d: cached backend missing", i)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); srv.onLead() }()
		go func() { defer wg.Done(); srv.onLose() }()
		wg.Wait()
		// A final loss guarantees the stopped end state regardless of which
		// racing call won the leaderWorkerMu first.
		srv.onLose()

		if srv.leaderWorkersStarted {
			t.Fatalf("iter %d: leaderWorkersStarted should be false after final onLose", i)
		}
		if srv.replayWorker != nil || srv.expirySweepWorker != nil {
			t.Fatalf("iter %d: central-quota worker should be nil after final onLose", i)
		}
		if b.FileGCWorkerRunning() {
			t.Fatalf("iter %d: per-tenant FileGC worker should be stopped after final onLose", i)
		}
		// Clean up the server (no-op stop path) and pool.
		srv.Close()
	}
}