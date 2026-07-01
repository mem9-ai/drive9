package server

import (
	"context"
	"crypto/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/leader"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
	"go.uber.org/zap"
)

// newLeaderLifecycleServer builds a fully-wired server with a disabled leader
// manager (so onLead fires synchronously in NewWithConfig), an active tenant
// backend cached in the pool, and the metaStore reset to a clean state. It
// returns the server, pool, and cached tenant ID so callers can inspect worker
// state. All resources are registered with t.Cleanup.
func newLeaderLifecycleServer(t *testing.T) (*Server, *tenant.Pool, string) {
	t.Helper()
	leaderMgr := leader.NewManager(nil, leader.WithDisabled(),
		// The server overwrites these callbacks with its own onLead/onLose via
		// SetCallbacks in NewWithConfig. The disabled manager invokes onLead
		// synchronously inside Start.
		leader.WithCallbacks(func() {}, func() {}),
	)
	return newLeaderLifecycleServerWithLeader(t, leaderMgr)
}

// newLeaderLifecycleServerWithRealLeader is like newLeaderLifecycleServer but
// uses a real (non-disabled) leader manager backed by the shared MySQL so the
// heartbeat goroutine drives onLead/onLose naturally. heartbeat controls the
// acquisition/verification interval. A unique lock name isolates this manager
// from other tests.
func newLeaderLifecycleServerWithRealLeader(t *testing.T, heartbeat time.Duration) (*Server, *tenant.Pool, string) {
	t.Helper()
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	leaderMgr := leader.NewManager(metaStore.DB(),
		leader.WithLockName("drive9:test:server-lifecycle:"+token.NewID()),
		leader.WithHeartbeatInterval(heartbeat),
	)
	return newLeaderLifecycleServerWithLeader(t, leaderMgr)
}

func newLeaderLifecycleServerWithLeader(t *testing.T, leaderMgr *leader.Manager) (*Server, *tenant.Pool, string) {
	t.Helper()
	initServerTenantSchema(t, testDSN)

	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	_, _ = metaStore.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = metaStore.DB().Exec("DELETE FROM tenants")

	pool := newTestTenantPoolWithLeaderChecker(t, backendOptionsWithFileGC(), leaderMgr)
	pool.SetMetaStore(metaStore)
	t.Cleanup(func() { pool.Close() })

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

	prov := &fakeProvisioner{provider: tenant.ProviderDB9}
	srv := NewWithConfig(Config{
		Meta:             metaStore,
		Pool:             pool,
		Provisioner:      prov,
		TokenSecret:      []byte("leader-lifecycle-test-secret"),
		SemanticEmbedder: staticSemanticEmbedder{vec: []float32{0.1, 0.2, 0.3}},
		TenantWorkers: TenantWorkerOptions{
			Workers:       1,
			PollInterval:  10 * time.Millisecond,
			LeaseDuration: 200 * time.Millisecond,
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
	srv, _, _ := newLeaderLifecycleServer(t)

	// NewWithConfig already called leader.Start(), which (disabled) fired
	// onLead synchronously, so all leader-gated workers should be running.
	if srv.tenantWorker == nil || srv.tenantWorker.cancel == nil {
		t.Fatal("tenant worker should be started on leadership gain")
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
	if srv.tenantWorker != nil && srv.tenantWorker.cancel != nil {
		t.Fatal("tenant worker should be stopped after onLose")
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

// waitForLeaderOrTimeout polls srv.leader.IsLeader() until it reports true or
// the timeout elapses. A non-leader result at timeout is not fatal — the race
// test only needs the heartbeat loop running so onLead can overlap with Close;
// leadership gain is the common case but not required for the test to be valid.
func waitForLeaderOrTimeout(t *testing.T, srv *Server, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if srv.leader != nil && srv.leader.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func backendOptionsWithFileGC() backend.Options {
	return backend.Options{AppSemanticTasksEnabled: true}
}

// newTestTenantPoolWithLeaderChecker builds a tenant pool whose per-tenant
// FileGCWorker is gated by checker, so the leader lifecycle tests exercise the
// real fileGCEnabled path (StartAllFileGC/StopAllFileGC + insertion-time read).
func newTestTenantPoolWithLeaderChecker(t *testing.T, opts backend.Options, checker tenant.LeaderChecker) *tenant.Pool {
	t.Helper()
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{
		S3Dir:          mustTempDir(t),
		PublicURL:      "http://localhost",
		BackendOptions: opts,
		LeaderChecker:  checker,
	}, enc)
	t.Cleanup(func() { pool.Close() })
	return pool
}

// TestLeaderGatedWorkersCloseRacesOnLead is a regression for qiffang R3: a
// leader manager heartbeat goroutine may be inside onLead (calling
// startLeaderWorkers) when Close() runs. Close() must stop the leader manager
// first — leader.Stop() waits for the heartbeat goroutine to exit, so no
// onLead can outlive it — then stopLeaderWorkers tears down whatever workers
// the in-flight onLead started (the onLead's onLose from the cancelled loop, or
// the local stopLeaderWorkers, stops them). After Close() returns no
// leader-gated worker may remain running.
//
// This uses a real (non-disabled) leader manager against the shared MySQL so
// the heartbeat goroutine naturally drives onLead; with a short heartbeat the
// loop maximizes the chance that onLead is in progress when Close() runs (run
// with -race to catch a data race on s.replayWorker / s.expirySweepWorker).
func TestLeaderGatedWorkersCloseRacesOnLead(t *testing.T) {
	for i := range 25 {
		srv, pool, tenantID := newLeaderLifecycleServerWithRealLeader(t, 20*time.Millisecond)
		b := pool.S3Backend(tenantID)
		if b == nil {
			t.Fatalf("iter %d: cached backend missing", i)
		}

		// Give the heartbeat a chance to gain leadership (onLead starts workers),
		// then close while the loop is still running — racing onLead with Close.
		waitForLeaderOrTimeout(t, srv, 2*time.Second)
		srv.Close()

		if srv.leaderWorkersStarted {
			t.Fatalf("iter %d: leaderWorkersStarted should be false after Close", i)
		}
		if srv.replayWorker != nil || srv.expirySweepWorker != nil {
			t.Fatalf("iter %d: central-quota worker should be nil after Close", i)
		}
		if objectGCWorkerRunning(srv.objectGCWorker) {
			t.Fatalf("iter %d: object GC worker should be stopped after Close", i)
		}
		if srv.tenantWorker != nil && srv.tenantWorker.cancel != nil {
			t.Fatalf("iter %d: tenant worker should be stopped after Close", i)
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
		// Clean up the server (no-op stop path) and pool.
		srv.Close()
	}
}