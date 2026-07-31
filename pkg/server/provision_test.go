package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/leader"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	tenantschema "github.com/mem9-ai/drive9/pkg/tenant/schema"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

type fakeProvisioner struct {
	provider                    string
	cloudProvider               string
	region                      string
	cluster                     *tenant.ClusterInfo
	initErr                     error
	provisionErr                error
	systemUserErr               error
	systemUsername              string
	systemPassword              string
	deprovisionErr              error
	quotaMarkErr                error
	quotaUpdateErr              error
	provisionCalls              atomic.Int32
	credentialCalls             atomic.Int32
	credentialQuotaCalls        atomic.Int32
	systemUserCalls             atomic.Int32
	deprovisionCalls            atomic.Int32
	quotaMarkCalls              atomic.Int32
	quotaUpdateCalls            atomic.Int32
	sharedPoolBatchCalls        atomic.Int32
	sharedPoolBatchMembers      atomic.Int32
	sharedPoolBatchRequests     chan []tenant.SharedDBPoolCreateRequest
	lastCredentialReq           tenant.CredentialProvisionRequest
	lastDeprovision             *tenant.ClusterInfo
	lastQuotaCluster            *tenant.ClusterInfo
	lastQuotaOptions            tenant.QuotaUpdateOptions
	lastCreateQuotaOptions      tenant.QuotaUpdateOptions
	defaultPublicKey            string
	defaultPrivateKey           string
	defaultSharedPublicKey      string
	defaultSharedPrivateKey     string
	sharedPoolMu                sync.Mutex
	lastSharedCredentialReq     tenant.CredentialProvisionRequest
	iamCalls                    atomic.Int32
	billingCalls                atomic.Int32
	billingErr                  error
	billingFree                 bool
	iamMu                       sync.Mutex
	iamCredentials              []tenant.CredentialProvisionRequest
	identityOrg                 string
	identityRole                string
	managedClusters             []tenant.CloudClusterInfo
	sharedPoolResults           []*tenant.SharedDBPoolInfo
	sharedPoolBatchErr          error
	sharedPoolPartialErr        error
	sharedPoolBatchStarted      chan struct{}
	sharedPoolBatchRelease      <-chan struct{}
	sharedPoolWaitCalls         atomic.Int32
	sharedPoolBatchLoadCalls    atomic.Int32
	sharedPoolBatchLoadMembers  atomic.Int32
	sharedPoolBatchLoadRequests chan []tenant.SharedDBPoolLoadRequest
	sharedPoolBatchLoadFunc     func([]tenant.SharedDBPoolLoadRequest) ([]*tenant.SharedDBPoolInfo, error)
	sharedPoolWaitErr           error
	sharedPoolLoadIDs           chan int64
	sharedPoolWaitRelease       <-chan struct{}
}

type earlyBindingProvisioner struct {
	*fakeProvisioner
	createStarted chan struct{}
	createRelease <-chan struct{}
	created       *tenant.ClusterInfo
	ready         *tenant.ClusterInfo
	waitStarted   chan struct{}
	waitRelease   <-chan struct{}
	waitErr       error
}

func (p *earlyBindingProvisioner) CreateClusterWithCredentialsAndQuota(_ context.Context, tenantID string, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.ClusterInfo, *tenant.QuotaCloudConfig, error) {
	p.lastCredentialReq = req
	p.lastCreateQuotaOptions = opts
	if p.createStarted != nil {
		close(p.createStarted)
	}
	if p.createRelease != nil {
		<-p.createRelease
	}
	out := *p.created
	out.TenantID = tenantID
	out.Provider = tenant.ProviderTiDBCloudNative
	return &out, nil, nil
}

func (p *earlyBindingProvisioner) WaitForClusterMetadataWithCredentials(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	p.lastCredentialReq = req
	close(p.waitStarted)
	if p.waitRelease != nil {
		<-p.waitRelease
	}
	if p.waitErr != nil {
		return cluster, p.waitErr
	}
	out := *p.ready
	out.TenantID = cluster.TenantID
	out.ClusterID = cluster.ClusterID
	out.Password = cluster.Password
	out.DBName = cluster.DBName
	out.Provider = tenant.ProviderTiDBCloudNative
	return &out, nil
}

type blockingDatabaseEnsurer struct {
	fakeProvisioner
	started     chan struct{}
	startedOnce sync.Once
	release     <-chan struct{}
}

func (f *blockingDatabaseEnsurer) EnsureDatabase(ctx context.Context, _ string) error {
	f.startedOnce.Do(func() { close(f.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.release:
		return errors.New("stop after provisioning heartbeat test")
	}
}

func TestBlockingDatabaseEnsurerSignalsStartedOnceAcrossRetries(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	close(release)
	ensurer := &blockingDatabaseEnsurer{started: started, release: release}
	for attempt := 0; attempt < 2; attempt++ {
		if err := ensurer.EnsureDatabase(context.Background(), ""); err == nil {
			t.Fatalf("EnsureDatabase attempt %d unexpectedly succeeded", attempt+1)
		}
	}
	select {
	case <-started:
	default:
		t.Fatal("EnsureDatabase did not signal started")
	}
}

type countingEncryptor struct {
	encrypt.Encryptor
	decryptCalls atomic.Int32
}

func (e *countingEncryptor) Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	e.decryptCalls.Add(1)
	return e.Encryptor.Decrypt(ctx, ciphertext)
}

type failingEncryptor struct {
	err error
}

func (e failingEncryptor) Encrypt(context.Context, []byte) ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return nil, fmt.Errorf("encrypt failed")
}

func (e failingEncryptor) Decrypt(context.Context, []byte) ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return nil, fmt.Errorf("decrypt failed")
}

func (f *fakeProvisioner) DefaultCredentials() (tenant.CredentialProvisionRequest, bool) {
	if f.defaultPublicKey == "" || f.defaultPrivateKey == "" {
		return tenant.CredentialProvisionRequest{}, false
	}
	return tenant.CredentialProvisionRequest{
		PublicKey:  f.defaultPublicKey,
		PrivateKey: f.defaultPrivateKey,
	}, true
}

func (f *fakeProvisioner) DefaultSharedCredentials() (tenant.CredentialProvisionRequest, bool) {
	if f.defaultSharedPublicKey == "" && f.defaultSharedPrivateKey == "" {
		return tenant.CredentialProvisionRequest{PublicKey: "shared-public", PrivateKey: "shared-private"}, true
	}
	if f.defaultSharedPublicKey == "" || f.defaultSharedPrivateKey == "" {
		return tenant.CredentialProvisionRequest{}, false
	}
	return tenant.CredentialProvisionRequest{PublicKey: f.defaultSharedPublicKey, PrivateKey: f.defaultSharedPrivateKey}, true
}

func (f *fakeProvisioner) ResolveAPIKeyIdentity(_ context.Context, req tenant.CredentialProvisionRequest) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	f.iamCalls.Add(1)
	f.iamMu.Lock()
	f.iamCredentials = append(f.iamCredentials, req)
	f.iamMu.Unlock()
	orgID := f.identityOrg
	if orgID == "" && f.cluster != nil {
		orgID = f.cluster.OrganizationID
	}
	if orgID == "" && len(f.managedClusters) > 0 {
		orgID = f.managedClusters[0].OrganizationID
	}
	if orgID == "" && len(f.sharedPoolResults) > 0 {
		orgID = f.sharedPoolResults[0].OrganizationID
	}
	if orgID == "" {
		orgID = "org-shared"
	}
	role := f.identityRole
	if role == "" {
		role = tenant.TiDBCloudRoleOrgOwner
	}
	return &tenant.TiDBCloudAPIKeyIdentity{OrganizationID: orgID, Role: role}, nil
}

func (f *fakeProvisioner) ResolveOrganizationPlan(_ context.Context, organizationID string, _ tenant.CredentialProvisionRequest) (*tenant.TiDBCloudOrganizationPlan, error) {
	f.billingCalls.Add(1)
	if f.billingErr != nil {
		return nil, f.billingErr
	}
	return &tenant.TiDBCloudOrganizationPlan{
		OrganizationID: organizationID,
		IsFree:         f.billingFree,
	}, nil
}

func (f *fakeProvisioner) iamCredentialsSnapshot() []tenant.CredentialProvisionRequest {
	f.iamMu.Lock()
	defer f.iamMu.Unlock()
	return append([]tenant.CredentialProvisionRequest(nil), f.iamCredentials...)
}

func (f *fakeProvisioner) ProviderType() string { return f.provider }

func TestDefaultTenantProviderIsIndependentFromProvisionerType(t *testing.T) {
	srv := NewWithConfig(Config{
		Provisioner:           &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared,
	})
	if srv.defaultTenantProvider != tenant.ProviderTiDBCloudNativeShared {
		t.Fatalf("defaultTenantProvider = %q, want %q", srv.defaultTenantProvider, tenant.ProviderTiDBCloudNativeShared)
	}
	if srv.provisioner.ProviderType() != tenant.ProviderTiDBCloudNative {
		t.Fatalf("provisioner provider = %q, want native Cloud implementation", srv.provisioner.ProviderType())
	}
}

func TestManagedSharedDBWorkerConfigDefaultsAndOverrides(t *testing.T) {
	defaults := NewWithConfig(Config{})
	t.Cleanup(defaults.Close)
	if defaults.managedSharedDBCloudBatchSize != 50 ||
		defaults.managedSharedDBMetadataWorkers != 15 || defaults.managedSharedDBMetadataBatchSize != 30 ||
		defaults.managedSharedDBMetadataPollInterval != 15*time.Second || defaults.managedSharedDBProvisioningWorkers != 100 ||
		defaults.tenantPoolReconcileInterval != 5*time.Second || defaults.tenantPoolReconcileWorkerRest != 5*time.Second ||
		defaults.managedSharedDBStuckTimeout != 30*time.Minute || defaults.tenantPoolReconcileWorkers != 15 ||
		defaults.managedSharedDBFailedCleanupInterval != time.Minute || defaults.managedSharedDBFailedCleanupBatchSize != 5 {
		t.Fatalf("managed shared defaults = cloud_batch(%d) metadata(%d,%d,%s) provisioning(%d) reconcile(%s,%d,%s) stuck(%s) failed_cleanup(%s,%d)",
			defaults.managedSharedDBCloudBatchSize,
			defaults.managedSharedDBMetadataWorkers, defaults.managedSharedDBMetadataBatchSize, defaults.managedSharedDBMetadataPollInterval,
			defaults.managedSharedDBProvisioningWorkers, defaults.tenantPoolReconcileInterval, defaults.tenantPoolReconcileWorkers,
			defaults.tenantPoolReconcileWorkerRest, defaults.managedSharedDBStuckTimeout,
			defaults.managedSharedDBFailedCleanupInterval, defaults.managedSharedDBFailedCleanupBatchSize)
	}
	overrides := NewWithConfig(Config{
		ManagedSharedDBCloudBatchSize:  12,
		ManagedSharedDBMetadataWorkers: 5, ManagedSharedDBMetadataBatchSize: 6, ManagedSharedDBMetadataPollInterval: 7 * time.Second,
		ManagedSharedDBProvisioningWorkers: 8, TenantPoolReconcileInterval: 9 * time.Second, ManagedSharedDBStuckTimeout: 11 * time.Minute,
		TenantPoolReconcileWorkers: 4, TenantPoolReconcileWorkerRest: 3 * time.Second,
		ManagedSharedDBFailedCleanupInterval: 2 * time.Minute, ManagedSharedDBFailedCleanupBatchSize: 3,
	})
	t.Cleanup(overrides.Close)
	if overrides.managedSharedDBCloudBatchSize != 12 ||
		overrides.managedSharedDBMetadataWorkers != 5 || overrides.managedSharedDBMetadataBatchSize != 6 ||
		overrides.managedSharedDBMetadataPollInterval != 7*time.Second || overrides.managedSharedDBProvisioningWorkers != 8 ||
		overrides.tenantPoolReconcileInterval != 9*time.Second || overrides.tenantPoolReconcileWorkers != 4 ||
		overrides.tenantPoolReconcileWorkerRest != 3*time.Second ||
		overrides.managedSharedDBStuckTimeout != 11*time.Minute ||
		overrides.managedSharedDBFailedCleanupInterval != 2*time.Minute || overrides.managedSharedDBFailedCleanupBatchSize != 3 {
		t.Fatalf("managed shared overrides were not retained")
	}
	capped := NewWithConfig(Config{ManagedSharedDBCloudBatchSize: 75, ManagedSharedDBMetadataWorkers: 16, ManagedSharedDBMetadataBatchSize: 31})
	t.Cleanup(capped.Close)
	if capped.managedSharedDBCloudBatchSize != 75 || capped.managedSharedDBMetadataWorkers != 15 || capped.managedSharedDBMetadataBatchSize != 30 {
		t.Fatalf("managed shared safety caps = cloud batch %d, metadata workers %d, metadata batch %d",
			capped.managedSharedDBCloudBatchSize, capped.managedSharedDBMetadataWorkers, capped.managedSharedDBMetadataBatchSize)
	}
}

func TestManagedSharedDBProvisioningSlotsIgnoreMetaConnectionBudget(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	metaStore.DB().SetMaxOpenConns(20)

	srv := NewWithConfig(Config{Meta: metaStore, ManagedSharedDBProvisioningWorkers: 100})
	t.Cleanup(srv.Close)
	if got := srv.managedSharedDBProvisioningConcurrency; got != 100 {
		t.Fatalf("effective provisioning concurrency = %d, want 100", got)
	}
	if got := cap(srv.managedSharedDBProvisioningSlots); got != 100 {
		t.Fatalf("provisioning slot capacity = %d, want 100", got)
	}
}

func TestManagedSharedDBProvisioningQueueErrorSkipsExpectedContention(t *testing.T) {
	for _, err := range []error{
		meta.ErrNotFound,
		fmt.Errorf("target lock: %w", tenant.ErrSharedDBSchemaEnsureBusy),
	} {
		if got := managedSharedDBProvisioningQueueError(err); got != nil {
			t.Fatalf("managedSharedDBProvisioningQueueError(%v) = %v, want nil", err, got)
		}
	}
	wantErr := errors.New("schema apply failed")
	if got := managedSharedDBProvisioningQueueError(wantErr); !errors.Is(got, wantErr) {
		t.Fatalf("managedSharedDBProvisioningQueueError(%v) = %v, want original error", wantErr, got)
	}
}

func TestNextManagedSharedDBStatusPageAdvancesAndWrapsKeysetCursor(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	ctx := context.Background()
	spendingLimit := meta.MaxTiDBCloudSpendingLimit
	ids := make([]int64, 0, 3)
	for i := 0; i < 3; i++ {
		id, err := metaStore.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
			TiDBCloudOrganizationID: fmt.Sprintf("org-keyset-%d", i), ProvisioningKey: bytes.Repeat([]byte{byte(i + 1)}, 32),
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
		})
		if err != nil {
			t.Fatalf("CreateManagedSharedDBPool %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	srv := &Server{meta: metaStore}
	var cursor int64
	first, err := srv.nextManagedSharedDBStatusPage(ctx, meta.SharedDBStatusPending, &cursor, 2)
	if err != nil || len(first) != 2 || first[0].ID != ids[0] || first[1].ID != ids[1] {
		t.Fatalf("first page = %+v, cursor=%d, err=%v", first, cursor, err)
	}
	second, err := srv.nextManagedSharedDBStatusPage(ctx, meta.SharedDBStatusPending, &cursor, 2)
	if err != nil || len(second) != 1 || second[0].ID != ids[2] {
		t.Fatalf("second page = %+v, cursor=%d, err=%v", second, cursor, err)
	}
	if cursor != 0 {
		t.Fatalf("cursor after short page = %d, want wrapped to zero", cursor)
	}
	third, err := srv.nextManagedSharedDBStatusPage(ctx, meta.SharedDBStatusPending, &cursor, 2)
	if err != nil || len(third) != 2 || third[0].ID != ids[0] || third[1].ID != ids[1] {
		t.Fatalf("wrapped page = %+v, cursor=%d, err=%v", third, cursor, err)
	}
}

func TestManagedSharedDBContinuationDoesNotStartOnFollower(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	mgr := leader.NewManager(metaStore.DB())
	srv := &Server{leader: mgr}
	called := make(chan struct{}, 1)
	if srv.startManagedSharedDBWorker(context.Background(), func(context.Context) { called <- struct{}{} }) {
		t.Fatal("follower started managed shared DB continuation")
	}
	select {
	case <-called:
		t.Fatal("follower ran managed shared DB continuation")
	case <-time.After(20 * time.Millisecond):
	}

	core, observed := observer.New(zap.InfoLevel)
	ctx := logger.WithContext(context.Background(), zap.New(core))
	srv.scheduleManagedSharedDBContinuations(ctx, []int64{22, 11})
	entries := observed.FilterMessage("managed_shared_db_continuation_deferred").All()
	if len(entries) != 1 {
		t.Fatalf("deferred continuation logs = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["reason"] != "not_leader" || fields["db_pool_count"] != int64(2) ||
		fields["first_db_pool_id"] != int64(11) || fields["last_db_pool_id"] != int64(22) {
		t.Fatalf("deferred continuation fields = %#v", fields)
	}
}

func TestManagedSharedDBResumeLoopDoesNotOverlapPasses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	interval := 40 * time.Millisecond
	started := make(chan time.Time, 2)
	release := make(chan struct{}, 2)
	completed := make(chan time.Time, 1)
	done := make(chan struct{})
	var running atomic.Int32
	var maxRunning atomic.Int32
	var attempts atomic.Int32
	go func() {
		runManagedSharedDBResumeLoop(ctx, interval, func(context.Context) {
			attempt := attempts.Add(1)
			current := running.Add(1)
			for {
				observed := maxRunning.Load()
				if current <= observed || maxRunning.CompareAndSwap(observed, current) {
					break
				}
			}
			started <- time.Now()
			<-release
			if attempt == 1 {
				completed <- time.Now()
			}
			running.Add(-1)
		})
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initial managed shared DB resume pass did not start")
	}
	time.Sleep(2 * interval)
	select {
	case <-started:
		t.Fatal("managed shared DB resume loop started an overlapping pass")
	default:
	}
	release <- struct{}{}
	var firstCompletedAt time.Time
	select {
	case firstCompletedAt = <-completed:
	case <-time.After(time.Second):
		t.Fatal("initial managed shared DB resume pass did not complete")
	}
	select {
	case secondStartedAt := <-started:
		if elapsed := secondStartedAt.Sub(firstCompletedAt); elapsed < interval {
			t.Fatalf("second resume pass started %s after completion, want at least %s", elapsed, interval)
		}
	case <-time.After(time.Second):
		t.Fatal("managed shared DB resume loop did not run after the first pass completed")
	}
	cancel()
	release <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("managed shared DB resume loop did not stop after cancellation")
	}
	if got := maxRunning.Load(); got != 1 {
		t.Fatalf("maximum concurrent resume passes = %d, want 1", got)
	}
}

func TestRunManagedSharedDBProvisioningQueueRequeuesWithoutBlockingWorker(t *testing.T) {
	var mu sync.Mutex
	order := make([]int64, 0, 3)
	attempts := make(map[int64]int)
	failed := runManagedSharedDBProvisioningQueue(context.Background(), 1, nil, []int64{1, 2},
		time.Second, 20*time.Millisecond, 20*time.Millisecond, 0,
		func(_ context.Context, dbID int64) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, dbID)
			attempts[dbID]++
			if dbID == 1 && attempts[dbID] == 1 {
				return errors.New("retry")
			}
			return nil
		}, nil)
	if len(failed) != 0 {
		t.Fatalf("failed jobs = %v", failed)
	}
	if !slices.Equal(order, []int64{1, 2, 1}) {
		t.Fatalf("provisioning order = %v, want failed job requeued behind ready work", order)
	}
}

func TestManagedSharedDBProvisioningBackoffCapsAtFifteenSeconds(t *testing.T) {
	if managedSharedDBProvisioningMaxBackoff != 15*time.Second {
		t.Fatalf("shared provisioning max backoff = %s, want 15s", managedSharedDBProvisioningMaxBackoff)
	}
	if managedSharedDBProvisioningCooldown != 10*time.Second {
		t.Fatalf("shared provisioning worker cooldown = %s, want 10s", managedSharedDBProvisioningCooldown)
	}
}

func TestManagedSharedDBProvisioningWorkerRestsBetweenJobs(t *testing.T) {
	const cooldown = 25 * time.Millisecond
	started := make([]time.Time, 0, 2)
	var mu sync.Mutex
	failed := runManagedSharedDBProvisioningQueue(context.Background(), 1, nil, []int64{1, 2},
		time.Second, time.Millisecond, time.Millisecond, cooldown,
		func(_ context.Context, _ int64) error {
			mu.Lock()
			started = append(started, time.Now())
			mu.Unlock()
			return nil
		}, nil)
	if len(failed) != 0 {
		t.Fatalf("failed jobs = %v", failed)
	}
	if len(started) != 2 {
		t.Fatalf("started jobs = %d, want 2", len(started))
	}
	if gap := started[1].Sub(started[0]); gap < cooldown {
		t.Fatalf("worker started next job after %s, want at least %s", gap, cooldown)
	}
}

func TestManagedSharedDBMetadataWorkerRefillsReadySlots(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(pool.Close)
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	rows := make([]*meta.SharedDB, 0, 4)
	for i := 0; i < 4; i++ {
		dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
			TiDBCloudOrganizationID: "org-metadata-batch", ProvisioningKey: bytes.Repeat([]byte{byte(i + 1)}, 32),
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
			PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := metaStore.UpdateManagedSharedDBPoolCloudResult(context.Background(), &meta.SharedDB{
			ID: dbID, TiDBCloudOrganizationID: "org-metadata-batch", ClusterID: fmt.Sprintf("cluster-%d", dbID),
			PasswordCipher: passwordCipher, Name: "tidbcloud_fs", TLSMode: "true",
		}); err != nil {
			t.Fatal(err)
		}
		row, err := metaStore.GetSharedDB(context.Background(), dbID)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	call := 0
	requestBatches := make([][]int64, 0, 3)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	prov.sharedPoolBatchLoadFunc = func(requests []tenant.SharedDBPoolLoadRequest) ([]*tenant.SharedDBPoolInfo, error) {
		call++
		ids := make([]int64, 0, len(requests))
		for _, request := range requests {
			ids = append(ids, request.DBPoolID)
		}
		requestBatches = append(requestBatches, ids)
		ready := requests
		if call == 1 {
			ready = requests[:1]
		}
		out := make([]*tenant.SharedDBPoolInfo, 0, len(ready))
		for _, request := range ready {
			out = append(out, &tenant.SharedDBPoolInfo{DBPoolID: request.DBPoolID, DBPoolUUID: request.DBPoolUUID,
				ClusterID: request.ClusterID, Host: "10.4.48.2", Port: 4000,
				Username: fmt.Sprintf("u%d.root", request.DBPoolID), DBName: "tidbcloud_fs"})
		}
		return out, nil
	}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov,
		managedSharedDBMetadataWorkers: 1, managedSharedDBMetadataBatchSize: 2,
		managedSharedDBMetadataPollInterval: time.Millisecond, managedSharedDBMetadataSlots: make(chan struct{}, 1)}
	readyIDs := make([]int64, 0, 4)
	srv.pollManagedSharedDBMetadataWithReady(context.Background(), rows, func(ids []int64) {
		readyIDs = append(readyIDs, ids...)
	})
	if len(requestBatches) != 3 || !slices.Equal(requestBatches[0], []int64{rows[0].ID, rows[1].ID}) ||
		!slices.Equal(requestBatches[1], []int64{rows[1].ID, rows[2].ID}) ||
		!slices.Equal(requestBatches[2], []int64{rows[3].ID}) {
		t.Fatalf("metadata request batches = %v", requestBatches)
	}
	if len(readyIDs) != 4 {
		t.Fatalf("ready IDs = %v, want all four", readyIDs)
	}
	if got := prov.iamCalls.Load(); got != 0 {
		t.Fatalf("IAM calls = %d, want 0", got)
	}
	for _, row := range rows {
		got, err := metaStore.GetSharedDB(context.Background(), row.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != meta.SharedDBStatusProvisioning {
			t.Fatalf("db pool %d status = %q, want provisioning", row.ID, got.Status)
		}
	}
}

func TestManagedSharedDBMetadataWorkerDoesNotKeepIncompletePoolAlive(t *testing.T) {
	origWindow := schemaInitRetryWindow
	schemaInitRetryWindow = 25 * time.Millisecond
	t.Cleanup(func() { schemaInitRetryWindow = origWindow })
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-metadata-heartbeat", ProvisioningKey: bytes.Repeat([]byte{3}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
		PasswordCipher: []byte("cipher"), Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpdateManagedSharedDBPoolCloudResult(context.Background(), &meta.SharedDB{
		ID: dbID, TiDBCloudOrganizationID: "org-metadata-heartbeat", ClusterID: "cluster-metadata-heartbeat",
		PasswordCipher: []byte("cipher"), Name: "tidbcloud_fs",
	}); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-20 * time.Minute).Truncate(time.Millisecond)
	if _, err := metaStore.DB().ExecContext(context.Background(), `UPDATE db_pool SET updated_at = ? WHERE db_id = ?`, old, dbID); err != nil {
		t.Fatal(err)
	}
	row, err := metaStore.GetSharedDB(context.Background(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	prov.sharedPoolBatchLoadFunc = func(requests []tenant.SharedDBPoolLoadRequest) ([]*tenant.SharedDBPoolInfo, error) {
		return []*tenant.SharedDBPoolInfo{{DBPoolID: requests[0].DBPoolID, DBPoolUUID: requests[0].DBPoolUUID,
			ClusterID: requests[0].ClusterID}}, nil
	}
	srv := &Server{meta: metaStore, provisioner: prov,
		managedSharedDBMetadataWorkers: 1, managedSharedDBMetadataBatchSize: 1,
		managedSharedDBMetadataPollInterval: 5 * time.Millisecond, managedSharedDBMetadataSlots: make(chan struct{}, 1)}
	srv.pollManagedSharedDBMetadataWithReady(context.Background(), []*meta.SharedDB{row}, nil)
	got, err := metaStore.GetSharedDB(context.Background(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.SharedDBStatusPending {
		t.Fatalf("status = %q, want pending while endpoint metadata is incomplete", got.Status)
	}
	if !got.UpdatedAt.Equal(old) {
		t.Fatalf("updated_at = %s, want unchanged incomplete-metadata timestamp %s", got.UpdatedAt, old)
	}
	srv.managedSharedDBStuckTimeout = 15 * time.Minute
	srv.reconcileStuckManagedSharedDBPoolsWithCtx(context.Background())
	got, err = metaStore.GetSharedDB(context.Background(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.SharedDBStatusFailed {
		t.Fatalf("status after stuck reconciliation = %q, want failed", got.Status)
	}
}

func TestManagedSharedDBMetadataRefillSlotSerializesAndRespectsContext(t *testing.T) {
	slot := make(chan struct{}, 1)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- withManagedSharedDBMetadataRefillSlot(context.Background(), slot, func() {
			close(firstStarted)
			<-releaseFirst
		})
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first metadata refill did not acquire the slot")
	}

	waiterCtx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- withManagedSharedDBMetadataRefillSlot(waiterCtx, slot, func() {
			t.Error("cancelled metadata refill entered the critical section")
		})
	}()
	cancel()
	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("cancelled metadata refill error = %v, want context.Canceled", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("cancelled metadata refill remained blocked on the slot")
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first metadata refill: %v", err)
	}
	select {
	case slot <- struct{}{}:
		<-slot
	default:
		t.Fatal("metadata refill slot was not released")
	}
}

func TestProvisionTiDBCloudNativeSharedPlansManagedPoolAndReturnsPending(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)

	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		defaultPublicKey: "public", defaultPrivateKey: "private",
		defaultSharedPublicKey: "shared-public", defaultSharedPrivateKey: "shared-private",
		identityOrg:             "customer-org",
		sharedPoolBatchRequests: make(chan []tenant.SharedDBPoolCreateRequest, 1),
		sharedPoolResults: []*tenant.SharedDBPoolInfo{{
			ClusterID: "cluster-shared", OrganizationID: "physical-org", Password: "root-pass", DBName: "tidbcloud_fs",
		}},
	}
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	srv := NewWithConfig(Config{
		Meta: metaStore, Pool: pool, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared,
		SharedDBSpendingLimit: 2_000_000,
		TokenSecret:           tokenSecret,
	})
	defer srv.Close()
	virtualLimit := int64(2000)
	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{
		CredentialProvisioner: &tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
		Quota:                 &quotaRequest{quotaFields: quotaFields{TiDBCloudSpendingLimit: &virtualLimit}},
	})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	if res.Provider != tenant.ProviderTiDBCloudNativeShared || res.Status != meta.TenantPending {
		t.Fatalf("result = provider %q status %q, want shared/pending", res.Provider, res.Status)
	}
	if res.OrganizationID != "customer-org" {
		t.Fatalf("organization ID = %q, want customer-org", res.OrganizationID)
	}
	tenantRow, err := metaStore.GetTenant(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if tenantRow.Status != meta.TenantPending || tenantRow.Provider != tenant.ProviderTiDBCloudNativeShared {
		t.Fatalf("tenant = provider %q status %q", tenantRow.Provider, tenantRow.Status)
	}
	fsID, err := metaStore.ResolveFsID(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetFsID: %v", err)
	}
	placement, err := metaStore.GetTenantPlacement(context.Background(), fsID)
	if err != nil {
		t.Fatalf("GetTenantPlacement: %v", err)
	}
	dbPool, err := metaStore.GetSharedDB(context.Background(), placement.DbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if dbPool.MaxTenants != 100 || dbPool.TenantCount != 1 || dbPool.SpendingLimit == nil || *dbPool.SpendingLimit != 2_000_000 {
		t.Fatalf("managed pool policy = %+v", dbPool)
	}
	if dbPool.TiDBCloudOrganizationID != "customer-org" {
		t.Fatalf("managed pool organization ID = %q, want customer-org", dbPool.TiDBCloudOrganizationID)
	}
	quota, err := metaStore.GetQuotaConfig(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetQuotaConfig: %v", err)
	}
	if quota.TiDBCloudSpendingLimit == nil || *quota.TiDBCloudSpendingLimit != virtualLimit {
		t.Fatalf("virtual spending limit = %#v, want %d", quota.TiDBCloudSpendingLimit, virtualLimit)
	}
	deadline := time.Now().Add(2 * time.Second)
	for prov.sharedPoolBatchCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if prov.sharedPoolBatchCalls.Load() != 1 {
		t.Fatalf("shared pool batch calls = %d, want 1", prov.sharedPoolBatchCalls.Load())
	}
	if got := prov.lastSharedCredentialRequest(); got.PublicKey != "shared-public" || got.PrivateKey != "shared-private" {
		t.Fatalf("shared physical credential = %+v, want configured shared credential", got)
	}
	requests := <-prov.sharedPoolBatchRequests
	if len(requests) != 1 || requests[0].CustomerOrganizationID != "customer-org" {
		t.Fatalf("shared physical create requests = %+v, want customer organization customer-org", requests)
	}
	iamCredentials := prov.iamCredentialsSnapshot()
	if len(iamCredentials) != 1 || iamCredentials[0].PublicKey != "public" || iamCredentials[0].PrivateKey != "private" {
		t.Fatalf("IAM credentials = %+v, want customer authorization only", iamCredentials)
	}
}

func TestProvisionFreeTiDBCloudNativeSharedUsesCustomerQuotaAndSharedPhysicalSpending(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)

	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		defaultPublicKey: "public", defaultPrivateKey: "private",
		defaultSharedPublicKey: "shared-public", defaultSharedPrivateKey: "shared-private",
		identityOrg:             "customer-org",
		billingFree:             true,
		sharedPoolBatchRequests: make(chan []tenant.SharedDBPoolCreateRequest, 1),
		sharedPoolResults: []*tenant.SharedDBPoolInfo{{
			ClusterID: "cluster-shared", OrganizationID: "physical-org", Password: "root-pass", DBName: "tidbcloud_fs",
		}},
	}
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	srv := NewWithConfig(Config{
		Meta: metaStore, Pool: pool, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared,
		SharedDBSpendingLimit: 2_000_000,
		TokenSecret:           tokenSecret,
	})
	defer srv.Close()
	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{
		CredentialProvisioner: &tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
	})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	if res.Provider != tenant.ProviderTiDBCloudNativeShared || res.Status != meta.TenantPending {
		t.Fatalf("result = provider %q status %q, want shared/pending", res.Provider, res.Status)
	}
	if res.OrganizationID != "customer-org" {
		t.Fatalf("organization ID = %q, want customer-org", res.OrganizationID)
	}
	tenantRow, err := metaStore.GetTenant(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if tenantRow.Status != meta.TenantPending || tenantRow.Provider != tenant.ProviderTiDBCloudNativeShared {
		t.Fatalf("tenant = provider %q status %q", tenantRow.Provider, tenantRow.Status)
	}
	fsID, err := metaStore.ResolveFsID(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetFsID: %v", err)
	}
	placement, err := metaStore.GetTenantPlacement(context.Background(), fsID)
	if err != nil {
		t.Fatalf("GetTenantPlacement: %v", err)
	}
	dbPool, err := metaStore.GetSharedDB(context.Background(), placement.DbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if dbPool.MaxTenants != 100 || dbPool.TenantCount != 1 || dbPool.SpendingLimit == nil || *dbPool.SpendingLimit != 2_000_000 {
		t.Fatalf("managed pool policy = %+v", dbPool)
	}
	if dbPool.TiDBCloudOrganizationID != "customer-org" {
		t.Fatalf("managed pool organization ID = %q, want customer-org", dbPool.TiDBCloudOrganizationID)
	}
	freeCount, err := metaStore.CountTiDBCloudFreeTenants(context.Background(), "customer-org")
	if err != nil || freeCount != 1 {
		t.Fatalf("customer free tenant count = %d, err=%v, want 1", freeCount, err)
	}
	quota, err := metaStore.GetQuotaConfig(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetQuotaConfig: %v", err)
	}
	if quota.MaxStorageBytes != DefaultTiDBCloudFreeMaxStorageBytes ||
		quota.MaxFileSizeBytes != DefaultTiDBCloudFreeMaxFileSizeBytes ||
		quota.MaxFileCount != DefaultTiDBCloudFreeMaxFileCount ||
		quota.TiDBCloudSpendingLimit == nil || *quota.TiDBCloudSpendingLimit != 0 {
		t.Fatalf("free logical tenant quota = %+v", quota)
	}
	deadline := time.Now().Add(2 * time.Second)
	for prov.sharedPoolBatchCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if prov.sharedPoolBatchCalls.Load() != 1 {
		t.Fatalf("shared pool batch calls = %d, want 1", prov.sharedPoolBatchCalls.Load())
	}
	if got := prov.lastSharedCredentialReq; got.PublicKey != "shared-public" || got.PrivateKey != "shared-private" {
		t.Fatalf("shared physical credential = %+v, want configured shared credential", got)
	}
	requests := <-prov.sharedPoolBatchRequests
	if len(requests) != 1 || requests[0].CustomerOrganizationID != "customer-org" {
		t.Fatalf("shared physical create requests = %+v, want customer organization customer-org", requests)
	}
	iamCredentials := prov.iamCredentialsSnapshot()
	if len(iamCredentials) != 1 || iamCredentials[0].PublicKey != "public" || iamCredentials[0].PrivateKey != "private" {
		t.Fatalf("IAM credentials = %+v, want customer authorization only", iamCredentials)
	}
}

func TestProvisionTiDBCloudNativeSharedFallsBackToHardCapacityAfterCreateFailure(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	p := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer p.Close()
	p.SetMetaStore(metaStore)
	provisionErr := errors.New("cloud create unavailable")
	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		sharedPoolBatchErr: provisionErr,
		managedClusters:    []tenant.CloudClusterInfo{{OrganizationID: "org-emergency", ClusterID: "cluster-existing"}},
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	activeID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-emergency", ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 2, SpendingLimit: &spendingTarget,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if _, err := metaStore.DB().ExecContext(context.Background(), `UPDATE db_pool SET status = ?, cluster_id = 'cluster-existing', db_host = 'h', db_port = 4000,
		db_user = 'u', db_password = 'c', db_name = 'shared_db' WHERE db_id = ?`, meta.SharedDBStatusActive, activeID); err != nil {
		t.Fatalf("activate emergency pool: %v", err)
	}
	for i := 1; i <= 2; i++ {
		tenantID := fmt.Sprintf("emergency-existing-%d", i)
		now := time.Now().UTC()
		if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{ID: tenantID, Status: meta.TenantPending, Provider: tenant.ProviderTiDBCloudNativeShared, DBPasswordCipher: []byte{}, SchemaVersion: 1, CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatalf("InsertTenant %s: %v", tenantID, err)
		}
		limit := int64(1000)
		if err := metaStore.SetQuotaConfigPatch(context.Background(), tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimit: &limit}); err != nil {
			t.Fatalf("SetQuotaConfigPatch %s: %v", tenantID, err)
		}
		fsID, err := metaStore.EnsureFsID(context.Background(), tenantID)
		if err != nil {
			t.Fatalf("EnsureFsID %s: %v", tenantID, err)
		}
		key := &meta.APIKey{ID: "key-" + tenantID, TenantID: tenantID, KeyName: "default", JWTCiphertext: []byte("cipher"), JWTHash: "hash-" + tenantID,
			TokenVersion: 1, Status: meta.APIKeyActive, ScopeKind: meta.APIKeyScopeKindOwner, IssuedAt: now, CreatedAt: now, UpdatedAt: now}
		if err := metaStore.CompleteSharedTenantProvision(context.Background(), tenantID, tenant.ProviderTiDBCloudNativeShared, &meta.TenantPlacement{FsID: fsID, DbID: activeID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared}, key); err != nil {
			t.Fatalf("CompleteSharedTenantProvision %s: %v", tenantID, err)
		}
	}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: p, Provisioner: prov, DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: make([]byte, 32)})
	defer srv.Close()
	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{CredentialProvisioner: &tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}})
	if err != nil {
		t.Fatalf("provisionTenant fallback: %v", err)
	}
	fsID, err := metaStore.ResolveFsID(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("ResolveFsID: %v", err)
	}
	placement, err := metaStore.GetTenantPlacement(context.Background(), fsID)
	if err != nil {
		t.Fatalf("GetTenantPlacement: %v", err)
	}
	if placement.DbID != activeID {
		t.Fatalf("fallback placement db = %d, want active pool %d", placement.DbID, activeID)
	}
	if prov.sharedPoolBatchCalls.Load() != 1 {
		t.Fatalf("physical create calls = %d, want 1", prov.sharedPoolBatchCalls.Load())
	}
	rows, err := metaStore.ListSharedDBsByStatus(context.Background(), meta.SharedDBStatusPending, 10)
	if err != nil {
		t.Fatalf("ListSharedDBsByStatus: %v", err)
	}
	if len(rows) != 1 || rows[0].ClusterID != "" || rows[0].TenantCount != 0 {
		t.Fatalf("unresolved provisional pool = %+v, want empty physical/count", rows)
	}
}

func TestProvisionTiDBCloudNativeSharedEmergencyUsesCandidateHardCap(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	p := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer p.Close()
	p.SetMetaStore(metaStore)
	provisionErr := errors.New("cloud create unavailable")
	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		sharedPoolBatchErr: provisionErr,
		managedClusters:    []tenant.CloudClusterInfo{{OrganizationID: "org-candidate-hard-cap", ClusterID: "cluster-existing"}},
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	createActive := func(maxTenants, tenantCount int, clusterID string) int64 {
		t.Helper()
		dbID, createErr := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
			TiDBCloudOrganizationID: "org-candidate-hard-cap", ProvisioningKey: bytes.Repeat([]byte{byte(maxTenants)}, 32),
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: maxTenants, SpendingLimit: &spendingTarget,
		})
		if createErr != nil {
			t.Fatalf("CreateManagedSharedDBPool: %v", createErr)
		}
		if _, createErr = metaStore.DB().ExecContext(context.Background(), `UPDATE db_pool
			SET status = ?, cluster_id = ?, db_host = ?, db_port = 4000, db_user = 'u', db_password = 'c',
				db_name = ?, tenant_count = ?, soft_cap_reached = 1 WHERE db_id = ?`,
			meta.SharedDBStatusActive, clusterID, "h-"+clusterID, "shared_"+clusterID, tenantCount, dbID); createErr != nil {
			t.Fatalf("activate emergency pool: %v", createErr)
		}
		return dbID
	}
	_ = createActive(50, 60, "cluster-old-small")
	wantDBID := createActive(100, 100, "cluster-current-size")

	srv := NewWithConfig(Config{Meta: metaStore, Pool: p, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: make([]byte, 32),
		SharedDBHardCapRatio: 1.2})
	defer srv.Close()
	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{
		CredentialProvisioner: &tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
	})
	if err != nil {
		t.Fatalf("provisionTenant fallback: %v", err)
	}
	placement, err := metaStore.GetTenantPlacement(context.Background(), mustResolveFsID(t, metaStore, res.TenantID))
	if err != nil {
		t.Fatalf("GetTenantPlacement: %v", err)
	}
	if placement.DbID != wantDBID {
		t.Fatalf("fallback placement db = %d, want candidate-sized pool %d", placement.DbID, wantDBID)
	}
}

func TestProvisionTiDBCloudNativeSharedRetriesCapacityRace(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	p := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer p.Close()
	p.SetMetaStore(metaStore)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		managedClusters: []tenant.CloudClusterInfo{{OrganizationID: "org-direct-race", ClusterID: "cluster-existing"}}}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	for i := 0; i < 2; i++ {
		dbID, createErr := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
			TiDBCloudOrganizationID: "org-direct-race", ProvisioningKey: bytes.Repeat([]byte{byte(i + 1)}, 32),
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: 1, SpendingLimit: &spendingTarget,
		})
		if createErr != nil {
			t.Fatalf("CreateManagedSharedDBPool: %v", createErr)
		}
		if _, createErr = metaStore.DB().ExecContext(context.Background(), `UPDATE db_pool
			SET status = ?, cluster_id = ?, db_host = ?, db_port = 4000, db_user = 'u', db_password = 'c',
				db_name = ? WHERE db_id = ?`, meta.SharedDBStatusActive, fmt.Sprintf("cluster-%d", i),
			fmt.Sprintf("h-%d", i), fmt.Sprintf("shared_%d", i), dbID); createErr != nil {
			t.Fatalf("activate pool: %v", createErr)
		}
	}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: p, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: make([]byte, 32)})
	defer srv.Close()

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, provisionErr := srv.provisionTenant(context.Background(), provisionTenantOptions{
				CredentialProvisioner: &tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
			})
			errs <- provisionErr
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for provisionErr := range errs {
		if provisionErr != nil {
			t.Fatalf("concurrent provision returned capacity race: %v", provisionErr)
		}
	}
	rows, err := metaStore.ListSharedDBsByStatus(context.Background(), meta.SharedDBStatusActive, 10)
	if err != nil {
		t.Fatalf("ListSharedDBsByStatus: %v", err)
	}
	if len(rows) != 2 || rows[0].TenantCount != 1 || rows[1].TenantCount != 1 {
		t.Fatalf("active pool counts = %+v, want one tenant in each pool", rows)
	}
}

func TestProvisionTiDBCloudNativeSharedResumesExistingPendingPool(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		sharedPoolResults: []*tenant.SharedDBPoolInfo{{
			ClusterID: "cluster-existing", OrganizationID: "org-shared", Password: "root-pass", DBName: "tidbcloud_fs",
		}},
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	provisioningKey := sharedDBProvisioningKey(tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"})
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-shared", ProvisioningKey: provisioningKey, CloudProvider: "aws", Region: "us-east-1",
		MaxTenants: 100, SpendingLimit: &spendingTarget,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	tokenSecret := make([]byte, 32)
	_, _ = rand.Read(tokenSecret)
	srv := NewWithConfig(Config{
		Meta: metaStore, Pool: pool, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: tokenSecret,
	})
	defer srv.Close()
	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{
		CredentialProvisioner: &tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
	})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	if res.Status != meta.TenantPending {
		t.Fatalf("status = %q, want pending", res.Status)
	}
	placement, err := metaStore.GetTenantPlacement(context.Background(), mustResolveFsID(t, metaStore, res.TenantID))
	if err != nil {
		t.Fatalf("GetTenantPlacement: %v", err)
	}
	if placement.DbID != dbID {
		t.Fatalf("placement db = %d, want existing pool %d", placement.DbID, dbID)
	}
	deadline := time.Now().Add(2 * time.Second)
	for prov.sharedPoolBatchCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if prov.sharedPoolBatchCalls.Load() != 1 {
		t.Fatalf("shared pool batch calls = %d, want resume call", prov.sharedPoolBatchCalls.Load())
	}
	rows, err := metaStore.ListSharedDBsByStatus(context.Background(), meta.SharedDBStatusPending, 10)
	if err != nil {
		t.Fatalf("ListSharedDBsByStatus: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != dbID {
		t.Fatalf("pending pools = %+v, want only %d", rows, dbID)
	}
}

func TestProvisionTiDBCloudNativeSharedUsesRegisteredManualDBPool(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("manual-pass"))
	if err != nil {
		t.Fatal(err)
	}
	dbID, err := metaStore.RegisterSharedDB(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-manual-shared", Host: "manual.example.com", Port: 4000,
		User: "root", PasswordCipher: passwordCipher, Name: "manual_shared", MaxTenants: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative,
		managedClusters: []tenant.CloudClusterInfo{{OrganizationID: "org-manual-shared"}}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: make([]byte, 32)})
	defer srv.Close()
	virtualLimit := int64(4321)
	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{
		CredentialProvisioner: &tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
		Quota:                 &quotaRequest{quotaFields: quotaFields{TiDBCloudSpendingLimit: &virtualLimit}},
	})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	placement, err := metaStore.GetTenantPlacement(context.Background(), mustResolveFsID(t, metaStore, res.TenantID))
	if err != nil {
		t.Fatal(err)
	}
	if placement.DbID != dbID {
		t.Fatalf("placement db = %d, want manual db pool %d", placement.DbID, dbID)
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 0 {
		t.Fatalf("managed physical create calls = %d, want 0", got)
	}
	quota, err := metaStore.GetQuotaConfig(context.Background(), res.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if quota.TiDBCloudSpendingLimit == nil || *quota.TiDBCloudSpendingLimit != virtualLimit {
		t.Fatalf("virtual spending limit = %#v, want %d", quota.TiDBCloudSpendingLimit, virtualLimit)
	}
}

func TestProvisionTiDBCloudNativeRejectsSpendingLimitOnRegisteredSharedDBPool(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("manual-pass"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.RegisterSharedDB(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-native-manual", Host: "manual.example.com", Port: 4000,
		User: "root", PasswordCipher: passwordCipher, Name: "manual_shared", MaxTenants: 100,
	}); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative,
		managedClusters: []tenant.CloudClusterInfo{{OrganizationID: "org-native-manual"}}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNative, TokenSecret: make([]byte, 32),
		DisableDatabaseAutoEmbedding: true})
	defer srv.Close()
	spendingLimit := int64(4321)
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		CredentialProvisioner: &tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
		Quota:                 &quotaRequest{quotaFields: quotaFields{TiDBCloudSpendingLimit: &spendingLimit}},
	})
	if err == nil || !strings.Contains(err.Error(), "spending limit requested for shared-pool tenant") {
		t.Fatalf("provisionTenant error = %v, want registered shared DB spending-limit rejection", err)
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 0 {
		t.Fatalf("shared DB physical create calls = %d, want 0", got)
	}
}

func TestProvisionFreeTiDBCloudNativeAllowsZeroSpendingLimitOnRegisteredSharedDBPool(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("manual-pass"))
	if err != nil {
		t.Fatal(err)
	}
	dbID, err := metaStore.RegisterSharedDB(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-native-manual-free", Host: "manual.example.com", Port: 4000,
		User: "root", PasswordCipher: passwordCipher, Name: "manual_shared", MaxTenants: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNative, TokenSecret: make([]byte, 32),
		DisableDatabaseAutoEmbedding: true})
	defer srv.Close()
	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{
		CredentialProvisioner: &tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"},
		TiDBCloudAccess:       &tiDBCloudAccessProfile{OrganizationID: "org-native-manual-free", IsFree: true},
	})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	placement, err := metaStore.GetTenantPlacement(context.Background(), mustResolveFsID(t, metaStore, res.TenantID))
	if err != nil {
		t.Fatal(err)
	}
	if placement.DbID != dbID {
		t.Fatalf("placement db = %d, want %d", placement.DbID, dbID)
	}
	cfg, err := metaStore.GetQuotaConfig(context.Background(), res.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != 0 {
		t.Fatalf("spending limit = %v, want 0", cfg.TiDBCloudSpendingLimit)
	}
}

func TestManagedSharedDBContinuationWaitsForConnectionMetadata(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-metadata-wait", ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1",
		MaxTenants: 100, SpendingLimit: &spendingTarget, PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpdateManagedSharedDBPoolCloudResult(context.Background(), &meta.SharedDB{
		ID: dbID, TiDBCloudOrganizationID: "org-metadata-wait", ClusterID: "cluster-metadata-wait",
		PasswordCipher: passwordCipher, Name: "tidbcloud_fs", TLSMode: "true",
	}); err != nil {
		t.Fatal(err)
	}
	waitErr := errors.New("metadata wait stopped")
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative,
		sharedPoolResults: []*tenant.SharedDBPoolInfo{{DBPoolID: dbID, ClusterID: "cluster-metadata-wait", OrganizationID: "org-metadata-wait"}},
		sharedPoolWaitErr: waitErr}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: make([]byte, 32)})
	defer srv.Close()
	err = srv.continueManagedSharedDBPool(context.Background(), dbID)
	if !errors.Is(err, waitErr) {
		t.Fatalf("continueManagedSharedDBPool error = %v, want %v", err, waitErr)
	}
	if got := prov.sharedPoolWaitCalls.Load(); got != 1 {
		t.Fatalf("shared metadata wait calls = %d, want 1", got)
	}
}

func TestManagedSharedDBContinuationRejectsProvisioningPoolWithoutConnectionMetadata(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(pool.Close)
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-provisioning-invariant", ProvisioningKey: bytes.Repeat([]byte{8}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
		PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.DB().ExecContext(context.Background(),
		`UPDATE db_pool SET status = ? WHERE db_id = ?`, meta.SharedDBStatusProvisioning, dbID); err != nil {
		t.Fatal(err)
	}
	row, err := metaStore.GetSharedDB(context.Background(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov}
	err = srv.continueManagedSharedDBPoolLocked(context.Background(), row, tenant.CredentialProvisionRequest{
		PublicKey: "shared-public", PrivateKey: "shared-private",
	})
	if err == nil || !strings.Contains(err.Error(), "incomplete connection metadata") {
		t.Fatalf("continueManagedSharedDBPoolLocked error = %v, want incomplete connection metadata", err)
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 0 {
		t.Fatalf("shared batch calls = %d, want 0", got)
	}
	if got := prov.sharedPoolWaitCalls.Load(); got != 0 {
		t.Fatalf("shared metadata wait calls = %d, want 0", got)
	}
}

func TestManagedSharedDBContinuationSkipsFailedPoolBeforeCloudCreate(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(pool.Close)
	pool.SetMetaStore(metaStore)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1"}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov, sharedDBMaxTenants: 100,
		sharedDBSpendingLimit: meta.MaxTiDBCloudSpendingLimit}
	row, err := srv.createManagedSharedDBPlan(context.Background(), "org-failed-continuation", bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatalf("createManagedSharedDBPlan: %v", err)
	}
	if err := metaStore.MarkSharedDBPoolFailed(context.Background(), row.ID); err != nil {
		t.Fatalf("MarkSharedDBPoolFailed: %v", err)
	}
	row, err = metaStore.GetSharedDB(context.Background(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.continueManagedSharedDBPoolLocked(context.Background(), row, tenant.CredentialProvisionRequest{}); err != nil {
		t.Fatalf("continue failed pool: %v", err)
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 0 {
		t.Fatalf("shared batch calls = %d, want no Cloud create for failed pool", got)
	}
}

func TestManagedSharedDBContinuationPassesCustomerOrganizationToPhysicalCreate(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(pool.Close)
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-continuation-label", ProvisioningKey: bytes.Repeat([]byte{7}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
		PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	row, err := metaStore.GetSharedDB(context.Background(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("stop after physical create")
	prov := &fakeProvisioner{
		provider:                tenant.ProviderTiDBCloudNative,
		sharedPoolBatchRequests: make(chan []tenant.SharedDBPoolCreateRequest, 1),
		sharedPoolResults: []*tenant.SharedDBPoolInfo{{
			DBPoolID: dbID, DBPoolUUID: row.UUID, ClusterID: "cluster-continuation-label", DBName: "tidbcloud_fs",
		}},
		sharedPoolPartialErr: wantErr,
	}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov}
	err = srv.continueManagedSharedDBPoolLocked(context.Background(), row, tenant.CredentialProvisionRequest{
		PublicKey: "shared-public", PrivateKey: "shared-private",
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("continueManagedSharedDBPoolLocked error = %v, want %v", err, wantErr)
	}
	requests := <-prov.sharedPoolBatchRequests
	if len(requests) != 1 || requests[0].CustomerOrganizationID != "org-continuation-label" {
		t.Fatalf("continuation physical create requests = %+v, want customer organization org-continuation-label", requests)
	}
}

func TestManagedSharedDBContinuationBatchesDoNotEnterBlockingMetadataWait(t *testing.T) {
	origWindow := schemaInitRetryWindow
	schemaInitRetryWindow = 100 * time.Millisecond
	t.Cleanup(func() {
		schemaInitRetryWindow = origWindow
	})
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(pool.Close)
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	createPool := func(clusterID string, provisioningByte byte) int64 {
		t.Helper()
		dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
			TiDBCloudOrganizationID: "org-scheduled-metadata", ProvisioningKey: bytes.Repeat([]byte{provisioningByte}, 32),
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
			PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := metaStore.UpdateManagedSharedDBPoolCloudResult(context.Background(), &meta.SharedDB{
			ID: dbID, TiDBCloudOrganizationID: "org-scheduled-metadata", ClusterID: clusterID,
			PasswordCipher: passwordCipher, Name: "tidbcloud_fs", TLSMode: "true",
		}); err != nil {
			t.Fatal(err)
		}
		return dbID
	}
	firstID := createPool("cluster-scheduled-first", 1)
	secondID := createPool("cluster-scheduled-second", 2)
	waitRelease := make(chan struct{})
	loadIDs := make(chan int64, 8)
	prov := &fakeProvisioner{
		provider:          tenant.ProviderTiDBCloudNative,
		defaultPublicKey:  "public",
		defaultPrivateKey: "private",
		sharedPoolResults: []*tenant.SharedDBPoolInfo{
			{DBPoolID: firstID, ClusterID: "cluster-scheduled-first", OrganizationID: "org-scheduled-metadata"},
			{DBPoolID: secondID, ClusterID: "cluster-scheduled-second", OrganizationID: "org-scheduled-metadata"},
		},
		sharedPoolLoadIDs:     loadIDs,
		sharedPoolWaitRelease: waitRelease,
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov, forkWorkerCtx: workerCtx}
	t.Cleanup(func() {
		cancel()
		srv.forkWorkerWG.Wait()
	})
	srv.scheduleManagedSharedDBContinuations(context.Background(), []int64{firstID, secondID})

	expectConcurrentIDs := func(ids <-chan int64, phase string) {
		t.Helper()
		seen := make(map[int64]bool, 2)
		for i := 0; i < 2; i++ {
			select {
			case got := <-ids:
				seen[got] = true
			case <-time.After(time.Second):
				t.Fatalf("%s continuation %d/2 was not attempted concurrently", phase, i+1)
			}
		}
		if !seen[firstID] || !seen[secondID] {
			t.Fatalf("%s continuation IDs = %v, want %d and %d", phase, seen, firstID, secondID)
		}
	}
	expectConcurrentIDs(loadIDs, "scheduled")
	if got := prov.sharedPoolWaitCalls.Load(); got != 0 {
		t.Fatalf("scheduled continuation metadata waiter calls = %d, want 0", got)
	}
	scheduledDone := make(chan struct{})
	go func() {
		srv.forkWorkerWG.Wait()
		close(scheduledDone)
	}()
	select {
	case <-scheduledDone:
	case <-time.After(time.Second):
		t.Fatal("scheduled continuation batch did not finish within its retry window")
	}

	resumeLoadIDs := make(chan int64, 8)
	prov.sharedPoolLoadIDs = resumeLoadIDs
	resumeCtx, cancelResume := context.WithCancel(context.Background())
	resumeDone := make(chan struct{})
	t.Cleanup(func() {
		cancelResume()
		<-resumeDone
	})
	go func() {
		defer close(resumeDone)
		srv.resumePendingManagedSharedDBPoolsWithCtx(resumeCtx)
	}()
	expectConcurrentIDs(resumeLoadIDs, "resumed")
	cancelResume()
	<-resumeDone
}

func TestManagedSharedDBContinuationKeepsRootAndSkipsSystemUser(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-root", ProvisioningKey: bytes.Repeat([]byte{9}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
		PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpdateManagedSharedDBPoolCloudResult(context.Background(), &meta.SharedDB{
		ID: dbID, TiDBCloudOrganizationID: "org-root", ClusterID: "cluster-root",
		Host: "127.0.0.1", Port: 1, User: "prefix.root", PasswordCipher: passwordCipher,
		Name: "tidbcloud_fs", TLSMode: "true",
	}); err != nil {
		t.Fatal(err)
	}
	row, err := metaStore.GetSharedDB(context.Background(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	systemUserErr := errors.New("system user creation must not run for shared DB")
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, systemUserErr: systemUserErr}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov, tidbCloudRBACCache: newTiDBCloudRBACCache(time.Hour)}
	err = srv.continueManagedSharedDBPoolLocked(context.Background(), row, tenant.CredentialProvisionRequest{
		PublicKey: "shared-public", PrivateKey: "shared-private",
	})
	if errors.Is(err, systemUserErr) {
		t.Fatalf("shared continuation attempted system-user creation: %v", err)
	}
	if got := prov.systemUserCalls.Load(); got != 0 {
		t.Fatalf("shared system-user calls = %d, want 0", got)
	}
}

func TestManagedSharedDBBatchCreateSharesPerPoolOwnershipWithDirectEnsure(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-batch-lock", ProvisioningKey: bytes.Repeat([]byte{2}, 32),
		CloudProvider: "aws", Region: "us-east-1",
		MaxTenants: 100, SpendingLimit: &spendingTarget, PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative,
		sharedPoolBatchStarted: started, sharedPoolBatchRelease: release,
		sharedPoolBatchRequests: make(chan []tenant.SharedDBPoolCreateRequest, 1),
		sharedPoolResults: []*tenant.SharedDBPoolInfo{{ClusterID: "cluster-batch-lock", OrganizationID: "org-batch-lock",
			Host: "db.example.com", Port: 4000, Username: "u.root", DBName: "tidbcloud_fs"}}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: make([]byte, 32)})
	defer srv.Close()
	batchDone := make(chan error, 1)
	go func() {
		_, err := srv.provisionManagedSharedDBPoolsBatch(context.Background(), []int64{dbID})
		batchDone <- err
	}()
	<-started
	ensureDone := make(chan error, 1)
	go func() {
		_, err := srv.ensureManagedSharedDBPhysical(context.Background(), dbID)
		ensureDone <- err
	}()
	secondCreateStarted := false
	select {
	case <-started:
		secondCreateStarted = true
	case <-time.After(250 * time.Millisecond):
	}
	close(release)
	if err := <-batchDone; err != nil {
		t.Fatalf("batch create: %v", err)
	}
	if err := <-ensureDone; err != nil {
		t.Fatalf("concurrent ensure: %v", err)
	}
	if secondCreateStarted {
		t.Fatal("concurrent ensure issued a duplicate physical create while batch create was in flight")
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 1 {
		t.Fatalf("physical create calls = %d, want 1", got)
	}
	requests := <-prov.sharedPoolBatchRequests
	if len(requests) != 1 || requests[0].CustomerOrganizationID != "org-batch-lock" {
		t.Fatalf("batch physical create requests = %+v, want customer organization org-batch-lock", requests)
	}
}

func createActiveManagedSharedDBForAllocationTest(t *testing.T, metaStore *meta.Store, organizationID string) int64 {
	t.Helper()
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: organizationID, ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.DB().ExecContext(context.Background(), `UPDATE db_pool SET status = ?,
		db_host = 'h', db_port = 4000, db_user = 'u', db_password = 'c', db_name = 'shared_db'
		WHERE db_id = ?`, meta.SharedDBStatusActive, dbID); err != nil {
		t.Fatal(err)
	}
	return dbID
}

func TestManagedSharedDBReservationSerializesSameOrganizationLocally(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	createActiveManagedSharedDBForAllocationTest(t, metaStore, "org-reservation-serial")
	srv := &Server{meta: metaStore}

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		_, _, err := srv.allocateManagedSharedDBForOrganization(context.Background(), "org-reservation-serial",
			tenant.CredentialProvisionRequest{}, func(*meta.SharedDB) error {
				close(firstStarted)
				<-releaseFirst
				return nil
			})
		firstDone <- err
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first reservation did not start")
	}
	go func() {
		_, _, err := srv.allocateManagedSharedDBForOrganization(context.Background(), "org-reservation-serial",
			tenant.CredentialProvisionRequest{}, func(*meta.SharedDB) error {
				close(secondStarted)
				return nil
			})
		secondDone <- err
	}()
	select {
	case <-secondStarted:
		t.Error("same-organization reservation entered concurrently")
	case <-time.After(250 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first reservation: %v", err)
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second reservation did not resume")
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second reservation: %v", err)
	}
	entries := 0
	srv.sharedDBReservationLocks.Range(func(_, _ any) bool {
		entries++
		return true
	})
	if entries != 0 {
		t.Fatalf("reservation lock entries after all users finished = %d, want 0", entries)
	}
}

func TestManagedSharedDBReservationKeepsOrganizationsParallel(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	createActiveManagedSharedDBForAllocationTest(t, metaStore, "org-reservation-a")
	createActiveManagedSharedDBForAllocationTest(t, metaStore, "org-reservation-b")
	srv := &Server{meta: metaStore}

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		_, _, err := srv.allocateManagedSharedDBForOrganization(context.Background(), "org-reservation-a",
			tenant.CredentialProvisionRequest{}, func(*meta.SharedDB) error {
				close(firstStarted)
				<-release
				return nil
			})
		firstDone <- err
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first organization reservation did not start")
	}
	go func() {
		_, _, err := srv.allocateManagedSharedDBForOrganization(context.Background(), "org-reservation-b",
			tenant.CredentialProvisionRequest{}, func(*meta.SharedDB) error {
				close(secondStarted)
				<-release
				return nil
			})
		secondDone <- err
	}()
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("different-organization reservation was serialized")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first organization reservation: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second organization reservation: %v", err)
	}
}

func TestManagedSharedDBReservationCoversPhysicalPlanningTransition(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	srv := &Server{meta: metaStore}
	const organizationID = "org-reservation-planning"
	const identity = "org:" + organizationID

	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- metaStore.WithSharedDBAllocationLock(context.Background(), identity, func(context.Context) error {
			close(holderStarted)
			<-releaseHolder
			return nil
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("physical planning lock holder did not start")
	}
	var releaseOnce sync.Once
	releasePlanning := func() { releaseOnce.Do(func() { close(releaseHolder) }) }
	defer func() {
		releasePlanning()
		select {
		case err := <-holderDone:
			if err != nil {
				t.Errorf("release physical planning lock: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("physical planning lock holder did not stop")
		}
	}()

	firstReserveEntered := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := srv.allocateManagedSharedDBForOrganization(context.Background(), organizationID,
			tenant.CredentialProvisionRequest{}, func(*meta.SharedDB) error {
				close(firstReserveEntered)
				return nil
			})
		firstDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if _, waiting := srv.sharedDBAllocationLocks.Load(identity); waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first allocation did not reach physical planning")
		}
		time.Sleep(time.Millisecond)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	if _, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: organizationID, ProvisioningKey: bytes.Repeat([]byte{2}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
	}); err != nil {
		t.Fatal(err)
	}

	secondReserveEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		_, _, err := srv.allocateManagedSharedDBForOrganization(context.Background(), organizationID,
			tenant.CredentialProvisionRequest{}, func(*meta.SharedDB) error {
				close(secondReserveEntered)
				return nil
			})
		secondDone <- err
	}()
	select {
	case <-secondReserveEntered:
		t.Error("same-organization reservation bypassed admission during physical planning")
	case <-time.After(250 * time.Millisecond):
	}

	releasePlanning()
	select {
	case <-firstReserveEntered:
	case <-time.After(time.Second):
		t.Fatal("first reservation did not finish physical planning")
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first allocation: %v", err)
	}
	select {
	case <-secondReserveEntered:
	case <-time.After(time.Second):
		t.Fatal("second reservation did not resume after physical planning")
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second allocation: %v", err)
	}
}

func TestSharedDBAllocationLockWaitRespectsContextAndCleansUp(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	srv := &Server{meta: metaStore}

	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- srv.withSharedDBAllocationLock(context.Background(), "org:cancel-lock", func(context.Context) error {
			close(holderStarted)
			<-releaseHolder
			return nil
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("allocation lock holder did not start")
	}

	waiterCtx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- srv.withSharedDBAllocationLock(waiterCtx, "org:cancel-lock", func(context.Context) error {
			return errors.New("cancelled waiter entered critical section")
		})
	}()
	cancel()
	select {
	case waitErr := <-waiterDone:
		if !errors.Is(waitErr, context.Canceled) {
			t.Errorf("cancelled waiter error = %v, want context.Canceled", waitErr)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("cancelled waiter remained blocked on the local allocation lock")
	}
	close(releaseHolder)
	if err := <-holderDone; err != nil {
		t.Fatalf("allocation lock holder: %v", err)
	}
	select {
	case waitErr := <-waiterDone:
		if !errors.Is(waitErr, context.Canceled) {
			t.Errorf("eventual cancelled waiter error = %v, want context.Canceled", waitErr)
		}
	default:
	}
	entries := 0
	srv.sharedDBAllocationLocks.Range(func(_, _ any) bool {
		entries++
		return true
	})
	if entries != 0 {
		t.Fatalf("allocation lock entries after all users finished = %d, want 0", entries)
	}
}

func TestEnsureManagedSharedDBPhysicalUsesPoolLockForKnownCluster(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-known-cluster", ProvisioningKey: bytes.Repeat([]byte{7}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
		PasswordCipher: []byte("cipher"), Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpdateManagedSharedDBPoolCloudResult(context.Background(), &meta.SharedDB{
		ID: dbID, TiDBCloudOrganizationID: "org-known-cluster", ClusterID: "cluster-known",
		Host: "db.example.com", Port: 4000, User: "prefix.root", PasswordCipher: []byte("cipher"), Name: "tidbcloud_fs", TLSMode: "true",
	}); err != nil {
		t.Fatal(err)
	}
	srv := &Server{meta: metaStore, provisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}}
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- srv.withSharedDBPoolWorkLock(context.Background(), dbID, func(context.Context) error {
			close(holderStarted)
			<-releaseHolder
			return nil
		})
	}()
	<-holderStarted
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = srv.ensureManagedSharedDBPhysical(ctx, dbID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("known-cluster ensure error = %v, want context deadline while pool lock is held", err)
	}
	close(releaseHolder)
	if err := <-holderDone; err != nil {
		t.Fatalf("pool lock holder: %v", err)
	}
}

func TestManagedSharedDBProvisioningDoesNotWaitForPoolWorkLock(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(pool.Close)
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-lock-free-provisioning", ProvisioningKey: bytes.Repeat([]byte{6}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
		PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpdateManagedSharedDBPoolCloudResult(context.Background(), &meta.SharedDB{
		ID: dbID, TiDBCloudOrganizationID: "org-lock-free-provisioning", ClusterID: "cluster-lock-free-provisioning",
		Host: "127.0.0.1", Port: 1, User: "prefix.root", PasswordCipher: passwordCipher,
		Name: "tidbcloud_fs", TLSMode: "true",
	}); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("schema provisioning reached without waiting for MetaDB pool-work lock")
	provisioner := &profileAwareFakeProvisioner{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		ensureDBErr:     wantErr,
	}
	srv := &Server{meta: metaStore, pool: pool, provisioner: provisioner}
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- srv.withSharedDBPoolWorkLock(context.Background(), dbID, func(context.Context) error {
			close(holderStarted)
			<-releaseHolder
			return nil
		})
	}()
	<-holderStarted

	continuationCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	continuationErr := srv.continueManagedSharedDBPoolOnce(continuationCtx, dbID)
	cancel()
	close(releaseHolder)
	if err := <-holderDone; err != nil {
		t.Fatalf("pool lock holder: %v", err)
	}
	if !errors.Is(continuationErr, wantErr) {
		t.Fatalf("provisioning continuation error = %v, want EnsureDatabase sentinel without waiting for pool-work lock", continuationErr)
	}
}

func TestManagedSharedDBLockedContinuationDecryptsRootPasswordOnce(t *testing.T) {
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	localEncryptor, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	passwordCipher, err := localEncryptor.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	countedEncryptor := &countingEncryptor{Encryptor: localEncryptor}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, countedEncryptor)
	t.Cleanup(pool.Close)

	wantErr := errors.New("stop after EnsureDatabase")
	provisioner := &profileAwareFakeProvisioner{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		ensureDBErr:     wantErr,
	}
	srv := &Server{pool: pool, provisioner: provisioner}
	err = srv.continueManagedSharedDBPoolLocked(context.Background(), &meta.SharedDB{
		ID:                      1,
		UUID:                    "uuid",
		TiDBCloudOrganizationID: "org",
		ClusterID:               "cluster",
		Host:                    "127.0.0.1",
		Port:                    4000,
		User:                    "prefix.root",
		PasswordCipher:          passwordCipher,
		Name:                    "tidbcloud_fs",
		TLSMode:                 "true",
		Status:                  meta.SharedDBStatusProvisioning,
	}, tenant.CredentialProvisionRequest{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("locked continuation error = %v, want %v", err, wantErr)
	}
	if got := countedEncryptor.decryptCalls.Load(); got != 1 {
		t.Fatalf("root password decrypt calls = %d, want 1", got)
	}
}

func TestManagedSharedDBProvisioningWorkerSkipsPendingCloudWork(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-provisioning-skips-pending",
		ProvisioningKey:         bytes.Repeat([]byte{8}, 32),
		CloudProvider:           "aws",
		Region:                  "us-east-1",
		MaxTenants:              100,
		SpendingLimit:           &spendingTarget,
		Name:                    "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	provisioner := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	srv := &Server{meta: metaStore, provisioner: provisioner}

	if err := srv.continueManagedSharedDBProvisioningOnce(context.Background(), dbID); err != nil {
		t.Fatalf("provisioning-only continuation for pending row: %v", err)
	}
	if got := provisioner.iamCalls.Load(); got != 0 {
		t.Fatalf("pending row IAM calls = %d, want 0", got)
	}
	if got := provisioner.sharedPoolBatchCalls.Load(); got != 0 {
		t.Fatalf("pending row Cloud create calls = %d, want 0", got)
	}
}

func TestManagedSharedDBProvisioningHeartbeatPreventsStuckFailure(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(pool.Close)
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-provisioning-heartbeat", ProvisioningKey: bytes.Repeat([]byte{5}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
		PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpdateManagedSharedDBPoolCloudResult(context.Background(), &meta.SharedDB{
		ID: dbID, TiDBCloudOrganizationID: "org-provisioning-heartbeat", ClusterID: "cluster-provisioning-heartbeat",
		Host: "127.0.0.1", Port: 1, User: "prefix.root", PasswordCipher: passwordCipher,
		Name: "tidbcloud_fs", TLSMode: "true",
	}); err != nil {
		t.Fatal(err)
	}
	staleAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	if _, err := metaStore.DB().ExecContext(context.Background(), `UPDATE db_pool SET updated_at = ? WHERE db_id = ?`, staleAt, dbID); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	provisioner := &blockingDatabaseEnsurer{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		started:         started,
		release:         release,
	}
	const stuckTimeout = 120 * time.Millisecond
	srv := &Server{
		meta: metaStore, pool: pool, provisioner: provisioner,
		managedSharedDBStuckTimeout: stuckTimeout,
	}
	continuationDone := make(chan error, 1)
	go func() {
		continuationDone <- srv.continueManagedSharedDBPoolOnce(context.Background(), dbID)
	}()
	<-started

	deadline := time.Now().Add(2 * time.Second)
	for {
		row, err := metaStore.GetSharedDB(context.Background(), dbID)
		if err != nil {
			t.Fatal(err)
		}
		if row.UpdatedAt.After(staleAt) {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			<-continuationDone
			t.Fatal("provisioning heartbeat did not refresh db_pool.updated_at")
		}
		time.Sleep(5 * time.Millisecond)
	}

	srv.reconcileStuckManagedSharedDBPoolsWithCtx(context.Background())
	row, err := metaStore.GetSharedDB(context.Background(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != meta.SharedDBStatusProvisioning {
		t.Fatalf("actively provisioning pool status = %q, want provisioning", row.Status)
	}

	close(release)
	if err := <-continuationDone; err == nil {
		t.Fatal("provisioning continuation unexpectedly succeeded")
	}
}

func TestManagedSharedDBProvisioningHeartbeatFailureDoesNotCancelSchemaWork(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	srv := &Server{meta: metaStore, managedSharedDBStuckTimeout: 40 * time.Millisecond}
	core, observed := observer.New(zap.WarnLevel)
	workCtx := logger.WithContext(context.Background(), zap.New(core))
	workFinished := false
	err = srv.withManagedSharedDBProvisioningHeartbeat(workCtx, 999_999, func(schemaCtx context.Context) error {
		deadline := time.NewTimer(time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-schemaCtx.Done():
				return fmt.Errorf("schema work context canceled by heartbeat: %w", schemaCtx.Err())
			case <-ticker.C:
				if observed.FilterMessage("managed_shared_db_pool_provisioning_heartbeat_failed").Len() > 0 {
					workFinished = true
					return nil
				}
			case <-deadline.C:
				return fmt.Errorf("heartbeat failure was not observed")
			}
		}
	})
	if err != nil {
		t.Fatalf("schema work after heartbeat refresh failure: %v", err)
	}
	if !workFinished {
		t.Fatal("schema work did not finish after heartbeat refresh failure")
	}
}

func TestManagedSharedDBProvisioningHeartbeatDoesNotWarnAfterStatusAdvance(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-heartbeat-advanced", ProvisioningKey: bytes.Repeat([]byte{6}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.DB().ExecContext(context.Background(), `UPDATE db_pool SET status = ? WHERE db_id = ?`, meta.SharedDBStatusActive, dbID); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore, managedSharedDBStuckTimeout: 40 * time.Millisecond}
	core, observed := observer.New(zap.WarnLevel)
	workCtx := logger.WithContext(context.Background(), zap.New(core))
	if err := srv.withManagedSharedDBProvisioningHeartbeat(workCtx, dbID, func(context.Context) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatalf("schema work after status advance: %v", err)
	}
	if got := observed.FilterMessage("managed_shared_db_pool_provisioning_heartbeat_failed").Len(); got != 0 {
		t.Fatalf("heartbeat warnings after status advance = %d, want 0", got)
	}
}

func TestManagedSharedDBBatchCreateSubmitsFiftyPoolsInOneRequest(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbIDs := make([]int64, 0, 50)
	for i := 0; i < 50; i++ {
		dbID, createErr := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
			TiDBCloudOrganizationID: "org-parallel-batches", ProvisioningKey: bytes.Repeat([]byte{3}, 32),
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
			PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		dbIDs = append(dbIDs, dbID)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	requests := make(chan []tenant.SharedDBPoolCreateRequest, 1)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, identityOrg: "org-parallel-batches",
		sharedPoolBatchStarted: started, sharedPoolBatchRelease: release, sharedPoolBatchRequests: requests}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov,
		tidbCloudRBACCache: newTiDBCloudRBACCache(time.Hour), managedSharedDBCloudBatchSize: 50}
	done := make(chan error, 1)
	go func() {
		_, batchErr := srv.provisionManagedSharedDBPoolsBatch(context.Background(), dbIDs)
		done <- batchErr
	}()

	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		close(release)
		<-done
		t.Fatal("Cloud batch request did not start")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("batch create: %v", err)
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 1 {
		t.Fatalf("batch calls = %d, want 1", got)
	}
	if got := prov.sharedPoolBatchMembers.Load(); got != 50 {
		t.Fatalf("batch members = %d, want 50", got)
	}
	if got := prov.sharedPoolBatchLoadCalls.Load(); got != 1 {
		t.Fatalf("batch adoption list calls = %d, want 1 for the whole physical wave", got)
	}
	if got := len(<-requests); got != 50 {
		t.Fatalf("batch size = %d, want 50", got)
	}
}

func TestManagedSharedDBBatchCreateDoesNotWaitForCompletedPlanningLocks(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	type testPool struct {
		organizationID  string
		provisioningKey []byte
		dbID            int64
	}
	testPools := []testPool{
		{organizationID: "org-batch-a", provisioningKey: bytes.Repeat([]byte{1}, 32)},
		{organizationID: "org-batch-b", provisioningKey: bytes.Repeat([]byte{2}, 32)},
	}
	dbIDs := make([]int64, 0, len(testPools))
	for i := range testPools {
		dbID, createErr := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
			TiDBCloudOrganizationID: testPools[i].organizationID, ProvisioningKey: testPools[i].provisioningKey,
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
			PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		testPools[i].dbID = dbID
		dbIDs = append(dbIDs, dbID)
	}

	requests := make(chan []tenant.SharedDBPoolCreateRequest, len(testPools))
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, sharedPoolBatchRequests: requests}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov,
		tidbCloudRBACCache: newTiDBCloudRBACCache(time.Hour), managedSharedDBCloudBatchSize: 10}
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- srv.withSharedDBAllocationLock(context.Background(),
			sharedDBAllocationIdentity(testPools[0].organizationID, testPools[0].provisioningKey),
			func(context.Context) error {
				close(holderStarted)
				<-releaseHolder
				return nil
			})
	}()
	<-holderStarted

	batchDone := make(chan error, 1)
	go func() {
		_, batchErr := srv.provisionManagedSharedDBPoolsBatchWithCredentials(
			context.Background(), dbIDs, tenant.CredentialProvisionRequest{})
		batchDone <- batchErr
	}()
	startedOrganizations := make(map[string]bool, len(testPools))
	for range testPools {
		select {
		case request := <-requests:
			if len(request) != 1 {
				close(releaseHolder)
				<-holderDone
				<-batchDone
				t.Fatalf("batch request = %+v, want one physical pool", request)
			}
			startedOrganizations[request[0].CustomerOrganizationID] = true
		case <-time.After(500 * time.Millisecond):
			close(releaseHolder)
			<-holderDone
			<-batchDone
			t.Fatal("Cloud create waited for an organization planning lock after its db_pool row existed")
		}
	}
	close(releaseHolder)
	if err := <-holderDone; err != nil {
		t.Fatalf("allocation lock holder: %v", err)
	}
	if err := <-batchDone; err != nil {
		t.Fatalf("batch create: %v", err)
	}
	for _, testPool := range testPools {
		if !startedOrganizations[testPool.organizationID] {
			t.Fatalf("organization %q Cloud create did not start while planning lock was held", testPool.organizationID)
		}
	}
}

func TestManagedSharedDBBatchCreateRunsSameOrganizationPoolsConcurrently(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	const organizationID = "org-batch-parallel"
	dbIDs := make([]int64, 0, 2)
	for i := 0; i < 2; i++ {
		dbID, createErr := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
			TiDBCloudOrganizationID: organizationID, ProvisioningKey: bytes.Repeat([]byte{byte(i + 1)}, 32),
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
			PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		dbIDs = append(dbIDs, dbID)
	}

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative,
		sharedPoolBatchStarted: started, sharedPoolBatchRelease: release}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov,
		tidbCloudRBACCache: newTiDBCloudRBACCache(time.Hour), managedSharedDBCloudBatchSize: 10}
	done := make(chan error, 2)
	for _, dbID := range dbIDs {
		go func() {
			_, batchErr := srv.provisionManagedSharedDBPoolsBatchWithCredentials(
				context.Background(), []int64{dbID}, tenant.CredentialProvisionRequest{})
			done <- batchErr
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("first same-organization Cloud create did not start")
	}
	secondStarted := false
	select {
	case <-started:
		secondStarted = true
	case <-time.After(250 * time.Millisecond):
	}
	close(release)
	for range dbIDs {
		if batchErr := <-done; batchErr != nil {
			t.Fatalf("batch create: %v", batchErr)
		}
	}
	if !secondStarted {
		t.Fatal("same-organization Cloud creates for different db pools were serialized")
	}
}

func TestManagedSharedDBBatchCreateReturnsTotalFailure(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-batch-failure", ProvisioningKey: bytes.Repeat([]byte{9}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
		PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("cloud batch rejected")
	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudNative, identityOrg: "org-batch-failure", sharedPoolBatchErr: wantErr,
	}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: make([]byte, 32)})
	defer srv.Close()

	_, err = srv.provisionManagedSharedDBPoolsBatch(context.Background(), []int64{dbID})
	if !errors.Is(err, wantErr) {
		t.Fatalf("batch error = %v, want %v", err, wantErr)
	}
}

func TestManagedSharedDBBatchCreateAdoptsExistingCloudPoolBeforeRetry(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	_, _ = rand.Read(master)
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()
	pool.SetMetaStore(metaStore)
	passwordCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-adopt-before-retry", ProvisioningKey: bytes.Repeat([]byte{7}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
		PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	row, err := metaStore.GetSharedDB(context.Background(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	prov.sharedPoolBatchLoadFunc = func(requests []tenant.SharedDBPoolLoadRequest) ([]*tenant.SharedDBPoolInfo, error) {
		if len(requests) != 1 || requests[0].DBPoolID != dbID || requests[0].DBPoolUUID != row.UUID || requests[0].ClusterID != "" {
			t.Fatalf("adoption requests = %+v", requests)
		}
		return []*tenant.SharedDBPoolInfo{{
			DBPoolID: dbID, DBPoolUUID: row.UUID, ClusterID: "cluster-adopted",
			OrganizationID: "org-adopt-before-retry", DBName: "tidbcloud_fs",
		}}, nil
	}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov,
		tidbCloudRBACCache: newTiDBCloudRBACCache(time.Hour), managedSharedDBCloudBatchSize: 50}
	_, err = srv.provisionManagedSharedDBPoolsBatchWithCredentials(context.Background(), []int64{dbID}, tenant.CredentialProvisionRequest{})
	if err != nil {
		t.Fatalf("batch create adoption: %v", err)
	}
	if got := prov.sharedPoolBatchLoadCalls.Load(); got != 1 {
		t.Fatalf("batch adoption list calls = %d, want 1", got)
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 0 {
		t.Fatalf("Cloud create calls = %d, want 0 after adoption", got)
	}
	persisted, err := metaStore.GetSharedDB(context.Background(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.ClusterID != "cluster-adopted" {
		t.Fatalf("persisted cluster_id = %q, want cluster-adopted", persisted.ClusterID)
	}
}

func TestManagedSharedDBBatchCreateRejectsUnsupportedProvisioner(t *testing.T) {
	srv := &Server{provisioner: nonBranchOnlyProvisioner{}}
	_, err := srv.provisionManagedSharedDBPoolsBatchWithCredentials(context.Background(), []int64{1}, tenant.CredentialProvisionRequest{})
	if err == nil || !strings.Contains(err.Error(), "does not support managed shared db pools") {
		t.Fatalf("batch create error = %v, want unsupported provisioner", err)
	}
}

func TestManagedSharedDBBatchCreatePersistsResultsAfterInvalidEntry(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	rows := make(map[int64]*meta.SharedDB, 2)
	requests := make([]tenant.SharedDBPoolCreateRequest, 0, 2)
	for i := 0; i < 2; i++ {
		dbID, createErr := metaStore.CreateManagedSharedDBPool(context.Background(), &meta.SharedDB{
			TiDBCloudOrganizationID: "org-partial-persist", ProvisioningKey: bytes.Repeat([]byte{byte(i + 1)}, 32),
			CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingTarget,
			PasswordCipher: []byte("cipher"), Name: "tidbcloud_fs",
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		row, loadErr := metaStore.GetSharedDB(context.Background(), dbID)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		rows[dbID] = row
		requests = append(requests, tenant.SharedDBPoolCreateRequest{DBPoolID: dbID, DBPoolUUID: row.UUID})
	}
	partialErr := errors.New("partial cloud response")
	second := rows[requests[1].DBPoolID]
	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudNative,
		sharedPoolResults: []*tenant.SharedDBPoolInfo{
			{DBPoolID: 999999, DBPoolUUID: "unknown", ClusterID: "cluster-unknown"},
			{DBPoolID: second.ID, DBPoolUUID: second.UUID, ClusterID: "cluster-persisted", Host: "db.example.com", Port: 4000, Username: "prefix.root", DBName: second.Name},
		},
		sharedPoolPartialErr: partialErr,
	}
	srv := &Server{meta: metaStore}
	err = srv.provisionManagedSharedDBPoolsBatchChunkClaimed(context.Background(), prov, requests, rows, tenant.CredentialProvisionRequest{})
	if !errors.Is(err, partialErr) {
		t.Fatalf("batch create error = %v, want partial cloud error", err)
	}
	if err == nil || !strings.Contains(err.Error(), "unknown db pool") {
		t.Fatalf("batch create error = %v, want invalid result error", err)
	}
	persisted, err := metaStore.GetSharedDB(context.Background(), second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.ClusterID != "cluster-persisted" {
		t.Fatalf("later cloud result cluster_id = %q, want cluster-persisted", persisted.ClusterID)
	}
}

func mustResolveFsID(t *testing.T, store *meta.Store, tenantID string) int64 {
	t.Helper()
	fsID, err := store.ResolveFsID(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("ResolveFsID: %v", err)
	}
	return fsID
}

func (f *fakeProvisioner) ProvisioningCloudProvider() string { return f.cloudProvider }

func (f *fakeProvisioner) ProvisioningRegion() string { return f.region }

func (f *fakeProvisioner) DefaultTiDBCloudSpendingLimit() int64 { return 1000 }

func (f *fakeProvisioner) ListManagedClusters(_ context.Context, _ tenant.CredentialProvisionRequest, _ tenant.ManagedClusterListOptions) (*tenant.ManagedClusterListResult, error) {
	return &tenant.ManagedClusterListResult{Clusters: append([]tenant.CloudClusterInfo(nil), f.managedClusters...)}, nil
}

func (f *fakeProvisioner) BatchProvisionSharedDBPoolsWithCredentials(ctx context.Context, requests []tenant.SharedDBPoolCreateRequest, cred tenant.CredentialProvisionRequest) ([]*tenant.SharedDBPoolInfo, error) {
	f.sharedPoolMu.Lock()
	f.lastSharedCredentialReq = cred
	f.sharedPoolMu.Unlock()
	if f.sharedPoolBatchRequests != nil {
		f.sharedPoolBatchRequests <- append([]tenant.SharedDBPoolCreateRequest(nil), requests...)
	}
	f.sharedPoolBatchCalls.Add(1)
	f.sharedPoolBatchMembers.Add(int32(len(requests)))
	if f.sharedPoolBatchStarted != nil {
		select {
		case f.sharedPoolBatchStarted <- struct{}{}:
		default:
		}
	}
	if f.sharedPoolBatchRelease != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.sharedPoolBatchRelease:
		}
	}
	if f.sharedPoolBatchErr != nil {
		return nil, f.sharedPoolBatchErr
	}
	if len(f.sharedPoolResults) > 0 {
		out := make([]*tenant.SharedDBPoolInfo, len(f.sharedPoolResults))
		for i, row := range f.sharedPoolResults {
			copyRow := *row
			if copyRow.DBPoolID == 0 && i < len(requests) {
				copyRow.DBPoolID = requests[i].DBPoolID
			}
			if copyRow.DBPoolUUID == "" && i < len(requests) {
				copyRow.DBPoolUUID = requests[i].DBPoolUUID
			}
			out[i] = &copyRow
		}
		return out, f.sharedPoolPartialErr
	}
	out := make([]*tenant.SharedDBPoolInfo, 0, len(requests))
	for _, req := range requests {
		out = append(out, &tenant.SharedDBPoolInfo{
			DBPoolID: req.DBPoolID, DBPoolUUID: req.DBPoolUUID, ClusterID: fmt.Sprintf("cluster-%d", req.DBPoolID),
			OrganizationID: "org-shared", Password: "root-pass", DBName: req.DatabaseName,
		})
	}
	return out, nil
}

func (f *fakeProvisioner) lastSharedCredentialRequest() tenant.CredentialProvisionRequest {
	f.sharedPoolMu.Lock()
	defer f.sharedPoolMu.Unlock()
	return f.lastSharedCredentialReq
}

func (f *fakeProvisioner) LoadSharedDBPoolWithCredentials(_ context.Context, dbPoolID int64, dbPoolUUID, clusterID string, _ tenant.CredentialProvisionRequest) (*tenant.SharedDBPoolInfo, error) {
	if f.sharedPoolLoadIDs != nil {
		select {
		case f.sharedPoolLoadIDs <- dbPoolID:
		default:
		}
	}
	if clusterID == "" {
		return nil, nil
	}
	for _, row := range f.sharedPoolResults {
		if row != nil && row.ClusterID == clusterID {
			copyRow := *row
			copyRow.DBPoolUUID = dbPoolUUID
			return &copyRow, nil
		}
	}
	return nil, nil
}

func (f *fakeProvisioner) BatchLoadSharedDBPoolsWithCredentials(_ context.Context, requests []tenant.SharedDBPoolLoadRequest, _ tenant.CredentialProvisionRequest) ([]*tenant.SharedDBPoolInfo, error) {
	f.sharedPoolBatchLoadCalls.Add(1)
	f.sharedPoolBatchLoadMembers.Add(int32(len(requests)))
	copyRequests := append([]tenant.SharedDBPoolLoadRequest(nil), requests...)
	if f.sharedPoolLoadIDs != nil {
		for _, request := range copyRequests {
			select {
			case f.sharedPoolLoadIDs <- request.DBPoolID:
			default:
			}
		}
	}
	if f.sharedPoolBatchLoadRequests != nil {
		f.sharedPoolBatchLoadRequests <- copyRequests
	}
	if f.sharedPoolBatchLoadFunc != nil {
		return f.sharedPoolBatchLoadFunc(copyRequests)
	}
	out := make([]*tenant.SharedDBPoolInfo, 0, len(requests))
	for _, request := range requests {
		for _, row := range f.sharedPoolResults {
			if row != nil && row.ClusterID == request.ClusterID {
				copyRow := *row
				copyRow.DBPoolID = request.DBPoolID
				copyRow.DBPoolUUID = request.DBPoolUUID
				out = append(out, &copyRow)
				break
			}
		}
	}
	return out, nil
}

func (f *fakeProvisioner) WaitForSharedDBPoolMetadataWithCredentials(ctx context.Context, dbPoolID int64, dbPoolUUID, clusterID string, _ tenant.CredentialProvisionRequest) (*tenant.SharedDBPoolInfo, error) {
	f.sharedPoolWaitCalls.Add(1)
	if f.sharedPoolWaitRelease != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.sharedPoolWaitRelease:
		}
	}
	if f.sharedPoolWaitErr != nil {
		return nil, f.sharedPoolWaitErr
	}
	for _, row := range f.sharedPoolResults {
		if row != nil && row.ClusterID == clusterID {
			copyRow := *row
			copyRow.DBPoolID = dbPoolID
			copyRow.DBPoolUUID = dbPoolUUID
			return &copyRow, nil
		}
	}
	return nil, nil
}

func (f *fakeProvisioner) InitSchema(_ context.Context, dsn string) error {
	if f.initErr != nil {
		return f.initErr
	}
	return nil
}

func (f *fakeProvisioner) EnsureSystemUser(_ context.Context, _ string, _ string) (string, string, error) {
	f.systemUserCalls.Add(1)
	if f.systemUserErr != nil {
		return "", "", f.systemUserErr
	}
	username := f.systemUsername
	if username == "" {
		username = "u1.tdc_fs_sys"
	}
	password := f.systemPassword
	if password == "" {
		password = "system-pass"
	}
	return username, password, nil
}

func (f *fakeProvisioner) Provision(_ context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	f.provisionCalls.Add(1)
	if f.provisionErr != nil {
		if f.cluster == nil {
			return nil, f.provisionErr
		}
		out := *f.cluster
		out.TenantID = tenantID
		out.Provider = f.provider
		return &out, f.provisionErr
	}
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	return &out, nil
}

func (f *fakeProvisioner) CreateClusterWithCredentialsAndQuota(_ context.Context, tenantID string, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.ClusterInfo, *tenant.QuotaCloudConfig, error) {
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		f.credentialQuotaCalls.Add(1)
	} else {
		f.credentialCalls.Add(1)
	}
	f.lastCredentialReq = req
	f.lastCreateQuotaOptions = opts
	if f.provisionErr != nil {
		if f.cluster == nil {
			return nil, nil, f.provisionErr
		}
		out := *f.cluster
		out.TenantID = tenantID
		out.Provider = f.provider
		return &out, nil, f.provisionErr
	}
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	var cloudCfg *tenant.QuotaCloudConfig
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		cloudCfg = &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: opts.TiDBCloudSpendingLimitMonthly}
	}
	return &out, cloudCfg, nil
}

func (f *fakeProvisioner) WaitForClusterMetadataWithCredentials(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	f.lastCredentialReq = req
	if cluster == nil {
		return nil, fmt.Errorf("cluster is required")
	}
	out := *cluster
	return &out, nil
}

func (f *fakeProvisioner) Deprovision(_ context.Context, cluster *tenant.ClusterInfo) error {
	if cluster != nil {
		out := *cluster
		f.lastDeprovision = &out
	}
	f.deprovisionCalls.Add(1)
	return f.deprovisionErr
}

func (f *fakeProvisioner) DeprovisionWithCredentials(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) error {
	f.lastCredentialReq = req
	if cluster != nil {
		out := *cluster
		f.lastDeprovision = &out
	}
	f.deprovisionCalls.Add(1)
	return f.deprovisionErr
}

func (f *fakeProvisioner) MarkQuotaUpdateStarted(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.QuotaCloudConfig, error) {
	f.quotaMarkCalls.Add(1)
	f.lastCredentialReq = req
	if cluster != nil {
		out := *cluster
		f.lastQuotaCluster = &out
	}
	if f.quotaMarkErr != nil {
		return nil, f.quotaMarkErr
	}
	return nil, nil
}

func (f *fakeProvisioner) UpdateQuota(_ context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
	f.quotaUpdateCalls.Add(1)
	f.lastCredentialReq = req
	f.lastQuotaOptions = opts
	if cluster != nil {
		out := *cluster
		f.lastQuotaCluster = &out
	}
	if f.quotaUpdateErr != nil {
		return nil, f.quotaUpdateErr
	}
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		return &tenant.QuotaCloudConfig{TiDBCloudSpendingLimitMonthly: opts.TiDBCloudSpendingLimitMonthly}, nil
	}
	return nil, nil
}

func (f *fakeProvisioner) ProvisionCallCount() int {
	return int(f.provisionCalls.Load())
}

func TestClientFacingErrorResponseMapsTiDBCloudClientErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{
			name: "invalid request",
			err: fmt.Errorf("update cluster spending limit: %w", &tenant.TiDBCloudAPIError{
				Operation:    "cluster spending limit update",
				StatusCode:   http.StatusBadRequest,
				UpstreamBody: `{"code":400,"message":"Scalable cluster can not set spending limit to 0.","details":[{"requestId":"202607090625337c3caba58b2eb378ca"}]}`,
			}),
			wantStatus: http.StatusBadRequest,
			wantBody:   "Scalable cluster can not set spending limit to 0",
		},
		{
			name: "invalid api key",
			err: fmt.Errorf("list managed clusters: %w", &tenant.TiDBCloudAPIError{
				Operation:  "cluster list",
				StatusCode: http.StatusUnauthorized,
			}),
			wantStatus: http.StatusUnauthorized,
			wantBody:   "invalid TiDB Cloud API key",
		},
		{
			name: "forbidden",
			err: fmt.Errorf("update quota: %w", &tenant.TiDBCloudAPIError{
				Operation:  "cluster spending limit update",
				StatusCode: http.StatusForbidden,
			}),
			wantStatus: http.StatusForbidden,
			wantBody:   "access denied",
		},
		{
			name:       "status-shaped generic error is not parsed",
			err:        errors.New("tidbcloud native cluster list status 401: attacker-controlled"),
			wantStatus: http.StatusBadGateway,
			wantBody:   "claim tenant pool tenant failed",
		},
		{
			name:       "insufficient IAM role hides resolver detail",
			err:        fmt.Errorf("%w: org:viewer SENSITIVE_RESOLVER_DETAIL", tenant.ErrTiDBCloudRoleInsufficient),
			wantStatus: http.StatusForbidden,
			wantBody:   tenant.ErrTiDBCloudRoleInsufficient.Error(),
		},
		{
			name:       "free tenant limit",
			err:        tenant.ErrTiDBCloudFreeTenantLimitReached,
			wantStatus: http.StatusForbidden,
			wantBody:   tenant.ErrTiDBCloudFreeTenantLimitReached.Error(),
		},
		{
			name:       "free quota lock busy",
			err:        tenant.ErrTiDBCloudFreeQuotaBusy,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   tenant.ErrTiDBCloudFreeQuotaBusy.Error(),
		},
		{
			name:       "generic error hides detail",
			err:        errors.New("internal upstream detail"),
			wantStatus: http.StatusBadGateway,
			wantBody:   "claim tenant pool tenant failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotStatus, gotMsg := clientFacingErrorResponse(http.StatusBadGateway, "claim tenant pool tenant failed", tc.err)
			if gotStatus != tc.wantStatus {
				t.Fatalf("status = %d, want %d; msg=%s", gotStatus, tc.wantStatus, gotMsg)
			}
			if !strings.Contains(gotMsg, tc.wantBody) {
				t.Fatalf("msg = %q, want containing %q", gotMsg, tc.wantBody)
			}
			if strings.Contains(gotMsg, "requestId") || strings.Contains(gotMsg, "details") {
				t.Fatalf("msg = %q, should not expose raw TiDB Cloud details", gotMsg)
			}
			if strings.Contains(gotMsg, "internal upstream detail") {
				t.Fatalf("msg = %q, should not expose generic upstream details", gotMsg)
			}
			if strings.Contains(gotMsg, "SENSITIVE_RESOLVER_DETAIL") {
				t.Fatalf("msg = %q, should not expose IAM resolver details", gotMsg)
			}
		})
	}
}

func waitForDeprovisionCalls(t *testing.T, prov *fakeProvisioner, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := prov.deprovisionCalls.Load(); got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("deprovision calls = %d, want %d", prov.deprovisionCalls.Load(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTiDBCloudOrgBindingNotFound(t *testing.T, metaStore *meta.Store, tenantID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := metaStore.GetTenantTiDBCloudOrgBinding(context.Background(), tenantID); errors.Is(err, meta.ErrNotFound) {
			return
		}
		if time.Now().After(deadline) {
			binding, err := metaStore.GetTenantTiDBCloudOrgBinding(context.Background(), tenantID)
			t.Fatalf("tidbcloud org binding = %#v, err = %v, want ErrNotFound", binding, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTenantClusterReference(t *testing.T, metaStore *meta.Store, tenantID, wantClusterID string) (status, provider, clusterID, host, user, dbName string, port int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := metaStore.DB().QueryRow(`
			SELECT status, provider, COALESCE(cluster_id, ''), db_host, db_port, db_user, db_name
			FROM tenants WHERE id = ?`,
			tenantID,
		).Scan(&status, &provider, &clusterID, &host, &port, &user, &dbName)
		if err != nil {
			t.Fatal(err)
		}
		if clusterID == wantClusterID {
			return status, provider, clusterID, host, user, dbName, port
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant cluster_id = %s, want %s", clusterID, wantClusterID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type nonEarlyBindingProvisioner struct {
	provider string
	cluster  *tenant.ClusterInfo
}

func (f *nonEarlyBindingProvisioner) ProviderType() string { return f.provider }

func (f *nonEarlyBindingProvisioner) ResolveAPIKeyIdentity(context.Context, tenant.CredentialProvisionRequest) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	return &tenant.TiDBCloudAPIKeyIdentity{
		OrganizationID: f.cluster.OrganizationID,
		Role:           tenant.TiDBCloudRoleOrgOwner,
	}, nil
}

func (f *nonEarlyBindingProvisioner) ResolveOrganizationPlan(_ context.Context, organizationID string, _ tenant.CredentialProvisionRequest) (*tenant.TiDBCloudOrganizationPlan, error) {
	return &tenant.TiDBCloudOrganizationPlan{OrganizationID: organizationID}, nil
}

func (f *nonEarlyBindingProvisioner) InitSchema(_ context.Context, _ string) error { return nil }

func (f *nonEarlyBindingProvisioner) Provision(_ context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	out := *f.cluster
	out.TenantID = tenantID
	out.Provider = f.provider
	return &out, nil
}

type profileAwareFakeProvisioner struct {
	fakeProvisioner
	mu                  sync.Mutex
	profileInitCalls    atomic.Int32
	ensureDBCalls       atomic.Int32
	lastProfile         tenantschema.TiDBAutoEmbeddingProfile
	lastProfileTenantID string
	ensureDBErr         error
	lastEnsureDSN       string
	callOrder           []string
}

func (f *profileAwareFakeProvisioner) EnsureDatabase(_ context.Context, dsn string) error {
	f.ensureDBCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastEnsureDSN = dsn
	f.callOrder = append(f.callOrder, "ensure")
	return f.ensureDBErr
}

func (f *profileAwareFakeProvisioner) InitSchemaForAutoEmbeddingProfile(ctx context.Context, _ string, profile tenantschema.TiDBAutoEmbeddingProfile) error {
	f.profileInitCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastProfile = profile
	f.lastProfileTenantID = tenantschema.TenantIDFromContext(ctx)
	f.callOrder = append(f.callOrder, "profile-init")
	return nil
}

func (f *profileAwareFakeProvisioner) callOrderString() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.callOrder, ",")
}

func TestSchemaInitForTenantEnsuresDatabaseBeforeAutoEmbeddingConfig(t *testing.T) {
	ensureErr := errors.New("database is not ready")
	prov := &profileAwareFakeProvisioner{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		ensureDBErr:     ensureErr,
	}
	srv := NewWithConfig(Config{
		Provisioner: prov,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
	})
	defer srv.Close()

	init := srv.schemaInitForTenant("tenant-native", tenant.ProviderTiDBCloudNative, func(context.Context, string) error {
		t.Fatal("fallback InitSchema was called")
		return nil
	})
	err := init(context.Background(), "u1.root:db-pass@tcp(db.example:4000)/tidbcloud_fs?parseTime=true&tls=true")
	if !errors.Is(err, ensureErr) {
		t.Fatalf("schema init error = %v, want ensure error", err)
	}
	if prov.ensureDBCalls.Load() != 1 {
		t.Fatalf("ensure DB calls = %d, want 1", prov.ensureDBCalls.Load())
	}
	if prov.profileInitCalls.Load() != 0 {
		t.Fatalf("profile init calls = %d, want 0", prov.profileInitCalls.Load())
	}
	if prov.lastEnsureDSN == "" {
		t.Fatal("ensure DB DSN was empty")
	}
}

func TestSchemaInitForTenantEnsuresDatabaseBeforeProfileInit(t *testing.T) {
	prov := &profileAwareFakeProvisioner{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
	}
	srv := NewWithConfig(Config{
		Provisioner: prov,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
	})
	defer srv.Close()

	init := srv.schemaInitForTenant("tenant-native", tenant.ProviderTiDBCloudNative, func(context.Context, string) error {
		t.Fatal("fallback InitSchema was called")
		return nil
	})
	err := init(context.Background(), "u1.root:db-pass@tcp(db.example:4000)/tidbcloud_fs?parseTime=true&tls=true")
	if err != nil {
		t.Fatalf("schema init: %v", err)
	}
	if prov.ensureDBCalls.Load() != 1 {
		t.Fatalf("ensure DB calls = %d, want 1", prov.ensureDBCalls.Load())
	}
	if prov.profileInitCalls.Load() != 1 {
		t.Fatalf("profile init calls = %d, want 1", prov.profileInitCalls.Load())
	}
	if got, want := prov.callOrderString(), "ensure,profile-init"; got != want {
		t.Fatalf("call order = %s, want %s", got, want)
	}
	prov.mu.Lock()
	lastProfileTenantID := prov.lastProfileTenantID
	prov.mu.Unlock()
	if lastProfileTenantID != "tenant-native" {
		t.Fatalf("profile init tenant id = %q, want tenant-native", lastProfileTenantID)
	}
}

func TestProvisionMarksTenantFailedWhenInitKeepsFailing(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, initErr: fmt.Errorf("boom"), cluster: &tenant.ClusterInfo{
		ClusterID: "bad-cluster",
		Host:      "127.0.0.1",
		Port:      3306,
		Username:  "root",
		Password:  "bad",
		DBName:    "bad",
	}}

	origWindow, origInitBackoff, origMaxBackoff := schemaInitRetryWindow, schemaInitInitialBackoff, schemaInitMaxBackoff
	schemaInitRetryWindow = 120 * time.Millisecond
	schemaInitInitialBackoff = 10 * time.Millisecond
	schemaInitMaxBackoff = 20 * time.Millisecond
	defer func() {
		schemaInitRetryWindow = origWindow
		schemaInitInitialBackoff = origInitBackoff
		schemaInitMaxBackoff = origMaxBackoff
	}()

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" {
		t.Fatalf("unexpected provision response: %+v", out)
	}
	apiKey := out["api_key"]
	if apiKey == "" {
		t.Fatal("empty api_key")
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), token.HashToken(apiKey))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := resolved.Tenant.ID

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantFailed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become failed in time, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestProvisionUsesConfiguredProvisioner(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host := "127.0.0.1"
	port := 3306
	if parsed.Addr != "" {
		h, p, ok := strings.Cut(parsed.Addr, ":")
		if ok {
			host = h
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "cluster-1",
		Host:      host,
		Port:      port,
		Username:  parsed.User,
		Password:  parsed.Passwd,
		DBName:    parsed.DBName,
	}}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero, "db_tls": false})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" || out["api_key"] == "" {
		t.Fatalf("unexpected provision response: %+v", out)
	}
	claims, err := token.ParseAndVerifyToken(tokenSecret, out["api_key"])
	if err != nil {
		t.Fatalf("ParseAndVerifyToken provision api key: %v", err)
	}
	hasAdmin := false
	for _, permission := range claims.JournalPermissions {
		if permission == JournalPermissionAdmin {
			hasAdmin = true
			break
		}
	}
	if !hasAdmin {
		t.Fatalf("provision api key journal_permissions = %#v, want %s", claims.JournalPermissions, JournalPermissionAdmin)
	}
	resolved, err := metaStore.ResolveByAPIKeyHash(context.Background(), token.HashToken(out["api_key"]))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := resolved.Tenant.ID
	if out["status"] != string(meta.TenantProvisioning) {
		t.Fatalf("expected provisioning response status, got %q", out["status"])
	}

	deadline := time.Now().Add(3 * time.Second)
	var status, provider, clusterID string
	for {
		row := metaStore.DB().QueryRow("SELECT status, provider, cluster_id FROM tenants WHERE id = ?", tenantID)
		if err := row.Scan(&status, &provider, &clusterID); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active in time: status=%s", status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if provider != tenant.ProviderTiDBZero || clusterID != "cluster-1" {
		t.Fatalf("unexpected tenant row: status=%s provider=%s cluster_id=%s", status, provider, clusterID)
	}
}

func TestProvisionTiDBCloudNativeUsesRequestCredentials(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1", cluster: &tenant.ClusterInfo{
		ClusterID:      "native-cluster-1",
		OrganizationID: "org-1",
		Host:           "db.example",
		Port:           4000,
		Username:       "u1.root",
		Password:       "db-pass",
		DBName:         "customer_db",
	}}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	spendingLimit := int64(10000)
	maxStorageSize := int64(1000)
	body, _ := json.Marshal(map[string]any{
		"public_key":               "public-1",
		"private_key":              "private-1",
		"tidbcloud_spending_limit": spendingLimit,
		"max_storage_size":         maxStorageSize,
	})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if got := prov.ProvisionCallCount(); got != 0 {
		t.Fatalf("plain provision calls = %d, want 0", got)
	}
	if got := prov.credentialCalls.Load(); got != 0 {
		t.Fatalf("credential provision calls = %d, want 0 when create-time quota is set", got)
	}
	if got := prov.credentialQuotaCalls.Load(); got != 1 {
		t.Fatalf("credential quota provision calls = %d, want 1", got)
	}
	if got := prov.quotaMarkCalls.Load(); got != 0 {
		t.Fatalf("quota mark calls = %d, want 0 for create-time quota", got)
	}
	if got := prov.quotaUpdateCalls.Load(); got != 0 {
		t.Fatalf("quota update calls = %d, want 0 for create-time quota", got)
	}
	if prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly == nil || *prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly != spendingLimit {
		t.Fatalf("create quota spending limit = %#v, want %d", prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly, spendingLimit)
	}
	if prov.lastCredentialReq.PublicKey != "public-1" || prov.lastCredentialReq.PrivateKey != "private-1" {
		t.Fatalf("credential request = %+v", prov.lastCredentialReq)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" || out["api_key"] == "" || out["status"] != string(meta.TenantProvisioning) {
		t.Fatalf("unexpected response: %+v", out)
	}
	if out["cloud_provider"] != "aws" || out["region"] != "us-east-1" {
		t.Fatalf("native cloud/region response = %+v", out)
	}
	if _, ok := out["mode"]; ok {
		t.Fatalf("native provision response unexpectedly included mode: %+v", out)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		var status string
		if err := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", out["tenant_id"]).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active in time: status=%s", status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if got := prov.systemUserCalls.Load(); got == 0 {
		t.Fatal("native system user setup was not called")
	}
	binding, err := metaStore.GetTenantTiDBCloudOrgBinding(context.Background(), out["tenant_id"])
	if err != nil {
		t.Fatalf("get tidbcloud org binding: %v", err)
	}
	if binding.OrganizationID != "org-1" || binding.ClusterID != "native-cluster-1" {
		t.Fatalf("binding = %#v", binding)
	}
	quotaCfg, err := metaStore.GetQuotaConfig(context.Background(), out["tenant_id"])
	if err != nil {
		t.Fatalf("get quota config: %v", err)
	}
	if quotaCfg.MaxStorageBytes != maxStorageSize*quotaStorageSizeBytes {
		t.Fatalf("quota max storage = %d, want %d", quotaCfg.MaxStorageBytes, maxStorageSize*quotaStorageSizeBytes)
	}

	var provider, dbName, dbUser string
	var passCipher []byte
	if err := metaStore.DB().QueryRow("SELECT provider, db_name, db_user, db_password FROM tenants WHERE id = ?", out["tenant_id"]).Scan(&provider, &dbName, &dbUser, &passCipher); err != nil {
		t.Fatal(err)
	}
	if provider != tenant.ProviderTiDBCloudNative || dbName != "customer_db" {
		t.Fatalf("tenant provider/db = %s/%s", provider, dbName)
	}
	if dbUser != "u1.tdc_fs_sys" {
		t.Fatalf("tenant db_user = %q, want system user", dbUser)
	}
	plain, err := pool.Decrypt(context.Background(), passCipher)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != "system-pass" {
		t.Fatalf("tenant db password = %q, want system password", plain)
	}
}

func TestProvisionTiDBCloudNativePersistsEarlyClusterBindingBeforeMetadataWait(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	release := make(chan struct{})
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-early-server", Password: "db-pass", DBName: "customer_db",
		},
		ready: &tenant.ClusterInfo{
			OrganizationID: "org-early-server", Host: "db.example", Port: 4000, Username: "u1.root",
		},
		waitStarted: make(chan struct{}),
		waitRelease: release,
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	type provisionOutcome struct {
		result *provisionTenantResult
		err    error
	}
	outcomes := make(chan provisionOutcome, 1)
	go func() {
		result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
			KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
			TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-early-server"},
		})
		outcomes <- provisionOutcome{result: result, err: err}
	}()

	select {
	case <-prov.waitStarted:
	case outcome := <-outcomes:
		t.Fatalf("provision returned before metadata wait: result=%+v err=%v", outcome.result, outcome.err)
	case <-time.After(2 * time.Second):
		t.Fatal("metadata wait did not start")
	}
	var tenantID, status, dbUser string
	if err := rt.meta.DB().QueryRow(`SELECT id, status, db_user FROM tenants WHERE cluster_id = ?`, "cluster-early-server").Scan(&tenantID, &status, &dbUser); err != nil {
		t.Fatalf("query early-bound tenant: %v", err)
	}
	if status != string(meta.TenantPending) || dbUser != "" {
		t.Fatalf("early-bound tenant status/user = %s/%q, want pending/empty", status, dbUser)
	}
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(context.Background(), tenantID)
	if err != nil || binding.OrganizationID != "org-early-server" || binding.ClusterID != "cluster-early-server" {
		t.Fatalf("early org binding = %+v, err=%v", binding, err)
	}

	close(release)
	outcome := <-outcomes
	if outcome.err != nil || outcome.result == nil || outcome.result.Status != meta.TenantProvisioning {
		t.Fatalf("provision outcome = %+v err=%v", outcome.result, outcome.err)
	}
	row, err := rt.meta.GetTenant(context.Background(), tenantID)
	if err != nil || row.Status != meta.TenantProvisioning || row.DBUser != "u1.root" || row.DBHost != "db.example" {
		t.Fatalf("final tenant = %+v, err=%v", row, err)
	}
}

func TestProvisionFreeTiDBCloudNativeHoldsQuotaLockUntilEarlyBindingAndReleasesBeforeMetadataWait(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	createRelease := make(chan struct{})
	waitRelease := make(chan struct{})
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		createStarted:   make(chan struct{}),
		createRelease:   createRelease,
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-free-lock", Password: "db-pass", DBName: "customer_db",
		},
		ready: &tenant.ClusterInfo{
			OrganizationID: "org-free-lock", Host: "db.example", Port: 4000, Username: "u1.root",
		},
		waitStarted: make(chan struct{}),
		waitRelease: waitRelease,
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	type provisionOutcome struct {
		result *provisionTenantResult
		err    error
	}
	outcomes := make(chan provisionOutcome, 1)
	go func() {
		result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
			KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
			TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-free-lock", IsFree: true},
		})
		outcomes <- provisionOutcome{result: result, err: err}
	}()

	select {
	case <-prov.createStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("cluster create did not start")
	}
	type quotaLockOutcome struct {
		release func() error
		err     error
	}
	lockOutcomes := make(chan quotaLockOutcome, 1)
	go func() {
		release, err := rt.meta.AcquireTiDBCloudFreeQuotaLock(context.Background(), "org-free-lock")
		lockOutcomes <- quotaLockOutcome{release: release, err: err}
	}()
	select {
	case outcome := <-lockOutcomes:
		if outcome.release != nil {
			_ = outcome.release()
		}
		t.Fatalf("quota lock was acquirable during cluster create: %v", outcome.err)
	case <-time.After(100 * time.Millisecond):
	}

	close(createRelease)
	select {
	case <-prov.waitStarted:
	case outcome := <-outcomes:
		t.Fatalf("provision returned before metadata wait: result=%+v err=%v", outcome.result, outcome.err)
	case <-time.After(2 * time.Second):
		t.Fatal("metadata wait did not start")
	}
	lockResult := <-lockOutcomes
	if lockResult.err != nil {
		t.Fatalf("quota lock remained held during metadata wait: %v", lockResult.err)
	}
	if err := lockResult.release(); err != nil {
		t.Fatalf("release verification lock: %v", err)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free-lock")
	if err != nil || count != 1 {
		t.Fatalf("free count during metadata wait = %d, err=%v, want 1", count, err)
	}

	close(waitRelease)
	outcome := <-outcomes
	if outcome.err != nil || outcome.result == nil {
		t.Fatalf("provision outcome = %+v err=%v", outcome.result, outcome.err)
	}
}

func TestProvisionTiDBCloudNativeRejectsMetadataOrganizationMismatchAndCleansCluster(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-org-mismatch", Password: "db-pass", DBName: "customer_db",
		},
		ready: &tenant.ClusterInfo{
			OrganizationID: "org-actual", Host: "db.example", Port: 4000, Username: "u1.root",
		},
		waitStarted: make(chan struct{}),
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
		TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-expected"},
	})
	if result != nil || err == nil {
		t.Fatalf("provision result=%+v err=%v, want organization mismatch", result, err)
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusBadGateway {
		t.Fatalf("provision error = %#v, want 502", err)
	}
	waitForDeprovisionCalls(t, prov.fakeProvisioner, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "cluster-org-mismatch" {
		t.Fatalf("deprovision cluster = %+v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq != cred {
		t.Fatalf("cleanup credentials = %+v, want %+v", prov.lastCredentialReq, cred)
	}
	var tenantID, status string
	if err := rt.meta.DB().QueryRow(`SELECT id, status FROM tenants WHERE id <> ? ORDER BY created_at DESC LIMIT 1`, rt.tenantID).Scan(&tenantID, &status); err != nil {
		t.Fatalf("query failed tenant: %v", err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want failed", status)
	}
}

func TestProvisionTiDBCloudNativeCleansClusterWhenEarlyBindingPersistenceFails(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-quota-1", Password: "db-pass", DBName: "customer_db",
		},
		ready:       &tenant.ClusterInfo{OrganizationID: "org-1", Host: "db.example", Port: 4000, Username: "u1.root"},
		waitStarted: make(chan struct{}),
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
		TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-1"},
	})
	if result != nil || err == nil {
		t.Fatalf("provision result=%+v err=%v, want early binding failure", result, err)
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusInternalServerError {
		t.Fatalf("provision error = %#v, want 500", err)
	}
	waitForDeprovisionCalls(t, prov.fakeProvisioner, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "cluster-quota-1" {
		t.Fatalf("deprovision cluster = %+v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq != cred {
		t.Fatalf("cleanup credentials = %+v, want %+v", prov.lastCredentialReq, cred)
	}
	var status string
	if err := rt.meta.DB().QueryRow(`SELECT status FROM tenants WHERE id <> ? ORDER BY created_at DESC LIMIT 1`, rt.tenantID).Scan(&status); err != nil {
		t.Fatalf("query failed tenant: %v", err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want failed", status)
	}
}

func TestProvisionTiDBCloudNativeFinalizationDoesNotOverwriteChangedStatus(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	release := make(chan struct{})
	prov := &earlyBindingProvisioner{
		fakeProvisioner: &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative},
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-finalize-cas", Password: "db-pass", DBName: "customer_db",
		},
		ready: &tenant.ClusterInfo{
			OrganizationID: "org-finalize-cas", Host: "db.example", Port: 4000, Username: "u1.root",
		},
		waitStarted: make(chan struct{}),
		waitRelease: release,
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	type provisionOutcome struct {
		result *provisionTenantResult
		err    error
	}
	outcomes := make(chan provisionOutcome, 1)
	go func() {
		result, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
			KeyName: "default", TokenVersion: 1, CredentialProvisioner: &cred,
			TiDBCloudAccess: &tiDBCloudAccessProfile{OrganizationID: "org-finalize-cas"},
		})
		outcomes <- provisionOutcome{result: result, err: err}
	}()
	select {
	case <-prov.waitStarted:
	case outcome := <-outcomes:
		t.Fatalf("provision returned before metadata wait: result=%+v err=%v", outcome.result, outcome.err)
	case <-time.After(2 * time.Second):
		t.Fatal("metadata wait did not start")
	}
	var tenantID string
	if err := rt.meta.DB().QueryRow(`SELECT id FROM tenants WHERE cluster_id = ?`, "cluster-finalize-cas").Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpdateTenantStatus(context.Background(), tenantID, meta.TenantFailed); err != nil {
		t.Fatal(err)
	}
	close(release)
	outcome := <-outcomes
	if outcome.result != nil || outcome.err == nil {
		t.Fatalf("provision outcome result=%+v err=%v, want lost-CAS failure", outcome.result, outcome.err)
	}
	row, err := rt.meta.GetTenant(context.Background(), tenantID)
	if err != nil || row.Status != meta.TenantFailed {
		t.Fatalf("tenant after lost CAS = %+v, err=%v", row, err)
	}
}

func TestProvisionFreeTiDBCloudTenantPersistsNativeBindingAndExplicitQuota(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 3)
	resp := rt.postProvision(t, map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 202: %s", resp.StatusCode, body)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	tenantID := out["tenant_id"]
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(context.Background(), tenantID)
	if err != nil || binding.OrganizationID != "org-free" {
		t.Fatalf("native binding = %+v, err=%v", binding, err)
	}
	cfg, err := rt.meta.GetQuotaConfig(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != DefaultTiDBCloudFreeMaxStorageBytes ||
		cfg.MaxFileSizeBytes != DefaultTiDBCloudFreeMaxFileSizeBytes ||
		cfg.MaxFileCount != DefaultTiDBCloudFreeMaxFileCount ||
		cfg.TiDBCloudSpendingLimit == nil || *cfg.TiDBCloudSpendingLimit != 0 {
		t.Fatalf("free quota = %+v", cfg)
	}
	if rt.provisioner.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly == nil || *rt.provisioner.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly != 0 {
		t.Fatalf("create spending limit = %+v, want explicit zero", rt.provisioner.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 1 {
		t.Fatalf("free tenant count = %d, err=%v, want 1", count, err)
	}
}

func TestProvisionFreeTiDBCloudTenantRejectsDisallowedQuotaBeforeMutation(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
		want string
	}{
		{
			name: "positive spending",
			body: map[string]any{"tidbcloud_spending_limit": int64(10)},
			want: tenant.ErrTiDBCloudFreeSpendingLimitForbidden.Error(),
		},
		{
			name: "storage over cap",
			body: map[string]any{"max_storage_size": int64(5121)},
			want: tenant.ErrTiDBCloudFreeQuotaExceeded.Error(),
		},
		{
			name: "unlimited file count",
			body: map[string]any{"max_file_count": int64(0)},
			want: tenant.ErrTiDBCloudFreeQuotaExceeded.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := newTiDBCloudFreeProvisionRuntime(t, 3)
			tt.body["public_key"] = "public-1"
			tt.body["private_key"] = "private-1"
			resp := rt.postProvision(t, tt.body)
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusForbidden || strings.TrimSpace(string(body)) != fmt.Sprintf(`{"error":%q}`, tt.want) {
				t.Fatalf("response = %d %s, want 403 %q", resp.StatusCode, body, tt.want)
			}
			var tenantRows int
			if err := rt.meta.DB().QueryRow(`SELECT COUNT(*) FROM tenants`).Scan(&tenantRows); err != nil {
				t.Fatal(err)
			}
			if tenantRows != 0 || rt.provisioner.credentialCalls.Load() != 0 || rt.provisioner.credentialQuotaCalls.Load() != 0 {
				t.Fatalf("mutation after rejection: tenants=%d credential=%d quota=%d", tenantRows, rt.provisioner.credentialCalls.Load(), rt.provisioner.credentialQuotaCalls.Load())
			}
		})
	}
}

func TestProvisionFreeTiDBCloudTenantEnforcesConfiguredTenantLimit(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 1)
	first := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(first.Body)
		t.Fatalf("first status = %d, want 202: %s", first.StatusCode, body)
	}
	second := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = second.Body.Close() }()
	body, err := io.ReadAll(second.Body)
	if err != nil {
		t.Fatal(err)
	}
	if second.StatusCode != http.StatusForbidden || strings.TrimSpace(string(body)) != fmt.Sprintf(`{"error":%q}`, tenant.ErrTiDBCloudFreeTenantLimitReached.Error()) {
		t.Fatalf("second response = %d %s", second.StatusCode, body)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 1 {
		t.Fatalf("free count = %d, err=%v, want 1", count, err)
	}
}

func TestProvisionFreeTiDBCloudTenantRejectsEveryNonZeroSpendingLimitWith403(t *testing.T) {
	for _, spendingLimit := range []int64{-1, 1, meta.MaxTiDBCloudSpendingLimit + 1} {
		t.Run(fmt.Sprintf("spending_%d", spendingLimit), func(t *testing.T) {
			rt := newTiDBCloudFreeProvisionRuntime(t, 3)
			resp := rt.postProvision(t, map[string]any{
				"public_key": "public-1", "private_key": "private-1",
				"tidbcloud_spending_limit": spendingLimit,
			})
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusForbidden {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 403: %s", resp.StatusCode, body)
			}
		})
	}
}

func TestProvisionFreeTiDBCloudTenantReleasesReservationWhenClusterCreateFails(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 1)
	rt.provisioner.cluster = nil
	rt.provisioner.provisionErr = errors.New("cluster create unavailable")
	resp := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502: %s", resp.StatusCode, body)
	}
	var status meta.TenantStatus
	if err := rt.meta.DB().QueryRowContext(context.Background(), "SELECT status FROM tenants").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != meta.TenantDeleted {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleted)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 0 {
		t.Fatalf("free tenant count = %d, err=%v, want 0", count, err)
	}
}

func TestProvisionFreeTiDBCloudTenantReleasesReservationAfterClusterCleanup(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 1)
	waitErr := errors.New("metadata unavailable")
	prov := &earlyBindingProvisioner{
		fakeProvisioner: rt.provisioner,
		created: &tenant.ClusterInfo{
			ClusterID: "cluster-cleanup", OrganizationID: "org-free", Password: "db-pass", DBName: "customer_db",
		},
		waitStarted: make(chan struct{}),
		waitErr:     waitErr,
	}
	rt.server.provisioner = prov
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err := rt.server.provisionTenant(context.Background(), provisionTenantOptions{
		CredentialProvisioner: &cred,
		TiDBCloudAccess:       &tiDBCloudAccessProfile{OrganizationID: "org-free", IsFree: true},
	})
	if !errors.Is(err, waitErr) {
		t.Fatalf("provision error = %v, want %v", err, waitErr)
	}
	waitForDeprovisionCalls(t, rt.provisioner, 1)
	deadline := time.Now().Add(2 * time.Second)
	for {
		var status meta.TenantStatus
		if err := rt.meta.DB().QueryRowContext(context.Background(), "SELECT status FROM tenants").Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == meta.TenantDeleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant status = %s, want %s after cleanup", status, meta.TenantDeleted)
		}
		time.Sleep(10 * time.Millisecond)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 0 {
		t.Fatalf("free tenant count = %d, err=%v, want 0", count, err)
	}
}

func TestProvisionNonFreeTiDBCloudTenantDoesNotConsumeFreeQuota(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 3)
	rt.provisioner.billingFree = false
	resp := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 202: %s", resp.StatusCode, body)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(context.Background(), out["tenant_id"])
	if err != nil || binding.OrganizationID != "org-free" {
		t.Fatalf("native binding = %+v, err=%v", binding, err)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(context.Background(), "org-free")
	if err != nil || count != 0 {
		t.Fatalf("free tenant count = %d, err=%v, want 0", count, err)
	}
}

func TestProvisionTiDBCloudBillingFailurePrecedesTenantMutation(t *testing.T) {
	rt := newTiDBCloudFreeProvisionRuntime(t, 3)
	rt.provisioner.billingErr = tenant.ErrTiDBCloudBillingUnavailable
	resp := rt.postProvision(t, map[string]any{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502: %s", resp.StatusCode, body)
	}
	var tenantRows int
	if err := rt.meta.DB().QueryRow(`SELECT COUNT(*) FROM tenants`).Scan(&tenantRows); err != nil {
		t.Fatal(err)
	}
	if tenantRows != 0 || rt.provisioner.credentialCalls.Load() != 0 || rt.provisioner.credentialQuotaCalls.Load() != 0 {
		t.Fatalf("mutation after Billing failure: tenants=%d credential=%d quota=%d", tenantRows, rt.provisioner.credentialCalls.Load(), rt.provisioner.credentialQuotaCalls.Load())
	}
}

func TestWriteProvisionTenantErrorMapsFreeQuotaBusyToRetryable503(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeProvisionTenantError(recorder, newProvisionTenantError(
		http.StatusServiceUnavailable,
		tenant.ErrTiDBCloudFreeQuotaBusy.Error(),
		tenant.ErrTiDBCloudFreeQuotaBusy,
	))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if recorder.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", recorder.Header().Get("Retry-After"))
	}
	if got, want := strings.TrimSpace(recorder.Body.String()), fmt.Sprintf(`{"error":%q}`, tenant.ErrTiDBCloudFreeQuotaBusy.Error()); got != want {
		t.Fatalf("body = %s, want %s", got, want)
	}
}

type tiDBCloudFreeProvisionRuntime struct {
	meta        *meta.Store
	provisioner *fakeProvisioner
	server      *Server
	httpServer  *httptest.Server
}

func newTiDBCloudFreeProvisionRuntime(t *testing.T, tenantLimit int) *tiDBCloudFreeProvisionRuntime {
	t.Helper()
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(pool.Close)
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	provisioner := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		identityOrg:   "org-free",
		billingFree:   true,
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-free-cluster",
			OrganizationID: "org-free",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  provisioner,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
		TiDBCloudFreePlanLimits: TiDBCloudFreePlanLimits{
			TenantCount:      tenantLimit,
			MaxStorageBytes:  DefaultTiDBCloudFreeMaxStorageBytes,
			MaxFileSizeBytes: DefaultTiDBCloudFreeMaxFileSizeBytes,
			MaxFileCount:     DefaultTiDBCloudFreeMaxFileCount,
		},
	})
	t.Cleanup(srv.Close)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return &tiDBCloudFreeProvisionRuntime{meta: metaStore, provisioner: provisioner, server: srv, httpServer: ts}
}

func (rt *tiDBCloudFreeProvisionRuntime) postProvision(t *testing.T, body map[string]any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, rt.httpServer.URL+"/v1/provision", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestProvisionTiDBCloudNativeCreateQuotaSkipsQuotaPatch(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-create-quota",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}

	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	maxStorageSize := int64(1000)
	spendingLimit := int64(10000)
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
		Quota: &quotaRequest{quotaFields: quotaFields{
			MaxStorageSize:         &maxStorageSize,
			TiDBCloudSpendingLimit: &spendingLimit,
		}},
	})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	if got := prov.credentialQuotaCalls.Load(); got != 1 {
		t.Fatalf("credential quota provision calls = %d, want 1", got)
	}
	if got := prov.quotaMarkCalls.Load(); got != 0 {
		t.Fatalf("quota mark calls = %d, want 0 for create-time quota", got)
	}
	if got := prov.quotaUpdateCalls.Load(); got != 0 {
		t.Fatalf("quota update calls = %d, want 0 for create-time quota", got)
	}
	if got := prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	if prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly == nil || *prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly != spendingLimit {
		t.Fatalf("create quota spending limit = %#v, want %d", prov.lastCreateQuotaOptions.TiDBCloudSpendingLimitMonthly, spendingLimit)
	}

	cfg, err := metaStore.GetQuotaConfig(context.Background(), res.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != maxStorageSize*quotaStorageSizeBytes {
		t.Fatalf("quota max storage = %d, want %d", cfg.MaxStorageBytes, maxStorageSize*quotaStorageSizeBytes)
	}
}

func TestProvisionSeedsQuotaConfigWithoutExplicitQuota(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-seed-quota",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}

	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{
		"public_key":  "public-1",
		"private_key": "private-1",
	})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["tenant_id"] == "" || out["api_key"] == "" || out["status"] != string(meta.TenantProvisioning) {
		t.Fatalf("unexpected response: %+v", out)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		var status string
		if err := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", out["tenant_id"]).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active in time: status=%s", status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	cfg, err := metaStore.GetQuotaConfig(context.Background(), out["tenant_id"])
	if err != nil {
		t.Fatalf("get quota config: %v", err)
	}
	if cfg.TiDBCloudSpendingLimit != nil {
		t.Fatalf("spending limit = %d, want nil", *cfg.TiDBCloudSpendingLimit)
	}
	if cfg.TiDBCloudSpendingLimitCheckedAt != nil {
		t.Fatalf("checked_at = %v, want nil", cfg.TiDBCloudSpendingLimitCheckedAt)
	}
}

func TestProvisionTiDBCloudNativeRequiresEarlyBindingProvisioner(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &nonEarlyBindingProvisioner{
		provider: tenant.ProviderTiDBCloudNative,
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-no-quota-provisioner",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want unsupported early-binding provisioner error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusInternalServerError {
		t.Fatalf("provision error = %#v, want 500 provisionTenantError", err)
	}
	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForTiDBCloudOrgBindingNotFound(t, metaStore, tenantID)
	var clusterID string
	if err := metaStore.DB().QueryRow("SELECT COALESCE(cluster_id, '') FROM tenants WHERE id = ?", tenantID).Scan(&clusterID); err != nil {
		t.Fatal(err)
	}
	if clusterID != "" {
		t.Fatalf("tenant cluster_id = %s, want empty after cleanup", clusterID)
	}
}

func TestProvisionTiDBCloudNativeCreateTimeQuotaLocalPersistenceErrorIsInternal(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	if _, err := metaStore.DB().Exec("RENAME TABLE tenant_quota_config TO tenant_quota_config_unavailable"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = metaStore.DB().Exec("RENAME TABLE tenant_quota_config_unavailable TO tenant_quota_config")
	})

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-quota-local-error",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	maxStorageSize := int64(1000)
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
		Quota: &quotaRequest{quotaFields: quotaFields{
			MaxStorageSize: &maxStorageSize,
		}},
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want quota persistence error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusInternalServerError {
		t.Fatalf("provision error = %#v, want 500 provisionTenantError", err)
	}
	waitForDeprovisionCalls(t, prov, 1)
	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForTiDBCloudOrgBindingNotFound(t, metaStore, tenantID)
}

func TestProvisionTiDBCloudNativeCleansClusterWhenOrgBindingMissing(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID: "native-cluster-missing-org",
			Host:      "db.example",
			Port:      4000,
			Username:  "u1.root",
			Password:  "db-pass",
			DBName:    "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want org binding error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusBadGateway {
		t.Fatalf("provision error = %#v, want 502 provisionTenantError", err)
	}
	waitForDeprovisionCalls(t, prov, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "native-cluster-missing-org" {
		t.Fatalf("deprovision cluster = %#v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq.PublicKey != "public-1" || prov.lastCredentialReq.PrivateKey != "private-1" {
		t.Fatalf("cleanup credentials = %+v", prov.lastCredentialReq)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	var bindingCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenant_tidbcloud_org_bindings WHERE tenant_id = ?", tenantID).Scan(&bindingCount); err != nil {
		t.Fatal(err)
	}
	if bindingCount != 0 {
		t.Fatalf("binding count = %d, want 0", bindingCount)
	}
}

func TestProvisionTiDBCloudNativeCleansClusterWhenProvisionReturnsClusterAndError(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		provisionErr:  fmt.Errorf("wait metadata failed"),
		cluster: &tenant.ClusterInfo{
			ClusterID: "native-cluster-provision-error",
			Password:  "db-pass",
			DBName:    "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want provision error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusBadGateway {
		t.Fatalf("provision error = %#v, want 502 provisionTenantError", err)
	}
	waitForDeprovisionCalls(t, prov, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "native-cluster-provision-error" {
		t.Fatalf("deprovision cluster = %#v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq.PublicKey != "public-1" || prov.lastCredentialReq.PrivateKey != "private-1" {
		t.Fatalf("cleanup credentials = %+v", prov.lastCredentialReq)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForTiDBCloudOrgBindingNotFound(t, metaStore, tenantID)
}

func TestProvisionTiDBCloudNativePersistsClusterReferenceWhenCleanupFails(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:       tenant.ProviderTiDBCloudNative,
		cloudProvider:  "aws",
		region:         "us-east-1",
		provisionErr:   fmt.Errorf("wait metadata failed"),
		deprovisionErr: fmt.Errorf("delete unavailable"),
		cluster: &tenant.ClusterInfo{
			ClusterID: "native-cluster-cleanup-error",
			Host:      "db.example",
			Port:      4000,
			Username:  "u1.root",
			Password:  "db-pass",
			DBName:    "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want provision error")
	}
	waitForDeprovisionCalls(t, prov, 1)

	var tenantID string
	if err := metaStore.DB().QueryRow("SELECT id FROM tenants LIMIT 1").Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	status, provider, clusterID, host, user, dbName, port := waitForTenantClusterReference(t, metaStore, tenantID, "native-cluster-cleanup-error")
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	if provider != tenant.ProviderTiDBCloudNative || clusterID != "native-cluster-cleanup-error" {
		t.Fatalf("tenant provider/cluster = %s/%s, want %s/native-cluster-cleanup-error", provider, clusterID, tenant.ProviderTiDBCloudNative)
	}
	if host != "db.example" || port != 4000 || user != "u1.root" || dbName != "customer_db" {
		t.Fatalf("tenant reference = %s:%d %s/%s, want db.example:4000 u1.root/customer_db", host, port, user, dbName)
	}
}

func TestProvisionTiDBCloudNativeCleansClusterWhenPasswordEncryptFails(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, failingEncryptor{err: fmt.Errorf("kms unavailable")})
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		cluster: &tenant.ClusterInfo{
			ClusterID:      "native-cluster-encrypt-error",
			OrganizationID: "org-1",
			Host:           "db.example",
			Port:           4000,
			Username:       "u1.root",
			Password:       "db-pass",
			DBName:         "customer_db",
		},
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{
		KeyName:               "default",
		TokenVersion:          1,
		CredentialProvisioner: &cred,
	})
	if err == nil {
		t.Fatal("provisionTenant error = nil, want encrypt error")
	}
	var provisionErr *provisionTenantError
	if !errors.As(err, &provisionErr) || provisionErr.status != http.StatusInternalServerError {
		t.Fatalf("provision error = %#v, want 500 provisionTenantError", err)
	}
	waitForDeprovisionCalls(t, prov, 1)
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "native-cluster-encrypt-error" {
		t.Fatalf("deprovision cluster = %#v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq.PublicKey != "public-1" || prov.lastCredentialReq.PrivateKey != "private-1" {
		t.Fatalf("cleanup credentials = %+v", prov.lastCredentialReq)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants").Scan(&tenantID, &status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForTiDBCloudOrgBindingNotFound(t, metaStore, tenantID)
	var clusterID string
	if err := metaStore.DB().QueryRow("SELECT COALESCE(cluster_id, '') FROM tenants WHERE id = ?", tenantID).Scan(&clusterID); err != nil {
		t.Fatal(err)
	}
	if clusterID != "" {
		t.Fatalf("tenant cluster_id = %s, want empty after cleanup", clusterID)
	}
}

func TestProvisionTiDBCloudNativeRequiresRequestCredentials(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", strings.NewReader(`{"public_key":"public-1"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if got := prov.credentialCalls.Load(); got != 0 {
		t.Fatalf("credential provision calls = %d, want 0", got)
	}
	var tenantCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenants").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	if tenantCount != 0 {
		t.Fatalf("tenant count = %d, want 0", tenantCount)
	}
}

func TestDecodeCredentialProvisionRequestRejectsTrailingJSON(t *testing.T) {
	body := strings.NewReader(`{"public_key":"public-1","private_key":"private-1"} {}`)
	req, _ := http.NewRequest(http.MethodPost, "/v1/provision", body)
	_, err := decodeCredentialProvisionRequest(httptest.NewRecorder(), req)
	if err == nil {
		t.Fatal("expected trailing JSON error")
	}
	if !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeCredentialProvisionRequestRejectsOversizedBody(t *testing.T) {
	body := strings.NewReader(`{"public_key":"` + strings.Repeat("x", int(maxCredentialProvisionBodyBytes)+1) + `","private_key":"private-1"}`)
	req, _ := http.NewRequest(http.MethodPost, "/v1/provision", body)
	_, err := decodeCredentialProvisionRequest(httptest.NewRecorder(), req)
	if err == nil {
		t.Fatal("expected oversized body error")
	}
	var maxErr *http.MaxBytesError
	if !errors.As(err, &maxErr) {
		t.Fatalf("error = %v, want MaxBytesError", err)
	}
}

func TestProvisionTiDBCloudNativeRejectsQuotaWithoutCredentials(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:          tenant.ProviderTiDBCloudNative,
		cluster:           &tenant.ClusterInfo{},
		defaultPublicKey:  "default-public",
		defaultPrivateKey: "default-private",
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	spendingLimit := int64(10000)
	body, _ := json.Marshal(map[string]any{
		"tidbcloud_spending_limit": spendingLimit,
	})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	if got := prov.credentialCalls.Load(); got != 0 {
		t.Fatalf("credential provision calls = %d, want 0", got)
	}
	if got := prov.quotaUpdateCalls.Load(); got != 0 {
		t.Fatalf("quota update calls = %d, want 0", got)
	}
	msg, _ := raw["error"].(string)
	if !strings.Contains(strings.ToLower(msg), "requires public_key and private_key when quota settings are provided") {
		t.Fatalf("error = %#v", raw)
	}
}

func TestProvisionTenantRejectsMissingNativeCredentialsBeforeInsert(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})
	defer srv.Close()

	_, err = srv.provisionTenant(context.Background(), provisionTenantOptions{KeyName: "default"})
	if err == nil {
		t.Fatal("expected missing native credentials error")
	}
	if got := prov.ProvisionCallCount(); got != 0 {
		t.Fatalf("plain provision calls = %d, want 0", got)
	}
	var tenantCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenants").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	if tenantCount != 0 {
		t.Fatalf("tenant count = %d, want 0", tenantCount)
	}
}

func TestProvisionRejectsCredentialsForNonNativeProvider(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	for _, provider := range []string{tenant.ProviderTiDBZero, tenant.ProviderDB9} {
		prov := &fakeProvisioner{provider: provider}
		srv := NewWithConfig(Config{
			Meta:        metaStore,
			Pool:        pool,
			Provisioner: prov,
			TokenSecret: tokenSecret,
		})

		ts := httptest.NewServer(srv)
		body, _ := json.Marshal(map[string]string{"public_key": "test", "private_key": "test"})
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			ts.Close()
			t.Fatalf("%s: request failed: %v", provider, err)
		}
		_ = resp.Body.Close()
		ts.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status=%d, want 400", provider, resp.StatusCode)
		}
	}
}

func TestProvisionPersistsEncryptedAutoEmbeddingProfile(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "cluster-profile",
		Host:      "127.0.0.1",
		Port:      4000,
		Username:  "root",
		Password:  "db-pass",
		DBName:    "tenant_db",
	}}
	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
		TiDBAutoEmbeddingAPIKey: "sk-profile-test",
	})
	defer srv.Close()

	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{KeyName: "default"})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	profile, err := metaStore.GetTenantAutoEmbeddingProfile(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetTenantAutoEmbeddingProfile: %v", err)
	}
	if profile.Model != "openai/text-embedding-3-small" {
		t.Fatalf("profile model = %q", profile.Model)
	}
	if profile.Dimensions != 1536 {
		t.Fatalf("profile dimensions = %d", profile.Dimensions)
	}
	if profile.OptionsJSON != `{"dimensions":1536}` {
		t.Fatalf("profile options_json = %q", profile.OptionsJSON)
	}
	if profile.APIBase != "" {
		t.Fatalf("profile api_base = %q", profile.APIBase)
	}
	if string(profile.APIKeyCipher) == "sk-profile-test" {
		t.Fatal("profile API key was stored in plaintext")
	}
	plain, err := pool.Decrypt(context.Background(), profile.APIKeyCipher)
	if err != nil {
		t.Fatalf("decrypt profile API key: %v", err)
	}
	if string(plain) != "sk-profile-test" {
		t.Fatalf("decrypted API key = %q", string(plain))
	}
}

func TestProvisionPersistsAutoEmbeddingProfileWhenDatabaseAutoEmbeddingDisabled(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{
		S3Dir:                        mustTempDir(t),
		PublicURL:                    "http://localhost",
		DisableDatabaseAutoEmbedding: true,
	}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{
		ClusterID: "cluster-disabled-profile",
		Host:      "127.0.0.1",
		Port:      4000,
		Username:  "root",
		Password:  "db-pass",
		DBName:    "tenant_db",
	}}
	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	res, err := srv.provisionTenant(context.Background(), provisionTenantOptions{KeyName: "default"})
	if err != nil {
		t.Fatalf("provisionTenant: %v", err)
	}
	profile, err := metaStore.GetTenantAutoEmbeddingProfile(context.Background(), res.TenantID)
	if err != nil {
		t.Fatalf("GetTenantAutoEmbeddingProfile: %v", err)
	}
	if profile.Model != "openai/text-embedding-3-small" || profile.Dimensions != 1536 {
		t.Fatalf("profile = %+v", profile)
	}
	if profile.EmbeddingMode != meta.TenantEmbeddingModeFTSOnly {
		t.Fatalf("embedding_mode=%q, want %q", profile.EmbeddingMode, meta.TenantEmbeddingModeFTSOnly)
	}
	if len(profile.APIKeyCipher) != 0 {
		t.Fatal("disabled auto-embedding profile should not store an empty API key cipher")
	}
}

func TestSchemaInitForTenantUsesFTSOnlyProfileWhenDatabaseAutoEmbeddingDisabled(t *testing.T) {
	prov := &profileAwareFakeProvisioner{
		fakeProvisioner: fakeProvisioner{provider: tenant.ProviderTiDBZero},
	}
	origInitFTSOnly := initTiDBTenantSchemaForFTSOnlyProfileFunc
	var ftsOnlyInitCalls int
	var ftsOnlyProfile tenantschema.TiDBAutoEmbeddingProfile
	initTiDBTenantSchemaForFTSOnlyProfileFunc = func(_ context.Context, _ string, profile tenantschema.TiDBAutoEmbeddingProfile) error {
		ftsOnlyInitCalls++
		ftsOnlyProfile = profile
		return nil
	}
	t.Cleanup(func() {
		initTiDBTenantSchemaForFTSOnlyProfileFunc = origInitFTSOnly
	})
	srv := NewWithConfig(Config{
		Provisioner:                  prov,
		DisableDatabaseAutoEmbedding: true,
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
	})
	defer srv.Close()

	fallbackCalled := false
	init := srv.schemaInitForTenant("tenant-disabled", tenant.ProviderTiDBZero, func(context.Context, string) error {
		fallbackCalled = true
		return nil
	})

	if err := init(context.Background(), "dsn"); err != nil {
		t.Fatalf("schema init: %v", err)
	}
	if fallbackCalled {
		t.Fatal("fallback InitSchema was called")
	}
	if prov.profileInitCalls.Load() != 0 {
		t.Fatalf("profile init calls = %d, want 0", prov.profileInitCalls.Load())
	}
	if ftsOnlyInitCalls != 1 {
		t.Fatalf("fts-only init calls = %d, want 1", ftsOnlyInitCalls)
	}
	if ftsOnlyProfile.Model != "openai/text-embedding-3-small" || ftsOnlyProfile.Dimensions != 1536 {
		t.Fatalf("fts-only init profile = %+v", ftsOnlyProfile)
	}
}

func TestInitTenantSchemaAsyncPersistsTargetSchemaVersion(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBPasswordCipher: []byte{},
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	profile := tenantschema.TiDBAutoEmbeddingProfile{
		Model:       "openai/text-embedding-v4",
		Dimensions:  1024,
		OptionsJSON: `{"dimensions":1024}`,
	}
	targetVersion, err := tenantschema.TiDBTenantSchemaVersionForEmbeddingModeProfile(tenantschema.TiDBEmbeddingModeFTSOnly, profile)
	if err != nil {
		t.Fatalf("target schema version: %v", err)
	}
	if err := metaStore.UpsertTenantAutoEmbeddingProfile(context.Background(), &meta.TenantAutoEmbeddingProfile{
		TenantID:      tenantID,
		EmbeddingMode: meta.TenantEmbeddingModeFTSOnly,
		Model:         profile.Model,
		Dimensions:    profile.Dimensions,
		OptionsJSON:   profile.OptionsJSON,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}

	srv := NewWithConfig(Config{Meta: metaStore})
	defer srv.Close()
	schemaInitCalls := 0
	// Direct invocation is blocking; production launches this function in a worker.
	srv.initTenantSchemaAsync(context.Background(), tenantID, "unused-dsn", tenant.ProviderTiDBZero, func(context.Context, string) error {
		schemaInitCalls++
		return nil
	})

	if schemaInitCalls != 1 {
		t.Fatalf("schema init calls = %d, want 1", schemaInitCalls)
	}
	updated, err := metaStore.GetTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if updated.Status != meta.TenantActive {
		t.Fatalf("status = %q, want %q", updated.Status, meta.TenantActive)
	}
	if updated.SchemaVersion != targetVersion {
		t.Fatalf("schema version = %d, want %d", updated.SchemaVersion, targetVersion)
	}
}

func TestAutoEmbeddingProfileForTenantEnsuresDefaultProfile(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	srv := NewWithConfig(Config{Meta: metaStore})
	defer srv.Close()

	profile, err := srv.autoEmbeddingProfileForTenant(context.Background(), "tenant-default-profile")
	if err != nil {
		t.Fatalf("autoEmbeddingProfileForTenant: %v", err)
	}
	if profile.schemaProfile.Model != tenantschema.DefaultTiDBAutoEmbeddingModel {
		t.Fatalf("profile model = %q", profile.schemaProfile.Model)
	}
	if profile.schemaProfile.Dimensions != tenantschema.DefaultTiDBAutoEmbeddingDimensions {
		t.Fatalf("profile dimensions = %d", profile.schemaProfile.Dimensions)
	}
	if profile.schemaProfile.OptionsJSON != `{"dimensions":1024}` {
		t.Fatalf("profile options_json = %q", profile.schemaProfile.OptionsJSON)
	}

	stored, err := metaStore.GetTenantAutoEmbeddingProfile(context.Background(), "tenant-default-profile")
	if err != nil {
		t.Fatalf("GetTenantAutoEmbeddingProfile: %v", err)
	}
	if stored.Model != tenantschema.DefaultTiDBAutoEmbeddingModel || stored.Dimensions != tenantschema.DefaultTiDBAutoEmbeddingDimensions {
		t.Fatalf("stored default profile = %+v", stored)
	}
}

func TestAutoEmbeddingProfileForTenantWithoutMetaUsesConfiguredDefault(t *testing.T) {
	srv := NewWithConfig(Config{
		TiDBAutoEmbeddingConfig: tenantschema.TiDBAutoEmbeddingConfig{
			Model:      "openai/text-embedding-3-small",
			Dimensions: 1536,
		},
	})
	defer srv.Close()

	profile, err := srv.autoEmbeddingProfileForTenant(context.Background(), "tenant-without-meta")
	if err != nil {
		t.Fatalf("autoEmbeddingProfileForTenant: %v", err)
	}
	if profile.schemaProfile.Model != "openai/text-embedding-3-small" {
		t.Fatalf("profile model = %q", profile.schemaProfile.Model)
	}
	if profile.schemaProfile.Dimensions != 1536 {
		t.Fatalf("profile dimensions = %d", profile.schemaProfile.Dimensions)
	}
}

func TestProvisionPersistsTenantBeforeProvisionFailure(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{
		provider:     tenant.ProviderTiDBZero,
		cluster:      &tenant.ClusterInfo{},
		provisionErr: fmt.Errorf("boom"),
	}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{"provider": tenant.ProviderTiDBZero})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadGateway)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow("SELECT id, status FROM tenants LIMIT 1").Scan(&tenantID, &status); err != nil {
		t.Fatalf("QueryRow tenant: %v", err)
	}
	if tenantID == "" {
		t.Fatal("expected tenant row to be persisted")
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	var keyCount int
	if err := metaStore.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys").Scan(&keyCount); err != nil {
		t.Fatal(err)
	}
	if keyCount != 0 {
		t.Fatalf("api key count = %d, want 0", keyCount)
	}
}

func TestProvisionCleansPartialClusterBeforeMarkingFailed(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBCloudNative,
		cluster: &tenant.ClusterInfo{
			ClusterID: "cluster-after-takeover",
			Host:      "db.example",
			Port:      4000,
			Username:  "u1.root",
			Password:  "secret",
			DBName:    "test",
		},
		provisionErr:      fmt.Errorf("provision native cluster cluster-after-takeover: limit rejected"),
		defaultPublicKey:  "default-public",
		defaultPrivateKey: "default-private",
	}

	srv := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        pool,
		Provisioner: prov,
		TokenSecret: tokenSecret,
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", nil)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadGateway)
	}

	var tenantID, status string
	if err := metaStore.DB().QueryRow(`
		SELECT id, status
		FROM tenants LIMIT 1`,
	).Scan(&tenantID, &status); err != nil {
		t.Fatalf("QueryRow tenant: %v", err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantFailed)
	}
	waitForDeprovisionCalls(t, prov, 1)
	waitForTenantClusterReference(t, metaStore, tenantID, "")
	if prov.lastDeprovision == nil || prov.lastDeprovision.ClusterID != "cluster-after-takeover" {
		t.Fatalf("deprovision cluster = %#v", prov.lastDeprovision)
	}
	if prov.lastCredentialReq.PublicKey != "default-public" || prov.lastCredentialReq.PrivateKey != "default-private" {
		t.Fatalf("cleanup credentials = %+v", prov.lastCredentialReq)
	}
}

func TestStartupResumesProvisioningTenantInit(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host := "127.0.0.1"
	port := 3306
	if parsed.Addr != "" {
		h, p, ok := strings.Cut(parsed.Addr, ":")
		if ok {
			host = h
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}

	passCipher, err := pool.Encrypt(context.Background(), []byte(parsed.Passwd))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := token.NewID()
	now := time.Now().UTC()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		DBHost:           host,
		DBPort:           port,
		DBUser:           parsed.User,
		DBPasswordCipher: passCipher,
		DBName:           parsed.DBName,
		DBTLS:            false,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})
	defer srv.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant did not become active after restart resume, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStartupMarksPendingTenantFailed(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter, origSweepEvery := pendingTenantStaleAfter, pendingTenantSweepEvery
	pendingTenantStaleAfter = time.Minute
	pendingTenantSweepEvery = time.Hour
	defer func() {
		pendingTenantStaleAfter = origStaleAfter
		pendingTenantSweepEvery = origSweepEvery
	}()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "",
		DBPort:           0,
		DBUser:           "",
		DBPasswordCipher: []byte{},
		DBName:           "",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})
	defer srv.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantFailed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending tenant did not become failed after startup resume, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStartupKeepsFreshPendingTenant(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tenantID := token.NewID()
	now := time.Now().UTC()
	origStaleAfter, origSweepEvery := pendingTenantStaleAfter, pendingTenantSweepEvery
	pendingTenantStaleAfter = time.Minute
	pendingTenantSweepEvery = time.Hour
	defer func() {
		pendingTenantStaleAfter = origStaleAfter
		pendingTenantSweepEvery = origSweepEvery
	}()
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "",
		DBPort:           0,
		DBUser:           "",
		DBPasswordCipher: []byte{},
		DBName:           "",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBZero, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: pool, Provisioner: prov, TokenSecret: []byte("abc")})
	defer srv.Close()

	time.Sleep(100 * time.Millisecond)
	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantPending) {
		t.Fatalf("fresh pending tenant status = %s, want %s", status, meta.TenantPending)
	}
}

func TestReconcilePendingDirectNativeClusterWithoutPoolOwnershipBecomesFailed(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantFailed) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantFailed)
	}
}

func TestReconcilePendingNativePoolBindingWithoutConnectionStaysPending(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-pool-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpsertTenantTiDBCloudOrgBinding(context.Background(), &meta.TenantTiDBCloudOrgBinding{
		TenantID: tenantID, OrganizationID: "org-pool-1", ClusterID: "cluster-pool-1",
		PoolID: "pool-1", PoolStatus: meta.TenantPoolBindingFree, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantPending) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantPending)
	}
}

func TestReconcilePendingSharedPoolTenantWithoutConnectionStaysPending(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	ctx := context.Background()
	spendingLimit := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-pending-shared", ProvisioningKey: make([]byte, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
		PasswordCipher: []byte("cipher"), Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID: tenantID, Status: meta.TenantPending, Provider: tenant.ProviderTiDBCloudNativeShared,
		DBPasswordCipher: []byte{}, DBTLS: true, SchemaVersion: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := metaStore.InsertTenant(ctx, &pendingTenant); err != nil {
		t.Fatal(err)
	}
	fsID, err := metaStore.EnsureFsID(ctx, tenantID)
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := metaStore.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared,
		Status: meta.SharedDBStatusActive,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement: %v", err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(ctx, pendingTenant)
	got, err := metaStore.GetTenant(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantPending {
		t.Fatalf("shared pool tenant status after reconcile = %s, want %s", got.Status, meta.TenantPending)
	}
}

func TestReconcilePendingReservationOnlyFreeTenantBecomesDeleted(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	if err := metaStore.SetQuotaConfigPatch(context.Background(), tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimit: &zero}); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleted) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantDeleted)
	}
}

func TestReconcilePendingReloadsTenantAfterConcurrentEarlyBinding(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	staleAt := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	staleSnapshot := meta.Tenant{
		ID: tenantID, Status: meta.TenantPending, Kind: meta.TenantKindLive,
		DBPasswordCipher: []byte{}, DBTLS: true, Provider: tenant.ProviderTiDBCloudNative,
		SchemaVersion: 1, CreatedAt: staleAt, UpdatedAt: staleAt,
	}
	if err := metaStore.InsertTenant(context.Background(), &staleSnapshot); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	if err := metaStore.SetQuotaConfigPatch(context.Background(), tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimit: &zero}); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.PersistTiDBCloudTenantClusterReference(context.Background(), tenantID, "org-concurrent-binding", &meta.Tenant{
		Provider: tenant.ProviderTiDBCloudNative, ClusterID: "cluster-concurrent-binding",
	}); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), staleSnapshot)

	got, err := metaStore.GetTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantPending || got.ClusterID != "cluster-concurrent-binding" {
		t.Fatalf("tenant after reconcile = status:%s cluster:%q, want pending early binding", got.Status, got.ClusterID)
	}
}

func TestReconcilePendingNativeTenantWithConnectionResumesSchemaInit(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	passCipher, err := pool.Encrypt(context.Background(), []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBHost:           "db.example",
		DBPort:           4000,
		DBUser:           "u1.root",
		DBPasswordCipher: passCipher,
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        "cluster-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}

	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative}
	srv := &Server{meta: metaStore, pool: pool, provisioner: prov}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	deadline := time.Now().Add(2 * time.Second)
	for {
		row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
		var status string
		if err := row.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(meta.TenantActive) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending native tenant did not resume schema init, status=%s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if prov.systemUserCalls.Load() != 1 {
		t.Fatalf("system user calls = %d, want 1", prov.systemUserCalls.Load())
	}
}

func TestReconcilePendingTenantDoesNotOverwriteChangedStatus(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	tenantID := token.NewID()
	now := time.Now().UTC().Add(-2 * time.Minute)
	origStaleAfter := pendingTenantStaleAfter
	pendingTenantStaleAfter = time.Minute
	defer func() { pendingTenantStaleAfter = origStaleAfter }()
	pendingTenant := meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBZero,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := metaStore.InsertTenant(context.Background(), &pendingTenant); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpdateTenantStatus(context.Background(), tenantID, meta.TenantProvisioning); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.reconcilePendingTenant(context.Background(), pendingTenant)

	row := metaStore.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", tenantID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantProvisioning) {
		t.Fatalf("status after reconcile = %s, want %s", status, meta.TenantProvisioning)
	}
}

func TestServerCloseCancelsSchemaInitRetryWorker(t *testing.T) {
	origWindow, origInitBackoff, origMaxBackoff := schemaInitRetryWindow, schemaInitInitialBackoff, schemaInitMaxBackoff
	schemaInitRetryWindow = time.Minute
	schemaInitInitialBackoff = 5 * time.Second
	schemaInitMaxBackoff = 5 * time.Second
	defer func() {
		schemaInitRetryWindow = origWindow
		schemaInitInitialBackoff = origInitBackoff
		schemaInitMaxBackoff = origMaxBackoff
	}()

	prov := &fakeProvisioner{
		provider: tenant.ProviderTiDBZero,
		cluster:  &tenant.ClusterInfo{},
		initErr:  fmt.Errorf("boom"),
	}
	srv := NewWithConfig(Config{
		Provisioner: prov,
		TokenSecret: []byte("abc"),
	})

	srv.startProvisionedTenantSchemaInit(context.Background(), &provisionTenantResult{
		TenantID:  "tenant-close-test",
		TenantDSN: "user:pass@tcp(localhost:3306)/db?parseTime=true",
		Provider:  tenant.ProviderTiDBZero,
	})

	// Let the worker enter the retry backoff path before closing the server.
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		srv.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Server.Close did not cancel schema init retry worker promptly")
	}
}

func TestProvisionTiDBCloudNativeRejectsPartialCredentials(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cluster: &tenant.ClusterInfo{}}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{"public_key": "only-pk"})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestProvisionTiDBCloudNativeUsesDefaultCredentialsWhenOmitted(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer pool.Close()

	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{
		provider:          tenant.ProviderTiDBCloudNative,
		cluster:           &tenant.ClusterInfo{ClusterID: "native-cluster-default", OrganizationID: "org-default"},
		defaultPublicKey:  "default-pk",
		defaultPrivateKey: "default-sk",
	}
	srv := NewWithConfig(Config{
		Meta:                         metaStore,
		Pool:                         pool,
		Provisioner:                  prov,
		TokenSecret:                  tokenSecret,
		DisableDatabaseAutoEmbedding: true,
	})
	defer srv.Close()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if prov.lastCredentialReq.PublicKey != "default-pk" || prov.lastCredentialReq.PrivateKey != "default-sk" {
		t.Fatalf("credentials = %s/%s, want default-pk/default-sk", prov.lastCredentialReq.PublicKey, prov.lastCredentialReq.PrivateKey)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	binding, err := metaStore.GetTenantTiDBCloudOrgBinding(context.Background(), out["tenant_id"])
	if err != nil {
		t.Fatalf("get tidbcloud org binding: %v", err)
	}
	if binding.OrganizationID != "org-default" || binding.ClusterID != "native-cluster-default" {
		t.Fatalf("binding = %#v", binding)
	}
}
