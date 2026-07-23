package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestTenantFailedCleanupAsyncCoalescesSameOrganizationWithoutQueuedRerun(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	s := &Server{
		tenantFailedCleanupRunner: func(context.Context, string, tenant.CredentialProvisionRequest) {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
		},
	}

	startCalls := make(chan struct{})
	var starters sync.WaitGroup
	for i := 0; i < 20; i++ {
		starters.Add(1)
		go func() {
			defer starters.Done()
			<-startCalls
			s.startTenantFailedCleanupAsync(context.Background(), "  org-one  ", tenant.CredentialProvisionRequest{})
		}()
	}
	close(startCalls)
	starters.Wait()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cleanup runner did not start")
	}

	// Calls arriving while the worker is active are dropped, not queued.
	for i := 0; i < 5; i++ {
		s.startTenantFailedCleanupAsync(context.Background(), "org-one", tenant.CredentialProvisionRequest{})
	}
	close(release)
	s.forkWorkerWG.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("cleanup runner calls = %d, want exactly 1", got)
	}
}

func TestTenantFailedCleanupAsyncHonorsCooldownWithoutInvokingRunner(t *testing.T) {
	var calls atomic.Int32
	s := &Server{
		tenantFailedCleanupRunner: func(context.Context, string, tenant.CredentialProvisionRequest) {
			calls.Add(1)
		},
	}

	s.startTenantFailedCleanupAsync(context.Background(), "org-cooldown", tenant.CredentialProvisionRequest{})
	s.forkWorkerWG.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("initial cleanup runner calls = %d, want 1", got)
	}

	s.startTenantFailedCleanupAsync(context.Background(), "org-cooldown", tenant.CredentialProvisionRequest{})
	s.forkWorkerWG.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("within-interval cleanup runner calls = %d, want 1", got)
	}

	value, ok := s.tenantFailedCleanupJobs.Load("org-cooldown")
	if !ok {
		t.Fatal("cleanup job state not stored")
	}
	state := value.(*tenantFailedCleanupJobState)
	state.mu.Lock()
	state.lastStarted = time.Now().Add(-2 * tenantFailedCleanupMinInterval)
	state.mu.Unlock()

	s.startTenantFailedCleanupAsync(context.Background(), "org-cooldown", tenant.CredentialProvisionRequest{})
	s.forkWorkerWG.Wait()
	if got := calls.Load(); got != 2 {
		t.Fatalf("after-interval cleanup runner calls = %d, want 2", got)
	}
}

func TestTenantFailedCleanupAsyncRunsDifferentOrganizationsIndependently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	s := &Server{
		tenantFailedCleanupRunner: func(_ context.Context, organizationID string, _ tenant.CredentialProvisionRequest) {
			started <- organizationID
			<-release
		},
	}

	s.startTenantFailedCleanupAsync(context.Background(), " org-a ", tenant.CredentialProvisionRequest{})
	s.startTenantFailedCleanupAsync(context.Background(), "org-b", tenant.CredentialProvisionRequest{})
	got := map[string]bool{}
	for len(got) < 2 {
		select {
		case organizationID := <-started:
			got[organizationID] = true
		case <-time.After(time.Second):
			t.Fatalf("started organizations = %v, want org-a and org-b", got)
		}
	}
	close(release)
	s.forkWorkerWG.Wait()
	if !got["org-a"] || !got["org-b"] {
		t.Fatalf("started organizations = %v, want normalized org-a and org-b", got)
	}
}

func TestTenantFailedCleanupAsyncSkipsEmptyOrganization(t *testing.T) {
	var calls atomic.Int32
	s := &Server{
		tenantFailedCleanupRunner: func(context.Context, string, tenant.CredentialProvisionRequest) {
			calls.Add(1)
		},
	}

	s.startTenantFailedCleanupAsync(context.Background(), " \t\n ", tenant.CredentialProvisionRequest{})
	s.forkWorkerWG.Wait()
	if got := calls.Load(); got != 0 {
		t.Fatalf("cleanup runner calls = %d, want 0 for empty organization", got)
	}
}

func TestTenantFailedCleanupAsyncRollsBackStateWhenServerRejectsWorker(t *testing.T) {
	var calls atomic.Int32
	s := &Server{
		tenantFailedCleanupRunner: func(context.Context, string, tenant.CredentialProvisionRequest) {
			calls.Add(1)
		},
	}
	previousStart := time.Now().Add(-2 * tenantFailedCleanupMinInterval)
	state := &tenantFailedCleanupJobState{lastStarted: previousStart}
	s.tenantFailedCleanupJobs.Store("org-closed", state)
	s.forkWorkerMu.Lock()
	s.forkWorkerClosed = true
	s.forkWorkerMu.Unlock()

	s.startTenantFailedCleanupAsync(context.Background(), "org-closed", tenant.CredentialProvisionRequest{})

	if got := calls.Load(); got != 0 {
		t.Fatalf("cleanup runner calls = %d, want 0 after server close", got)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.active {
		t.Fatal("cleanup job remained active after worker start was rejected")
	}
	if !state.lastStarted.Equal(previousStart) {
		t.Fatalf("cleanup last started = %s, want preserved %s", state.lastStarted, previousStart)
	}
}

func TestTenantPoolClaimReturnsWithoutWaitingForFailedCleanupAndPassesCredentials(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{OrganizationID: failedCleanupTestOrganizationID}},
	}}
	started := make(chan tenant.CredentialProvisionRequest, 1)
	release := make(chan struct{})
	rt.server.tenantFailedCleanupRunner = func(_ context.Context, organizationID string, cred tenant.CredentialProvisionRequest) {
		if organizationID != failedCleanupTestOrganizationID {
			t.Errorf("cleanup organization = %q, want %q", organizationID, failedCleanupTestOrganizationID)
		}
		started <- cred
		<-release
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	wantCred := tenant.CredentialProvisionRequest{PublicKey: "request-public", PrivateKey: "request-private"}
	type claimResult struct {
		result            *provisionTenantResult
		pool              *meta.TenantPool
		claimed           bool
		sharedPoolMatched bool
		err               error
	}
	claimDone := make(chan claimResult, 1)
	go func() {
		result, pool, claimed, sharedPoolMatched, err := rt.server.claimAdminTenantFromPool(ctx, wantCred, nil)
		claimDone <- claimResult{result, pool, claimed, sharedPoolMatched, err}
	}()

	var gotCred tenant.CredentialProvisionRequest
	select {
	case gotCred = <-started:
	case <-time.After(time.Second):
		t.Fatal("cleanup runner did not start")
	}
	select {
	case got := <-claimDone:
		if got.err != nil || got.result != nil || got.pool != nil || got.claimed || got.sharedPoolMatched {
			t.Fatalf("claim result = %+v, want non-pool miss", got)
		}
	case <-time.After(time.Second):
		t.Fatal("claim waited for asynchronous failed-tenant cleanup")
	}
	if gotCred != wantCred {
		t.Fatalf("cleanup credentials = %#v, want %#v", gotCred, wantCred)
	}
	close(release)
	rt.server.forkWorkerWG.Wait()
}

func TestTenantPoolClaimTriggersDirectFailedCleanupWithoutLogicalPool(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{OrganizationID: failedCleanupTestOrganizationID}},
	}}
	old := time.Now().UTC().Add(-time.Hour)
	directTenantID := rt.tenantID
	setFailedCleanupTenant(t, rt, directTenantID, tenant.ProviderTiDBCloudNative, "cluster-direct", "", old)
	upsertFailedCleanupNativeBinding(t, rt, directTenantID, "cluster-direct", "", meta.TenantPoolBindingUsed)
	claimedTenantID := insertFailedCleanupTenant(t, rt, tenant.ProviderTiDBCloudNative, "cluster-claimed", "", old.Add(-time.Hour))
	upsertFailedCleanupNativeBinding(t, rt, claimedTenantID, "cluster-claimed", "pool-claimed", meta.TenantPoolBindingUsed)
	wantCred := tenant.CredentialProvisionRequest{PublicKey: "public-direct", PrivateKey: "private-direct"}

	result, pool, claimed, sharedPoolMatched, err := rt.server.claimAdminTenantFromPool(ctx, wantCred, nil)
	if err != nil || result != nil || pool != nil || claimed || sharedPoolMatched {
		t.Fatalf("claim result = result=%+v pool=%+v claimed=%v shared=%v err=%v, want non-pool miss",
			result, pool, claimed, sharedPoolMatched, err)
	}
	rt.server.forkWorkerWG.Wait()

	assertFailedCleanupTenantStatus(t, rt, directTenantID, meta.TenantDeleted)
	assertFailedCleanupTenantStatus(t, rt, claimedTenantID, meta.TenantFailed)
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want direct tenant only", got)
	}
	if got := rt.prov.lastCredentialsSnapshot(); got != wantCred {
		t.Fatalf("cleanup deprovision credentials = %#v, want %#v", got, wantCred)
	}
}

func TestTenantPoolClaimResultUnaffectedByFailedCleanupError(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	rt.prov.listPages = []*tenant.ManagedClusterListResult{{
		Clusters: []tenant.CloudClusterInfo{{OrganizationID: failedCleanupTestOrganizationID}},
	}}
	old := time.Now().UTC().Add(-time.Hour)
	setFailedCleanupTenant(t, rt, rt.tenantID, tenant.ProviderTiDBCloudNative, "cluster-cleanup-fails", "", old)
	upsertFailedCleanupNativeBinding(t, rt, rt.tenantID, "cluster-cleanup-fails", "", meta.TenantPoolBindingUsed)
	cleanupErr := errors.New("cloud cleanup failed")
	rt.prov.deprovisionErr = cleanupErr

	result, pool, claimed, sharedPoolMatched, err := rt.server.claimAdminTenantFromPool(ctx,
		tenant.CredentialProvisionRequest{PublicKey: "public-failure", PrivateKey: "private-failure"}, nil)
	if err != nil || result != nil || pool != nil || claimed || sharedPoolMatched {
		t.Fatalf("claim result = result=%+v pool=%+v claimed=%v shared=%v err=%v, want unaffected non-pool miss",
			result, pool, claimed, sharedPoolMatched, err)
	}
	rt.server.forkWorkerWG.Wait()

	assertFailedCleanupTenantStatus(t, rt, rt.tenantID, meta.TenantFailed)
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
}
