package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/tidbcloudnative"
	"go.uber.org/zap"
)

// sharedDBTLSMode is the mysql driver TLS mode persisted on managed shared DB
// pool rows. Local Docker TiDB has no TLS; Cloud public uses verified TLS;
// private endpoint uses skip-verify.
func sharedDBTLSMode() string {
	return tidbcloudnative.DBTLSModeForBackend()
}

const (
	defaultManagedSharedDBMaxTenants      = 100
	defaultSharedTenantSpendingLimit      = int64(1000)
	sharedTenantActivationBatchSize       = 100
	managedSharedDBProvisioningMaxBackoff = 15 * time.Second
	managedSharedDBProvisioningCooldown   = 10 * time.Second
)

var errSharedDBConnectionMetadataNotReady = errors.New("shared DB pool connection metadata is not ready")

func sharedDBConnectionReady(db *meta.SharedDB) bool {
	return db != nil && db.Status == meta.SharedDBStatusActive &&
		strings.TrimSpace(db.Host) != "" && db.Port > 0 &&
		strings.TrimSpace(db.User) != "" && len(db.PasswordCipher) > 0 &&
		strings.TrimSpace(db.Name) != ""
}

type defaultTiDBCloudSpendingLimitProvider interface {
	DefaultTiDBCloudSpendingLimit() int64
}

type defaultSharedDatabaseNameProvider interface {
	DefaultSharedDatabaseName() string
}

func (s *Server) managedSharedDBPolicy() (maxTenants int, spendingLimit int64) {
	maxTenants = s.sharedDBMaxTenants
	if maxTenants <= 0 {
		maxTenants = defaultManagedSharedDBMaxTenants
	}
	spendingLimit = s.sharedDBSpendingLimit
	if spendingLimit <= 0 {
		spendingLimit = DefaultManagedSharedDBSpendingLimit
	}
	return maxTenants, spendingLimit
}

func (s *Server) managedSharedDBHardCap(softCap int) (int, error) {
	ratio := s.sharedDBHardCapRatio
	if ratio < 1 || ratio != ratio {
		ratio = DefaultSharedDBHardCapRatio
	}
	return meta.SharedDBHardCap(softCap, ratio)
}

func (s *Server) sharedTenantVirtualSpendingLimit(opts provisionTenantOptions) int64 {
	if opts.Quota != nil && opts.Quota.TiDBCloudSpendingLimit != nil {
		return *opts.Quota.TiDBCloudSpendingLimit
	}
	if provider, ok := s.provisioner.(defaultTiDBCloudSpendingLimitProvider); ok {
		return provider.DefaultTiDBCloudSpendingLimit()
	}
	return defaultSharedTenantSpendingLimit
}

func sharedDBProvisioningKey(req tenant.CredentialProvisionRequest) []byte {
	sum := sha256.Sum256([]byte(strings.TrimSpace(req.PublicKey)))
	return sum[:]
}

func sharedDBAllocationIdentity(organizationID string, provisioningKey []byte) string {
	if organizationID != "" {
		return "org:" + organizationID
	}
	return fmt.Sprintf("credential:%x", provisioningKey)
}

type sharedDBLocalLock struct {
	mu        sync.Mutex
	semaphore chan struct{}
	refs      int
}

func acquireSharedDBLocalLock(ctx context.Context, locks *sync.Map, identity string) (*sharedDBLocalLock, error) {
	for {
		candidate := &sharedDBLocalLock{semaphore: make(chan struct{}, 1)}
		value, _ := locks.LoadOrStore(identity, candidate)
		localLock := value.(*sharedDBLocalLock)
		localLock.mu.Lock()
		current, loaded := locks.Load(identity)
		if !loaded || current != localLock {
			localLock.mu.Unlock()
			continue
		}
		localLock.refs++
		localLock.mu.Unlock()

		select {
		case localLock.semaphore <- struct{}{}:
			return localLock, nil
		case <-ctx.Done():
			releaseSharedDBLocalLock(locks, identity, localLock, false)
			return nil, ctx.Err()
		}
	}
}

func releaseSharedDBLocalLock(locks *sync.Map, identity string, localLock *sharedDBLocalLock, held bool) {
	if held {
		<-localLock.semaphore
	}
	localLock.mu.Lock()
	localLock.refs--
	if localLock.refs == 0 {
		locks.CompareAndDelete(identity, localLock)
	}
	localLock.mu.Unlock()
}

func (s *Server) withSharedDBAllocationLock(ctx context.Context, identity string, fn func(context.Context) error) error {
	localLock, err := acquireSharedDBLocalLock(ctx, &s.sharedDBAllocationLocks, identity)
	if err != nil {
		return err
	}
	defer releaseSharedDBLocalLock(&s.sharedDBAllocationLocks, identity, localLock, true)
	return s.meta.WithSharedDBAllocationLock(ctx, identity, fn)
}

func (s *Server) withSharedDBPoolWorkLock(ctx context.Context, dbID int64, fn func(context.Context) error) error {
	identity := fmt.Sprintf("pool-work:%d", dbID)
	localLock, err := acquireSharedDBLocalLock(ctx, &s.sharedDBAllocationLocks, identity)
	if err != nil {
		return err
	}
	defer releaseSharedDBLocalLock(&s.sharedDBAllocationLocks, identity, localLock, true)
	return s.meta.WithSharedDBPoolWorkLock(ctx, dbID, fn)
}

func (s *Server) withSharedDBReservationLock(ctx context.Context, identity string, fn func() error) error {
	localLock, err := acquireSharedDBLocalLock(ctx, &s.sharedDBReservationLocks, identity)
	if err != nil {
		return err
	}
	defer releaseSharedDBLocalLock(&s.sharedDBReservationLocks, identity, localLock, true)
	return fn()
}

func managedSharedDBDSN(info *meta.SharedDB, password string) string {
	query := "parseTime=true"
	// Prefer backend-correct TLS. Local Docker TiDB is plaintext; a previously
	// persisted "true"/"skip-verify" (warm-pool batch bug) would hang activation.
	// IsLocalClustersBackend already implies sharedDBTLSMode() == "".
	tlsMode := info.TLSMode
	if tidbcloudnative.IsLocalClustersBackend() {
		tlsMode = ""
	}
	if tlsMode != "" {
		query += "&tls=" + tlsMode
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", info.User, password, info.Host, info.Port, info.Name, query)
}

func (s *Server) sharedDBCloudCredentials() (tenant.CredentialProvisionRequest, error) {
	provider, ok := s.provisioner.(tenant.SharedCredentialProvider)
	if !ok {
		return tenant.CredentialProvisionRequest{}, fmt.Errorf("shared TiDB Cloud credentials are not configured")
	}
	cred, ok := provider.DefaultSharedCredentials()
	if !ok {
		return tenant.CredentialProvisionRequest{}, fmt.Errorf("shared TiDB Cloud credentials are not configured")
	}
	return cred, nil
}

func generateManagedSharedDBRootPassword(length int) (string, error) {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if length <= 0 {
		return "", fmt.Errorf("password length must be positive")
	}
	out := make([]byte, length)
	max := big.NewInt(int64(len(chars)))
	for i := range out {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		out[i] = chars[n.Int64()]
	}
	return string(out), nil
}

func (s *Server) materializeSharedTenantQuota(ctx context.Context, tenantID string, opts provisionTenantOptions) error {
	cfg, err := s.sharedTenantQuotaConfig(tenantID, opts)
	if err != nil {
		return err
	}
	if err := s.meta.SetQuotaConfig(ctx, cfg); err != nil {
		return fmt.Errorf("materialize shared tenant quota: %w", err)
	}
	return nil
}

func (s *Server) sharedTenantQuotaConfig(tenantID string, opts provisionTenantOptions) (*meta.QuotaConfig, error) {
	virtualLimit := s.sharedTenantVirtualSpendingLimit(opts)
	var patch meta.QuotaConfigPatch
	if opts.Quota != nil {
		req := *opts.Quota
		req.TenantID = tenantID
		if err := s.validateQuotaSetRequest(req); err != nil {
			return nil, err
		}
		var err error
		patch, err = quotaConfigPatchFromRequest(req)
		if err != nil {
			return nil, err
		}
	}
	cfg := &meta.QuotaConfig{
		TenantID:               tenantID,
		MaxStorageBytes:        meta.DefaultMaxStorageBytes(),
		MaxFileSizeBytes:       meta.DefaultMaxFileSizeBytes(),
		MaxFileCount:           0,
		MaxMediaLLMFiles:       meta.DefaultMaxMediaLLMFiles(),
		MaxVideoLLMFiles:       meta.DefaultMaxVideoLLMFiles(),
		TiDBCloudSpendingLimit: &virtualLimit,
	}
	if patch.MaxStorageBytes != nil {
		cfg.MaxStorageBytes = *patch.MaxStorageBytes
	}
	if patch.MaxFileSizeBytes != nil {
		cfg.MaxFileSizeBytes = *patch.MaxFileSizeBytes
	}
	if patch.MaxFileCount != nil {
		cfg.MaxFileCount = *patch.MaxFileCount
	}
	return cfg, nil
}

func (s *Server) allocateManagedSharedDB(ctx context.Context, cred tenant.CredentialProvisionRequest, reserve func(*meta.SharedDB) error) (sharedDB *meta.SharedDB, created bool, err error) {
	organizationID, resolveErr := s.firstManagedOrganization(ctx, cred)
	if resolveErr != nil {
		return nil, false, fmt.Errorf("resolve tidbcloud organization: %w", resolveErr)
	}
	return s.allocateManagedSharedDBForOrganization(ctx, organizationID, cred, reserve)
}

func (s *Server) createManagedSharedDBPlan(ctx context.Context, organizationID string, provisioningKey []byte) (*meta.SharedDB, error) {
	row, err := s.newManagedSharedDBPlan(ctx, organizationID, provisioningKey)
	if err != nil {
		return nil, err
	}
	id, err := s.meta.CreateManagedSharedDBPool(ctx, row)
	if err != nil {
		return nil, err
	}
	row.ID = id
	return row, nil
}

func (s *Server) newManagedSharedDBPlan(ctx context.Context, organizationID string, provisioningKey []byte) (*meta.SharedDB, error) {
	maxTenants, fixedTarget := s.managedSharedDBPolicy()
	cloudProvider, region := provisioningCloudRegion(s.provisioner)
	rootPassword, err := generateManagedSharedDBRootPassword(24)
	if err != nil {
		return nil, err
	}
	passwordCipher, err := s.pool.Encrypt(ctx, []byte(rootPassword))
	if err != nil {
		return nil, fmt.Errorf("encrypt shared db root password: %w", err)
	}
	row := &meta.SharedDB{
		UUID:                    uuid.NewString(),
		TiDBCloudOrganizationID: organizationID,
		ProvisioningKey:         append([]byte(nil), provisioningKey...),
		CloudProvider:           cloudProvider,
		Region:                  region,
		Role:                    meta.SharedDBRoleShared,
		MaxTenants:              maxTenants,
		SpendingLimit:           &fixedTarget,
		PasswordCipher:          passwordCipher,
		Name:                    s.defaultSharedDatabaseName(),
		Status:                  meta.SharedDBStatusPending,
	}
	return row, nil
}

func (s *Server) allocateManagedSharedDBForOrganization(ctx context.Context, organizationID string, cred tenant.CredentialProvisionRequest, reserve func(*meta.SharedDB) error) (sharedDB *meta.SharedDB, created bool, err error) {
	provisioningKey := sharedDBProvisioningKey(cred)
	identity := sharedDBAllocationIdentity(organizationID, provisioningKey)
	planAndReserve := func() error {
		return s.withSharedDBAllocationLock(ctx, identity, func(lockCtx context.Context) error {
			for attempt := 0; attempt < 2; attempt++ {
				candidate, findErr := s.meta.FindSharedDBForAllocation(lockCtx, organizationID)
				if findErr == nil {
					sharedDB = candidate
				} else if !errors.Is(findErr, meta.ErrNotFound) {
					return findErr
				} else if organizationID != "" {
					manual, manualErr := s.meta.FindSharedDBForOrg(lockCtx, organizationID)
					if manualErr == nil && manual.SpendingLimit == nil {
						sharedDB = manual
					} else if manualErr != nil && !errors.Is(manualErr, meta.ErrNotFound) {
						return manualErr
					}
				}
				if sharedDB == nil {
					var createErr error
					sharedDB, createErr = s.createManagedSharedDBPlan(lockCtx, organizationID, provisioningKey)
					if createErr != nil {
						return createErr
					}
					created = true
				}
				if reserve == nil {
					return nil
				}
				reserveErr := reserve(sharedDB)
				if reserveErr == nil {
					return nil
				}
				if !errors.Is(reserveErr, meta.ErrSharedDBCapacityExhausted) {
					return reserveErr
				}
				sharedDB = nil
				created = false
			}
			return meta.ErrSharedDBCapacityExhausted
		})
	}
	if reserve == nil {
		err = planAndReserve()
		return sharedDB, created, err
	}
	// Capacity reservation is transactional in CompleteSharedTenant* and does
	// not need a cross-pod lock while existing capacity is available. Serialize
	// this Pod's complete same-organization lookup, optional physical planning,
	// and reservation transition so a fast-path worker cannot steal a pool that
	// another local worker just created but has not reserved yet. Different
	// organizations and Pods remain independent.
	err = s.withSharedDBReservationLock(ctx, identity, func() error {
		for attempt := 0; attempt < 2; attempt++ {
			candidate, findErr := s.meta.FindSharedDBForAllocation(ctx, organizationID)
			if errors.Is(findErr, meta.ErrNotFound) {
				break
			}
			if findErr != nil {
				return findErr
			}
			reserveErr := reserve(candidate)
			if reserveErr == nil {
				sharedDB = candidate
				return nil
			}
			if !errors.Is(reserveErr, meta.ErrSharedDBCapacityExhausted) {
				return reserveErr
			}
		}
		return planAndReserve()
	})
	return sharedDB, created, err
}

func (s *Server) defaultSharedDatabaseName() string {
	if provider, ok := s.provisioner.(defaultSharedDatabaseNameProvider); ok {
		if name := strings.TrimSpace(provider.DefaultSharedDatabaseName()); name != "" {
			return name
		}
	}
	return "tidbcloud_fs"
}

func (s *Server) scheduleManagedSharedDBContinuation(ctx context.Context, dbID int64) {
	s.scheduleManagedSharedDBContinuations(ctx, []int64{dbID})
}

func (s *Server) scheduleManagedSharedDBContinuations(ctx context.Context, dbIDs []int64) {
	if len(dbIDs) == 0 {
		return
	}
	ids := append([]int64(nil), dbIDs...)
	slices.Sort(ids)
	if s.startManagedSharedDBWorker(ctx, func(workerCtx context.Context) {
		s.runManagedSharedDBContinuations(workerCtx, ids)
	}) {
		return
	}
	reason := "server_stopping"
	if s.leader != nil {
		reason = "not_leader"
	}
	logger.Info(ctx, "managed_shared_db_continuation_deferred",
		zap.String("reason", reason),
		zap.Int("db_pool_count", len(ids)),
		zap.Int64("first_db_pool_id", ids[0]),
		zap.Int64("last_db_pool_id", ids[len(ids)-1]))
}

func (s *Server) startManagedSharedDBWorker(ctx context.Context, fn func(context.Context)) bool {
	if s.leader == nil {
		return s.startServerWorker(ctx, fn)
	}
	s.leaderWorkerMu.Lock()
	if !s.leaderWorkersStarted || !s.leader.IsLeader() || s.leaderWorkerCtx == nil {
		s.leaderWorkerMu.Unlock()
		return false
	}
	workerCtx := contextWithTrace(s.leaderWorkerCtx, ctx)
	s.leaderWorkerWG.Add(1)
	s.leaderWorkerMu.Unlock()
	go func() {
		defer s.leaderWorkerWG.Done()
		fn(workerCtx)
	}()
	return true
}

func (s *Server) runManagedSharedDBContinuations(ctx context.Context, dbIDs []int64) {
	pendingWithoutCluster := make([]int64, 0, len(dbIDs))
	for _, dbID := range dbIDs {
		row, err := s.meta.GetSharedDB(ctx, dbID)
		if err != nil {
			logger.Warn(ctx, "managed_shared_db_pool_continue_load_failed", zap.Int64("db_pool_id", dbID), zap.Error(err))
			continue
		}
		if row.Status == meta.SharedDBStatusPending && strings.TrimSpace(row.ClusterID) == "" {
			pendingWithoutCluster = append(pendingWithoutCluster, dbID)
		}
	}
	if len(pendingWithoutCluster) > 0 {
		cred, err := s.sharedDBCloudCredentials()
		if err != nil {
			logger.Warn(ctx, "managed_shared_db_pool_create_credentials_failed", zap.Error(err))
		} else if _, err := s.provisionManagedSharedDBPoolsBatchWithCredentials(ctx, pendingWithoutCluster, cred); err != nil {
			logger.Warn(ctx, "managed_shared_db_pool_batch_create_failed", zap.Int("db_pool_count", len(pendingWithoutCluster)), zap.Error(err))
		}
	}

	pending := make([]*meta.SharedDB, 0, len(dbIDs))
	provisioning := make([]int64, 0, len(dbIDs))
	for _, dbID := range dbIDs {
		row, err := s.meta.GetSharedDB(ctx, dbID)
		if err != nil {
			continue
		}
		switch row.Status {
		case meta.SharedDBStatusPending:
			if strings.TrimSpace(row.ClusterID) != "" {
				pending = append(pending, row)
			}
		case meta.SharedDBStatusProvisioning:
			provisioning = append(provisioning, row.ID)
		}
	}
	var provisioningWG sync.WaitGroup
	startProvisioning := func(ids []int64) {
		if len(ids) == 0 {
			return
		}
		ids = append([]int64(nil), ids...)
		provisioningWG.Add(1)
		go func() {
			defer provisioningWG.Done()
			s.runManagedSharedDBProvisioning(ctx, ids)
		}()
	}
	startProvisioning(provisioning)
	if len(pending) > 0 {
		s.pollManagedSharedDBMetadataWithReady(ctx, pending, startProvisioning)
	}
	provisioningWG.Wait()
}

func (s *Server) runManagedSharedDBProvisioning(ctx context.Context, dbIDs []int64) {
	owned := make([]int64, 0, len(dbIDs))
	for _, dbID := range dbIDs {
		if dbID <= 0 {
			continue
		}
		if _, loaded := s.managedSharedDBProvisioningJobs.LoadOrStore(dbID, struct{}{}); !loaded {
			owned = append(owned, dbID)
		}
	}
	if len(owned) == 0 {
		return
	}
	defer func() {
		for _, dbID := range owned {
			s.managedSharedDBProvisioningJobs.Delete(dbID)
		}
	}()
	workers := s.managedSharedDBProvisioningConcurrency
	if workers <= 0 {
		if s.managedSharedDBProvisioningSlots != nil {
			maxOpen := 0
			if s.meta != nil && s.meta.DB() != nil {
				maxOpen = s.meta.DB().Stats().MaxOpenConnections
			}
			logger.Error(ctx, "managed_shared_db_pool_provisioning_disabled",
				zap.Int("configured_workers", s.managedSharedDBProvisioningWorkers),
				zap.Int("meta_max_open_connections", maxOpen))
			return
		}
		workers = s.managedSharedDBProvisioningWorkers
		if workers <= 0 {
			workers = DefaultManagedSharedDBProvisioningWorkers
		}
	}
	failed := runManagedSharedDBProvisioningQueue(ctx, workers, s.managedSharedDBProvisioningSlots, owned,
		schemaInitRetryWindow, schemaInitInitialBackoff, managedSharedDBProvisioningMaxBackoff,
		managedSharedDBProvisioningCooldown,
		func(jobCtx context.Context, dbID int64) error {
			err := s.continueManagedSharedDBPoolOnce(jobCtx, dbID)
			if errors.Is(err, meta.ErrNotFound) {
				return nil
			}
			return err
		}, func(dbID int64, attempt int, retryIn time.Duration, err error) {
			logger.Warn(ctx, "managed_shared_db_pool_provisioning_retry", zap.Int64("db_pool_id", dbID),
				zap.Int("attempt", attempt), zap.Duration("retry_in", retryIn), zap.Error(err))
		})
	for dbID, err := range failed {
		logger.Warn(ctx, "managed_shared_db_pool_provisioning_failed", zap.Int64("db_pool_id", dbID), zap.Error(err))
	}
}

type managedSharedDBProvisioningJob struct {
	dbID     int64
	attempt  int
	backoff  time.Duration
	deadline time.Time
}

type managedSharedDBProvisioningResult struct {
	job *managedSharedDBProvisioningJob
	err error
}

func runManagedSharedDBProvisioningQueue(
	ctx context.Context,
	workers int,
	slots chan struct{},
	dbIDs []int64,
	retryWindow, initialBackoff, maxBackoff, workerCooldown time.Duration,
	run func(context.Context, int64) error,
	onRetry func(int64, int, time.Duration, error),
) map[int64]error {
	failed := make(map[int64]error)
	if workers <= 0 || len(dbIDs) == 0 {
		return failed
	}
	if retryWindow <= 0 {
		retryWindow = schemaInitRetryWindow
	}
	if initialBackoff <= 0 {
		initialBackoff = schemaInitInitialBackoff
	}
	if maxBackoff < initialBackoff {
		maxBackoff = initialBackoff
	}
	deadline := time.Now().Add(retryWindow)
	jobs := make(chan *managedSharedDBProvisioningJob, len(dbIDs))
	results := make(chan managedSharedDBProvisioningResult, workers)
	retries := make(chan *managedSharedDBProvisioningJob, len(dbIDs))
	done := make(chan struct{})
	var stopOnce sync.Once
	stopWorkers := func() { stopOnce.Do(func() { close(done) }) }
	seen := make(map[int64]struct{}, len(dbIDs))
	remaining := 0
	for _, dbID := range dbIDs {
		if dbID <= 0 {
			continue
		}
		if _, ok := seen[dbID]; ok {
			continue
		}
		seen[dbID] = struct{}{}
		jobs <- &managedSharedDBProvisioningJob{dbID: dbID, attempt: 1, backoff: initialBackoff, deadline: deadline}
		remaining++
	}
	var wg sync.WaitGroup
	for range min(workers, remaining) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			first := true
			for {
				if !first && workerCooldown > 0 {
					timer := time.NewTimer(workerCooldown)
					select {
					case <-done:
						timer.Stop()
						return
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
				var job *managedSharedDBProvisioningJob
				select {
				case <-done:
					return
				case <-ctx.Done():
					return
				case job = <-jobs:
				}
				if slots != nil {
					select {
					case slots <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
				err := run(ctx, job.dbID)
				if slots != nil {
					<-slots
				}
				select {
				case results <- managedSharedDBProvisioningResult{job: job, err: err}:
				case <-ctx.Done():
					return
				}
				first = false
			}
		}()
	}
	for remaining > 0 {
		select {
		case <-ctx.Done():
			stopWorkers()
			wg.Wait()
			return failed
		case job := <-retries:
			select {
			case jobs <- job:
			case <-ctx.Done():
				stopWorkers()
				wg.Wait()
				return failed
			}
		case result := <-results:
			if result.err == nil {
				remaining--
				continue
			}
			job := result.job
			retryIn := min(job.backoff, time.Until(job.deadline))
			if retryIn <= 0 {
				failed[job.dbID] = result.err
				remaining--
				continue
			}
			if onRetry != nil {
				onRetry(job.dbID, job.attempt, retryIn, result.err)
			}
			job.attempt++
			job.backoff = min(job.backoff*2, maxBackoff)
			go func(job *managedSharedDBProvisioningJob, delay time.Duration) {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-done:
				case <-ctx.Done():
				case <-timer.C:
					select {
					case retries <- job:
					case <-done:
					case <-ctx.Done():
					}
				}
			}(job, retryIn)
		}
	}
	stopWorkers()
	wg.Wait()
	return failed
}

func withManagedSharedDBMetadataRefillSlot(ctx context.Context, slot chan struct{}, refill func()) error {
	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	case <-ctx.Done():
		return ctx.Err()
	}
	refill()
	return nil
}

func (s *Server) nextManagedSharedDBStatusPage(ctx context.Context, status string, cursor *int64, limit int) ([]*meta.SharedDB, error) {
	if cursor == nil {
		return nil, fmt.Errorf("shared db status cursor is required")
	}
	rows, err := s.meta.ListSharedDBsByStatusAfter(ctx, status, *cursor, limit)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 && *cursor > 0 {
		*cursor = 0
		rows, err = s.meta.ListSharedDBsByStatusAfter(ctx, status, 0, limit)
		if err != nil {
			return nil, err
		}
	}
	if len(rows) > 0 && len(rows) < limit {
		*cursor = 0
	} else if len(rows) > 0 {
		*cursor = rows[len(rows)-1].ID
	}
	return rows, nil
}

func (s *Server) pollManagedSharedDBMetadataWithReady(ctx context.Context, rows []*meta.SharedDB, onReady func([]int64)) {
	owned := make([]*meta.SharedDB, 0, len(rows))
	for _, row := range rows {
		if row == nil || row.ID <= 0 {
			continue
		}
		if _, loaded := s.managedSharedDBMetadataJobs.LoadOrStore(row.ID, struct{}{}); !loaded {
			owned = append(owned, row)
		}
	}
	if len(owned) == 0 {
		return
	}
	defer func() {
		for _, row := range owned {
			s.managedSharedDBMetadataJobs.Delete(row.ID)
		}
	}()
	loader, ok := s.provisioner.(tenant.SharedDBPoolBatchLoader)
	if !ok {
		logger.Warn(ctx, "managed_shared_db_pool_metadata_batch_unsupported")
		return
	}
	cred, err := s.sharedDBCloudCredentials()
	if err != nil {
		logger.Warn(ctx, "managed_shared_db_pool_metadata_credentials_failed", zap.Error(err))
		return
	}
	batchSize := s.managedSharedDBMetadataBatchSize
	if batchSize <= 0 {
		batchSize = DefaultManagedSharedDBMetadataBatchSize
	}
	workers := s.managedSharedDBMetadataWorkers
	if workers <= 0 {
		workers = DefaultManagedSharedDBMetadataWorkers
	}
	pollInterval := s.managedSharedDBMetadataPollInterval
	if pollInterval <= 0 {
		pollInterval = DefaultManagedSharedDBMetadataPollInterval
	}
	queue := append([]*meta.SharedDB(nil), owned...)
	known := make(map[int64]struct{}, len(owned))
	for _, row := range owned {
		known[row.ID] = struct{}{}
	}
	var refillAfterID int64
	for _, row := range owned {
		refillAfterID = max(refillAfterID, row.ID)
	}
	var queueMu sync.Mutex
	refillSlot := make(chan struct{}, 1)
	takeLocked := func(limit int) []*meta.SharedDB {
		limit = min(limit, len(queue))
		out := append([]*meta.SharedDB(nil), queue[:limit]...)
		queue = queue[limit:]
		return out
	}
	take := func(limit int) []*meta.SharedDB {
		if limit <= 0 {
			return nil
		}
		queueMu.Lock()
		if len(queue) >= limit {
			out := takeLocked(limit)
			queueMu.Unlock()
			return out
		}
		queueMu.Unlock()

		var out []*meta.SharedDB
		if err := withManagedSharedDBMetadataRefillSlot(ctx, refillSlot, func() {
			// Another worker may have refilled the queue while this worker was
			// waiting for the single refill slot.
			queueMu.Lock()
			if len(queue) >= limit {
				out = takeLocked(limit)
				queueMu.Unlock()
				return
			}
			queueMu.Unlock()

			fresh, listErr := s.nextManagedSharedDBStatusPage(ctx, meta.SharedDBStatusPending, &refillAfterID, 1000)
			if listErr != nil {
				logger.Warn(ctx, "managed_shared_db_pool_metadata_refill_failed", zap.Error(listErr))
			}
			queueMu.Lock()
			defer queueMu.Unlock()
			if listErr == nil {
				for _, row := range fresh {
					if strings.TrimSpace(row.ClusterID) == "" {
						continue
					}
					if _, ok := known[row.ID]; ok {
						continue
					}
					if _, loaded := s.managedSharedDBMetadataJobs.LoadOrStore(row.ID, struct{}{}); loaded {
						continue
					}
					known[row.ID] = struct{}{}
					owned = append(owned, row)
					queue = append(queue, row)
				}
			}
			out = takeLocked(limit)
		}); err != nil {
			return nil
		}
		return out
	}
	workerCount := min(workers, (len(owned)+batchSize-1)/batchSize)
	deadline := time.Now().Add(schemaInitRetryWindow)
	var wg sync.WaitGroup
	for range workerCount {
		group := take(batchSize)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.managedSharedDBMetadataSlots != nil {
				select {
				case s.managedSharedDBMetadataSlots <- struct{}{}:
					defer func() { <-s.managedSharedDBMetadataSlots }()
				case <-ctx.Done():
					return
				}
			}
			attempt := 1
			for len(group) > 0 && time.Now().Before(deadline) {
				requests := make([]tenant.SharedDBPoolLoadRequest, 0, len(group))
				byID := make(map[int64]*meta.SharedDB, len(group))
				for _, row := range group {
					requests = append(requests, tenant.SharedDBPoolLoadRequest{DBPoolID: row.ID, DBPoolUUID: row.UUID, ClusterID: row.ClusterID})
					byID[row.ID] = row
				}
				infos, loadErr := loader.BatchLoadSharedDBPoolsWithCredentials(ctx, requests, cred)
				ready := make(map[int64]struct{}, len(infos))
				readyIDs := make([]int64, 0, len(infos))
				for _, info := range infos {
					if info == nil {
						continue
					}
					row := byID[info.DBPoolID]
					if row == nil {
						continue
					}
					if strings.TrimSpace(info.Host) == "" || info.Port <= 0 || strings.TrimSpace(info.Username) == "" {
						// Seeing the same incomplete Cloud object is not durable
						// lifecycle progress. Preserve updated_at so the stuck
						// watchdog can expire a pool whose metadata never becomes ready.
						continue
					}
					name := info.DBName
					if name == "" {
						name = row.Name
					}
					if err := s.meta.UpdateManagedSharedDBPoolCloudResult(ctx, &meta.SharedDB{ID: row.ID, TiDBCloudOrganizationID: row.TiDBCloudOrganizationID, ClusterID: info.ClusterID, Host: info.Host, Port: info.Port, User: info.Username, PasswordCipher: row.PasswordCipher, Name: name, TLSMode: row.TLSMode}); err != nil {
						logger.Warn(ctx, "managed_shared_db_pool_metadata_persist_failed", zap.Int64("db_pool_id", row.ID), zap.Error(err))
						continue
					}
					ready[row.ID] = struct{}{}
					readyIDs = append(readyIDs, row.ID)
				}
				if loadErr != nil {
					logger.Warn(ctx, "managed_shared_db_pool_metadata_retry", zap.Int("attempt", attempt), zap.Int("db_pool_count", len(group)), zap.Duration("retry_in", pollInterval), zap.Error(loadErr))
				}
				if len(ready) > 0 {
					remaining := group[:0]
					for _, row := range group {
						if _, ok := ready[row.ID]; !ok {
							remaining = append(remaining, row)
						}
					}
					group = remaining
					group = append(group, take(len(ready))...)
					if onReady != nil {
						onReady(readyIDs)
					}
				}
				if len(group) == 0 {
					return
				}
				timer := time.NewTimer(min(pollInterval, time.Until(deadline)))
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				attempt++
			}
		}()
	}
	wg.Wait()
}

func (s *Server) resumeProvisioningManagedSharedDBPoolsWithCtx(ctx context.Context) {
	rows, err := s.nextManagedSharedDBStatusPage(ctx, meta.SharedDBStatusProvisioning, &s.managedSharedDBProvisioningResumeCursor, 1000)
	if err != nil {
		logger.Warn(ctx, "managed_shared_db_pool_resume_list_failed", zap.String("status", meta.SharedDBStatusProvisioning), zap.Error(err))
		return
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	s.runManagedSharedDBProvisioning(ctx, ids)
}

func (s *Server) resumeActiveManagedSharedDBTenantsWithCtx(ctx context.Context) {
	rows, err := s.meta.ListActiveSharedDBsWithProvisioningTenantsAfter(ctx, s.managedSharedDBActivationResumeCursor, 1000)
	if err != nil {
		logger.Warn(ctx, "managed_shared_db_pool_activation_resume_list_failed", zap.Error(err))
		return
	}
	if len(rows) == 0 && s.managedSharedDBActivationResumeCursor > 0 {
		s.managedSharedDBActivationResumeCursor = 0
		rows, err = s.meta.ListActiveSharedDBsWithProvisioningTenantsAfter(ctx, 0, 1000)
		if err != nil {
			logger.Warn(ctx, "managed_shared_db_pool_activation_resume_list_failed", zap.Error(err))
			return
		}
	}
	if len(rows) > 0 && len(rows) < 1000 {
		s.managedSharedDBActivationResumeCursor = 0
	} else if len(rows) > 0 {
		s.managedSharedDBActivationResumeCursor = rows[len(rows)-1].ID
	}
	for _, row := range rows {
		for {
			activated, activateErr := s.meta.ActivateSharedTenantsBatch(ctx, row.ID, sharedTenantActivationBatchSize)
			if activateErr != nil {
				logger.Warn(ctx, "managed_shared_db_pool_activation_resume_failed",
					zap.Int64("db_pool_id", row.ID), zap.String("db_pool_uuid", row.UUID), zap.Error(activateErr))
				break
			}
			if activated < sharedTenantActivationBatchSize {
				break
			}
		}
	}
}

func (s *Server) resumePendingManagedSharedDBPoolsWithCtx(ctx context.Context) {
	rows, err := s.nextManagedSharedDBStatusPage(ctx, meta.SharedDBStatusPending, &s.managedSharedDBPendingResumeCursor, 1000)
	if err != nil {
		logger.Warn(ctx, "managed_shared_db_pool_resume_list_failed", zap.String("status", meta.SharedDBStatusPending), zap.Error(err))
		return
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	s.runManagedSharedDBContinuations(ctx, ids)
}

func (s *Server) continueManagedSharedDBPool(ctx context.Context, dbID int64) error {
	for metadataAttempt := 0; metadataAttempt < 2; metadataAttempt++ {
		err := s.continueManagedSharedDBPoolOnce(ctx, dbID)
		if !errors.Is(err, errSharedDBConnectionMetadataNotReady) {
			return err
		}
		waiter, ok := s.provisioner.(tenant.SharedDBPoolMetadataWaiter)
		if !ok {
			return err
		}
		poolInfo, loadErr := s.meta.GetSharedDB(ctx, dbID)
		if loadErr != nil {
			return loadErr
		}
		if poolInfo.ClusterID == "" {
			return err
		}
		cred, credErr := s.sharedDBCloudCredentials()
		if credErr != nil {
			return credErr
		}
		if _, waitErr := waiter.WaitForSharedDBPoolMetadataWithCredentials(ctx, dbID, poolInfo.UUID, poolInfo.ClusterID, cred); waitErr != nil {
			return waitErr
		}
	}
	return fmt.Errorf("%w: db pool %d", errSharedDBConnectionMetadataNotReady, dbID)
}

func (s *Server) continueManagedSharedDBPoolOnce(ctx context.Context, dbID int64) error {
	for attempt := 0; attempt < 2; attempt++ {
		poolInfo, err := s.meta.GetSharedDB(ctx, dbID)
		if err != nil {
			return err
		}
		cred, err := s.sharedDBCloudCredentials()
		if err != nil {
			return err
		}
		restartWithOrganization := false
		continuePool := func(lockCtx context.Context) error {
			current, loadErr := s.meta.GetSharedDB(lockCtx, dbID)
			if loadErr != nil {
				return loadErr
			}
			if strings.TrimSpace(poolInfo.TiDBCloudOrganizationID) != strings.TrimSpace(current.TiDBCloudOrganizationID) {
				restartWithOrganization = true
				return nil
			}
			return s.continueManagedSharedDBPoolLocked(lockCtx, current, cred)
		}
		err = s.withSharedDBPoolWorkLock(ctx, dbID, continuePool)
		if err != nil {
			return err
		}
		if !restartWithOrganization {
			return nil
		}
	}
	return fmt.Errorf("db pool %d allocation identity kept changing", dbID)
}

// ensureManagedSharedDBPhysicalLocked performs only the physical create/adopt
// stage. It deliberately does not wait for endpoint readiness, system-user
// setup, schema initialization, or activation so direct requests can fall back
// to an existing active pool when Cloud create fails.
func (s *Server) ensureManagedSharedDBPhysicalLocked(ctx context.Context, poolInfo *meta.SharedDB, cred tenant.CredentialProvisionRequest) (*meta.SharedDB, error) {
	if poolInfo == nil {
		return nil, fmt.Errorf("managed shared db pool is required")
	}
	switch poolInfo.Status {
	case meta.SharedDBStatusFailed, meta.SharedDBStatusDraining:
		return nil, fmt.Errorf("managed db pool %d is not creatable in status %q: %w", poolInfo.ID, poolInfo.Status, meta.ErrNotFound)
	case meta.SharedDBStatusPending, meta.SharedDBStatusProvisioning, meta.SharedDBStatusActive:
	default:
		return nil, fmt.Errorf("managed db pool %d has unsupported status %q", poolInfo.ID, poolInfo.Status)
	}
	if poolInfo.ClusterID != "" && poolInfo.Host != "" && poolInfo.Port > 0 && poolInfo.User != "" {
		return poolInfo, nil
	}
	provisioner, ok := s.provisioner.(tenant.SharedDBPoolProvisioner)
	if !ok {
		return nil, fmt.Errorf("provisioner does not support managed shared db pools")
	}
	if len(poolInfo.PasswordCipher) == 0 || poolInfo.Name == "" {
		rootPassword, err := generateManagedSharedDBRootPassword(24)
		if err != nil {
			return nil, err
		}
		passwordCipher, err := s.pool.Encrypt(ctx, []byte(rootPassword))
		if err != nil {
			return nil, fmt.Errorf("encrypt shared db root password: %w", err)
		}
		if err := s.meta.PrepareManagedSharedDBPoolRoot(ctx, poolInfo.ID, passwordCipher, s.defaultSharedDatabaseName()); err != nil {
			return nil, err
		}
		poolInfo, err = s.meta.GetSharedDB(ctx, poolInfo.ID)
		if err != nil {
			return nil, err
		}
	}
	plainRootPassword, err := s.pool.Decrypt(ctx, poolInfo.PasswordCipher)
	if err != nil {
		return nil, fmt.Errorf("decrypt shared db root password: %w", err)
	}
	if poolInfo.SpendingLimit == nil {
		return nil, fmt.Errorf("managed db pool %d has no spending target", poolInfo.ID)
	}
	result, err := provisioner.LoadSharedDBPoolWithCredentials(ctx, poolInfo.ID, poolInfo.UUID, poolInfo.ClusterID, cred)
	if err != nil {
		if errors.Is(err, tenant.ErrSharedDBPoolAmbiguous) {
			if markErr := s.meta.MarkSharedDBPoolFailed(ctx, poolInfo.ID); markErr != nil {
				return nil, errors.Join(err, markErr)
			}
		}
		return nil, err
	}
	var provisionErr error
	if result == nil {
		if poolInfo.ClusterID != "" {
			return nil, fmt.Errorf("managed shared cluster %q was not found", poolInfo.ClusterID)
		}
		results, createErr := provisioner.BatchProvisionSharedDBPoolsWithCredentials(ctx, []tenant.SharedDBPoolCreateRequest{{
			DBPoolID: poolInfo.ID, DBPoolUUID: poolInfo.UUID,
			CustomerOrganizationID: strings.TrimSpace(poolInfo.TiDBCloudOrganizationID), DatabaseName: poolInfo.Name,
			RootPassword: string(plainRootPassword), SpendingLimitMonthly: *poolInfo.SpendingLimit,
		}}, cred)
		provisionErr = createErr
		if createErr != nil && len(results) == 0 {
			return nil, createErr
		}
		if len(results) != 1 || results[0] == nil || results[0].DBPoolID != poolInfo.ID || results[0].DBPoolUUID != poolInfo.UUID {
			return nil, fmt.Errorf("shared db pool create returned no unique result for %d", poolInfo.ID)
		}
		result = results[0]
	}
	logicalOrganizationID := strings.TrimSpace(poolInfo.TiDBCloudOrganizationID)
	if logicalOrganizationID == "" {
		return nil, fmt.Errorf("managed shared db pool customer organization is required")
	}
	if result.DBName == "" {
		result.DBName = poolInfo.Name
	}
	tlsMode := sharedDBTLSMode()
	if err := s.meta.UpdateManagedSharedDBPoolCloudResult(ctx, &meta.SharedDB{
		ID: poolInfo.ID, TiDBCloudOrganizationID: logicalOrganizationID, ClusterID: result.ClusterID,
		Host: result.Host, Port: result.Port, User: result.Username, PasswordCipher: poolInfo.PasswordCipher,
		Name: result.DBName, TLSMode: tlsMode,
	}); err != nil {
		return nil, err
	}
	poolInfo, err = s.meta.GetSharedDB(ctx, poolInfo.ID)
	if err != nil {
		return nil, err
	}
	if provisionErr != nil {
		return nil, provisionErr
	}
	return poolInfo, nil
}

func (s *Server) ensureManagedSharedDBPhysical(ctx context.Context, dbID int64) (*meta.SharedDB, error) {
	for attempt := 0; attempt < 2; attempt++ {
		poolInfo, err := s.meta.GetSharedDB(ctx, dbID)
		if err != nil {
			return nil, err
		}
		cred, err := s.sharedDBCloudCredentials()
		if err != nil {
			return nil, err
		}
		var result *meta.SharedDB
		restartWithOrganization := false
		ensurePool := func(lockCtx context.Context) error {
			current, loadErr := s.meta.GetSharedDB(lockCtx, dbID)
			if loadErr != nil {
				return loadErr
			}
			if strings.TrimSpace(poolInfo.TiDBCloudOrganizationID) != strings.TrimSpace(current.TiDBCloudOrganizationID) {
				restartWithOrganization = true
				return nil
			}
			var ensureErr error
			result, ensureErr = s.ensureManagedSharedDBPhysicalLocked(lockCtx, current, cred)
			return ensureErr
		}
		err = s.withSharedDBPoolWorkLock(ctx, dbID, ensurePool)
		if err != nil {
			return nil, err
		}
		if !restartWithOrganization {
			return result, nil
		}
	}
	return nil, fmt.Errorf("db pool %d allocation identity kept changing", dbID)
}

// continueManagedSharedDBPoolLocked keeps one per-pool lock through physical
// ensure, schema ensure, and activation. It is the same work lock used by the
// batch create path, preventing duplicate physical creation and overlapping
// schema attempts without serializing unrelated pools in one organization.
func (s *Server) continueManagedSharedDBPoolLocked(ctx context.Context, poolInfo *meta.SharedDB, cred tenant.CredentialProvisionRequest) error {
	dbID := poolInfo.ID
	var err error
	switch poolInfo.Status {
	case meta.SharedDBStatusActive, meta.SharedDBStatusFailed, meta.SharedDBStatusDraining:
		return nil
	case meta.SharedDBStatusPending, meta.SharedDBStatusProvisioning:
	default:
		return fmt.Errorf("managed db pool %d has unsupported status %q", dbID, poolInfo.Status)
	}
	provisioner, ok := s.provisioner.(tenant.SharedDBPoolProvisioner)
	if !ok {
		return fmt.Errorf("provisioner does not support managed shared db pools")
	}
	if len(poolInfo.PasswordCipher) == 0 || poolInfo.Name == "" {
		rootPassword, passwordErr := generateManagedSharedDBRootPassword(24)
		if passwordErr != nil {
			return passwordErr
		}
		passwordCipher, encryptErr := s.pool.Encrypt(ctx, []byte(rootPassword))
		if encryptErr != nil {
			return fmt.Errorf("encrypt shared db root password: %w", encryptErr)
		}
		if err := s.meta.PrepareManagedSharedDBPoolRoot(ctx, dbID, passwordCipher, s.defaultSharedDatabaseName()); err != nil {
			return err
		}
		poolInfo, err = s.meta.GetSharedDB(ctx, dbID)
		if err != nil {
			return err
		}
	}
	plainRootPassword, err := s.pool.Decrypt(ctx, poolInfo.PasswordCipher)
	if err != nil {
		return fmt.Errorf("decrypt shared db root password: %w", err)
	}
	needsMetadata := poolInfo.ClusterID == "" || poolInfo.Host == "" || poolInfo.Port <= 0 || poolInfo.User == ""
	if poolInfo.Status == meta.SharedDBStatusProvisioning && needsMetadata {
		return fmt.Errorf("provisioning db pool %d has incomplete connection metadata", dbID)
	}
	if needsMetadata {
		if poolInfo.SpendingLimit == nil {
			return fmt.Errorf("managed db pool %d has no spending target", dbID)
		}
		result, loadErr := provisioner.LoadSharedDBPoolWithCredentials(ctx, dbID, poolInfo.UUID, poolInfo.ClusterID, cred)
		if loadErr != nil {
			if errors.Is(loadErr, tenant.ErrSharedDBPoolAmbiguous) {
				if markErr := s.meta.MarkSharedDBPoolFailed(ctx, dbID); markErr != nil {
					return errors.Join(loadErr, markErr)
				}
			}
			return loadErr
		}
		var provisionErr error
		if result == nil {
			if poolInfo.ClusterID != "" {
				return fmt.Errorf("managed shared cluster %q was not found", poolInfo.ClusterID)
			}
			results, createErr := provisioner.BatchProvisionSharedDBPoolsWithCredentials(ctx, []tenant.SharedDBPoolCreateRequest{{
				DBPoolID: dbID, DBPoolUUID: poolInfo.UUID,
				CustomerOrganizationID: strings.TrimSpace(poolInfo.TiDBCloudOrganizationID), DatabaseName: poolInfo.Name,
				RootPassword: string(plainRootPassword), SpendingLimitMonthly: *poolInfo.SpendingLimit,
			}}, cred)
			provisionErr = createErr
			if createErr != nil && len(results) == 0 {
				return createErr
			}
			if len(results) != 1 || results[0] == nil || results[0].DBPoolID != dbID || results[0].DBPoolUUID != poolInfo.UUID {
				return fmt.Errorf("shared db pool create returned no unique result for %d", dbID)
			}
			result = results[0]
		}
		logicalOrganizationID := strings.TrimSpace(poolInfo.TiDBCloudOrganizationID)
		if logicalOrganizationID == "" {
			return fmt.Errorf("managed shared db pool customer organization is required")
		}
		if result.DBName == "" {
			result.DBName = poolInfo.Name
		}
		tlsMode := sharedDBTLSMode()
		if err := s.meta.UpdateManagedSharedDBPoolCloudResult(ctx, &meta.SharedDB{
			ID: dbID, TiDBCloudOrganizationID: logicalOrganizationID, ClusterID: result.ClusterID,
			Host: result.Host, Port: result.Port, User: result.Username, PasswordCipher: poolInfo.PasswordCipher,
			Name: result.DBName, TLSMode: tlsMode,
		}); err != nil {
			return err
		}
		poolInfo, err = s.meta.GetSharedDB(ctx, dbID)
		if err != nil {
			return err
		}
		if provisionErr != nil {
			return provisionErr
		}
	}
	if poolInfo.Host == "" || poolInfo.Port <= 0 || poolInfo.User == "" || len(poolInfo.PasswordCipher) == 0 || poolInfo.Name == "" {
		return fmt.Errorf("%w: db pool %d", errSharedDBConnectionMetadataNotReady, dbID)
	}
	plain, err := s.pool.Decrypt(ctx, poolInfo.PasswordCipher)
	if err != nil {
		return fmt.Errorf("decrypt shared db root password: %w", err)
	}
	dsn := managedSharedDBDSN(poolInfo, string(plain))
	if ensurer, ok := s.provisioner.(tenantDatabaseEnsurer); ok {
		if err := ensurer.EnsureDatabase(ctx, dsn); err != nil {
			return err
		}
	}
	if err := s.pool.EnsureSharedDBReady(ctx, dbID); err != nil {
		return err
	}
	if updater, ok := s.provisioner.(tenant.QuotaUpdater); ok && poolInfo.SpendingLimit != nil {
		_, patchErr := updater.UpdateQuota(ctx, &tenant.ClusterInfo{
			ClusterID: poolInfo.ClusterID, OrganizationID: poolInfo.TiDBCloudOrganizationID,
			Provider: tenant.ProviderTiDBCloudNative,
		}, cred, tenant.QuotaUpdateOptions{TiDBCloudSpendingLimitMonthly: poolInfo.SpendingLimit})
		if patchErr != nil {
			logger.Warn(ctx, "managed_shared_db_spending_limit_sync_failed",
				zap.Int64("db_pool_id", dbID),
				zap.String("db_pool_uuid", poolInfo.UUID),
				zap.String("tidbcloud_org_id", poolInfo.TiDBCloudOrganizationID),
				zap.Error(patchErr))
		}
	}
	if err := s.meta.ActivateSharedDBPool(ctx, dbID); err != nil {
		return err
	}
	for {
		activated, activateErr := s.meta.ActivateSharedTenantsBatch(ctx, dbID, sharedTenantActivationBatchSize)
		if activateErr != nil {
			return activateErr
		}
		if activated < sharedTenantActivationBatchSize {
			return nil
		}
	}
}
