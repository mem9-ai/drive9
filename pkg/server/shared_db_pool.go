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
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"go.uber.org/zap"
)

const (
	defaultManagedSharedDBMaxTenants    = 100
	defaultManagedSharedDBSpendingLimit = int64(10_000_000)
	defaultSharedTenantSpendingLimit    = int64(1000)
	sharedTenantActivationBatchSize     = 100
)

var errSharedDBConnectionMetadataNotReady = errors.New("shared DB pool connection metadata is not ready")

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
	spendingLimit = s.sharedDBDefaultSpendingLimit
	if spendingLimit <= 0 {
		spendingLimit = defaultManagedSharedDBSpendingLimit
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

func managedSharedDBDSN(info *meta.SharedDB, password string) string {
	query := "parseTime=true"
	if info.TLSMode != "" {
		query += "&tls=" + info.TLSMode
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", info.User, password, info.Host, info.Port, info.Name, query)
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
	virtualLimit := s.sharedTenantVirtualSpendingLimit(opts)
	if opts.Quota != nil {
		req := *opts.Quota
		req.TenantID = tenantID
		if err := s.validateQuotaSetRequest(req); err != nil {
			return err
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
	if opts.Quota != nil {
		if opts.Quota.MaxStorageSize != nil {
			cfg.MaxStorageBytes = *opts.Quota.MaxStorageSize
		}
		if opts.Quota.MaxFileSize != nil {
			cfg.MaxFileSizeBytes = *opts.Quota.MaxFileSize
		}
		if opts.Quota.MaxFileCount != nil {
			cfg.MaxFileCount = *opts.Quota.MaxFileCount
		}
	}
	if err := s.meta.SetQuotaConfig(ctx, cfg); err != nil {
		return fmt.Errorf("materialize shared tenant quota: %w", err)
	}
	return nil
}

func (s *Server) allocateManagedSharedDB(ctx context.Context, cred tenant.CredentialProvisionRequest, tenantSpendingLimit int64, reserve func(*meta.SharedDB) error) (sharedDB *meta.SharedDB, created bool, err error) {
	organizationID, resolveErr := s.firstManagedOrganization(ctx, cred)
	if resolveErr != nil {
		return nil, false, fmt.Errorf("resolve tidbcloud organization: %w", resolveErr)
	}
	provisioningKey := sharedDBProvisioningKey(cred)
	identity := sharedDBAllocationIdentity(organizationID, provisioningKey)
	err = s.meta.WithSharedDBAllocationLock(ctx, identity, func(lockCtx context.Context) error {
		for attempt := 0; attempt < 2; attempt++ {
			candidate, findErr := s.meta.FindSharedDBForAllocation(lockCtx, organizationID, provisioningKey, tenantSpendingLimit)
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
				maxTenants, configuredFloor := s.managedSharedDBPolicy()
				fixedTarget := configuredFloor
				if tenantSpendingLimit >= maxInt32 {
					return meta.ErrSharedDBSpendingLimitExceeded
				}
				if tenantSpendingLimit+1 > fixedTarget {
					fixedTarget = tenantSpendingLimit + 1
				}
				cloudProvider, region := provisioningCloudRegion(s.provisioner)
				rootPassword, passwordErr := generateManagedSharedDBRootPassword(24)
				if passwordErr != nil {
					return passwordErr
				}
				passwordCipher, encryptErr := s.pool.Encrypt(lockCtx, []byte(rootPassword))
				if encryptErr != nil {
					return fmt.Errorf("encrypt shared db root password: %w", encryptErr)
				}
				id, createErr := s.meta.CreateManagedSharedDBPool(lockCtx, &meta.SharedDB{
					TiDBCloudOrganizationID: organizationID,
					ProvisioningKey:         provisioningKey,
					CloudProvider:           cloudProvider,
					Region:                  region,
					MaxTenants:              maxTenants,
					SpendingLimit:           &fixedTarget,
					PasswordCipher:          passwordCipher,
					Name:                    s.defaultSharedDatabaseName(),
				})
				if createErr != nil {
					return createErr
				}
				sharedDB, createErr = s.meta.GetSharedDB(lockCtx, id)
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
			if !errors.Is(reserveErr, meta.ErrSharedDBCapacityExhausted) && !errors.Is(reserveErr, meta.ErrSharedDBSpendingLimitExceeded) {
				return reserveErr
			}
			sharedDB = nil
			created = false
		}
		return meta.ErrSharedDBCapacityExhausted
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

func (s *Server) scheduleManagedSharedDBContinuation(ctx context.Context, dbID int64, cred tenant.CredentialProvisionRequest) {
	s.scheduleManagedSharedDBContinuations(ctx, []int64{dbID}, cred)
}

// scheduleManagedSharedDBContinuations runs one request batch sequentially.
// Every continuation acquires the same organization-level named lock. Starting
// one goroutine per DB pool would let all waiters pin dedicated meta *sql.Conn
// values and can exhaust the connection pool before the lock holder finishes.
func (s *Server) scheduleManagedSharedDBContinuations(ctx context.Context, dbIDs []int64, cred tenant.CredentialProvisionRequest) {
	if len(dbIDs) == 0 {
		return
	}
	ids := append([]int64(nil), dbIDs...)
	slices.Sort(ids)
	s.startServerWorker(ctx, func(workerCtx context.Context) {
		for _, dbID := range ids {
			if workerCtx.Err() != nil {
				return
			}
			for attempt := 0; attempt < 2; attempt++ {
				err := s.continueManagedSharedDBPool(workerCtx, dbID, cred)
				if err == nil {
					break
				}
				if attempt == 1 {
					logger.Warn(workerCtx, "managed_shared_db_pool_continue_failed",
						zap.Int64("db_pool_id", dbID), zap.Error(err))
					break
				}
				timer := time.NewTimer(time.Second)
				select {
				case <-workerCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	})
}

func (s *Server) resumeManagedSharedDBPoolsWithCtx(ctx context.Context) {
	cred := resolveDefaultCredentials(s.provisioner)
	if cred == nil {
		return
	}
	rows, err := s.meta.ListSharedDBsByStatus(ctx, meta.SharedDBStatusProvisioning, 1000)
	if err != nil {
		logger.Warn(ctx, "managed_shared_db_pool_resume_list_failed", zap.Error(err))
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		if err := s.continueManagedSharedDBPool(ctx, row.ID, *cred); err != nil {
			logger.Warn(ctx, "managed_shared_db_pool_resume_failed",
				zap.Int64("db_pool_id", row.ID), zap.Error(err))
		}
	}
}

func (s *Server) continueManagedSharedDBPool(ctx context.Context, dbID int64, cred tenant.CredentialProvisionRequest) error {
	for metadataAttempt := 0; metadataAttempt < 2; metadataAttempt++ {
		err := s.continueManagedSharedDBPoolOnce(ctx, dbID, cred)
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
		if _, waitErr := waiter.WaitForSharedDBPoolMetadataWithCredentials(ctx, dbID, poolInfo.ClusterID, cred); waitErr != nil {
			return waitErr
		}
	}
	return fmt.Errorf("%w: db pool %d", errSharedDBConnectionMetadataNotReady, dbID)
}

func (s *Server) continueManagedSharedDBPoolOnce(ctx context.Context, dbID int64, cred tenant.CredentialProvisionRequest) error {
	for attempt := 0; attempt < 2; attempt++ {
		poolInfo, err := s.meta.GetSharedDB(ctx, dbID)
		if err != nil {
			return err
		}
		identity := sharedDBAllocationIdentity(poolInfo.TiDBCloudOrganizationID, poolInfo.ProvisioningKey)
		restartWithOrganization := false
		err = s.meta.WithSharedDBAllocationLock(ctx, identity, func(lockCtx context.Context) error {
			current, loadErr := s.meta.GetSharedDB(lockCtx, dbID)
			if loadErr != nil {
				return loadErr
			}
			if poolInfo.TiDBCloudOrganizationID == "" && current.TiDBCloudOrganizationID != "" {
				restartWithOrganization = true
				return nil
			}
			return s.continueManagedSharedDBPoolLocked(lockCtx, current, cred)
		})
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
	result, err := provisioner.LoadSharedDBPoolWithCredentials(ctx, poolInfo.ID, poolInfo.ClusterID, cred)
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
			DBPoolID: poolInfo.ID, DatabaseName: poolInfo.Name, RootPassword: string(plainRootPassword),
			SpendingLimitMonthly: *poolInfo.SpendingLimit,
		}}, cred)
		provisionErr = createErr
		if createErr != nil && len(results) == 0 {
			return nil, createErr
		}
		if len(results) != 1 || results[0] == nil || results[0].DBPoolID != poolInfo.ID {
			return nil, fmt.Errorf("shared db pool create returned no unique result for %d", poolInfo.ID)
		}
		result = results[0]
	}
	if result.OrganizationID == "" {
		result.OrganizationID = poolInfo.TiDBCloudOrganizationID
	}
	if result.DBName == "" {
		result.DBName = poolInfo.Name
	}
	tlsMode := "true"
	if !dbTLSForProvisionedTenant(tenant.ProviderTiDBCloudNativeShared) {
		tlsMode = "skip-verify"
	}
	if err := s.meta.UpdateManagedSharedDBPoolCloudResult(ctx, &meta.SharedDB{
		ID: poolInfo.ID, TiDBCloudOrganizationID: result.OrganizationID, ClusterID: result.ClusterID,
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

func (s *Server) ensureManagedSharedDBPhysical(ctx context.Context, dbID int64, cred tenant.CredentialProvisionRequest) (*meta.SharedDB, error) {
	poolInfo, err := s.meta.GetSharedDB(ctx, dbID)
	if err != nil {
		return nil, err
	}
	identity := sharedDBAllocationIdentity(poolInfo.TiDBCloudOrganizationID, poolInfo.ProvisioningKey)
	var result *meta.SharedDB
	err = s.meta.WithSharedDBAllocationLock(ctx, identity, func(lockCtx context.Context) error {
		current, err := s.meta.GetSharedDB(lockCtx, dbID)
		if err != nil {
			return err
		}
		var ensureErr error
		result, ensureErr = s.ensureManagedSharedDBPhysicalLocked(lockCtx, current, cred)
		return ensureErr
	})
	return result, err
}

// continueManagedSharedDBPoolLocked intentionally keeps the organization
// allocation lock through physical ensure, system-user setup, schema ensure,
// and activation. Continuations are dispatched sequentially and are not a
// request-QPS path; the wider lock preserves the existing single-owner Cloud
// mutation model and prevents another allocator from observing half-ready
// connection metadata. The readiness poll is the one long wait kept outside.
func (s *Server) continueManagedSharedDBPoolLocked(ctx context.Context, poolInfo *meta.SharedDB, cred tenant.CredentialProvisionRequest) error {
	dbID := poolInfo.ID
	var err error
	if poolInfo.Status == meta.SharedDBStatusActive {
		return nil
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
	if needsMetadata {
		if poolInfo.SpendingLimit == nil {
			return fmt.Errorf("managed db pool %d has no spending target", dbID)
		}
		result, loadErr := provisioner.LoadSharedDBPoolWithCredentials(ctx, dbID, poolInfo.ClusterID, cred)
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
				DBPoolID: dbID, DatabaseName: poolInfo.Name, RootPassword: string(plainRootPassword),
				SpendingLimitMonthly: *poolInfo.SpendingLimit,
			}}, cred)
			provisionErr = createErr
			if createErr != nil && len(results) == 0 {
				return createErr
			}
			if len(results) != 1 || results[0] == nil || results[0].DBPoolID != dbID {
				return fmt.Errorf("shared db pool create returned no unique result for %d", dbID)
			}
			result = results[0]
		}
		if result.OrganizationID == "" {
			result.OrganizationID = poolInfo.TiDBCloudOrganizationID
		}
		if result.DBName == "" {
			result.DBName = poolInfo.Name
		}
		tlsMode := "true"
		if !dbTLSForProvisionedTenant(tenant.ProviderTiDBCloudNativeShared) {
			tlsMode = "skip-verify"
		}
		if err := s.meta.UpdateManagedSharedDBPoolCloudResult(ctx, &meta.SharedDB{
			ID: dbID, TiDBCloudOrganizationID: result.OrganizationID, ClusterID: result.ClusterID,
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
	if userProvisioner, ok := s.provisioner.(nativeSystemUserProvisioner); ok {
		username, password, ensureErr := userProvisioner.EnsureSystemUser(ctx, dsn, fmt.Sprintf("db-pool-%d", dbID))
		if ensureErr != nil {
			return ensureErr
		}
		passwordCipher, encryptErr := s.pool.Encrypt(ctx, []byte(password))
		if encryptErr != nil {
			return encryptErr
		}
		if err := s.meta.UpdateManagedSharedDBPoolCloudResult(ctx, &meta.SharedDB{
			ID: dbID, TiDBCloudOrganizationID: poolInfo.TiDBCloudOrganizationID, ClusterID: poolInfo.ClusterID,
			Host: poolInfo.Host, Port: poolInfo.Port, User: username, PasswordCipher: passwordCipher,
			Name: poolInfo.Name, TLSMode: poolInfo.TLSMode,
		}); err != nil {
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
				zap.Int64("db_pool_id", dbID), zap.Error(patchErr))
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
