package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
)

// runBackfillQuota bootstraps the central server DB quota counters and
// tenant_file_meta rows from each tenant's local database. This is a one-time
// migration tool to be run before switching quota_source to "server".
//
// Environment:
//
//	DRIVE9_META_DSN      — central server DB DSN (required)
//	DRIVE9_MASTER_KEY    — AES master key hex for decrypting tenant DB passwords (required unless KMS)
//	DRIVE9_ENCRYPT_TYPE  — "local_aes" (default) or "kms"
//	DRIVE9_ENCRYPT_KEY   — KMS key ARN (when encrypt type is kms)
//	DRIVE9_S3_REGION     — AWS region for KMS (default us-east-1)
func runBackfillQuota(args []string) error {
	dryRun := false
	for _, arg := range args {
		switch arg {
		case "--dry-run":
			dryRun = true
		default:
			return fmt.Errorf("unknown flag %q; usage: drive9-server backfill-quota [--dry-run]", arg)
		}
	}

	ctx := context.Background()

	metaDSN := os.Getenv("DRIVE9_META_DSN")
	if metaDSN == "" {
		return fmt.Errorf("DRIVE9_META_DSN is required")
	}

	store, err := meta.Open(metaDSN)
	if err != nil {
		return fmt.Errorf("open meta store: %w", err)
	}
	defer func() { _ = store.Close() }()

	enc, err := buildEncryptor()
	if err != nil {
		return fmt.Errorf("create encryptor: %w", err)
	}

	tenants, err := store.ListTenantsByStatus(ctx, meta.TenantActive, 10000)
	if err != nil {
		return fmt.Errorf("list active tenants: %w", err)
	}
	logger.Info(ctx, "backfill_quota_start",
		zap.Int("tenant_count", len(tenants)),
		zap.Bool("dry_run", dryRun))

	var totalTenants, totalFiles, totalErrors int
	for i, t := range tenants {
		tenantStart := time.Now()
		stats, err := backfillTenant(ctx, store, enc, &t, dryRun)
		elapsed := time.Since(tenantStart)
		if err != nil {
			totalErrors++
			logger.Error(ctx, "backfill_quota_tenant_error",
				zap.String("tenant_id", t.ID),
				zap.Int("index", i+1),
				zap.Int("total", len(tenants)),
				zap.Error(err))
			continue
		}
		totalTenants++
		totalFiles += stats.files
		logger.Info(ctx, "backfill_quota_tenant_done",
			zap.String("tenant_id", t.ID),
			zap.Int("index", i+1),
			zap.Int("total", len(tenants)),
			zap.Int("files", stats.files),
			zap.Int64("storage_bytes", stats.storageBytes),
			zap.Int64("media_files", stats.mediaFiles),
			zap.Duration("elapsed", elapsed),
			zap.Bool("dry_run", dryRun))
	}

	logger.Info(ctx, "backfill_quota_complete",
		zap.Int("tenants_ok", totalTenants),
		zap.Int("tenants_error", totalErrors),
		zap.Int("total_files", totalFiles),
		zap.Bool("dry_run", dryRun))
	return nil
}

type backfillStats struct {
	files        int
	storageBytes int64
	mediaFiles   int64
}

func backfillTenant(ctx context.Context, metaStore *meta.Store, enc encrypt.Encryptor, t *meta.Tenant, dryRun bool) (*backfillStats, error) {
	pass, err := enc.Decrypt(ctx, t.DBPasswordCipher)
	if err != nil {
		return nil, fmt.Errorf("decrypt db password: %w", err)
	}
	query := "parseTime=true"
	if t.DBTLS {
		query += "&tls=true"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", t.DBUser, string(pass), t.DBHost, t.DBPort, t.DBName, query)
	tenantStore, err := datastore.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open tenant datastore: %w", err)
	}
	defer func() { _ = tenantStore.Close() }()

	var stats backfillStats
	cursor := ""
	for {
		files, next, err := tenantStore.ListConfirmedFileSummaries(ctx, cursor, 500)
		if err != nil {
			return nil, fmt.Errorf("list confirmed files (cursor=%q): %w", cursor, err)
		}
		for _, f := range files {
			stats.files++
			stats.storageBytes += f.SizeBytes
			isMedia := isMediaContentType(f.ContentType)
			if isMedia {
				stats.mediaFiles++
			}
			if !dryRun {
				if err := metaStore.UpsertFileMeta(ctx, &meta.FileMeta{
					TenantID:  t.ID,
					FileID:    f.FileID,
					SizeBytes: f.SizeBytes,
					IsMedia:   isMedia,
				}); err != nil {
					return nil, fmt.Errorf("upsert file meta %s: %w", f.FileID, err)
				}
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}

	// Set the aggregate counters.
	if !dryRun {
		if err := metaStore.SetQuotaCounters(ctx, t.ID, stats.storageBytes, stats.mediaFiles); err != nil {
			return nil, fmt.Errorf("set quota counters: %w", err)
		}
	}

	return &stats, nil
}

func isMediaContentType(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(ct))
	return strings.HasPrefix(ct, "image/") || strings.HasPrefix(ct, "audio/")
}

func buildEncryptor() (encrypt.Encryptor, error) {
	encryptType := envOr("DRIVE9_ENCRYPT_TYPE", "local_aes")
	masterHex := os.Getenv("DRIVE9_MASTER_KEY")
	kmsKey := os.Getenv("DRIVE9_ENCRYPT_KEY")

	eKey := masterHex
	eType := encrypt.Type(encryptType)
	if eType == encrypt.TypeKMS {
		eKey = kmsKey
	}
	if eKey == "" && eType != encrypt.TypeKMS {
		// Attempt hex decode — if DRIVE9_MASTER_KEY is not set, fail early.
		return nil, fmt.Errorf("DRIVE9_MASTER_KEY is required for encrypting/decrypting tenant DB passwords")
	}
	if eType != encrypt.TypeKMS {
		if _, err := hex.DecodeString(eKey); err != nil {
			return nil, fmt.Errorf("invalid DRIVE9_MASTER_KEY hex: %w", err)
		}
	}
	return encrypt.New(context.Background(), encrypt.Config{
		Type:   eType,
		Key:    eKey,
		Region: envOr("DRIVE9_S3_REGION", "us-east-1"),
	})
}
