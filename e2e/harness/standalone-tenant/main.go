// Command standalone-tenant registers an existing (standalone) tenant directly
// in the meta DB and issues an owner API key for it, bypassing cluster
// provisioning. It exists so e2e scripts can simulate a pre-existing
// standalone tenant coexisting with shared-pool tenants.
package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

func main() {
	var (
		metaDSN    = flag.String("meta-dsn", "", "control-plane meta DSN (required)")
		masterHex  = flag.String("master-key", "", "hex master key for local_aes encryption (required)")
		tokenHex   = flag.String("token-secret", "", "hex token signing key (required)")
		tenantID   = flag.String("tenant-id", "", "tenant UUID to register (required)")
		dbHost     = flag.String("db-host", "127.0.0.1", "standalone tenant DB host")
		dbPort     = flag.Int("db-port", 4000, "standalone tenant DB port")
		dbUser     = flag.String("db-user", "root", "standalone tenant DB user")
		dbPassword = flag.String("db-password", "", "standalone tenant DB password (plaintext, encrypted before persist)")
		dbName     = flag.String("db-name", "", "standalone tenant DB name (required)")
		dbTLS      = flag.Bool("db-tls", false, "standalone tenant DB TLS")
		provider   = flag.String("provider", "tidb_zero", "provider string recorded on the tenant row")
		clusterID  = flag.String("cluster-id", "", "cluster id recorded on the tenant row (optional)")
		keyName    = flag.String("key-name", "default", "API key name")
		skipEnsure = flag.Bool("skip-ensure", false, "init the standalone DB with the base (no FTS/vector) schema and stamp the tenant schema_version so Acquire-time ensure is skipped; for engines without FTS/vector support such as self-hosted TiDB")
	)
	flag.Parse()
	if *metaDSN == "" || *masterHex == "" || *tokenHex == "" || *tenantID == "" || *dbName == "" {
		fmt.Fprintln(os.Stderr, "required: -meta-dsn -master-key -token-secret -tenant-id -db-name")
		os.Exit(2)
	}
	ctx := context.Background()

	store, err := meta.OpenContext(ctx, *metaDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open meta store:", err)
		os.Exit(1)
	}
	defer func() { _ = store.Close() }()

	masterKey, err := hex.DecodeString(*masterHex)
	if err != nil {
		fmt.Fprintln(os.Stderr, "decode master key:", err)
		os.Exit(1)
	}
	enc, err := encrypt.New(ctx, encrypt.Config{Type: encrypt.TypeLocalAES, Key: hex.EncodeToString(masterKey)})
	if err != nil {
		fmt.Fprintln(os.Stderr, "create encryptor:", err)
		os.Exit(1)
	}
	passCipher, err := enc.Encrypt(ctx, []byte(*dbPassword))
	if err != nil {
		fmt.Fprintln(os.Stderr, "encrypt db password:", err)
		os.Exit(1)
	}

	now := time.Now().UTC()
	if err := store.InsertTenant(ctx, &meta.Tenant{
		ID:               *tenantID,
		Status:           meta.TenantActive,
		Kind:             meta.TenantKindLive,
		DBHost:           *dbHost,
		DBPort:           *dbPort,
		DBUser:           *dbUser,
		DBPasswordCipher: passCipher,
		DBName:           *dbName,
		DBTLS:            *dbTLS,
		Provider:         *provider,
		ClusterID:        *clusterID,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "insert tenant:", err)
		os.Exit(1)
	}

	if *skipEnsure {
		if err := initStandaloneBaseSchema(ctx, store, *tenantID, *dbUser, *dbPassword, *dbHost, *dbPort, *dbName, *dbTLS); err != nil {
			fmt.Fprintln(os.Stderr, "init standalone base schema:", err)
			os.Exit(1)
		}
	}

	tokenSecret, err := hex.DecodeString(*tokenHex)
	if err != nil {
		fmt.Fprintln(os.Stderr, "decode token secret:", err)
		os.Exit(1)
	}
	rawToken, err := token.IssueTokenWithJournalPermissions(tokenSecret, *tenantID, 1, time.Time{}, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "issue token:", err)
		os.Exit(1)
	}
	cipherToken, err := enc.Encrypt(ctx, []byte(rawToken))
	if err != nil {
		fmt.Fprintln(os.Stderr, "encrypt token:", err)
		os.Exit(1)
	}
	if err := store.InsertAPIKey(ctx, &meta.APIKey{
		ID:            token.NewID(),
		TenantID:      *tenantID,
		KeyName:       *keyName,
		JWTCiphertext: cipherToken,
		JWTHash:       token.HashToken(rawToken),
		TokenVersion:  1,
		Status:        meta.APIKeyActive,
		ScopeKind:     meta.APIKeyScopeKindOwner,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "insert api key:", err)
		os.Exit(1)
	}
	// stdout carries only the raw API key; everything else goes to stderr.
	fmt.Println(rawToken)
}

// initStandaloneBaseSchema applies the base (no FTS/vector) tenant schema to
// the standalone DB and stamps the tenant's schema_version with the fts_only
// target version, so the pool's Acquire-time ensure — which would require
// FTS/vector indexes this engine cannot build — is skipped entirely.
func initStandaloneBaseSchema(ctx context.Context, store *meta.Store, tenantID, user, pass, host string, port int, dbName string, tls bool) error {
	query := "parseTime=true"
	if tls {
		query += "&tls=true"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", user, pass, host, port, dbName, query)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	for _, stmt := range schema.MySQLNoEmbeddingTenantSchemaStatements() {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			msg := err.Error()
			if strings.Contains(msg, "Duplicate key name") || strings.Contains(msg, "already exists") || strings.Contains(msg, "Duplicate column") {
				continue
			}
			return fmt.Errorf("apply base schema: %w", err)
		}
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO tenant_auto_embedding_profiles (tenant_id) VALUES (?) ON DUPLICATE KEY UPDATE tenant_id = tenant_id`, tenantID); err != nil {
		return fmt.Errorf("ensure embedding profile: %w", err)
	}
	profile, err := store.GetTenantAutoEmbeddingProfile(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("load embedding profile: %w", err)
	}
	version, err := schema.TiDBTenantSchemaVersionForFTSOnlyProfile(schema.TiDBAutoEmbeddingProfile{
		Model:       profile.Model,
		Dimensions:  profile.Dimensions,
		OptionsJSON: profile.OptionsJSON,
	})
	if err != nil {
		return fmt.Errorf("compute fts_only schema version: %w", err)
	}
	if err := store.UpdateTenantSchemaVersion(ctx, tenantID, version); err != nil {
		return fmt.Errorf("stamp schema version: %w", err)
	}
	return nil
}
