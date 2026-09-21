package db9

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant/schema"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	db9TestDSNEnv       = "DRIVE9_TEST_DB9_DSN"
	db9TestImage        = "postgres:17-alpine"
	db9TestPort         = "5432/tcp"
	db9TestPassword     = "drive9-test"
	db9TestStartTimeout = 2 * time.Minute
)

// TestPromotionTerminalResultUpgradeRoundTrip exercises the PostgreSQL type
// conversion used for an already-bootstrapped DB9 tenant. The exact terminal
// result bytes are part of lost-response recovery: JSONB rewrites them, while
// the upgraded TEXT column must round-trip them unchanged in both the retained
// full import row and the compact tombstone.
//
// The pre-upgrade row intentionally remains a digest mismatch after the type
// conversion. The runtime validator must fail such an unreleased/corrupt row
// closed; a schema migration cannot reconstruct bytes JSONB already discarded.
func TestPromotionTerminalResultUpgradeRoundTrip(t *testing.T) {
	db := openPromotionPostgres(t)
	ctx := context.Background()

	for _, table := range []string{"promotion_import_tombstones", "promotion_imports"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
		if _, err := db.ExecContext(ctx, `CREATE TABLE `+table+` (
			id BIGSERIAL PRIMARY KEY,
			terminal_result_blob JSONB,
			terminal_result_digest VARCHAR(64)
		)`); err != nil {
			t.Fatalf("create legacy %s: %v", table, err)
		}
	}

	terminalResult := `{ "z": 2, "a": {"value": 1} }`
	wantDigest := promotionTerminalResultDigest(terminalResult)
	for _, table := range []string{"promotion_imports", "promotion_import_tombstones"} {
		if _, err := db.ExecContext(ctx, `INSERT INTO `+table+`
			(terminal_result_blob, terminal_result_digest) VALUES ($1, $2)`, terminalResult, wantDigest); err != nil {
			t.Fatalf("insert legacy %s result: %v", table, err)
		}
		var got string
		if err := db.QueryRowContext(ctx, `SELECT terminal_result_blob::text FROM `+table+` WHERE id = 1`).Scan(&got); err != nil {
			t.Fatalf("read legacy %s result: %v", table, err)
		}
		if got == terminalResult || promotionTerminalResultDigest(got) == wantDigest {
			t.Fatalf("legacy %s JSONB unexpectedly preserved exact terminal bytes: %q", table, got)
		}
	}

	upgrades := promotionTerminalResultUpgradeStatements(t)
	for _, stmt := range upgrades {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("execute DB9 terminal-result upgrade %q: %v", stmt, err)
		}
	}

	for _, table := range []string{"promotion_imports", "promotion_import_tombstones"} {
		var dataType string
		if err := db.QueryRowContext(ctx, `SELECT data_type FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = 'terminal_result_blob'`, table).
			Scan(&dataType); err != nil {
			t.Fatalf("read upgraded %s column type: %v", table, err)
		}
		if dataType != "text" {
			t.Fatalf("upgraded %s terminal_result_blob type = %q, want text", table, dataType)
		}

		var legacyBlob, legacyDigest string
		if err := db.QueryRowContext(ctx, `SELECT terminal_result_blob, terminal_result_digest FROM `+table+` WHERE id = 1`).
			Scan(&legacyBlob, &legacyDigest); err != nil {
			t.Fatalf("read upgraded legacy %s result: %v", table, err)
		}
		if promotionTerminalResultDigest(legacyBlob) == legacyDigest {
			t.Fatalf("legacy %s JSONB bytes became trusted during upgrade", table)
		}

		if _, err := db.ExecContext(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("delete legacy %s result: %v", table, err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO `+table+`
			(terminal_result_blob, terminal_result_digest) VALUES ($1, $2)`, terminalResult, wantDigest); err != nil {
			t.Fatalf("insert upgraded %s result: %v", table, err)
		}

		var gotBlob, gotDigest string
		if err := db.QueryRowContext(ctx, `SELECT terminal_result_blob, terminal_result_digest FROM `+table+` WHERE id = 2`).
			Scan(&gotBlob, &gotDigest); err != nil {
			t.Fatalf("round-trip upgraded %s result: %v", table, err)
		}
		if gotBlob != terminalResult || gotDigest != wantDigest || promotionTerminalResultDigest(gotBlob) != gotDigest {
			t.Fatalf("upgraded %s result = (%q, %q), want exact (%q, %q)",
				table, gotBlob, gotDigest, terminalResult, wantDigest)
		}
	}
}

func promotionTerminalResultUpgradeStatements(t *testing.T) []string {
	t.Helper()
	wanted := map[string]bool{
		"promotion_imports":           false,
		"promotion_import_tombstones": false,
	}
	var upgrades []string
	for _, stmt := range schema.PromotionDB9SchemaStatements() {
		lower := strings.ToLower(strings.TrimSpace(stmt))
		for table := range wanted {
			prefix := "alter table " + table + " alter column terminal_result_blob type text"
			if strings.HasPrefix(lower, prefix) {
				wanted[table] = true
				upgrades = append(upgrades, stmt)
			}
		}
	}
	for table, found := range wanted {
		if !found {
			t.Fatalf("DB9 schema omits terminal-result upgrade for %s", table)
		}
	}
	return upgrades
}

func promotionTerminalResultDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func openPromotionPostgres(t *testing.T) *sql.DB {
	t.Helper()
	if dsn := strings.TrimSpace(os.Getenv(db9TestDSNEnv)); dsn != "" {
		return openPromotionPostgresDSN(t, dsn)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db9TestStartTimeout)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        db9TestImage,
			ExposedPorts: []string{db9TestPort},
			Env: map[string]string{
				"POSTGRES_PASSWORD": db9TestPassword,
				"POSTGRES_DB":       "drive9_test",
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort(db9TestPort),
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			).WithDeadline(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start PostgreSQL test container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("PostgreSQL test container host: %v", err)
	}
	port, err := container.MappedPort(ctx, db9TestPort)
	if err != nil {
		t.Fatalf("PostgreSQL test container port: %v", err)
	}
	dsn := fmt.Sprintf("postgres://postgres:%s@%s:%s/drive9_test?sslmode=disable", db9TestPassword, host, port.Port())
	return openPromotionPostgresDSN(t, dsn)
}

func openPromotionPostgresDSN(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if err := db.PingContext(ctx); err == nil {
			return db
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ping PostgreSQL test database: %v", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
