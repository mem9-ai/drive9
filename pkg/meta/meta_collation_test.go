package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The OAuth subject columns are case-sensitive identities. Declaring them without
// a collation lets the database default decide, and a case-insensitive default
// maps "SUBJ" onto the binding stored for "subj" — another tenant's owner key.
func TestMetaSubjectKeyColumnsPinBinaryCollation(t *testing.T) {
	spec := mustMetaSpec(t)

	bindings := mustMetaTableSpec(t, spec, "tenant_external_bindings")
	subjectKey, ok := bindings.columns["subject_key"]
	if !ok {
		t.Fatal("tenant_external_bindings schema missing subject_key")
	}
	if subjectKey.collation != "utf8mb4_bin" {
		t.Fatalf("subject_key collation = %q, want utf8mb4_bin", subjectKey.collation)
	}
	if !strings.Contains(strings.ToUpper(subjectKey.addSQL), "COLLATE UTF8MB4_BIN") {
		t.Fatalf("subject_key addSQL = %q, want a pinned collation", subjectKey.addSQL)
	}

	apiKeys := mustMetaTableSpec(t, spec, "tenant_api_keys")
	issuedBy, ok := apiKeys.columns["issued_by_subject_key"]
	if !ok {
		t.Fatal("tenant_api_keys schema missing issued_by_subject_key")
	}
	if issuedBy.collation != "utf8mb4_bin" {
		t.Fatalf("issued_by_subject_key collation = %q, want utf8mb4_bin", issuedBy.collation)
	}
}

// A fresh control plane gets the pinned collation from CREATE TABLE.
func TestMigrateCreatesBinarySubjectKeyCollation(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	for _, target := range binarySubjectKeyColumns {
		var collation sql.NullString
		if err := s.DB().QueryRowContext(ctx, `SELECT collation_name FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
			target.table, target.column).Scan(&collation); err != nil {
			t.Fatalf("read %s.%s collation: %v", target.table, target.column, err)
		}
		if collation.String != "utf8mb4_bin" {
			t.Fatalf("%s.%s collation = %q, want utf8mb4_bin", target.table, target.column, collation.String)
		}
	}
}

// An installation whose subject column was created with a case-insensitive
// collation is a contract mismatch that must be reported (and repairable), not
// silently accepted.
func TestDiffMetaTableMetaReportsCollationMismatch(t *testing.T) {
	spec := mustMetaSpec(t)
	table := mustMetaTableSpec(t, spec, "tenant_external_bindings")
	observed := metaTableMeta{
		tableName: "tenant_external_bindings",
		columns: map[string]metaColumnMeta{
			"provider":    {columnType: "varchar(64)", collation: "utf8mb4_general_ci"},
			"subject_key": {columnType: "varchar(512)", collation: "utf8mb4_general_ci"},
			"tenant_id":   {columnType: "varchar(64)", collation: "utf8mb4_general_ci"},
		},
	}
	diffs := diffMetaTableMeta(table, observed, table.createStatement)
	var found *metaSchemaDiff
	for i := range diffs {
		if diffs[i].columnName == "subject_key" && diffs[i].kind == metaSchemaDiffColumnType {
			found = &diffs[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("diff did not report the subject_key collation, got %#v", diffs)
	}
	if !strings.Contains(found.detail, "collation") || !strings.Contains(found.detail, "utf8mb4_bin") {
		t.Fatalf("diff detail = %q, want the collation mismatch", found.detail)
	}
	if !isSafeMetaRepairDiff(*found, nil) {
		t.Fatalf("collation repair is not considered safe: %q", found.repairSQL)
	}
}

// The migration converts an installation whose subject columns predate the
// pinned collation, and the case-folded lookup stops resolving afterwards.
func TestEnsureBinarySubjectKeyCollationConvertsExistingInstall(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	db := s.DB()

	// Simulate the old schema: both subject columns case-insensitive, with their
	// indexes rebuilt around the downgrade (TiDB refuses a collation change while
	// an index covers the column, which is also why the migration drops it).
	downgrade := func(table, column, definition, dropIndex string, addIndex string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, dropIndex); err != nil {
			t.Fatalf("drop %s index: %v", table, err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", table, column, definition)); err != nil {
			t.Fatalf("downgrade %s.%s: %v", table, column, err)
		}
		if _, err := db.ExecContext(ctx, addIndex); err != nil {
			t.Fatalf("recreate %s index: %v", table, err)
		}
	}
	downgrade("tenant_external_bindings", "subject_key",
		"VARCHAR(512) COLLATE utf8mb4_general_ci NOT NULL",
		"ALTER TABLE tenant_external_bindings DROP INDEX uk_external_binding_subject",
		"ALTER TABLE tenant_external_bindings ADD UNIQUE INDEX uk_external_binding_subject (provider, subject_key)")
	downgrade("tenant_api_keys", "issued_by_subject_key",
		"VARCHAR(512) COLLATE utf8mb4_general_ci NOT NULL DEFAULT ''",
		"ALTER TABLE tenant_api_keys DROP INDEX idx_api_keys_issuer",
		"ALTER TABLE tenant_api_keys ADD INDEX idx_api_keys_issuer (tenant_id, issued_by_provider, issued_by_subject_key, status, created_at)")

	now := time.Now().UTC()
	if err := s.InsertExternalBinding(ctx, &ExternalBinding{
		Provider: "slock", SubjectKey: "subj", TenantID: "victim-tenant", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("insert binding: %v", err)
	}

	// The vulnerability: a case-variant subject resolves to the victim's binding.
	if got, err := s.GetExternalBinding(ctx, "slock", "SUBJ"); err != nil || got == nil || got.TenantID != "victim-tenant" {
		t.Fatalf("case-insensitive lookup = %v, %v; want the victim binding (pre-fix shape)", got, err)
	}

	// A real migration converts it. This also proves the conversion runs before
	// the repair planner would issue a bare MODIFY, which TiDB refuses while an
	// index covers the column.
	reopened, err := Open(testDSN)
	if err != nil {
		t.Fatalf("migrate a case-insensitive install: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	for _, target := range binarySubjectKeyColumns {
		var collation sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT collation_name FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
			target.table, target.column).Scan(&collation); err != nil {
			t.Fatalf("read %s.%s collation: %v", target.table, target.column, err)
		}
		if collation.String != "utf8mb4_bin" {
			t.Fatalf("%s.%s collation = %q, want utf8mb4_bin", target.table, target.column, collation.String)
		}
		var indexed bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (
				SELECT 1 FROM information_schema.statistics
				WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?)`,
			target.table, target.indexName).Scan(&indexed); err != nil {
			t.Fatalf("read %s.%s index: %v", target.table, target.indexName, err)
		}
		if !indexed {
			t.Fatalf("%s.%s was not recreated by the migration", target.table, target.indexName)
		}
	}

	// After the conversion the case-variant no longer resolves, the exact subject
	// still does, and a full migration is a no-op.
	if _, err := s.GetExternalBinding(ctx, "slock", "SUBJ"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("case-variant lookup error = %v, want ErrNotFound", err)
	}
	if got, err := s.GetExternalBinding(ctx, "slock", "subj"); err != nil || got == nil || got.TenantID != "victim-tenant" {
		t.Fatalf("exact lookup = %v, %v; want the victim binding", got, err)
	}
	if err := ensureBinarySubjectKeyCollation(ctx, db); err != nil {
		t.Fatalf("second ensureBinarySubjectKeyCollation: %v", err)
	}
}

// TiDB commits each DDL statement on its own, so a migration interrupted between
// the index drop and its recreation leaves the column binary with no index. The
// retry must restore the index instead of skipping the column as "already binary"
// — otherwise the schema planner refuses to recreate a unique index on an existing
// table and every later startup fails.
func TestBinarySubjectKeyCollationRestoresIndexAfterInterruptedRun(t *testing.T) {
	cases := []struct {
		name string
		// prepare leaves the database in the state a crash between the two DDL
		// statements would produce.
		prepare func(t *testing.T, db *sql.DB)
	}{
		{
			name: "crashed after MODIFY COLUMN",
			prepare: func(t *testing.T, db *sql.DB) {
				t.Helper()
				// Column already binary, index missing.
				if _, err := db.ExecContext(context.Background(),
					"ALTER TABLE tenant_external_bindings DROP INDEX uk_external_binding_subject"); err != nil {
					t.Fatalf("drop index: %v", err)
				}
			},
		},
		{
			name: "crashed after DROP INDEX",
			prepare: func(t *testing.T, db *sql.DB) {
				t.Helper()
				// Index missing and the column still case-insensitive.
				if _, err := db.ExecContext(context.Background(),
					"ALTER TABLE tenant_external_bindings DROP INDEX uk_external_binding_subject"); err != nil {
					t.Fatalf("drop index: %v", err)
				}
				if _, err := db.ExecContext(context.Background(),
					"ALTER TABLE tenant_external_bindings MODIFY COLUMN subject_key VARCHAR(512) COLLATE utf8mb4_general_ci NOT NULL"); err != nil {
					t.Fatalf("downgrade column: %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newControlStore(t)
			tc.prepare(t, s.DB())

			reopened, err := Open(testDSN)
			if err != nil {
				t.Fatalf("migration did not recover from an interrupted run: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })

			var collation sql.NullString
			if err := reopened.DB().QueryRowContext(context.Background(), `SELECT collation_name
				FROM information_schema.columns
				WHERE table_schema = DATABASE() AND table_name = 'tenant_external_bindings'
					AND column_name = 'subject_key'`).Scan(&collation); err != nil {
				t.Fatalf("read collation: %v", err)
			}
			if collation.String != "utf8mb4_bin" {
				t.Fatalf("subject_key collation = %q, want utf8mb4_bin", collation.String)
			}
			var indexed bool
			if err := reopened.DB().QueryRowContext(context.Background(), `SELECT EXISTS (
					SELECT 1 FROM information_schema.statistics
					WHERE table_schema = DATABASE() AND table_name = 'tenant_external_bindings'
						AND index_name = 'uk_external_binding_subject')`).Scan(&indexed); err != nil {
				t.Fatalf("read index: %v", err)
			}
			if !indexed {
				t.Fatal("unique index was not restored after an interrupted run")
			}
		})
	}
}
