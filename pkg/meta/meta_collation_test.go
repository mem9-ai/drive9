package meta

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

// The OAuth subject columns are case-sensitive identities. Declaring them without
// a collation lets the database default decide, and a case-insensitive default
// maps "SUBJ" onto the binding stored for "subj" — another tenant's owner key.
func TestMetaSubjectKeyColumnsPinBinaryCollation(t *testing.T) {
	spec := mustMetaSpec(t)
	targets := metaPinnedCollationTargets(spec)
	want := map[string]string{
		"tenant_external_bindings.subject_key":  "utf8mb4_bin",
		"tenant_api_keys.issued_by_subject_key": "utf8mb4_bin",
	}
	got := map[string]string{}
	for _, target := range targets {
		got[target.table+"."+target.column] = target.collation
	}
	for key, collation := range want {
		if got[key] != collation {
			t.Fatalf("%s pinned collation = %q, want %q (targets: %v)", key, got[key], collation, got)
		}
	}

	// Deriving the targets from the contract is what keeps the DDL and the
	// migration in step: the definition and the index the migration recreates come
	// from the same parsed statements.
	byKey := map[string]metaCollationTarget{}
	for _, target := range targets {
		byKey[target.table+"."+target.column] = target
	}
	binding := byKey["tenant_external_bindings.subject_key"]
	if !strings.Contains(strings.ToUpper(binding.modifySQL), "COLLATE UTF8MB4_BIN") {
		t.Fatalf("subject_key modifySQL = %q, want the pinned collation", binding.modifySQL)
	}
	if len(binding.indexes) != 1 || binding.indexes[0].name != "uk_external_binding_subject" || !binding.indexes[0].unique {
		t.Fatalf("subject_key indexes = %#v, want the unique binding subject index", binding.indexes)
	}
	issuer := byKey["tenant_api_keys.issued_by_subject_key"]
	if len(issuer.indexes) != 1 || issuer.indexes[0].name != "idx_api_keys_issuer" || issuer.indexes[0].unique {
		t.Fatalf("issued_by_subject_key indexes = %#v, want the non-unique issuer index", issuer.indexes)
	}
}

// A fresh control plane gets the pinned collation from CREATE TABLE. It is checked
// on a database whose default is case-insensitive: on the shared test database the
// TiDB default is already utf8mb4_bin, so the same assertion would pass without the
// contract pinning anything.
func TestMigrateCreatesBinarySubjectKeyCollationOnCaseInsensitiveDatabase(t *testing.T) {
	s := freshControlStoreInDatabase(t, "utf8mb4_general_ci")
	ctx := context.Background()
	for _, target := range metaPinnedCollationTargets(mustMetaSpec(t)) {
		var collation sql.NullString
		if err := s.DB().QueryRowContext(ctx, `SELECT collation_name FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
			target.table, target.column).Scan(&collation); err != nil {
			t.Fatalf("read %s.%s collation: %v", target.table, target.column, err)
		}
		if collation.String != target.collation {
			t.Fatalf("%s.%s collation = %q, want %q", target.table, target.column, collation.String, target.collation)
		}
	}
}

// An installation whose subject column was created with a case-insensitive
// collation is a contract mismatch that must be reported, not silently accepted.
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
	// The repair is owned by the migration pre-step: a bare MODIFY cannot run while
	// an index covers the column on TiDB, so the planner must not claim one.
	if found.repairSQL != "" {
		t.Fatalf("collation mismatch carries a repair (%q)", found.repairSQL)
	}
	if isSafeMetaRepairDiff(*found, nil) {
		t.Fatal("a bare MODIFY cannot run while an index covers the column")
	}
}

// The premise the migration rests on: with an index in place the bare collation
// change is refused by TiDB (error 8200) and only succeeds once the index is
// dropped. That is why the migration drops and recreates instead of leaving the
// change to the repair planner.
func TestBareCollationModifyIsRefusedWhileIndexed(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	db := s.DB()
	downgradeSubjectKey(t, db)

	_, err := db.ExecContext(ctx,
		"ALTER TABLE tenant_external_bindings MODIFY COLUMN subject_key VARCHAR(512) COLLATE utf8mb4_bin NOT NULL")
	if err == nil {
		t.Skip("this engine allows a collation change on an indexed column, so no drop is needed")
	}
	if !metaCollationRefusedOverIndex(err) {
		t.Fatalf("bare MODIFY failed with an unexpected error: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE tenant_external_bindings DROP INDEX uk_external_binding_subject"); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"ALTER TABLE tenant_external_bindings MODIFY COLUMN subject_key VARCHAR(512) COLLATE utf8mb4_bin NOT NULL"); err != nil {
		t.Fatalf("MODIFY after dropping the index: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"ALTER TABLE tenant_external_bindings ADD UNIQUE INDEX uk_external_binding_subject (provider, subject_key)"); err != nil {
		t.Fatalf("recreate the index: %v", err)
	}
}

// The migration converts an installation whose subject columns predate the pinned
// collation, and the case-folded lookup stops resolving afterwards.
func TestEnsurePinnedCollationsConvertsExistingInstall(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	db := s.DB()
	downgradeSubjectKey(t, db)

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

	reopened, err := Open(testDSN)
	if err != nil {
		t.Fatalf("migrate a case-insensitive install: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	assertPinnedCollationAndIndex(t, reopened.DB(), "tenant_external_bindings", "subject_key", "uk_external_binding_subject")

	if _, err := s.GetExternalBinding(ctx, "slock", "SUBJ"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("case-variant lookup error = %v, want ErrNotFound", err)
	}
	if got, err := s.GetExternalBinding(ctx, "slock", "subj"); err != nil || got == nil || got.TenantID != "victim-tenant" {
		t.Fatalf("exact lookup = %v, %v; want the victim binding", got, err)
	}

	// A second migration is a no-op: the schema state is identical before and after.
	before := collationAndIndexState(t, reopened.DB(), "tenant_external_bindings", "subject_key", "uk_external_binding_subject")
	again, err := Open(testDSN)
	if err != nil {
		t.Fatalf("second migration: %v", err)
	}
	t.Cleanup(func() { _ = again.Close() })
	if after := collationAndIndexState(t, again.DB(), "tenant_external_bindings", "subject_key", "uk_external_binding_subject"); after != before {
		t.Fatalf("second migration changed the schema: %q -> %q", before, after)
	}
}

// Binding writes take the schema gate, so a migration that has to remove the
// unique subject index cannot overlap one: the write waits for the migration and
// then runs with the index back in place.
func TestInsertExternalBindingWaitsForSchemaLock(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	binding := &ExternalBinding{Provider: "slock", SubjectKey: "gated", TenantID: "t", CreatedAt: now, UpdatedAt: now}

	holding := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	// Registered before the holder starts: a failed assertion below must not leave
	// the holder (and its advisory lock) behind, or a repeated run of this test in
	// the same process would block on the lock it still holds.
	holderDone := make(chan struct{})
	acquireErr := make(chan error, 1)
	t.Cleanup(func() {
		closeRelease()
		<-holderDone
	})
	go func() {
		defer close(holderDone)
		acquireErr <- s.WithExternalBindingSchemaLock(ctx, func(lockCtx context.Context, _ *sql.Conn) error {
			close(holding)
			<-release
			return nil
		})
	}()
	select {
	case err := <-acquireErr:
		// The callback ran to completion, so the gate was never held open.
		if err != nil {
			t.Fatalf("hold the schema lock: %v", err)
		}
		t.Fatal("schema lock callback returned before the test released it")
	case <-holding:
	case <-time.After(10 * time.Second):
		t.Fatal("the schema lock was never acquired")
	}

	inserted := make(chan error, 1)
	go func() { inserted <- s.InsertExternalBinding(ctx, binding) }()
	select {
	case err := <-inserted:
		t.Fatalf("binding insert did not wait for the schema lock (err = %v)", err)
	case <-time.After(150 * time.Millisecond):
	}
	closeRelease()
	select {
	case err := <-inserted:
		if err != nil {
			t.Fatalf("binding insert after the lock was released: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("binding insert never completed after the schema lock was released")
	}
	if err := <-acquireErr; err != nil {
		t.Fatalf("release the schema lock: %v", err)
	}
	<-holderDone
	if got, err := s.GetExternalBinding(ctx, "slock", "gated"); err != nil || got == nil {
		t.Fatalf("binding was not stored: %v, %v", got, err)
	}
}

// TiDB commits each DDL statement on its own, so a migration interrupted between
// the index drop and its recreation leaves the column binary with no index. The
// retry must restore the index instead of skipping the column as "already binary"
// — otherwise the schema planner refuses to recreate a unique index on an existing
// table and every later startup fails.
func TestPinnedCollationsRestoreIndexAfterInterruptedRun(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(t *testing.T, db *sql.DB)
	}{
		{
			name: "crashed after MODIFY COLUMN",
			prepare: func(t *testing.T, db *sql.DB) {
				t.Helper()
				// Column already binary, both indexes missing.
				dropIndex(t, db, "tenant_external_bindings", "uk_external_binding_subject")
				dropIndex(t, db, "tenant_api_keys", "idx_api_keys_issuer")
			},
		},
		{
			name: "crashed after DROP INDEX",
			prepare: func(t *testing.T, db *sql.DB) {
				t.Helper()
				// Indexes missing and the subject column still case-insensitive.
				dropIndex(t, db, "tenant_external_bindings", "uk_external_binding_subject")
				if _, err := db.ExecContext(context.Background(),
					"ALTER TABLE tenant_external_bindings MODIFY COLUMN subject_key VARCHAR(512) COLLATE utf8mb4_general_ci NOT NULL"); err != nil {
					t.Fatalf("downgrade column: %v", err)
				}
				dropIndex(t, db, "tenant_api_keys", "idx_api_keys_issuer")
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

			assertPinnedCollationAndIndex(t, reopened.DB(), "tenant_external_bindings", "subject_key", "uk_external_binding_subject")
			// The non-unique issuer index is restored through the same pre-step, not
			// only the unique one.
			assertPinnedCollationAndIndex(t, reopened.DB(), "tenant_api_keys", "issued_by_subject_key", "idx_api_keys_issuer")
		})
	}
}

// A run that crashed after dropping the index leaves it missing; the recovery runs
// under the same gate binding writers take, so a writer that arrives while the
// index is absent waits for the repair instead of inserting into the gap.
func TestPinnedCollationRecoveryIsSerializedWithWriters(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	db := s.DB()
	now := time.Now().UTC()
	if err := s.InsertExternalBinding(ctx, &ExternalBinding{
		Provider: "slock", SubjectKey: "before-the-crash", TenantID: "t", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("insert before the crash: %v", err)
	}
	// The state a crash between DROP INDEX and ADD INDEX leaves behind.
	dropIndex(t, db, "tenant_external_bindings", "uk_external_binding_subject")
	// A write into that gap fails closed rather than trusting the missing index.
	if err := s.InsertExternalBinding(ctx, &ExternalBinding{
		Provider: "slock", SubjectKey: "into-the-gap", TenantID: "t", CreatedAt: now, UpdatedAt: now,
	}); err == nil {
		t.Fatal("insert succeeded while the unique subject index was missing")
	}

	holding := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_ = s.WithExternalBindingSchemaLock(ctx, func(lockCtx context.Context, _ *sql.Conn) error {
			close(holding)
			<-release
			return nil
		})
	}()
	t.Cleanup(func() {
		closeRelease()
		<-holderDone
	})
	select {
	case <-holding:
	case <-time.After(10 * time.Second):
		t.Fatal("the schema lock was never acquired")
	}

	// Both the migration and the writer have to wait for the gate this test holds.
	recovered := make(chan error, 1)
	go func() {
		reopened, err := Open(testDSN)
		if err == nil {
			_ = reopened.Close()
		}
		recovered <- err
	}()
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- s.InsertExternalBinding(ctx, &ExternalBinding{
			Provider: "slock", SubjectKey: "during-recovery", TenantID: "t", CreatedAt: now, UpdatedAt: now,
		})
	}()
	for _, ch := range []chan error{recovered, writeErr} {
		select {
		case err := <-ch:
			t.Fatalf("work proceeded while the schema gate was held (err = %v)", err)
		case <-time.After(150 * time.Millisecond):
		}
	}
	closeRelease()
	<-holderDone

	if err := <-recovered; err != nil {
		t.Fatalf("recover the index after a crash: %v", err)
	}
	// The writer that queued behind the gate either saw the recovered index and
	// stored the row, or observed the index missing and failed closed. Neither may
	// land a write in the gap.
	switch err := <-writeErr; {
	case err == nil:
		if got, getErr := s.GetExternalBinding(ctx, "slock", "during-recovery"); getErr != nil || got == nil {
			t.Fatalf("binding written after the recovery is not readable: %v, %v", got, getErr)
		}
	case strings.Contains(err.Error(), "is missing"):
		// Fail-closed: the recovery had not run yet when the write was admitted.
	default:
		t.Fatalf("writer after the recovery: %v", err)
	}
	assertPinnedCollationAndIndex(t, db, "tenant_external_bindings", "subject_key", "uk_external_binding_subject")
	if got, err := s.GetExternalBinding(ctx, "slock", "before-the-crash"); err != nil || got == nil {
		t.Fatalf("pre-crash binding: %v, %v", got, err)
	}
	// With the index back, a write is admitted again.
	if err := s.InsertExternalBinding(ctx, &ExternalBinding{
		Provider: "slock", SubjectKey: "after-recovery", TenantID: "t", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("insert after the recovery: %v", err)
	}
}

// Recreating the unique index must report duplicate rows instead of failing with a
// raw duplicate-key error, so an operator sees what to clean up.
func TestPinnedCollationsRefuseToRecreateUniqueIndexOverDuplicates(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	db := s.DB()
	dropIndex(t, db, "tenant_external_bindings", "uk_external_binding_subject")
	// The shared test database is reused by every test in the package, and the
	// migration runs before the per-test reset, so the leftovers have to go.
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, `DELETE FROM tenant_external_bindings WHERE subject_key = 'dup'`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE tenant_external_bindings
			ADD UNIQUE INDEX uk_external_binding_subject (provider, subject_key)`)
	})
	for i := 0; i < 2; i++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO tenant_external_bindings
			(provider, subject_key, tenant_id, created_at, updated_at)
			VALUES ('slock', 'dup', 'tenant', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))`); err != nil {
			t.Fatalf("insert duplicate %d: %v", i, err)
		}
	}

	_, err := Open(testDSN)
	if err == nil {
		t.Fatal("migration recreated a unique index over duplicate rows")
	}
	if !strings.Contains(err.Error(), "duplicate rows") {
		t.Fatalf("migration error = %v, want the duplicate preflight message", err)
	}
}

// downgradeSubjectKey puts the binding subject column back into the pre-fix shape:
// case-insensitive, with its unique index rebuilt around the downgrade (TiDB
// refuses the collation change while the index covers the column).
func downgradeSubjectKey(t *testing.T, db *sql.DB) {
	t.Helper()
	dropIndex(t, db, "tenant_external_bindings", "uk_external_binding_subject")
	if _, err := db.ExecContext(context.Background(),
		"ALTER TABLE tenant_external_bindings MODIFY COLUMN subject_key VARCHAR(512) COLLATE utf8mb4_general_ci NOT NULL"); err != nil {
		t.Fatalf("downgrade subject_key: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		"ALTER TABLE tenant_external_bindings ADD UNIQUE INDEX uk_external_binding_subject (provider, subject_key)"); err != nil {
		t.Fatalf("recreate subject index: %v", err)
	}
}

func dropIndex(t *testing.T, db *sql.DB, table, index string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", table, index)); err != nil {
		t.Fatalf("drop %s.%s: %v", table, index, err)
	}
}

func collationAndIndexState(t *testing.T, db *sql.DB, table, column, index string) string {
	t.Helper()
	var collation sql.NullString
	if err := db.QueryRowContext(context.Background(), `SELECT collation_name FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`, table, column).Scan(&collation); err != nil {
		t.Fatalf("read %s.%s collation: %v", table, column, err)
	}
	var indexed bool
	if err := db.QueryRowContext(context.Background(), `SELECT EXISTS (
			SELECT 1 FROM information_schema.statistics
			WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?)`, table, index).Scan(&indexed); err != nil {
		t.Fatalf("read %s.%s index: %v", table, index, err)
	}
	return fmt.Sprintf("collation=%s indexed=%t", collation.String, indexed)
}

func assertPinnedCollationAndIndex(t *testing.T, db *sql.DB, table, column, index string) {
	t.Helper()
	if got := collationAndIndexState(t, db, table, column, index); got != "collation=utf8mb4_bin indexed=true" {
		t.Fatalf("%s.%s state = %q, want collation=utf8mb4_bin indexed=true", table, column, got)
	}
}

// freshControlStoreInDatabase migrates a brand-new database whose default
// collation is the given one, so a fresh-install assertion is meaningful on an
// engine whose own default already matches.
func freshControlStoreInDatabase(t *testing.T, collation string) *Store {
	t.Helper()
	// The name has to stay short: the migration lock is named after the database and
	// MySQL caps a lock name at 64 characters.
	sum := sha256.Sum256([]byte(t.Name()))
	name := fmt.Sprintf("meta_fresh_%x", sum[:6])
	admin, err := sql.Open("mysql", metaTestDSNWithDatabase(t, ""))
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name)); err != nil {
		t.Fatalf("drop scratch database: %v", err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s` COLLATE %s", name, collation)); err != nil {
		t.Fatalf("create scratch database: %v", err)
	}
	t.Cleanup(func() {
		cleanup, err := sql.Open("mysql", metaTestDSNWithDatabase(t, ""))
		if err != nil {
			return
		}
		defer func() { _ = cleanup.Close() }()
		_, _ = cleanup.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name))
	})
	store, err := Open(metaTestDSNWithDatabase(t, name))
	if err != nil {
		t.Fatalf("open scratch store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// metaTestDSNWithDatabase rewrites the test DSN onto another database (empty means
// no database selected). The DSN is parsed rather than sliced: a password may
// contain any character the driver accepts, including "?" and "/".
func metaTestDSNWithDatabase(t *testing.T, name string) string {
	t.Helper()
	cfg, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatalf("parse test DSN: %v", err)
	}
	cfg.DBName = name
	return cfg.FormatDSN()
}
