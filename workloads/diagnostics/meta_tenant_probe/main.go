// Command meta_tenant_probe decomposes the cost of Store.ListDir on a tenant
// database.
//
// It must run inside the deployment namespace (same service account and secret
// as drive9-server): the tenant DB password is stored encrypted in the meta DB
// and KMS decryption needs the pod's IRSA role. In shared-pool deployments the
// connection lives on the `db_pool` row instead of the tenant row, and each
// tenant keeps its own schema inside the shared cluster, so the tool discovers
// the schema that actually holds the probed directory.
//
// Usage:
//
//	meta_tenant_probe <tenant-id> [parent-path] [iterations]
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

// sqlQuery, when set from the "sql:" argument, is executed verbatim (after
// {schema} and {fs_id} substitution) and the probe exits without running the
// timing sweep.
var sqlQuery string

// lockProbe, when set from the "locks:" argument, runs the delete-path lock
// experiment instead of the timing sweep.
var lockProbe bool

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: meta_tenant_probe <tenant-id> [parent-path | sql:<query>] [iterations]")
		os.Exit(2)
	}
	tenantID := strings.TrimSpace(os.Args[1])
	parentPath := "/benchmark"
	if len(os.Args) > 2 && strings.TrimSpace(os.Args[2]) != "" {
		arg := strings.TrimSpace(os.Args[2])
		if strings.HasPrefix(arg, "sql:") {
			// Read-only escape hatch: run one or more ';'-separated statements after
			// the tenant schema and fs_id are resolved, then exit. {schema} and
			// {fs_id} are substituted before execution.
			sqlQuery = strings.TrimSpace(strings.TrimPrefix(arg, "sql:"))
		} else if arg == "locks:" {
			// Concurrency experiment for the delete-path lock question.
			lockProbe = true
		} else {
			parentPath = arg
		}
	}
	// file_nodes stores directory parents with a trailing slash.
	if parentPath != "/" {
		parentPath = strings.TrimRight(parentPath, "/") + "/"
	}
	iterations := 20
	if len(os.Args) > 3 {
		if parsed, err := strconv.Atoi(os.Args[3]); err == nil && parsed > 0 {
			iterations = parsed
		}
	}
	ctx := context.Background()
	if err := run(ctx, tenantID, parentPath, iterations); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type connection struct {
	host     string
	port     int
	user     string
	password []byte
	dbName   string
	skipTLS  bool
	provider string
	source   string
}

func run(ctx context.Context, tenantID, parentPath string, iterations int) error {
	metaDSN := strings.TrimSpace(os.Getenv("DRIVE9_META_DSN"))
	if metaDSN == "" {
		return fmt.Errorf("DRIVE9_META_DSN is empty")
	}
	metaStore, err := meta.OpenContext(ctx, metaDSN)
	if err != nil {
		return fmt.Errorf("open meta store: %w", err)
	}
	defer func() { _ = metaStore.Close() }()

	tenantRecord, err := metaStore.GetTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("load tenant %s: %w", tenantID, err)
	}
	conn := connection{
		host:     tenantRecord.DBHost,
		port:     tenantRecord.DBPort,
		user:     tenantRecord.DBUser,
		password: tenantRecord.DBPasswordCipher,
		dbName:   tenantRecord.DBName,
		skipTLS:  tenantRecord.DBTLS,
		provider: tenantRecord.Provider,
		source:   "tenant row",
	}
	if conn.host == "" || len(conn.password) == 0 {
		pool, err := loadSharedPool(ctx, metaDSN)
		if err != nil {
			return fmt.Errorf("tenant row has no connection and db_pool lookup failed: %w", err)
		}
		conn = *pool
	}
	fmt.Printf("connection source=%s provider=%s host=%s port=%d db=%s user=%s tls=%v\n",
		conn.source, conn.provider, conn.host, conn.port, conn.dbName, conn.user, conn.skipTLS)

	encType := encrypt.Type(strings.TrimSpace(os.Getenv("DRIVE9_ENCRYPT_TYPE")))
	if encType == "" {
		encType = encrypt.TypeLocalAES
	}
	encKey := os.Getenv("DRIVE9_MASTER_KEY")
	switch encType {
	case encrypt.TypeKMS, encrypt.TypeAliyunKMS, encrypt.TypeTencentKMS:
		encKey = os.Getenv("DRIVE9_ENCRYPT_KEY")
	}
	enc, err := encrypt.New(ctx, encrypt.Config{Type: encType, Key: encKey, Region: os.Getenv("AWS_REGION")})
	if err != nil {
		return fmt.Errorf("init encryptor (%s): %w", encType, err)
	}
	password, err := enc.Decrypt(ctx, conn.password)
	if err != nil {
		return fmt.Errorf("decrypt tenant db password: %w", err)
	}

	dsn := tenant.FormatTenantMySQLDSN(conn.user, string(password), conn.host, conn.port, conn.dbName, conn.skipTLS, conn.provider)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open tenant db: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(2)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping tenant db: %w", err)
	}
	fmt.Println("connected to shared cluster")

	hash := datastore.StorageRefHash(parentPath)
	fsID, err := loadFsID(ctx, metaDSN, tenantID)
	if err != nil {
		return fmt.Errorf("resolve fs_id for tenant %s: %w", tenantID, err)
	}
	fmt.Printf("fs_id=%d (shared schema discriminator: the real ListDir filters fn.fs_id)\n", fsID)

	schema, rows, err := discoverSchema(ctx, db, fsID, hash, parentPath)
	if err != nil {
		return err
	}
	fmt.Printf("schema=%s parent_path=%q entries=%d hash=%s\n", schema, parentPath, rows, hash)

	if sqlQuery != "" {
		for _, stmt := range strings.Split(sqlQuery, ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			stmt = strings.ReplaceAll(stmt, "{schema}", schema)
			stmt = strings.ReplaceAll(stmt, "{fs_id}", strconv.FormatInt(fsID, 10))
			fmt.Printf("\nSQL> %s\n", stmt)
			printRows(ctx, db, stmt, 0)
		}
		return nil
	}

	if lockProbe {
		return runLockProbe(ctx, db, schema, fsID)
	}

	fmt.Printf("baseline SELECT 1: %s\n", describe(measure(ctx, db, iterations, func(ctx context.Context) error {
		var one int
		return db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
	})))
	fmt.Println("file_nodes indexes:")
	printRows(ctx, db, "SHOW INDEX FROM `"+schema+"`.file_nodes", 0)

	variants := []struct {
		name string
		sql  string
	}{
		{"base(file_nodes only)", listBase(schema)},
		{"+inodes join", listInodes(schema)},
		{"+semantic join (current ListDir)", listCurrent(schema)},
	}
	for _, variant := range variants {
		fmt.Printf("timing %-32s %s\n", variant.name, describe(measure(ctx, db, iterations, func(ctx context.Context) error {
			return drain(ctx, db, variant.sql, fsID, hash, parentPath)
		})))
	}
	fmt.Println("EXPLAIN ANALYZE (base, file_nodes only):")
	printRows(ctx, db, "EXPLAIN ANALYZE "+listBase(schema), 0, fsID, hash, parentPath)
	fmt.Println("EXPLAIN ANALYZE (+semantic join, current ListDir):")
	printRows(ctx, db, "EXPLAIN ANALYZE "+listCurrent(schema), 0, fsID, hash, parentPath)
	fmt.Println("statements_summary (real queries touching parent_path):")
	printRows(ctx, db, `SELECT digest_text, exec_count, ROUND(avg_latency/1000000,2) avg_ms,
		ROUND(max_latency/1000000,2) max_ms, plan_digest
		FROM information_schema.statements_summary
		WHERE schema_name = ? AND digest_text LIKE '%parent_path%'
		ORDER BY avg_latency DESC LIMIT 5`, 5, schema)
	fmt.Println("table statistics state:")
	printRows(ctx, db, `SELECT table_name, column_name, count, modify_count, last_analyze_time
		FROM mysql.stats_meta m JOIN information_schema.columns c
		ON c.table_name = m.table_name WHERE m.table_name = 'file_nodes' LIMIT 3`, 3)
	return nil
}

func loadSharedPool(ctx context.Context, metaDSN string) (*connection, error) {
	db, err := sql.Open("mysql", metaDSN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	row := db.QueryRowContext(ctx, `SELECT COALESCE(db_host,''), COALESCE(db_port,0), COALESCE(db_user,''),
		db_password, COALESCE(db_name,''), COALESCE(db_tls,''), COALESCE(cloud_provider,'')
		FROM db_pool WHERE role = 'shared' ORDER BY tenant_count DESC LIMIT 1`)
	var host, user, dbName, tlsMode, provider string
	var port int
	var cipher []byte
	if err := row.Scan(&host, &port, &user, &cipher, &dbName, &tlsMode, &provider); err != nil {
		return nil, err
	}
	return &connection{
		host: host, port: port, user: user, password: cipher, dbName: dbName,
		skipTLS:  tlsMode == "skip-verify" || strings.Contains(strings.ToLower(tlsMode), "skip"),
		provider: provider, source: "db_pool(shared)",
	}, nil
}

func loadFsID(ctx context.Context, metaDSN, tenantID string) (int64, error) {
	db, err := sql.Open("mysql", metaDSN)
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()
	var fsID int64
	if err := db.QueryRowContext(ctx, `SELECT fs_id FROM fs_registry WHERE tenant_id = ?`, tenantID).Scan(&fsID); err != nil {
		return 0, err
	}
	return fsID, nil
}

func discoverSchema(ctx context.Context, db *sql.DB, fsID int64, hash, parentPath string) (string, int64, error) {
	fmt.Println("databases visible to this user:")
	printRows(ctx, db, "SHOW DATABASES", 0)

	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_name = 'file_nodes'`)
	if err != nil {
		return "", 0, fmt.Errorf("list schemas: %w", err)
	}
	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			_ = rows.Close()
			return "", 0, err
		}
		schemas = append(schemas, schema)
	}
	_ = rows.Close()
	sort.Strings(schemas)
	best, bestRows := "", int64(-1)
	for _, schema := range schemas {
		var count int64
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM `"+schema+"`.file_nodes WHERE fs_id = ? AND parent_path_hash = ? AND parent_path = ?",
			fsID, hash, parentPath).Scan(&count); err != nil {
			continue
		}
		var total int64
		_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+schema+"`.file_nodes WHERE fs_id = ?", fsID).Scan(&total)
		fmt.Printf("  schema %-40s entries=%d fs_rows=%d\n", schema, count, total)
		if total > 0 {
			printRows(ctx, db, "SELECT parent_path, COUNT(*) rows_in_dir FROM `"+schema+"`.file_nodes WHERE fs_id = ? GROUP BY parent_path ORDER BY rows_in_dir DESC LIMIT 8", 8, fsID)
		}
		if count > bestRows {
			best, bestRows = schema, count
		}
	}
	if best == "" || bestRows <= 0 {
		return "", 0, fmt.Errorf("no schema under %s contains %q", strings.Join(schemas, ","), parentPath)
	}
	return best, bestRows, nil
}

func listBase(schema string) string {
	return fmt.Sprintf(`SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at
		FROM %s.file_nodes fn WHERE fn.fs_id = ? AND fn.parent_path_hash = ? AND fn.parent_path = ? ORDER BY fn.name`, schema)
}

func listInodes(schema string) string {
	return fmt.Sprintf(`SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		i.inode_id, i.size_bytes, i.revision, i.mode, i.status, i.created_at, i.confirmed_at
		FROM %s.file_nodes fn
		LEFT JOIN %s.inodes i ON (COALESCE(fn.inode_id, fn.file_id) = i.inode_id AND i.status = 'CONFIRMED')
		WHERE fn.fs_id = ? AND fn.parent_path_hash = ? AND fn.parent_path = ? ORDER BY fn.name`, schema, schema)
}

func listCurrent(schema string) string {
	return fmt.Sprintf(`SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		i.inode_id, i.size_bytes, i.revision, i.mode, i.status, i.created_at, i.confirmed_at,
		s.embedding_revision
		FROM %s.file_nodes fn
		LEFT JOIN %s.inodes i ON (COALESCE(fn.inode_id, fn.file_id) = i.inode_id AND i.status = 'CONFIRMED')
		LEFT JOIN %s.semantic s ON (i.inode_id = s.inode_id)
		WHERE fn.fs_id = ? AND fn.parent_path_hash = ? AND fn.parent_path = ? ORDER BY fn.name`, schema, schema, schema)
}

func drain(ctx context.Context, db *sql.DB, query string, args ...any) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([]any, len(cols))
	pointers := make([]any, len(cols))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return err
		}
	}
	return rows.Err()
}

func measure(ctx context.Context, db *sql.DB, iterations int, fn func(context.Context) error) []time.Duration {
	samples := make([]time.Duration, 0, iterations)
	for i := 0; i < iterations; i++ {
		start := time.Now()
		if err := fn(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "probe iteration failed: %v\n", err)
			continue
		}
		samples = append(samples, time.Since(start))
	}
	return samples
}

func describe(samples []time.Duration) string {
	if len(samples) == 0 {
		return "n=0"
	}
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	pick := func(q float64) time.Duration {
		index := int(float64(len(ordered)) * q)
		if index >= len(ordered) {
			index = len(ordered) - 1
		}
		return ordered[index]
	}
	var total time.Duration
	for _, value := range ordered {
		total += value
	}
	return fmt.Sprintf("n=%d p50=%.2fms p95=%.2fms mean=%.2fms min=%.2fms max=%.2fms",
		len(ordered), ms(pick(0.5)), ms(pick(0.95)), ms(total)/float64(len(ordered)), ms(ordered[0]), ms(ordered[len(ordered)-1]))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func printRows(ctx context.Context, db *sql.DB, query string, limit int, args ...any) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query failed: %v\n", err)
		return
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "columns: %v\n", err)
		return
	}
	printed := 0
	for rows.Next() {
		values := make([]any, len(cols))
		pointers := make([]any, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			return
		}
		cells := make([]string, len(cols))
		for i, value := range values {
			cells[i] = formatCell(value)
		}
		fmt.Println("  " + strings.Join(cells, " | "))
		printed++
		if limit > 0 && printed >= limit {
			break
		}
	}
}

func formatCell(value any) string {
	switch typed := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(typed)
	default:
		return fmt.Sprintf("%v", typed)
	}
}
