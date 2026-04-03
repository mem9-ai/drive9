package server

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/internal/testmysql"
)

type testTenantConnInfo struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	TLS      bool
}

func resetServerTestState(t *testing.T, dsn string, db *sql.DB) {
	t.Helper()

	initServerTenantSchema(t, dsn)
	testmysql.ResetDB(t, db)
	resetServerMetaTables(t, db)
}

func resetServerMetaTables(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, query := range []string{"DELETE FROM tenant_api_keys", "DELETE FROM tenants"} {
		if _, err := db.Exec(query); err != nil && !isMissingTableErr(err) {
			t.Fatalf("reset server meta tables: %v", err)
		}
	}
}

func isMissingTableErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "doesn't exist") || strings.Contains(msg, "unknown table")
}

func parseTestTenantConnInfo(t *testing.T, dsn string) testTenantConnInfo {
	t.Helper()

	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	host, port := "127.0.0.1", 3306
	if parsed.Addr != "" {
		h, p, _ := strings.Cut(parsed.Addr, ":")
		if h != "" {
			host = h
		}
		if p != "" {
			if _, err := fmt.Sscanf(p, "%d", &port); err != nil {
				t.Fatalf("parse mysql port %q: %v", p, err)
			}
		}
	}
	return testTenantConnInfo{
		Host:     host,
		Port:     port,
		User:     parsed.User,
		Password: parsed.Passwd,
		DBName:   parsed.DBName,
		TLS:      parsed.TLSConfig != "" && !strings.EqualFold(parsed.TLSConfig, "false"),
	}
}
