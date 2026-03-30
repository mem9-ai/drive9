package testmysql

import (
	"database/sql"
	"fmt"
	"regexp"

	"github.com/go-sql-driver/mysql"
)

var dbNameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// PrepareIsolatedDatabase derives a dedicated database from a shared DSN so
// package-level test suites can avoid clobbering each other's tables.
func PrepareIsolatedDatabase(baseDSN, suffix string) (string, error) {
	parsed, err := mysql.ParseDSN(baseDSN)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}

	baseName := parsed.DBName
	if baseName == "" {
		baseName = "dat9_test"
	}
	dbName := dbNameSanitizer.ReplaceAllString(baseName+"_"+suffix, "_")
	if dbName == "" {
		return "", fmt.Errorf("invalid derived database name")
	}

	adminCfg := *parsed
	adminCfg.DBName = ""
	admin, err := sql.Open("mysql", adminCfg.FormatDSN())
	if err != nil {
		return "", fmt.Errorf("open admin db: %w", err)
	}
	defer func() { _ = admin.Close() }()
	if err := admin.Ping(); err != nil {
		return "", fmt.Errorf("ping admin db: %w", err)
	}
	if _, err := admin.Exec("CREATE DATABASE IF NOT EXISTS `" + dbName + "`"); err != nil {
		return "", fmt.Errorf("create database %s: %w", dbName, err)
	}

	parsed.DBName = dbName
	return parsed.FormatDSN(), nil
}
