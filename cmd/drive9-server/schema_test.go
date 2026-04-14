package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestSchemaDumpInitSQLByProvider(t *testing.T) {
	for _, provider := range []string{"tidb_zero", "tidb_cloud_starter"} {
		t.Run(provider, func(t *testing.T) {
			out := captureSchemaStdout(t, func() {
				if err := runSchemaCommand([]string{"dump-init-sql", "--provider", provider}); err != nil {
					t.Fatalf("dump provider schema: %v", err)
				}
			})

			if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS files") {
				t.Fatalf("dump missing files table: %q", out)
			}
			if !strings.Contains(out, "GENERATED ALWAYS AS (EMBED_TEXT") {
				t.Fatalf("dump missing auto-embedding expression: %q", out)
			}
			if !strings.Contains(out, "CREATE INDEX idx_task_claim_type ON semantic_tasks") {
				t.Fatalf("dump missing semantic_tasks index: %q", out)
			}
			if !strings.Contains(out, ";\n") {
				t.Fatalf("dump missing SQL statement terminators: %q", out)
			}
		})
	}
}

func TestSchemaDumpInitSQLByProviderIncludesVault(t *testing.T) {
	out := captureSchemaStdout(t, func() {
		if err := runSchemaCommand([]string{"dump-init-sql", "--provider", "db9"}); err != nil {
			t.Fatalf("dump provider schema: %v", err)
		}
	})

	if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS vault_deks") {
		t.Fatalf("db9 dump missing vault_deks: %q", out)
	}
	if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS vault_audit_log") {
		t.Fatalf("db9 dump missing vault_audit_log: %q", out)
	}
}

func TestSchemaDumpInitSQLRequiresProvider(t *testing.T) {
	err := runSchemaCommand([]string{"dump-init-sql"})
	if err == nil {
		t.Fatal("expected missing provider to fail")
	}
	if !strings.Contains(err.Error(), "--provider") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func captureSchemaStdout(t *testing.T, fn func()) string {
	t.Helper()

	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}

	os.Stdout = writer
	t.Cleanup(func() {
		os.Stdout = originalStdout
	})

	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, reader)
		done <- buf.String()
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}
	data := <-done
	if err := reader.Close(); err != nil {
		t.Fatalf("close stdout reader: %v", err)
	}
	return data
}
