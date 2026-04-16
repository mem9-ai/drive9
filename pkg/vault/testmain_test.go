package vault

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
)

var testDB *sql.DB

func TestMain(m *testing.M) {
	inst, err := testmysql.Start(context.Background())
	if err != nil {
		log.Fatalf("setup mysql test instance: %v", err)
	}

	db, err := sql.Open("mysql", inst.DSN)
	if err != nil {
		log.Fatalf("open test db: %v", err)
	}
	if err := schema.ExecSchemaStatements(db, schema.VaultTiDBSchemaStatements()); err != nil {
		log.Fatalf("init vault schema: %v", err)
	}
	testDB = db

	code := m.Run()

	_ = db.Close()
	if err := inst.Close(context.Background()); err != nil {
		log.Printf("teardown mysql test instance: %v", err)
	}
	os.Exit(code)
}
