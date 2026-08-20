package vault

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

var testDB *sql.DB

func TestMain(m *testing.M) {
	inst, err := testtidb.Start(context.Background())
	if err != nil {
		log.Fatalf("setup tidb test instance: %v", err)
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
		log.Printf("teardown tidb test instance: %v", err)
	}
	os.Exit(code)
}
