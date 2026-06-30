package server

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/mem9-ai/drive9/internal/testmysql"
)

var testDSN string

func TestMain(m *testing.M) {
	_ = os.Setenv("DRIVE9_META_DB_MAX_OPEN_CONNS", "8")
	_ = os.Setenv("DRIVE9_META_DB_MAX_IDLE_CONNS", "0")
	_ = os.Setenv("DRIVE9_USER_DB_MAX_OPEN_CONNS", "2")
	_ = os.Setenv("DRIVE9_USER_DB_MAX_IDLE_CONNS", "0")
	_ = os.Setenv("DRIVE9_USER_SCHEMA_DB_MAX_OPEN_CONNS", "4")
	_ = os.Setenv("DRIVE9_USER_SCHEMA_DB_MAX_IDLE_CONNS", "0")

	inst, err := testmysql.Start(context.Background())
	if err != nil {
		log.Fatalf("setup mysql test instance: %v", err)
	}
	testDSN = inst.DSN

	code := m.Run()
	if err := inst.Close(context.Background()); err != nil {
		log.Printf("teardown mysql test instance: %v", err)
	}
	os.Exit(code)
}
