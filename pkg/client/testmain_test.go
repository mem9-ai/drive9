package client

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
)

var testDSN string

func TestMain(m *testing.M) {
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
