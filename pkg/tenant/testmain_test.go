package tenant

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/mem9-ai/drive9/internal/testtidb"
)

var testDSN string

func TestMain(m *testing.M) {
	inst, err := testtidb.Start(context.Background())
	if err != nil {
		log.Fatalf("setup tidb test instance: %v", err)
	}
	testDSN = inst.DSN

	code := m.Run()
	if err := inst.Close(context.Background()); err != nil {
		log.Printf("teardown mysql test instance: %v", err)
	}
	os.Exit(code)
}
