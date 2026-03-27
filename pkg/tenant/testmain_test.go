package tenant

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
)

var testDSN string

func TestMain(m *testing.M) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("skip DB tests (no Docker): %v", r)
			}
		}()
		inst, err := testmysql.Start(context.Background())
		if err != nil {
			log.Printf("skip DB tests: %v", err)
			return
		}
		testDSN = inst.DSN
	}()

	code := m.Run()

	os.Exit(code)
}
