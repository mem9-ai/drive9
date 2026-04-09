package starter

import (
	"context"
	"strings"
	"testing"
)

func TestProvisionerInitSchemaValidatesSchema(t *testing.T) {
	p := &Provisioner{}
	err := p.InitSchema(context.Background(), "ignored-dsn")
	if err == nil {
		t.Fatal("expected starter schema validation to reject invalid dsn")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unknown network") && !strings.Contains(strings.ToLower(err.Error()), "missing the slash separating the database name") {
		t.Fatalf("unexpected starter schema validation error: %v", err)
	}
}
