package tenant

import (
	"context"
	"testing"
)

func TestStarterProvisionerInitSchemaIsNoop(t *testing.T) {
	p := &StarterProvisioner{}
	if err := p.InitSchema(context.Background(), "ignored-dsn"); err != nil {
		t.Fatalf("InitSchema should be a no-op for starter: %v", err)
	}
}
