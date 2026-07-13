package schema

import (
	"context"
	"testing"
)

func TestTenantIDContext(t *testing.T) {
	ctx := WithTenantID(context.Background(), "tenant-1")
	if got := TenantIDFromContext(ctx); got != "tenant-1" {
		t.Fatalf("tenant id = %q, want tenant-1", got)
	}

	var nilCtx context.Context
	if got := WithTenantID(nilCtx, "tenant-1"); got != nil {
		t.Fatalf("nil context = %#v, want nil", got)
	}
	if got := TenantIDFromContext(nilCtx); got != "" {
		t.Fatalf("nil context tenant id = %q, want empty", got)
	}
	if got := TenantIDFromContext(WithTenantID(context.Background(), "")); got != "" {
		t.Fatalf("empty tenant id = %q, want empty", got)
	}
}
