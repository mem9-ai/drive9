package schema

import (
	"context"
	"testing"

	"go.uber.org/zap"
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

func TestTenantSchemaLogFieldsIncludesTenantID(t *testing.T) {
	fields := tenantSchemaLogFields(WithTenantID(context.Background(), "tenant-1"), zap.String("mode", "fts-only"))
	if len(fields) != 2 {
		t.Fatalf("field count = %d, want 2", len(fields))
	}
	if fields[0].Key != "tenant_id" || fields[0].String != "tenant-1" {
		t.Fatalf("first field = %+v, want tenant_id=tenant-1", fields[0])
	}
	if fields[1].Key != "mode" || fields[1].String != "fts-only" {
		t.Fatalf("second field = %+v, want mode=fts-only", fields[1])
	}

	fields = tenantSchemaLogFields(context.Background(), zap.String("mode", "fts-only"))
	if len(fields) != 1 {
		t.Fatalf("field count without tenant id = %d, want 1", len(fields))
	}
	if fields[0].Key != "mode" || fields[0].String != "fts-only" {
		t.Fatalf("field without tenant id = %+v, want mode=fts-only", fields[0])
	}
}
