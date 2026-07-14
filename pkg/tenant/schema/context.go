package schema

import "context"

type tenantIDContextKey struct{}

// WithTenantID annotates schema-init contexts so user_schema DB pool metrics can
// retain tenant attribution without changing public schema-init function shapes.
// This is intentionally separate from pkg/tenantctx's request-scope tenant ID:
// schema setup can also run outside a request and must be tagged explicitly.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	if ctx == nil || tenantID == "" {
		return ctx
	}
	return context.WithValue(ctx, tenantIDContextKey{}, tenantID)
}

// TenantIDFromContext returns the tenant attribution attached with WithTenantID.
func TenantIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	tenantID, _ := ctx.Value(tenantIDContextKey{}).(string)
	return tenantID
}
