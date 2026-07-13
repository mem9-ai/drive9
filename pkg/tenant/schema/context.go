package schema

import "context"

type tenantIDContextKey struct{}

// WithTenantID annotates schema-init contexts so user_schema DB pool metrics can
// retain tenant attribution without changing public schema-init function shapes.
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
