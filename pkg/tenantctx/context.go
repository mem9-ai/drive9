// Package tenantctx provides lightweight tenant ID propagation via context.
package tenantctx

import "context"

type key struct{}

// WithTenantID returns a context carrying tenantID when it is non-empty.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	if ctx == nil || tenantID == "" {
		return ctx
	}
	return context.WithValue(ctx, key{}, tenantID)
}

// TenantIDFromContext returns the tenant ID stored in ctx, if any.
func TenantIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	tenantID, _ := ctx.Value(key{}).(string)
	return tenantID
}