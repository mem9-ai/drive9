package datastore

// Scope describes how a Store addresses tenant rows in the underlying
// database. The current schema shape is standalone: per-tenant databases
// whose tables carry no extra tenant discriminator beyond their own keys.
type Scope struct{}

// StandaloneScope returns the Scope for a per-tenant database.
func StandaloneScope() Scope {
	return Scope{}
}

// TenantCol returns the tenant-discriminator column name used by journal and
// vault tables.
func (s Scope) TenantCol() string {
	return "tenant_id"
}

// TenantArg returns the tenant-discriminator value for journal and vault
// queries.
func (s Scope) TenantArg(tenantID string) any {
	return tenantID
}

// And prefixes a WHERE predicate. pred must be a fixed string, never user
// input.
func (s Scope) And(pred string) string {
	return pred
}

// AndAs is And with a table-alias qualifier, for JOIN queries.
func (s Scope) AndAs(alias, pred string) string {
	return pred
}

// AndOn appends a join condition for LEFT JOINs.
func (s Scope) AndOn(cond, alias string) string {
	return cond
}

// Args returns the bind arguments unchanged.
func (s Scope) Args(args ...any) []any {
	return args
}

// InsCols returns the INSERT column list unchanged.
func (s Scope) InsCols(cols string) string {
	return cols
}

// InsVals returns the INSERT VALUES placeholder list unchanged.
func (s Scope) InsVals(vals string) string {
	return vals
}

// SelCols returns the SELECT column list unchanged.
func (s Scope) SelCols(cols string) string {
	return cols
}

// tenantCol returns the tenant-discriminator column name for journal/vault
// tables under this Store's schema shape.
func (s *Store) tenantCol() string { return s.scope.TenantCol() }

// tenantArg returns the bind value for the tenant discriminator of
// journal/vault tables.
func (s *Store) tenantArg(tenantID string) any { return s.scope.TenantArg(tenantID) }
