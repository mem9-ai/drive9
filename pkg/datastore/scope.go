package datastore

// Scope describes how a Store addresses tenant rows in the underlying
// database, and is the single injection point for the two schema shapes
// (docs/TENANT_DB_REDESIGN.md §15.3):
//
//   - standalone: per-tenant databases whose tables have no fs_id column
//     (every pre-existing tenant DB). Scope emits no fs_id predicates.
//   - shared: a database shared by many tenants whose tables carry an
//     fs_id BIGINT column as the leading row key. Scope injects fs_id into
//     every predicate and column list.
//
// A Store holds one Scope fixed at construction; datastore methods never
// take tenant/fs parameters for Core FS tables. Journal and vault tables
// already carry a tenant discriminator column, so for those tables Scope
// only switches the column name and value (TenantCol/TenantArg).
type Scope struct {
	fsID   int64
	shared bool
}

// StandaloneScope returns the Scope for a per-tenant database with the
// current (no fs_id column) schema shape. fsID is the tenant's registered
// internal id; it is retained for logging and future routing use but is
// never emitted into SQL.
func StandaloneScope(fsID int64) Scope {
	return Scope{fsID: fsID}
}

// SharedScope returns the Scope for a shared database whose tables carry an
// fs_id column. fsID must be the tenant's registered id from fs_registry.
func SharedScope(fsID int64) Scope {
	return Scope{fsID: fsID, shared: true}
}

// Shared reports whether the Store addresses a shared-schema database.
func (s Scope) Shared() bool { return s.shared }

// FsID returns the tenant's internal numeric id (0 when unknown).
func (s Scope) FsID() int64 { return s.fsID }

// TenantCol returns the tenant-discriminator column name used by journal and
// vault tables: "tenant_id" in standalone shape, "fs_id" in shared shape.
func (s Scope) TenantCol() string {
	if s.shared {
		return "fs_id"
	}
	return "tenant_id"
}

// TenantArg returns the tenant-discriminator value for journal and vault
// queries: the tenant UUID string in standalone shape, the internal fs_id in
// shared shape.
func (s Scope) TenantArg(tenantID string) any {
	if s.shared {
		return s.fsID
	}
	return tenantID
}

// And prefixes a WHERE predicate with "fs_id = ? AND " in shared shape and
// returns it unchanged in standalone shape. pred must be a fixed string,
// never user input.
func (s Scope) And(pred string) string {
	if s.shared {
		return "fs_id = ? AND " + pred
	}
	return pred
}

// AndAs is And with a table-alias qualifier, for JOIN queries: shared shape
// emits "<alias>.fs_id = ? AND <pred>". Filter the driving table of the join;
// entity ids (inode_id, file_id, layer_id, workspace_id, ...) are globally
// unique, so a one-sided fs_id predicate is sufficient for correctness.
func (s Scope) AndAs(alias, pred string) string {
	if s.shared {
		return alias + ".fs_id = ? AND " + pred
	}
	return pred
}

// Args prepends the fs_id bind argument in shared shape and returns args
// unchanged in standalone shape.
func (s Scope) Args(args ...any) []any {
	if s.shared {
		return append([]any{s.fsID}, args...)
	}
	return args
}

// InsCols prefixes an INSERT column list with "fs_id, " in shared shape and
// returns it unchanged in standalone shape.
func (s Scope) InsCols(cols string) string {
	if s.shared {
		return "fs_id, " + cols
	}
	return cols
}

// InsVals prefixes an INSERT VALUES placeholder list with "?, " in shared
// shape and returns it unchanged in standalone shape.
func (s Scope) InsVals(vals string) string {
	if s.shared {
		return "?, " + vals
	}
	return vals
}

// tenantCol returns the tenant-discriminator column name for journal/vault
// tables under this Store's schema shape.
func (s *Store) tenantCol() string { return s.scope.TenantCol() }

// tenantArg returns the bind value for the tenant discriminator of
// journal/vault tables: tenantID in standalone shape, the scope's fs_id in
// shared shape.
func (s *Store) tenantArg(tenantID string) any { return s.scope.TenantArg(tenantID) }
