package schema

import "testing"

// TestFSLayerSharedSchemaMatchesStandaloneModuloFsID pins the shared FS layer
// DDL to the standalone one: every shared table must be the standalone table
// plus an fs_id BIGINT NOT NULL discriminator column in first position, with
// fs_id prefixed onto the primary key and every unique key / index. The
// comparison uses the MySQL variant, which carries no TiDB-only CLUSTERED
// keyword.
func TestFSLayerSharedSchemaMatchesStandaloneModuloFsID(t *testing.T) {
	assertSharedDriftParity(t, FSLayerTiDBSchemaStatements(), FSLayerMySQLSharedSchemaStatements())
}

// TestFSLayerTiDBSharedSchemaDeclaresClusteredPKs ensures the TiDB variant
// declares every composite primary key CLUSTERED (TiDB defaults composite PKs
// to NONCLUSTERED, which would scatter each tenant's rows), and that the
// MySQL variant differs only by the removed keyword.
func TestFSLayerTiDBSharedSchemaDeclaresClusteredPKs(t *testing.T) {
	assertClusteredVariantParity(t, FSLayerTiDBSharedSchemaStatements(), FSLayerMySQLSharedSchemaStatements(), 5)
}
