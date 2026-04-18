package vault

import (
	"database/sql"

	"github.com/mem9-ai/dat9/pkg/tenant/schema"
)

// SchemaStatements returns the vault DDL statements (TiDB/MySQL-compatible).
// The canonical DDL lives in pkg/tenant/schema.VaultTiDBSchemaStatements();
// this is a convenience re-export.
func SchemaStatements() []string {
	return schema.VaultTiDBSchemaStatements()
}

// InitSchema creates the vault tables in the tenant database.
func InitSchema(db *sql.DB) error {
	return schema.ExecSchemaStatements(db, SchemaStatements())
}
