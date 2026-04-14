// Package schemadump provides tenant init schema export helpers.
package schemadump

import (
	"fmt"

	"github.com/mem9-ai/dat9/pkg/tenant"
	tenantdb9 "github.com/mem9-ai/dat9/pkg/tenant/db9"
	tenantschema "github.com/mem9-ai/dat9/pkg/tenant/schema"
)

const usage = "usage: drive9-server schema dump-init-sql --provider <db9|tidb_zero|tidb_cloud_starter>"

// ResolveProvider normalizes a schema dump provider selection.
func ResolveProvider(provider string) (string, error) {
	if provider == "" {
		return "", fmt.Errorf("%s", usage)
	}
	normalized, err := tenant.NormalizeProvider(provider)
	if err != nil {
		return "", err
	}
	return normalized, nil
}

// Statements returns the exact init schema DDL statements for a provider.
func Statements(provider string) ([]string, error) {
	switch provider {
	case tenant.ProviderDB9:
		return tenantschema.CloneStatements(tenantdb9.InitSchemaStatements()), nil
	case tenant.ProviderTiDBZero, tenant.ProviderTiDBCloudStarter:
		return tenantschema.InitTiDBTenantSchemaStatementsForMode(tenantschema.TiDBEmbeddingModeAuto)
	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}

// SQLText formats the init schema DDL as executable SQL text.
func SQLText(provider string) (string, error) {
	stmts, err := Statements(provider)
	if err != nil {
		return "", err
	}
	return tenantschema.FormatStatementsSQL(stmts), nil
}

// Usage returns the supported schema dump command syntax.
func Usage() string {
	return usage
}
