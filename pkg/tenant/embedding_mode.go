package tenant

import (
	"fmt"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

// TiDBEmbeddingModeForTenantMode maps the persisted tenant-level embedding
// mode to the TiDB schema mode used by schema ensure/validation.
func TiDBEmbeddingModeForTenantMode(mode string) (schema.TiDBEmbeddingMode, error) {
	switch mode {
	case meta.TenantEmbeddingModeAuto:
		return schema.TiDBEmbeddingModeAuto, nil
	case meta.TenantEmbeddingModeFTSOnly:
		return schema.TiDBEmbeddingModeFTSOnly, nil
	default:
		return schema.TiDBEmbeddingModeUnknown, fmt.Errorf("unsupported tenant embedding mode %q", mode)
	}
}

// TiDBTenantSchemaVersionForEmbeddingMode derives the profile-specific TiDB
// schema version for a persisted tenant-level embedding mode.
func TiDBTenantSchemaVersionForEmbeddingMode(mode string, profile schema.TiDBAutoEmbeddingProfile) (int, error) {
	tidbMode, err := TiDBEmbeddingModeForTenantMode(mode)
	if err != nil {
		return 0, fmt.Errorf("map tenant embedding mode %q: %w", mode, err)
	}
	version, err := schema.TiDBTenantSchemaVersionForEmbeddingModeProfile(tidbMode, profile)
	if err != nil {
		return 0, fmt.Errorf("derive tenant schema version for mode %q: %w", mode, err)
	}
	return version, nil
}
