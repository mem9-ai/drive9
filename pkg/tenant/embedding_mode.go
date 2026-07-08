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
