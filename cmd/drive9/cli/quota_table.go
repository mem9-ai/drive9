package cli

import (
	"fmt"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
)

func quotaTableHeader(includeKind bool) string {
	columns := []string{"TENANT_ID", "STATUS"}
	if includeKind {
		columns = append(columns, "KIND")
	}
	columns = append(columns,
		"MAX_STORAGE",
		"MAX_FILE_SIZE",
		"MAX_FILE_COUNT",
		"MAX_MEDIA_LLM_FILES",
		"MAX_VIDEO_LLM_FILES",
		"SPENDING_LIMIT",
		"STORAGE_USED",
		"RESERVED",
		"FILE_COUNT",
		"MEDIA_FILE_COUNT",
		"VIDEO_FILE_COUNT",
	)
	return strings.Join(columns, "\t")
}

func quotaTableRow(tenantID, status, kind string, quota *client.AdminTenantQuota, includeKind bool) string {
	values := []string{tenantID, status}
	if includeKind {
		values = append(values, kind)
	}
	values = append(values,
		adminQuotaMaxStorage(quota),
		adminQuotaMaxFileSize(quota),
		adminQuotaMaxFileCount(quota),
		adminQuotaMaxMediaLLMFiles(quota),
		adminQuotaMaxVideoLLMFiles(quota),
		adminQuotaSpendingLimit(quota),
		adminQuotaStorageUsed(quota),
		adminQuotaReserved(quota),
		adminQuotaFileCount(quota),
		adminQuotaMediaFileCount(quota),
		adminQuotaVideoFileCount(quota),
	)
	return strings.Join(values, "\t")
}

func adminQuotaMaxStorage(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return fmt.Sprintf("%d Mi", quota.Config.MaxStorageSize)
}

func adminQuotaMaxFileSize(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return fmt.Sprintf("%d Mi", quota.Config.MaxFileSize)
}

func adminQuotaMaxFileCount(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return formatQuotaLimit(quota.Config.MaxFileCount)
}

func adminQuotaMaxMediaLLMFiles(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return formatQuotaLimit(quota.Config.MaxMediaLLMFiles)
}

func adminQuotaMaxVideoLLMFiles(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return formatQuotaLimit(quota.Config.MaxVideoLLMFiles)
}

func formatQuotaLimit(limit int64) string {
	if limit == 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%d", limit)
}

func adminQuotaSpendingLimit(quota *client.AdminTenantQuota) string {
	if quota == nil || quota.Config.TiDBCloudSpendingLimit == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *quota.Config.TiDBCloudSpendingLimit)
}

func adminQuotaStorageUsed(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return formatBytes(quota.Usage.StorageBytes)
}

func adminQuotaReserved(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return formatBytes(quota.Usage.ReservedBytes)
}

func adminQuotaFileCount(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return fmt.Sprintf("%d", quota.Usage.FileCount)
}

func adminQuotaMediaFileCount(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return fmt.Sprintf("%d", quota.Usage.MediaFileCount)
}

func adminQuotaVideoFileCount(quota *client.AdminTenantQuota) string {
	if quota == nil {
		return "-"
	}
	return fmt.Sprintf("%d", quota.Usage.VideoFileCount)
}
