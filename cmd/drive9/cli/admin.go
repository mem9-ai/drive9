package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/mem9-ai/drive9/pkg/client"
)

func Admin(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminUsage())
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, adminUsage())
		return nil
	case "quota":
		return adminQuota(args[1:])
	case "tenant", "tenants":
		return adminTenant(args[1:])
	default:
		return fmt.Errorf("unknown admin command %q\n%s", args[0], adminUsage())
	}
}

func adminQuota(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminQuotaUsage())
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, adminQuotaUsage())
		return nil
	case "set":
		return quotaSet(args[1:])
	case "list":
		return adminTenantList(args[1:], true)
	default:
		return fmt.Errorf("unknown admin quota command %q\n%s", args[0], adminQuotaUsage())
	}
}

func adminTenant(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminTenantUsage())
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, adminTenantUsage())
		return nil
	case "list":
		return adminTenantList(args[1:], false)
	case "get":
		return adminTenantGet(args[1:])
	case "delete":
		return adminTenantDelete(args[1:])
	default:
		return fmt.Errorf("unknown admin tenant command %q\n%s", args[0], adminTenantUsage())
	}
}

func adminTenantList(args []string, includeQuotaDefault bool) error {
	serverFlag := ""
	serverGiven := false
	regionCodeFlag := ""
	regionCodeGiven := false
	publicKeyFlag := ""
	publicKeyGiven := false
	privateKeyFlag := ""
	privateKeyGiven := false
	pageSize := 0
	page := 0
	includeQuota := includeQuotaDefault
	asJSON := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, adminTenantListUsage())
			return nil
		case "--server":
			if i+1 >= len(args) {
				return fmt.Errorf("--server requires an argument")
			}
			i++
			serverFlag = args[i]
			serverGiven = true
		case "--region-code":
			if i+1 >= len(args) {
				return fmt.Errorf("--region-code requires an argument")
			}
			i++
			regionCodeFlag = args[i]
			regionCodeGiven = true
		case "--tidbcloud-public-key":
			if i+1 >= len(args) {
				return fmt.Errorf("--tidbcloud-public-key requires an argument")
			}
			i++
			publicKeyFlag = args[i]
			publicKeyGiven = true
		case "--tidbcloud-private-key":
			if i+1 >= len(args) {
				return fmt.Errorf("--tidbcloud-private-key requires an argument")
			}
			i++
			privateKeyFlag = args[i]
			privateKeyGiven = true
		case "--page-size":
			if i+1 >= len(args) {
				return fmt.Errorf("--page-size requires an argument")
			}
			i++
			v, err := parsePositiveQuotaInt64Flag("--page-size", args[i])
			if err != nil {
				return err
			}
			pageSize = int(v)
		case "--page":
			if i+1 >= len(args) {
				return fmt.Errorf("--page requires an argument")
			}
			i++
			v, err := parsePositiveQuotaInt64Flag("--page", args[i])
			if err != nil {
				return err
			}
			page = int(v)
		case "--include-quota":
			includeQuota = true
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("unknown flag %q\n%s", args[i], adminTenantListUsage())
		}
	}
	if err := rejectEmptyFlag("server", strings.TrimSpace(serverFlag), serverGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("region-code", strings.TrimSpace(regionCodeFlag), regionCodeGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(publicKeyFlag), publicKeyGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(privateKeyFlag), privateKeyGiven); err != nil {
		return err
	}
	r := ResolveCredentials()
	publicKey, privateKey := adminTiDBCloudKeys(publicKeyFlag, privateKeyFlag)
	server, err := quotaServer(serverFlag, regionCodeFlag, r.Server, true)
	if err != nil {
		return err
	}
	if _, err := quotaRequest("admin", publicKey, privateKey); err != nil {
		return err
	}
	requestQuota := includeQuota
	if !asJSON {
		requestQuota = true
	}
	out, err := client.New(server, "").AdminListTenants(context.Background(), client.AdminTenantListRequest{
		PublicKey:    publicKey,
		PrivateKey:   privateKey,
		PageSize:     pageSize,
		Page:         page,
		IncludeQuota: requestQuota,
	})
	if err != nil {
		return quotaAPIError("list admin tenants", err)
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	return printAdminTenantList(out, requestQuota)
}

func adminTenantGet(args []string) error {
	tenantID, server, publicKey, privateKey, asJSON, err := parseAdminTenantIDCommand(args, adminTenantGetUsage())
	if err != nil {
		if errors.Is(err, errHelpRequested{}) {
			return nil
		}
		return err
	}
	out, err := client.New(server, "").AdminGetTenant(context.Background(), client.QuotaRequest{
		TenantID: tenantID, PublicKey: publicKey, PrivateKey: privateKey,
	})
	if err != nil {
		return quotaAPIError("get admin tenant", err)
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	return printAdminTenantTable([]client.AdminTenant{*out}, true)
}

func printAdminTenantList(out *client.AdminTenantListResponse, includeQuota bool) error {
	if out == nil {
		return fmt.Errorf("admin tenant list response is empty")
	}
	if err := printAdminTenantTable(out.Tenants, includeQuota); err != nil {
		return err
	}
	if out.NextPage != 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(w, "next_page=%d\tpage=%d\tpage_size=%d\n", out.NextPage, out.Page, out.PageSize)
		return w.Flush()
	}
	return nil
}

func printAdminTenantTable(tenants []client.AdminTenant, includeQuota bool) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	if includeQuota {
		_, _ = fmt.Fprintln(w, "TENANT_ID\tSTATUS\tKIND\tMAX_STORAGE\tMAX_FILE_SIZE\tMAX_FILE_COUNT\tSPENDING_LIMIT\tSTORAGE_USED\tRESERVED\tFILE_COUNT")
		for _, t := range tenants {
			quota := t.Quota
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				t.TenantID,
				t.Status,
				t.Kind,
				adminQuotaMaxStorage(quota),
				adminQuotaMaxFileSize(quota),
				adminQuotaMaxFileCount(quota),
				adminQuotaSpendingLimit(quota),
				adminQuotaStorageUsed(quota),
				adminQuotaReserved(quota),
				adminQuotaFileCount(quota),
			)
		}
	} else {
		_, _ = fmt.Fprintln(w, "TENANT_ID\tSTATUS\tKIND")
		for _, t := range tenants {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", t.TenantID, t.Status, t.Kind)
		}
	}
	return w.Flush()
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
	if quota.Config.MaxFileCount == 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%d", quota.Config.MaxFileCount)
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

func adminTenantDelete(args []string) error {
	tenantID, server, publicKey, privateKey, asJSON, err := parseAdminTenantIDCommand(args, adminTenantDeleteUsage())
	if err != nil {
		if errors.Is(err, errHelpRequested{}) {
			return nil
		}
		return err
	}
	out, err := client.New(server, "").AdminDeleteTenant(context.Background(), client.AdminTenantDeleteRequest{
		TenantID: tenantID, PublicKey: publicKey, PrivateKey: privateKey,
	})
	if err != nil {
		return quotaAPIError("delete admin tenant", err)
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	fmt.Printf("tenant %s delete status: %s\n", out.TenantID, out.Status)
	return nil
}

func parseAdminTenantIDCommand(args []string, usage string) (tenantID, server, publicKey, privateKey string, asJSON bool, err error) {
	serverFlag := ""
	serverGiven := false
	regionCodeFlag := ""
	regionCodeGiven := false
	publicKeyFlag := ""
	publicKeyGiven := false
	privateKeyFlag := ""
	privateKeyGiven := false
	tenantGiven := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, usage)
			return "", "", "", "", false, errHelpRequested{}
		case "--server":
			if i+1 >= len(args) {
				return "", "", "", "", false, fmt.Errorf("--server requires an argument")
			}
			i++
			serverFlag = args[i]
			serverGiven = true
		case "--region-code":
			if i+1 >= len(args) {
				return "", "", "", "", false, fmt.Errorf("--region-code requires an argument")
			}
			i++
			regionCodeFlag = args[i]
			regionCodeGiven = true
		case "--tenant-id":
			if i+1 >= len(args) {
				return "", "", "", "", false, fmt.Errorf("--tenant-id requires an argument")
			}
			i++
			tenantID = args[i]
			tenantGiven = true
		case "--tidbcloud-public-key":
			if i+1 >= len(args) {
				return "", "", "", "", false, fmt.Errorf("--tidbcloud-public-key requires an argument")
			}
			i++
			publicKeyFlag = args[i]
			publicKeyGiven = true
		case "--tidbcloud-private-key":
			if i+1 >= len(args) {
				return "", "", "", "", false, fmt.Errorf("--tidbcloud-private-key requires an argument")
			}
			i++
			privateKeyFlag = args[i]
			privateKeyGiven = true
		case "--json":
			asJSON = true
		default:
			return "", "", "", "", false, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
		}
	}
	if err := rejectEmptyFlag("server", strings.TrimSpace(serverFlag), serverGiven); err != nil {
		return "", "", "", "", false, err
	}
	if err := rejectEmptyFlag("region-code", strings.TrimSpace(regionCodeFlag), regionCodeGiven); err != nil {
		return "", "", "", "", false, err
	}
	if err := rejectEmptyFlag("tenant-id", strings.TrimSpace(tenantID), tenantGiven); err != nil {
		return "", "", "", "", false, err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(publicKeyFlag), publicKeyGiven); err != nil {
		return "", "", "", "", false, err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(privateKeyFlag), privateKeyGiven); err != nil {
		return "", "", "", "", false, err
	}
	r := ResolveCredentials()
	server, err = quotaServer(serverFlag, regionCodeFlag, r.Server, true)
	if err != nil {
		return "", "", "", "", false, err
	}
	publicKey, privateKey = adminTiDBCloudKeys(publicKeyFlag, privateKeyFlag)
	req, err := quotaRequest(strings.TrimSpace(tenantID), publicKey, privateKey)
	if err != nil {
		return "", "", "", "", false, err
	}
	return req.TenantID, server, req.PublicKey, req.PrivateKey, asJSON, nil
}

func adminTiDBCloudKeys(publicKeyFlag, privateKeyFlag string) (string, string) {
	envPublicKey := consumeEnv(EnvTiDBCloudPublicKey)
	envPrivateKey := consumeEnv(EnvTiDBCloudPrivateKey)
	publicKey := strings.TrimSpace(publicKeyFlag)
	if publicKey == "" {
		publicKey = envPublicKey
	}
	privateKey := strings.TrimSpace(privateKeyFlag)
	if privateKey == "" {
		privateKey = envPrivateKey
	}
	return publicKey, privateKey
}

type errHelpRequested struct{}

func (errHelpRequested) Error() string { return "help requested" }

func adminUsage() string {
	return `usage: drive9 admin <tenant|quota> <command> [flags]`
}

func adminQuotaUsage() string {
	return `usage: drive9 admin quota <list|set> [flags]`
}

func adminTenantUsage() string {
	return `usage: drive9 admin tenant <list|get|delete> [flags]`
}

func adminTenantListUsage() string {
	return `usage: drive9 admin tenant list [flags]

flags:
  --server URL                    server URL
  --region-code CODE              TiDBCloud mode region code; ignored when --server is set
  --tidbcloud-public-key KEY      TiDB Cloud public key
  --tidbcloud-private-key KEY     TiDB Cloud private key
  --page-size N                   page size; default 10
  --page N                        page number; default 1
  --include-quota                 include quota in tenant list
  --json                          output result as JSON`
}

func adminTenantGetUsage() string {
	return `usage: drive9 admin tenant get --tenant-id ID [flags]`
}

func adminTenantDeleteUsage() string {
	return `usage: drive9 admin tenant delete --tenant-id ID [flags]`
}
