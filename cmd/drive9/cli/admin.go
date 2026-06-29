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
	case "tenant", "tenants":
		return adminTenant(args[1:])
	case "pool":
		return adminTenantPool(args[1:])
	default:
		return fmt.Errorf("unknown admin command %q\n%s", args[0], adminUsage())
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
	case "create":
		return adminTenantCreate(args[1:])
	case "list":
		return adminTenantList(args[1:], false)
	case "get":
		return adminTenantGet(args[1:])
	case "delete":
		return adminTenantDelete(args[1:])
	case "set-quota":
		return quotaSet(args[1:])
	case "pool":
		return adminTenantPool(args[1:])
	default:
		return fmt.Errorf("unknown admin tenant command %q\n%s", args[0], adminTenantUsage())
	}
}

func adminTenantCreate(args []string) error {
	serverFlag := ""
	serverGiven := false
	regionCodeFlag := ""
	regionCodeGiven := false
	publicKeyFlag := ""
	publicKeyGiven := false
	privateKeyFlag := ""
	privateKeyGiven := false
	var maxStorageSize *int64
	var maxFileSize *int64
	var maxFileCount *int64
	var tidbCloudSpendingLimit *int64
	asJSON := false

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, adminTenantCreateUsage())
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
		case "--max-storage-size":
			if i+1 >= len(args) {
				return fmt.Errorf("--max-storage-size requires an argument")
			}
			i++
			v, err := parsePositiveQuotaInt64Flag("--max-storage-size", args[i])
			if err != nil {
				return err
			}
			maxStorageSize = &v
		case "--max-file-size":
			if i+1 >= len(args) {
				return fmt.Errorf("--max-file-size requires an argument")
			}
			i++
			v, err := parsePositiveQuotaInt64Flag("--max-file-size", args[i])
			if err != nil {
				return err
			}
			maxFileSize = &v
		case "--max-file-count":
			if i+1 >= len(args) {
				return fmt.Errorf("--max-file-count requires an argument")
			}
			i++
			v, err := parseNonNegativeQuotaInt64Flag("--max-file-count", args[i])
			if err != nil {
				return err
			}
			maxFileCount = &v
		case "--tidbcloud-spending-limit":
			if i+1 >= len(args) {
				return fmt.Errorf("--tidbcloud-spending-limit requires an argument")
			}
			i++
			v, err := parseNonNegativeQuotaInt64Flag("--tidbcloud-spending-limit", args[i])
			if err != nil {
				return err
			}
			tidbCloudSpendingLimit = &v
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("unknown flag %q\n%s", args[i], adminTenantCreateUsage())
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
	out, err := client.New(server, "").AdminCreateTenant(context.Background(), client.AdminTenantCreateRequest{
		PublicKey:              publicKey,
		PrivateKey:             privateKey,
		MaxStorageSize:         maxStorageSize,
		MaxFileSize:            maxFileSize,
		MaxFileCount:           maxFileCount,
		TiDBCloudSpendingLimit: tidbCloudSpendingLimit,
	})
	if err != nil {
		return quotaAPIError("create admin tenant", err)
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	return printAdminTenantCreateResponse(out)
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
	if out == nil {
		return fmt.Errorf("admin tenant get response is empty")
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
	return printAdminTenantDeleteResponse(out)
}

func adminTenantPool(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminTenantPoolUsage())
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, adminTenantPoolUsage())
		return nil
	case "create":
		return adminTenantPoolCreate(args[1:])
	case "get":
		return adminTenantPoolGet(args[1:])
	case "update":
		return adminTenantPoolUpdate(args[1:])
	case "delete":
		return adminTenantPoolDelete(args[1:])
	default:
		return fmt.Errorf("unknown admin pool command %q\n%s", args[0], adminTenantPoolUsage())
	}
}

func adminTenantPoolCreate(args []string) error {
	req, server, asJSON, err := parseAdminTenantPoolCommand(args, adminTenantPoolCreateUsage(), true)
	if err != nil {
		if errors.Is(err, errHelpRequested{}) {
			return nil
		}
		return err
	}
	if req.PoolSize == nil || *req.PoolSize <= 0 {
		return fmt.Errorf("--pool-size must be positive")
	}
	out, err := client.New(server, "").AdminCreateTenantPool(context.Background(), req)
	if err != nil {
		return quotaAPIError("create admin tenant pool", err)
	}
	return printAdminTenantPoolResponse(out, asJSON)
}

func adminTenantPoolGet(args []string) error {
	req, server, asJSON, err := parseAdminTenantPoolCommand(args, adminTenantPoolGetUsage(), false)
	if err != nil {
		if errors.Is(err, errHelpRequested{}) {
			return nil
		}
		return err
	}
	out, err := client.New(server, "").AdminGetTenantPool(context.Background(), req)
	if err != nil {
		return quotaAPIError("get admin tenant pool", err)
	}
	return printAdminTenantPoolResponse(out, asJSON)
}

func adminTenantPoolUpdate(args []string) error {
	req, server, asJSON, err := parseAdminTenantPoolCommand(args, adminTenantPoolUpdateUsage(), true)
	if err != nil {
		if errors.Is(err, errHelpRequested{}) {
			return nil
		}
		return err
	}
	if req.PoolSize == nil || *req.PoolSize <= 0 {
		return fmt.Errorf("--pool-size must be positive")
	}
	out, err := client.New(server, "").AdminUpdateTenantPool(context.Background(), req)
	if err != nil {
		return quotaAPIError("update admin tenant pool", err)
	}
	return printAdminTenantPoolResponse(out, asJSON)
}

func adminTenantPoolDelete(args []string) error {
	req, server, asJSON, err := parseAdminTenantPoolCommand(args, adminTenantPoolDeleteUsage(), false)
	if err != nil {
		if errors.Is(err, errHelpRequested{}) {
			return nil
		}
		return err
	}
	out, err := client.New(server, "").AdminDeleteTenantPool(context.Background(), req)
	if err != nil {
		return quotaAPIError("delete admin tenant pool", err)
	}
	return printAdminTenantPoolResponse(out, asJSON)
}

func parseAdminTenantPoolCommand(args []string, usage string, allowPoolSize bool) (req client.AdminTenantPoolRequest, server string, asJSON bool, err error) {
	serverFlag := ""
	serverGiven := false
	regionCodeFlag := ""
	regionCodeGiven := false
	publicKeyFlag := ""
	publicKeyGiven := false
	privateKeyFlag := ""
	privateKeyGiven := false
	poolSizeGiven := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, usage)
			return req, "", false, errHelpRequested{}
		case "--server":
			if i+1 >= len(args) {
				return req, "", false, fmt.Errorf("--server requires an argument")
			}
			i++
			serverFlag = args[i]
			serverGiven = true
		case "--region-code":
			if i+1 >= len(args) {
				return req, "", false, fmt.Errorf("--region-code requires an argument")
			}
			i++
			regionCodeFlag = args[i]
			regionCodeGiven = true
		case "--tidbcloud-public-key":
			if i+1 >= len(args) {
				return req, "", false, fmt.Errorf("--tidbcloud-public-key requires an argument")
			}
			i++
			publicKeyFlag = args[i]
			publicKeyGiven = true
		case "--tidbcloud-private-key":
			if i+1 >= len(args) {
				return req, "", false, fmt.Errorf("--tidbcloud-private-key requires an argument")
			}
			i++
			privateKeyFlag = args[i]
			privateKeyGiven = true
		case "--pool-size":
			if !allowPoolSize {
				return req, "", false, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
			}
			if i+1 >= len(args) {
				return req, "", false, fmt.Errorf("--pool-size requires an argument")
			}
			i++
			v, parseErr := parseNonNegativeQuotaInt64Flag("--pool-size", args[i])
			if parseErr != nil {
				return req, "", false, parseErr
			}
			poolSize := int(v)
			req.PoolSize = &poolSize
			poolSizeGiven = true
		case "--json":
			asJSON = true
		default:
			return req, "", false, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
		}
	}
	if allowPoolSize && !poolSizeGiven {
		return req, "", false, fmt.Errorf("--pool-size is required")
	}
	if err := rejectEmptyFlag("server", strings.TrimSpace(serverFlag), serverGiven); err != nil {
		return req, "", false, err
	}
	if err := rejectEmptyFlag("region-code", strings.TrimSpace(regionCodeFlag), regionCodeGiven); err != nil {
		return req, "", false, err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(publicKeyFlag), publicKeyGiven); err != nil {
		return req, "", false, err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(privateKeyFlag), privateKeyGiven); err != nil {
		return req, "", false, err
	}
	r := ResolveCredentials()
	server, err = quotaServer(serverFlag, regionCodeFlag, r.Server, true)
	if err != nil {
		return req, "", false, err
	}
	req.PublicKey, req.PrivateKey = adminTiDBCloudKeys(publicKeyFlag, privateKeyFlag)
	if strings.TrimSpace(req.PublicKey) == "" || strings.TrimSpace(req.PrivateKey) == "" {
		return req, "", false, fmt.Errorf("TiDB Cloud credentials are required; pass --tidbcloud-public-key and --tidbcloud-private-key or set %s/%s", EnvTiDBCloudPublicKey, EnvTiDBCloudPrivateKey)
	}
	return req, server, asJSON, nil
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
	return `usage: drive9 admin <command> [arguments]

commands:
  tenant create [flags]             create a TiDBCloud Mode tenant
  tenant list [flags]               list TiDBCloud Mode tenants
  tenant get --tenant-id ID         show one tenant and quota
  tenant delete --tenant-id ID      delete one tenant
  tenant set-quota --tenant-id ID   set quota for one tenant
  pool <command> [flags]            manage the tenant pool

global admin flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON when supported

examples:
  drive9 admin tenant create --region-code aws-ap-southeast-1 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant list --region-code aws-ap-southeast-1 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant get --tenant-id tnt_xxx \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant set-quota --tenant-id tnt_xxx --max-storage-size 102400 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant delete --tenant-id tnt_xxx \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin pool create --pool-size 10 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}

func adminTenantUsage() string {
	return `usage: drive9 admin tenant <command> [arguments]

commands:
  create [flags]                   create a TiDBCloud Mode tenant
  list [flags]                     list TiDBCloud Mode tenants
  get --tenant-id ID               show one tenant and quota
  delete --tenant-id ID            delete one tenant
  set-quota --tenant-id ID         set quota for one tenant

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tenant-id ID                   drive9 tenant id for get/delete/set-quota
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --max-storage-size Mi            set-quota: max confirmed+reserved storage size in Mi
  --max-file-size Mi               set-quota: max single file size in Mi
  --max-file-count N               set-quota: max confirmed file count; 0 means unlimited
  --tidbcloud-spending-limit N     set-quota: TiDB Cloud Cluster Spending Limit
  --json                           output result as JSON when supported

examples:
  drive9 admin tenant create --region-code aws-ap-southeast-1 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant list --page-size 20 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant delete --tenant-id tnt_xxx \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant set-quota --tenant-id tnt_xxx --max-storage-size 102400 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}

func adminTenantCreateUsage() string {
	return `usage: drive9 admin tenant create [flags]

create a TiDBCloud Mode tenant using TiDB Cloud credentials. Optional quota
flags use the same units and validation as "drive9 admin tenant set-quota".

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --max-storage-size Mi            max confirmed+reserved storage size in Mi
  --max-file-size Mi               max single file size in Mi; must not exceed server DRIVE9_MAX_UPLOAD_BYTES
  --max-file-count N               max confirmed file count; 0 means unlimited
  --tidbcloud-spending-limit N     TiDB Cloud Cluster Spending Limit; must be non-negative
  --json                           output result as JSON

examples:
  drive9 admin tenant create --region-code aws-ap-southeast-1 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant create --max-storage-size 102400 --max-file-size 1024 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}

func adminTenantListUsage() string {
	return `usage: drive9 admin tenant list [flags]

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --page-size N                    page size; default 10, max 100
  --page N                         page number; default 1
  --include-quota                  include quota in JSON output; text output includes quota columns
  --json                           output result as JSON

examples:
  drive9 admin tenant list --page-size 20 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant list --json --include-quota \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}

func adminTenantGetUsage() string {
	return `usage: drive9 admin tenant get --tenant-id ID [flags]

show one TiDBCloud Mode tenant. The text output includes quota columns.

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tenant-id ID                   drive9 tenant id
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON

example:
  drive9 admin tenant get --tenant-id tnt_xxx \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}

func adminTenantDeleteUsage() string {
	return `usage: drive9 admin tenant delete --tenant-id ID [flags]

delete one TiDBCloud Mode tenant. This requires TiDB Cloud credentials with
cluster label patch permission.

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tenant-id ID                   drive9 tenant id
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON

example:
  drive9 admin tenant delete --tenant-id tnt_xxx \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}

func adminTenantPoolUsage() string {
	return `usage: drive9 admin pool <command> [arguments]

commands:
  create --pool-size N             create a tenant pool
  get                              show the tenant pool
  update --pool-size N             update tenant pool size
  delete                           delete the tenant pool's free tenants

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --pool-size N                    pool target free tenant count
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON

examples:
  drive9 admin pool create --pool-size 10 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin pool get \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin pool update --pool-size 20 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin pool delete \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}

func adminTenantPoolCreateUsage() string {
	return `usage: drive9 admin pool create --pool-size N [flags]

create a TiDBCloud Mode tenant pool. New pool clusters are labeled free.

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --pool-size N                    number of free tenants to pre-create; must be positive
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON`
}

func adminTenantPoolGetUsage() string {
	return `usage: drive9 admin pool get [flags]

show the TiDBCloud Mode tenant pool.

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON`
}

func adminTenantPoolUpdateUsage() string {
	return `usage: drive9 admin pool update --pool-size N [flags]

update TiDBCloud Mode tenant pool size. Increasing creates free tenants;
decreasing deletes newest free tenants.

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --pool-size N                    target free tenant count; must be positive
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON`
}

func adminTenantPoolDeleteUsage() string {
	return `usage: drive9 admin pool delete [flags]

delete the TiDBCloud Mode tenant pool's free tenants and remove the pool.

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON`
}

func printAdminTenantCreateResponse(out *client.AdminTenantCreateResponse) error {
	if out == nil {
		return fmt.Errorf("admin tenant create response is empty")
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TENANT_ID\tSTATUS\tCLOUD_PROVIDER\tREGION\tAPI_KEY")
	_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
		out.TenantID,
		out.Status,
		emptyAsDash(out.CloudProvider),
		emptyAsDash(out.Region),
		out.APIKey,
	)
	return w.Flush()
}

func printAdminTenantPoolResponse(out *client.AdminTenantPoolResponse, asJSON bool) error {
	if out == nil {
		return fmt.Errorf("admin tenant pool response is empty")
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "POOL_ID\tORGANIZATION_ID\tPOOL_SIZE\tFREE_SIZE\tSTATUS")
	_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n",
		out.PoolID,
		emptyAsDash(out.OrganizationID),
		out.PoolSize,
		out.FreeSize,
		out.Status,
	)
	return w.Flush()
}

func printAdminTenantDeleteResponse(out *client.AdminTenantDeleteResponse) error {
	if out == nil {
		return fmt.Errorf("admin tenant delete response is empty")
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TENANT_ID\tSTATUS")
	_, _ = fmt.Fprintf(w, "%s\t%s\n", out.TenantID, out.Status)
	return w.Flush()
}

func emptyAsDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}
