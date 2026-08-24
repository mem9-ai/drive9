package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/mem9-ai/drive9/pkg/client"
)

func adminObjectBackend(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminObjectBackendUsage())
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, adminObjectBackendUsage())
		return nil
	case "ls", "list":
		return adminObjectBackendList(args[1:])
	case "add":
		return adminObjectBackendAdd(args[1:])
	case "rm", "delete":
		return adminObjectBackendDelete(args[1:])
	default:
		return fmt.Errorf("unknown admin object-backend command %q\n%s", args[0], adminObjectBackendUsage())
	}
}

func adminObjectBackendUsage() string {
	return `usage: drive9 admin object-backend <add|ls|rm> [flags]

manage org-level object-store credentials (TiDB Cloud AK/SK required).
Secrets are never printed. Tenant API keys cannot change this mapping.

commands:
  add     register a bucket + credential
  ls      list backends (no secrets)
  rm      delete a backend by id

flags:
  --scheme s3|cos|tos|oss
  --bucket NAME
  --endpoint URL
  --region NAME
  --prefix PATH                    optional extra prefix above the tenant namespace
  --credential-kind static|role
  --role-arn ARN                   required for --credential-kind=role
  --access-key-id KEY              required for static; optional for role (else server IAM)
  --secret-access-key SECRET       required for static
  --external-id ID
  --force-path-style
  --tidbcloud-public-key KEY
  --tidbcloud-private-key KEY
  --json
`
}

func adminObjectBackendList(args []string) error {
	c, publicKey, privateKey, asJSON, err := resolveAdminObjectFlags(args, false, "")
	if err != nil {
		return err
	}
	rows, err := c.AdminListObjectBackends(context.Background(), publicKey, privateKey)
	if err != nil {
		return err
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{"backends": rows})
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tSCHEME\tBUCKET\tKIND\tENDPOINT")
	for _, row := range rows {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", row.ID, row.Scheme, row.Bucket, row.CredentialKind, row.Endpoint)
	}
	return w.Flush()
}

func adminObjectBackendAdd(args []string) error {
	var (
		scheme, bucket, endpoint, region, kind, roleARN, accessKey, secret, external, prefix string
		forcePathStyle                                                                       bool
	)
	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--scheme":
			i++
			if i >= len(args) {
				return fmt.Errorf("--scheme requires an argument")
			}
			scheme = args[i]
		case "--bucket":
			i++
			if i >= len(args) {
				return fmt.Errorf("--bucket requires an argument")
			}
			bucket = args[i]
		case "--endpoint":
			i++
			if i >= len(args) {
				return fmt.Errorf("--endpoint requires an argument")
			}
			endpoint = args[i]
		case "--region":
			i++
			if i >= len(args) {
				return fmt.Errorf("--region requires an argument")
			}
			region = args[i]
		case "--prefix":
			i++
			if i >= len(args) {
				return fmt.Errorf("--prefix requires an argument")
			}
			prefix = args[i]
		case "--credential-kind":
			i++
			if i >= len(args) {
				return fmt.Errorf("--credential-kind requires an argument")
			}
			kind = args[i]
		case "--role-arn":
			i++
			if i >= len(args) {
				return fmt.Errorf("--role-arn requires an argument")
			}
			roleARN = args[i]
		case "--access-key-id":
			i++
			if i >= len(args) {
				return fmt.Errorf("--access-key-id requires an argument")
			}
			accessKey = args[i]
		case "--secret-access-key":
			i++
			if i >= len(args) {
				return fmt.Errorf("--secret-access-key requires an argument")
			}
			secret = args[i]
		case "--external-id":
			i++
			if i >= len(args) {
				return fmt.Errorf("--external-id requires an argument")
			}
			external = args[i]
		case "--force-path-style":
			forcePathStyle = true
		default:
			filtered = append(filtered, args[i])
		}
	}
	c, publicKey, privateKey, asJSON, err := resolveAdminObjectFlags(filtered, false, "")
	if err != nil {
		return err
	}
	out, err := c.AdminCreateObjectBackend(context.Background(), client.AdminObjectBackendCreateRequest{
		PublicKey:       publicKey,
		PrivateKey:      privateKey,
		Scheme:          scheme,
		Endpoint:        endpoint,
		Region:          region,
		ForcePathStyle:  forcePathStyle,
		Bucket:          bucket,
		Prefix:          prefix,
		CredentialKind:  kind,
		RoleARN:         roleARN,
		AccessKeyID:     accessKey,
		SecretAccessKey: secret,
		ExternalID:      external,
	})
	if err != nil {
		return err
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	fmt.Printf("id: %s\nscheme: %s\nbucket: %s\n", out.ID, out.Scheme, out.Bucket)
	return nil
}

func adminObjectBackendDelete(args []string) error {
	id := ""
	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] == "--id" {
			i++
			if i >= len(args) {
				return fmt.Errorf("--id requires an argument")
			}
			id = args[i]
			continue
		}
		filtered = append(filtered, args[i])
	}
	if id == "" && len(filtered) > 0 && filtered[0] != "" && filtered[0][0] != '-' {
		id = filtered[0]
		filtered = filtered[1:]
	}
	c, publicKey, privateKey, _, err := resolveAdminObjectFlags(filtered, false, "")
	if err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("usage: drive9 admin object-backend rm --id ID")
	}
	return c.AdminDeleteObjectBackend(context.Background(), id, publicKey, privateKey)
}

func adminTenantObjectNamespace(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminTenantObjectNamespaceUsage())
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, adminTenantObjectNamespaceUsage())
		return nil
	case "get":
		return adminTenantObjectNamespaceGet(args[1:])
	case "set":
		return adminTenantObjectNamespaceSet(args[1:])
	case "clear":
		return adminTenantObjectNamespaceClear(args[1:])
	default:
		return fmt.Errorf("unknown object-namespace command %q\n%s", args[0], adminTenantObjectNamespaceUsage())
	}
}

func adminTenantObjectNamespaceUsage() string {
	return `usage: drive9 admin tenant object-namespace <get|set|clear> --tenant-id ID [flags]

bind this drive9 tenant to a customer-owned object prefix id.
Empty namespace means object-store mint is not allowed for the tenant.

flags:
  --tenant-id ID                   required
  --namespace-id ID                required for set; customer id, not drive9 tenant_id
  --tidbcloud-public-key KEY
  --tidbcloud-private-key KEY
  --json
`
}

func adminTenantObjectNamespaceGet(args []string) error {
	c, publicKey, privateKey, asJSON, tenantID, err := resolveAdminObjectFlagsTenant(args)
	if err != nil {
		return err
	}
	out, err := c.AdminGetObjectNamespace(context.Background(), tenantID, publicKey, privateKey)
	if err != nil {
		return err
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	fmt.Printf("tenant_id: %s\nnamespace_id: %s\n", out.TenantID, out.NamespaceID)
	return nil
}

func adminTenantObjectNamespaceSet(args []string) error {
	ns := ""
	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] == "--namespace-id" {
			i++
			if i >= len(args) {
				return fmt.Errorf("--namespace-id requires an argument")
			}
			ns = args[i]
			continue
		}
		filtered = append(filtered, args[i])
	}
	c, publicKey, privateKey, asJSON, tenantID, err := resolveAdminObjectFlagsTenant(filtered)
	if err != nil {
		return err
	}
	if ns == "" {
		return fmt.Errorf("--namespace-id is required")
	}
	if strings.ContainsAny(ns, "/\\") || strings.Contains(ns, "..") {
		return fmt.Errorf("namespace_id must not contain slashes or ..")
	}
	out, err := c.AdminSetObjectNamespace(context.Background(), tenantID, ns, publicKey, privateKey)
	if err != nil {
		return err
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	fmt.Printf("tenant_id: %s\nnamespace_id: %s\n", out.TenantID, out.NamespaceID)
	return nil
}

func adminTenantObjectNamespaceClear(args []string) error {
	c, publicKey, privateKey, _, tenantID, err := resolveAdminObjectFlagsTenant(args)
	if err != nil {
		return err
	}
	return c.AdminClearObjectNamespace(context.Background(), tenantID, publicKey, privateKey)
}

func resolveAdminObjectFlags(args []string, requireTenant bool, _ string) (*client.Client, string, string, bool, error) {
	c, pk, sk, asJSON, _, err := parseAdminObjectCommon(args, requireTenant)
	return c, pk, sk, asJSON, err
}

func resolveAdminObjectFlagsTenant(args []string) (*client.Client, string, string, bool, string, error) {
	return parseAdminObjectCommon(args, true)
}

func parseAdminObjectCommon(args []string, requireTenant bool) (*client.Client, string, string, bool, string, error) {
	serverFlag := ""
	regionCode := ""
	publicKey := ""
	privateKey := ""
	tenantID := ""
	asJSON := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--server":
			i++
			if i >= len(args) {
				return nil, "", "", false, "", fmt.Errorf("--server requires an argument")
			}
			serverFlag = args[i]
		case "--region-code":
			i++
			if i >= len(args) {
				return nil, "", "", false, "", fmt.Errorf("--region-code requires an argument")
			}
			regionCode = args[i]
		case "--tidbcloud-public-key":
			i++
			if i >= len(args) {
				return nil, "", "", false, "", fmt.Errorf("--tidbcloud-public-key requires an argument")
			}
			publicKey = args[i]
		case "--tidbcloud-private-key":
			i++
			if i >= len(args) {
				return nil, "", "", false, "", fmt.Errorf("--tidbcloud-private-key requires an argument")
			}
			privateKey = args[i]
		case "--tenant-id":
			i++
			if i >= len(args) {
				return nil, "", "", false, "", fmt.Errorf("--tenant-id requires an argument")
			}
			tenantID = args[i]
		case "--json":
			asJSON = true
		default:
			return nil, "", "", false, "", fmt.Errorf("unknown flag %q", args[i])
		}
	}
	publicKey, privateKey = adminTiDBCloudKeys(publicKey, privateKey)
	if publicKey == "" || privateKey == "" {
		return nil, "", "", false, "", fmt.Errorf("TiDB Cloud public and private keys are required")
	}
	if requireTenant && tenantID == "" {
		return nil, "", "", false, "", fmt.Errorf("--tenant-id is required")
	}
	r := ResolveCredentials()
	server, err := quotaServer(serverFlag, regionCode, r.Server, true)
	if err != nil {
		return nil, "", "", false, "", err
	}
	return client.New(server, ""), publicKey, privateKey, asJSON, tenantID, nil
}
