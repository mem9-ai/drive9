package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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
	case "get":
		return adminObjectBackendGet(args[1:])
	case "add":
		return adminObjectBackendAdd(args[1:])
	case "update":
		return adminObjectBackendUpdate(args[1:])
	case "rm", "delete":
		return adminObjectBackendDelete(args[1:])
	default:
		return fmt.Errorf("unknown admin object-backend command %q\n%s", args[0], adminObjectBackendUsage())
	}
}

func adminObjectBackendUsage() string {
	return `usage: drive9 admin object-backend <add|get|ls|update|rm> [flags]

manage org-level object-store credentials (TiDB Cloud AK/SK required).
Secrets are never printed. Tenant API keys cannot change this mapping.
An org may register multiple backends for the same bucket (different prefix
or endpoint). update rotates keys without delete+add.

commands:
  add     register a bucket + credential
  get     show one backend by id (no secrets)
  ls      list backends (no secrets)
  update  patch a backend by id (key rotation)
  rm      delete a backend by id

flags:
  --id ID
  --name LABEL
  --scheme s3|cos|tos|oss|gs|az
  --bucket NAME
  --endpoint URL                   object data-plane endpoint
  --sts-endpoint URL               STS/RAM/CAM endpoint (MinIO STS, custom)
  --region NAME
  --account-id ID                  Tencent APPID, Azure account, etc.
  --prefix PATH                    optional extra prefix above the tenant namespace
  --credential-kind static|role
  --role-arn ARN                   required for --credential-kind=role; also TOS/OSS
  --access-key-id KEY              required for static HMAC; Azure account name
  --secret-access-key SECRET       HMAC secret, Azure account key, or GCS SA JSON
                                   (or $DRIVE9_OBJECT_SECRET_ACCESS_KEY)
  --external-id ID
  --max-session-ttl SECONDS
  --force-path-style
  --no-force-path-style            update only
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
	_, _ = fmt.Fprintln(w, "ID\tNAME\tSCHEME\tBUCKET\tPREFIX\tKIND\tENDPOINT")
	for _, row := range rows {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", row.ID, row.Name, row.Scheme, row.Bucket, row.Prefix, row.CredentialKind, row.Endpoint)
	}
	return w.Flush()
}

func adminObjectBackendAdd(args []string) error {
	f, filtered, err := parseObjectBackendFieldFlags(args, false)
	if err != nil {
		return err
	}
	c, publicKey, privateKey, asJSON, err := resolveAdminObjectFlags(filtered, false, "")
	if err != nil {
		return err
	}
	out, err := c.AdminCreateObjectBackend(context.Background(), client.AdminObjectBackendCreateRequest{
		PublicKey:       publicKey,
		PrivateKey:      privateKey,
		Name:            f.name,
		Scheme:          f.scheme,
		Endpoint:        f.endpoint,
		STSEndpoint:     f.stsEndpoint,
		Region:          f.region,
		AccountID:       f.accountID,
		ForcePathStyle:  f.forcePathStyle,
		Bucket:          f.bucket,
		Prefix:          f.prefix,
		CredentialKind:  f.kind,
		RoleARN:         f.roleARN,
		AccessKeyID:     f.accessKey,
		SecretAccessKey: f.secret,
		ExternalID:      f.external,
		MaxSessionTTL:   f.maxTTL,
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

func adminObjectBackendGet(args []string) error {
	f, filtered, err := parseObjectBackendFieldFlags(args, true)
	if err != nil {
		return err
	}
	c, publicKey, privateKey, asJSON, err := resolveAdminObjectFlags(filtered, false, "")
	if err != nil {
		return err
	}
	if f.id == "" {
		return fmt.Errorf("usage: drive9 admin object-backend get --id ID")
	}
	out, err := c.AdminGetObjectBackend(context.Background(), f.id, publicKey, privateKey)
	if err != nil {
		return err
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	fmt.Printf("id: %s\nname: %s\nscheme: %s\nbucket: %s\nprefix: %s\nendpoint: %s\nsts_endpoint: %s\nregion: %s\naccount_id: %s\nkind: %s\nrole_arn: %s\nmax_session_ttl_sec: %d\n",
		out.ID, out.Name, out.Scheme, out.Bucket, out.Prefix, out.Endpoint, out.STSEndpoint, out.Region, out.AccountID, out.CredentialKind, out.RoleARN, out.MaxSessionTTL)
	return nil
}

func adminObjectBackendUpdate(args []string) error {
	f, filtered, err := parseObjectBackendFieldFlags(args, true)
	if err != nil {
		return err
	}
	c, publicKey, privateKey, asJSON, err := resolveAdminObjectFlags(filtered, false, "")
	if err != nil {
		return err
	}
	if f.id == "" {
		return fmt.Errorf("usage: drive9 admin object-backend update --id ID [flags]")
	}
	in := client.AdminObjectBackendUpdateRequest{PublicKey: publicKey, PrivateKey: privateKey}
	if f.nameSet {
		in.Name = &f.name
	}
	if f.schemeSet {
		in.Scheme = &f.scheme
	}
	if f.endpointSet {
		in.Endpoint = &f.endpoint
	}
	if f.stsEndpointSet {
		in.STSEndpoint = &f.stsEndpoint
	}
	if f.regionSet {
		in.Region = &f.region
	}
	if f.accountIDSet {
		in.AccountID = &f.accountID
	}
	if f.forcePathStyleSet {
		v := f.forcePathStyle
		in.ForcePathStyle = &v
	}
	if f.bucketSet {
		in.Bucket = &f.bucket
	}
	if f.prefixSet {
		in.Prefix = &f.prefix
	}
	if f.kindSet {
		in.CredentialKind = &f.kind
	}
	if f.roleARNSet {
		in.RoleARN = &f.roleARN
	}
	if f.accessKeySet {
		in.AccessKeyID = &f.accessKey
	}
	if f.secretSet {
		in.SecretAccessKey = &f.secret
	}
	if f.externalSet {
		in.ExternalID = &f.external
	}
	if f.maxTTLSet {
		v := f.maxTTL
		in.MaxSessionTTL = &v
	}
	out, err := c.AdminUpdateObjectBackend(context.Background(), f.id, in)
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
		return fmt.Errorf("namespace_id must not contain slashes or parent-directory segments")
	}
	if strings.ContainsAny(ns, "*?") {
		return fmt.Errorf("namespace_id must not contain wildcard characters")
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

type objectBackendFieldFlags struct {
	id, name, scheme, bucket, endpoint, stsEndpoint, region, accountID string
	kind, roleARN, accessKey, secret, external, prefix                 string
	forcePathStyle                                                     bool
	maxTTL                                                             int
	idSet, nameSet, schemeSet, bucketSet, endpointSet, stsEndpointSet  bool
	regionSet, accountIDSet, kindSet, roleARNSet, accessKeySet         bool
	secretSet, externalSet, prefixSet, forcePathStyleSet, maxTTLSet    bool
}

func parseObjectBackendFieldFlags(args []string, allowPositionalID bool) (objectBackendFieldFlags, []string, error) {
	var f objectBackendFieldFlags
	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		need := func(flag string) (string, error) {
			i++
			if i >= len(args) {
				return "", fmt.Errorf("%s requires an argument", flag)
			}
			return args[i], nil
		}
		switch args[i] {
		case "--id":
			v, err := need("--id")
			if err != nil {
				return f, nil, err
			}
			f.id, f.idSet = v, true
		case "--name":
			v, err := need("--name")
			if err != nil {
				return f, nil, err
			}
			f.name, f.nameSet = v, true
		case "--scheme":
			v, err := need("--scheme")
			if err != nil {
				return f, nil, err
			}
			f.scheme, f.schemeSet = v, true
		case "--bucket":
			v, err := need("--bucket")
			if err != nil {
				return f, nil, err
			}
			f.bucket, f.bucketSet = v, true
		case "--endpoint":
			v, err := need("--endpoint")
			if err != nil {
				return f, nil, err
			}
			f.endpoint, f.endpointSet = v, true
		case "--sts-endpoint":
			v, err := need("--sts-endpoint")
			if err != nil {
				return f, nil, err
			}
			f.stsEndpoint, f.stsEndpointSet = v, true
		case "--region":
			v, err := need("--region")
			if err != nil {
				return f, nil, err
			}
			f.region, f.regionSet = v, true
		case "--account-id":
			v, err := need("--account-id")
			if err != nil {
				return f, nil, err
			}
			f.accountID, f.accountIDSet = v, true
		case "--prefix":
			v, err := need("--prefix")
			if err != nil {
				return f, nil, err
			}
			f.prefix, f.prefixSet = v, true
		case "--credential-kind":
			v, err := need("--credential-kind")
			if err != nil {
				return f, nil, err
			}
			f.kind, f.kindSet = v, true
		case "--role-arn":
			v, err := need("--role-arn")
			if err != nil {
				return f, nil, err
			}
			f.roleARN, f.roleARNSet = v, true
		case "--access-key-id":
			v, err := need("--access-key-id")
			if err != nil {
				return f, nil, err
			}
			f.accessKey, f.accessKeySet = v, true
		case "--secret-access-key":
			v, err := need("--secret-access-key")
			if err != nil {
				return f, nil, err
			}
			f.secret, f.secretSet = v, true
		case "--external-id":
			v, err := need("--external-id")
			if err != nil {
				return f, nil, err
			}
			f.external, f.externalSet = v, true
		case "--max-session-ttl":
			v, err := need("--max-session-ttl")
			if err != nil {
				return f, nil, err
			}
			n, convErr := strconv.Atoi(v)
			if convErr != nil {
				return f, nil, fmt.Errorf("--max-session-ttl must be an integer")
			}
			f.maxTTL, f.maxTTLSet = n, true
		case "--force-path-style":
			f.forcePathStyle, f.forcePathStyleSet = true, true
		case "--no-force-path-style":
			f.forcePathStyle, f.forcePathStyleSet = false, true
		default:
			filtered = append(filtered, args[i])
		}
	}
	if allowPositionalID && f.id == "" && len(filtered) > 0 && filtered[0] != "" && filtered[0][0] != '-' {
		f.id = filtered[0]
		f.idSet = true
		filtered = filtered[1:]
	}
	if !f.secretSet {
		if v := strings.TrimSpace(os.Getenv("DRIVE9_OBJECT_SECRET_ACCESS_KEY")); v != "" {
			f.secret, f.secretSet = v, true
		}
	}
	return f, filtered, nil
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
