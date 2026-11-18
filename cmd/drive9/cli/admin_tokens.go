package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const adminGenerateTokenTimeout = 30 * time.Second

func adminTenantGenerateToken(args []string) error {
	if migrationWantsHelp(args) {
		_, _ = fmt.Fprintln(os.Stdout, adminTenantGenerateTokenUsage())
		return nil
	}
	var (
		serverFlag      string
		serverGiven     bool
		regionCodeFlag  string
		regionCodeGiven bool
		publicKeyFlag   string
		publicKeyGiven  bool
		privateKeyFlag  string
		privateKeyGiven bool
		tenantID        string
		tenantIDGiven   bool
		keyName         string
		keyNameGiven    bool
		ttlRaw          string
		ttlGiven        bool
		scopes          []client.FSScopeGrant
		asJSON          bool
		tokenOnly       bool
	)
	for i := 0; i < len(args); i++ {
		switch args[i] {
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
		case "--tenant-id":
			if i+1 >= len(args) {
				return fmt.Errorf("--tenant-id requires an argument")
			}
			i++
			tenantID = args[i]
			tenantIDGiven = true
		case "--key-name":
			if i+1 >= len(args) {
				return fmt.Errorf("--key-name requires an argument")
			}
			i++
			keyName = args[i]
			keyNameGiven = true
		case "--ttl":
			if i+1 >= len(args) {
				return fmt.Errorf("--ttl requires an argument")
			}
			i++
			ttlRaw = args[i]
			ttlGiven = true
		case "--allow":
			if i+1 >= len(args) {
				return fmt.Errorf("--allow requires an argument")
			}
			i++
			scope, err := parseTokenAllow(args[i])
			if err != nil {
				return fmt.Errorf("invalid --allow %q: %w", args[i], err)
			}
			scopes = append(scopes, scope)
		case "--json":
			asJSON = true
		case "--print", "--token-only":
			tokenOnly = true
		default:
			return fmt.Errorf("unknown flag %q\n%s", args[i], adminTenantGenerateTokenUsage())
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
	if err := rejectEmptyFlag("tenant-id", strings.TrimSpace(tenantID), tenantIDGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("key-name", strings.TrimSpace(keyName), keyNameGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("ttl", strings.TrimSpace(ttlRaw), ttlGiven); err != nil {
		return err
	}
	if asJSON && tokenOnly {
		return fmt.Errorf("--json and --token-only are mutually exclusive")
	}

	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return fmt.Errorf("--tenant-id is required\n%s", adminTenantGenerateTokenUsage())
	}
	if strings.ContainsAny(tenantID, " \t\r\n/?#\\") {
		return fmt.Errorf("tenant id %q contains an unsupported character", tenantID)
	}
	keyName = strings.TrimSpace(keyName)
	if len(keyName) > 64 {
		return fmt.Errorf("--key-name must be at most 64 bytes")
	}

	var ttlSeconds *int64
	if ttlGiven {
		ttl, err := time.ParseDuration(strings.TrimSpace(ttlRaw))
		if err != nil {
			return fmt.Errorf("invalid --ttl %q: %w", ttlRaw, err)
		}
		if ttl < time.Second {
			return fmt.Errorf("--ttl must be at least 1s")
		}
		sec := int64(ttl / time.Second)
		ttlSeconds = &sec
	} else if len(scopes) > 0 {
		return fmt.Errorf("--ttl is required when --allow is specified")
	}

	r := ResolveCredentials()
	publicKey, privateKey := adminTiDBCloudKeys(publicKeyFlag, privateKeyFlag)
	if publicKey == "" || privateKey == "" {
		return fmt.Errorf("TiDB Cloud credentials are required; pass --tidbcloud-public-key and --tidbcloud-private-key or set %s/%s", EnvTiDBCloudPublicKey, EnvTiDBCloudPrivateKey)
	}
	server, err := quotaServer(serverFlag, regionCodeFlag, r.Server, true)
	if err != nil {
		return err
	}
	server = strings.TrimSpace(server)
	if server == "" {
		return fmt.Errorf("server URL is required; pass --server or set %s", EnvServer)
	}

	ctx, cancel := context.WithTimeout(context.Background(), adminGenerateTokenTimeout)
	defer cancel()
	out, err := client.New(server, "").AdminGenerateToken(ctx, client.AdminGenerateTokenRequest{
		TenantID:   tenantID,
		KeyName:    keyName,
		TTLSeconds: ttlSeconds,
		Scopes:     scopes,
		PublicKey:  publicKey,
		PrivateKey: privateKey,
	})
	if err != nil {
		return quotaAPIError("generate tenant token", err)
	}

	switch {
	case tokenOnly:
		_, err := fmt.Fprintln(os.Stdout, out.Token)
		return err
	case asJSON:
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	default:
		return printAdminGenerateTokenResponse(out)
	}
}

func printAdminGenerateTokenResponse(out *client.AdminGenerateTokenResponse) error {
	if out == nil {
		return fmt.Errorf("admin generate token response is empty")
	}
	expiresAt := "-"
	if out.ExpiresAt != nil && !out.ExpiresAt.IsZero() {
		expiresAt = out.ExpiresAt.UTC().Format(time.RFC3339)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TENANT_ID\tTOKEN_ID\tKEY_NAME\tSCOPE_KIND\tEXPIRES_AT\tTOKEN")
	_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
		emptyAsDash(out.TenantID),
		emptyAsDash(out.TokenID),
		emptyAsDash(out.KeyName),
		emptyAsDash(out.ScopeKind),
		expiresAt,
		out.Token,
	)
	return w.Flush()
}

func adminTenantGenerateTokenUsage() string {
	return `usage: drive9 admin tenant generate-token --tenant-id ID [flags]

generate an owner or fs_scoped API key for an active tenant using TiDB Cloud
credentials (for example, on the target region after a migration reaches
"switched").

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tenant-id ID                   drive9 tenant id
  --key-name NAME                  optional key label (default: "default", max 64 bytes)
  --ttl DURATION                   optional lifetime such as 24h (required with --allow)
  --allow PREFIX:OPS               repeatable scoped grant; omit for an owner key
  --token-only                     print only the raw bearer token
  --print                          alias for --token-only
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON

examples:
  drive9 admin tenant generate-token --tenant-id tnt_xxx \
    --server https://target.example.com \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>

  drive9 admin tenant generate-token --tenant-id tnt_xxx --token-only \
    --ttl 24h --allow /workspace:read,list,write \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}
