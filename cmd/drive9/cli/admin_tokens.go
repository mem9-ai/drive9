package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const adminGenerateTokenTimeout = 30 * time.Second

func nextAdminTokenFlagValue(flagName string, args []string, i int) (string, error) {
	if i+1 >= len(args) {
		return "", fmt.Errorf("%s requires an argument", flagName)
	}
	val := args[i+1]
	if strings.HasPrefix(val, "--") || val == "-h" || val == "-help" {
		return "", fmt.Errorf("%s requires a value, got flag %q", flagName, val)
	}
	return val, nil
}

func adminTenantGenerateToken(args []string) error {
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
		tokenOnlyFlag   string
	)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help":
			_, _ = fmt.Fprintln(os.Stdout, adminTenantGenerateTokenUsage())
			return nil
		case "help":
			if i == 0 {
				_, _ = fmt.Fprintln(os.Stdout, adminTenantGenerateTokenUsage())
				return nil
			}
			return fmt.Errorf("unknown flag %q\n%s", args[i], adminTenantGenerateTokenUsage())
		case "--server":
			val, err := nextAdminTokenFlagValue("--server", args, i)
			if err != nil {
				return err
			}
			i++
			serverFlag = val
			serverGiven = true
		case "--region-code":
			val, err := nextAdminTokenFlagValue("--region-code", args, i)
			if err != nil {
				return err
			}
			i++
			regionCodeFlag = val
			regionCodeGiven = true
		case "--tidbcloud-public-key":
			val, err := nextAdminTokenFlagValue("--tidbcloud-public-key", args, i)
			if err != nil {
				return err
			}
			i++
			publicKeyFlag = val
			publicKeyGiven = true
		case "--tidbcloud-private-key":
			val, err := nextAdminTokenFlagValue("--tidbcloud-private-key", args, i)
			if err != nil {
				return err
			}
			i++
			privateKeyFlag = val
			privateKeyGiven = true
		case "--tenant-id":
			val, err := nextAdminTokenFlagValue("--tenant-id", args, i)
			if err != nil {
				return err
			}
			i++
			tenantID = val
			tenantIDGiven = true
		case "--key-name":
			val, err := nextAdminTokenFlagValue("--key-name", args, i)
			if err != nil {
				return err
			}
			i++
			keyName = val
			keyNameGiven = true
		case "--ttl":
			val, err := nextAdminTokenFlagValue("--ttl", args, i)
			if err != nil {
				return err
			}
			i++
			ttlRaw = val
			ttlGiven = true
		case "--allow":
			val, err := nextAdminTokenFlagValue("--allow", args, i)
			if err != nil {
				return err
			}
			i++
			scope, err := parseTokenAllow(val)
			if err != nil {
				return fmt.Errorf("invalid --allow %q: %w", val, err)
			}
			scopes = append(scopes, scope)
		case "--json":
			asJSON = true
		case "--print", "--token-only":
			tokenOnly = true
			tokenOnlyFlag = args[i]
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
		return fmt.Errorf("--json and %s are mutually exclusive", tokenOnlyFlag)
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
	warnNonLoopbackHTTPServer(server)

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

func warnNonLoopbackHTTPServer(rawURL string) {
	u, err := url.Parse(rawURL)
	if err != nil || !strings.EqualFold(u.Scheme, "http") {
		return
	}
	host := strings.TrimSpace(u.Hostname())
	if host == "" || strings.EqualFold(host, "localhost") {
		return
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return
	}
	_, _ = fmt.Fprintf(os.Stderr, "warning: sending TiDB Cloud credentials over plaintext HTTP to %s\n", host)
}

func adminTenantGenerateTokenUsage() string {
	return `usage: drive9 admin tenant generate-token --tenant-id ID [flags]

generate an owner or fs_scoped API key for an active TiDB Cloud Mode tenant using
TiDB Cloud organization credentials (POST /v1/tokens/generate; for example, on
the target region after a migration reaches "switched").

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tenant-id ID                   drive9 tenant id
  --key-name NAME                  optional key label (default: "default", max 64 bytes)
  --ttl DURATION                   optional owner-key lifetime or required scoped-key lifetime (up to 8760h)
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
