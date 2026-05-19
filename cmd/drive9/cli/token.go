package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Token manages workspace-zone scoped filesystem tokens.
func Token(args []string) error {
	if len(args) == 0 {
		return tokenUsageErr()
	}
	switch args[0] {
	case "issue":
		return TokenIssue(args[1:])
	case "revoke":
		return TokenRevoke(args[1:])
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprint(os.Stdout, tokenUsage())
		return nil
	default:
		return fmt.Errorf("unknown token command %q\n%s", args[0], tokenUsage())
	}
}

func tokenUsage() string {
	return `usage: drive9 token <command> [arguments]

commands:
  issue --subject <name> --ttl <duration> --allow <prefix:ops>
                       issue an fs_scoped token
  revoke <token_id>   revoke an fs_scoped token

issue flags:
  --subject <name>    label stored as token key_name
  --ttl <duration>    required positive duration, e.g. 1h, 24h
  --allow <prefix:ops>
                       repeatable; ops are comma-separated read,list,search,write,delete
  --json              print full JSON response
  --token-only        print only the bearer token
`
}

func tokenUsageErr() error {
	return fmt.Errorf("%s", tokenUsage())
}

func TokenIssue(args []string) error {
	var (
		subject   string
		ttlRaw    string
		asJSON    bool
		tokenOnly bool
		scopes    []client.FSScopeGrant
	)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--subject":
			if i+1 >= len(args) {
				return fmt.Errorf("--subject requires a value")
			}
			i++
			subject = args[i]
		case "--ttl":
			if i+1 >= len(args) {
				return fmt.Errorf("--ttl requires a value")
			}
			i++
			ttlRaw = args[i]
		case "--allow":
			if i+1 >= len(args) {
				return fmt.Errorf("--allow requires a value")
			}
			i++
			scope, err := parseTokenAllow(args[i])
			if err != nil {
				return fmt.Errorf("invalid --allow %q: %w", args[i], err)
			}
			scopes = append(scopes, scope)
		case "--json":
			asJSON = true
		case "--token-only":
			tokenOnly = true
		default:
			return fmt.Errorf("unknown flag %q", args[i])
		}
	}
	if asJSON && tokenOnly {
		return fmt.Errorf("--json and --token-only are mutually exclusive")
	}
	if strings.TrimSpace(subject) == "" {
		return fmt.Errorf("--subject is required")
	}
	if ttlRaw == "" {
		return fmt.Errorf("--ttl is required")
	}
	ttl, err := time.ParseDuration(ttlRaw)
	if err != nil {
		return fmt.Errorf("invalid --ttl %q: %w", ttlRaw, err)
	}
	if ttl < time.Second {
		return fmt.Errorf("--ttl must be at least 1s")
	}
	if len(scopes) == 0 {
		return fmt.Errorf("at least one --allow is required")
	}

	c, err := newTokenManagementClientFromEnv()
	if err != nil {
		return err
	}
	resp, err := c.IssueScopedToken(context.Background(), client.IssueScopedTokenRequest{
		Subject:    strings.TrimSpace(subject),
		TTLSeconds: int64(ttl / time.Second),
		Scopes:     scopes,
	})
	if err != nil {
		return err
	}
	switch {
	case tokenOnly:
		_, _ = fmt.Fprintln(os.Stdout, resp.Token)
	case asJSON:
		return writeJSON(resp)
	default:
		_, _ = fmt.Fprintf(os.Stdout, "token=%s\n", resp.Token)
		_, _ = fmt.Fprintf(os.Stdout, "token_id=%s\n", resp.TokenID)
		if resp.ExpiresAt != nil {
			_, _ = fmt.Fprintf(os.Stdout, "expires_at=%s\n", resp.ExpiresAt.Format(time.RFC3339))
		}
		for _, scope := range resp.Scopes {
			_, _ = fmt.Fprintf(os.Stdout, "scope=%s:%s\n", scope.Prefix, strings.Join(scope.Ops, ","))
		}
	}
	return nil
}

func TokenRevoke(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: drive9 token revoke <token_id>")
	}
	tokenID := strings.TrimSpace(args[0])
	if tokenID == "" {
		return fmt.Errorf("token_id is required")
	}
	c, err := newTokenManagementClientFromEnv()
	if err != nil {
		return err
	}
	return c.RevokeScopedToken(context.Background(), tokenID)
}

func parseTokenAllow(raw string) (client.FSScopeGrant, error) {
	idx := strings.LastIndex(raw, ":")
	if idx <= 0 || idx == len(raw)-1 {
		return client.FSScopeGrant{}, fmt.Errorf("expected <prefix>:<ops>")
	}
	prefix := strings.TrimSpace(raw[:idx])
	if prefix == "" {
		return client.FSScopeGrant{}, fmt.Errorf("prefix is required")
	}
	ops, err := parseTokenOps(raw[idx+1:])
	if err != nil {
		return client.FSScopeGrant{}, err
	}
	return client.FSScopeGrant{Prefix: prefix, Ops: ops}, nil
}

func parseTokenOps(raw string) ([]string, error) {
	seen := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		op := strings.TrimSpace(part)
		if op == "" {
			return nil, fmt.Errorf("empty op")
		}
		switch op {
		case "read", "list", "search", "write", "delete":
			seen[op] = true
		default:
			return nil, fmt.Errorf("unknown op %q", op)
		}
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("empty ops")
	}
	if seen["search"] && !seen["read"] {
		return nil, fmt.Errorf("search op requires read")
	}
	out := make([]string, 0, len(seen))
	for _, op := range []string{"read", "list", "search", "write", "delete"} {
		if seen[op] {
			out = append(out, op)
		}
	}
	return out, nil
}

func newTokenManagementClientFromEnv() (*client.Client, error) {
	r := ResolveCredentials()
	if r.Kind != CredentialOwner {
		return nil, fmt.Errorf("missing tenant API key; set %s or run drive9 create", EnvAPIKey)
	}
	return client.New(r.Server, r.APIKey), nil
}
