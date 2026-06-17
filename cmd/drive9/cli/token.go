package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
)

// Token manages workspace-zone scoped filesystem tokens.
//
// Per task #11 / Plan B consensus (#drive9:f2a8e33e):
//   - `drive9 token issue`  — issue a scoped capability (server)
//   - `drive9 token revoke` — revoke a scoped capability (server)
//
// are the canonical token namespace operations.
//
// `drive9 token list` / `drive9 token forget` are kept as hidden
// deprecation-warning aliases that point users to the canonical
// `drive9 ctx list --type fs_scoped` / `drive9 ctx rm` paths. They
// are NOT advertised in `drive9 token --help`. One release cycle
// later they should be deleted entirely.
func Token(args []string) error {
	if len(args) == 0 {
		return tokenUsageErr()
	}
	if IsHelpArg(args[0]) {
		_, _ = fmt.Fprint(os.Stdout, tokenUsage())
		return nil
	}
	switch args[0] {
	case "issue":
		return TokenIssue(args[1:])
	case "revoke":
		return TokenRevoke(args[1:])
	case "list", "ls":
		// Deprecated alias; warn on stderr then dispatch through
		// the canonical ctx-list filter path. Keeps existing
		// scripts working for one release.
		fmt.Fprintln(os.Stderr, "WARNING: `drive9 token list` is deprecated; use `drive9 ctx list --type fs_scoped` instead.")
		fmt.Fprintln(os.Stderr, "         This command will be removed in a future release.")
		return Ctx([]string{"list", "--type", "fs_scoped"})
	case "forget":
		// Deprecated alias; warn on stderr then dispatch through
		// `drive9 ctx rm <name>` which owns local namespace cleanup.
		fmt.Fprintln(os.Stderr, "WARNING: `drive9 token forget` is deprecated; use `drive9 ctx rm <name>` instead.")
		fmt.Fprintln(os.Stderr, "         This command will be removed in a future release.")
		return Ctx(append([]string{"rm"}, args[1:]...))
	default:
		return fmt.Errorf("unknown token command %q\n%s", args[0], tokenUsage())
	}
}

func tokenUsage() string {
	return `usage: drive9 token <command> [arguments]

commands:
  issue [name] --ttl <duration> --allow <prefix:ops>
                       issue an fs_scoped token; name is local only when set
  revoke <name>        revoke a locally named fs_scoped token (deletes local name on success)
  revoke -             read a token from stdin and revoke it
  revoke --api-key-file <path>
                       read the target token from a file and revoke it

issue flags:
  --subject <name>    optional server-side audit label; not a revoke key
  --ttl <duration>    required positive duration, e.g. 1h, 24h
  --allow <prefix:ops>
                       repeatable; ops are comma-separated read,list,search,write,delete
  --json              print full JSON response
  --print             print only the bearer token
  --token-only        alias for --print

To list local contexts (including fs_scoped tokens):
  drive9 ctx list                     all local contexts
  drive9 ctx list --type fs_scoped    only scoped tokens
  drive9 ctx list --scoped            shorthand for --type fs_scoped

To remove a local context name without revoking the server credential:
  drive9 ctx rm <name>
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
		name      string
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
		case "--print", "--token-only":
			tokenOnly = true
		default:
			if strings.HasPrefix(args[i], "-") {
				return fmt.Errorf("unknown flag %q", args[i])
			}
			if name != "" {
				return fmt.Errorf("unexpected argument %q", args[i])
			}
			name = args[i]
		}
	}
	if asJSON && tokenOnly {
		return fmt.Errorf("--json and --token-only are mutually exclusive")
	}
	name = strings.TrimSpace(name)
	if name != "" {
		cfg := loadConfig()
		if _, exists := cfg.Contexts[name]; exists {
			return fmt.Errorf("context %q already exists; use a different name or remove the local context with: drive9 ctx rm %s", name, name)
		}
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

	r := ResolveCredentials()
	c, err := newTokenManagementClientFromResolved(r)
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
	if name != "" {
		if err := saveScopedTokenContext(name, r.Server, resp); err != nil {
			return rollbackIssuedTokenAfterSaveFailure(c, resp.Token, err)
		}
	}
	switch {
	case tokenOnly:
		_, _ = fmt.Fprintln(os.Stdout, resp.Token)
	case asJSON:
		return writeJSON(resp)
	default:
		if name != "" {
			_, _ = fmt.Fprintf(os.Stdout, "name=%s\n", name)
		}
		_, _ = fmt.Fprintf(os.Stdout, "token=%s\n", resp.Token)
		if resp.ExpiresAt != nil {
			_, _ = fmt.Fprintf(os.Stdout, "expires_at=%s\n", resp.ExpiresAt.Format(time.RFC3339))
		}
		for _, scope := range resp.Scopes {
			_, _ = fmt.Fprintf(os.Stdout, "scope=%s:%s\n", scope.Prefix, strings.Join(scope.Ops, ","))
		}
	}
	return nil
}

func rollbackIssuedTokenAfterSaveFailure(c *client.Client, apiKey string, saveErr error) error {
	if strings.TrimSpace(apiKey) == "" {
		return fmt.Errorf("issued token but failed to save local context; no token returned for rollback: %w", saveErr)
	}
	if err := c.RevokeScopedTokenByAPIKey(context.Background(), apiKey); err != nil {
		return fmt.Errorf("issued token but failed to save local context and rollback revoke failed; token=%s; save error: %w; revoke error: %v", apiKey, saveErr, err)
	}
	return fmt.Errorf("failed to save local token context; issued token was revoked: %w", saveErr)
}

func TokenList(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: drive9 token list")
	}
	cfg := loadConfig()
	names := make([]string, 0, len(cfg.Contexts))
	for name, ctx := range cfg.Contexts {
		if ctx != nil && ctx.Type == PrincipalFSScoped {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		fmt.Println("no local fs_scoped tokens")
		return nil
	}
	sort.Strings(names)
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "CURRENT\tNAME\tTYPE\tSCOPE\tEXPIRES_AT\tSTATUS")
	now := time.Now()
	for _, name := range names {
		ctx := cfg.Contexts[name]
		cur := " "
		if name == cfg.CurrentContext {
			cur = "*"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			cur, name, ctx.Type, renderScope(ctx.Scope, string(ctx.Type), false),
			formatExpiresAt(ctx.ExpiresAt), ctxStatus(ctx, now))
	}
	return w.Flush()
}

func TokenForget(args []string) error {
	if len(args) != 1 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("usage: drive9 token forget <name>")
	}
	name := strings.TrimSpace(args[0])
	cfg := loadConfig()
	ctx, ok := cfg.Contexts[name]
	if !ok || ctx == nil || ctx.Type != PrincipalFSScoped {
		return fmt.Errorf("fs_scoped token context %q not found", name)
	}
	delete(cfg.Contexts, name)
	if cfg.CurrentContext == name {
		cfg.CurrentContext = ""
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf("forgot local token %q\n", name)
	return nil
}

func TokenRevoke(args []string) error {
	var apiKeyFile string
	pos := make([]string, 0, 1)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--api-key-file":
			if i+1 >= len(args) {
				return fmt.Errorf("--api-key-file requires a value")
			}
			i++
			apiKeyFile = args[i]
		default:
			pos = append(pos, args[i])
		}
	}
	if apiKeyFile != "" && len(pos) > 0 {
		return fmt.Errorf("--api-key-file cannot be combined with a token name or id")
	}
	if apiKeyFile == "" && len(pos) != 1 {
		return fmt.Errorf("usage: drive9 token revoke <name|->")
	}

	var (
		targetAPIKey string
		localName    string
		legacyID     string
		err          error
	)
	switch {
	case apiKeyFile != "":
		targetAPIKey, err = readTokenAPIKeyFile(apiKeyFile)
	case pos[0] == "-":
		targetAPIKey, err = readTokenAPIKeyStdin()
	default:
		target := strings.TrimSpace(pos[0])
		if target == "" {
			return fmt.Errorf("token name is required")
		}
		if strings.HasPrefix(target, token.TokenPrefix) || strings.HasPrefix(target, token.LegacyTokenPrefix) {
			return fmt.Errorf("refusing token secret in argv; pipe it with: drive9 token revoke -")
		}
		cfg := loadConfig()
		if ctx := cfg.Contexts[target]; ctx != nil && ctx.Type == PrincipalFSScoped && ctx.APIKey != "" {
			targetAPIKey = ctx.APIKey
			localName = target
		} else {
			legacyID = target
		}
	}
	if err != nil {
		return err
	}
	c, err := newTokenManagementClientFromEnv()
	if err != nil {
		return err
	}
	if targetAPIKey != "" {
		if err := c.RevokeScopedTokenByAPIKey(context.Background(), targetAPIKey); err != nil {
			return err
		}
		if localName != "" {
			if err := forgetLocalScopedTokenContext(localName); err != nil {
				return err
			}
		}
		return nil
	}
	return c.RevokeScopedToken(context.Background(), legacyID)
}

func saveScopedTokenContext(name, server string, resp *client.IssueScopedTokenResponse) error {
	cfg := loadConfig()
	if _, exists := cfg.Contexts[name]; exists {
		return fmt.Errorf("context %q already exists; use a different name or remove the local context with: drive9 ctx rm %s", name, name)
	}
	if server == "" {
		server = cfg.ResolveServer()
	}
	ctx := &Context{
		Type:      PrincipalFSScoped,
		Server:    server,
		APIKey:    resp.Token,
		Scope:     tokenScopeStrings(resp.Scopes),
		ExpiresAt: timeValue(resp.ExpiresAt),
	}
	if _, err := ctxAdd(cfg, name, ctx); err != nil {
		return err
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}

func forgetLocalScopedTokenContext(name string) error {
	cfg := loadConfig()
	ctx := cfg.Contexts[name]
	if ctx == nil || ctx.Type != PrincipalFSScoped {
		return nil
	}
	delete(cfg.Contexts, name)
	if cfg.CurrentContext == name {
		cfg.CurrentContext = ""
	}
	return saveConfig(cfg)
}

func tokenScopeStrings(scopes []client.FSScopeGrant) []string {
	out := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		out = append(out, fmt.Sprintf("%s:%s", scope.Prefix, strings.Join(scope.Ops, ",")))
	}
	return out
}

func timeValue(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func readTokenAPIKeyStdin() (string, error) {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", fmt.Errorf("read token from stdin: %w", err)
	}
	apiKey := strings.TrimSpace(string(data))
	if apiKey == "" {
		return "", fmt.Errorf("api_key is required")
	}
	return apiKey, nil
}

func readTokenAPIKeyFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read token file: %w", err)
	}
	apiKey := strings.TrimSpace(string(data))
	if apiKey == "" {
		return "", fmt.Errorf("api_key is required")
	}
	return apiKey, nil
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
	return newTokenManagementClientFromResolved(ResolveCredentials())
}

func newTokenManagementClientFromResolved(r ResolvedCredentials) (*client.Client, error) {
	if r.Kind != CredentialOwner {
		return nil, fmt.Errorf("missing tenant API key; set %s or run drive9 create", EnvAPIKey)
	}
	return client.New(r.Server, r.APIKey), nil
}
