package cli

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Ctx dispatches drive9 ctx subcommands per spec §13.2.
//
// The user-facing verbs are: show / add / import / fork / ls / use / rm.
//
// Bare `drive9 ctx` is the shorthand form of `drive9 ctx show`.
func Ctx(args []string) error {
	if len(args) == 0 {
		return ctxShowCmd(nil)
	}
	if IsHelpArg(args[0]) {
		_, _ = fmt.Fprintln(os.Stdout, ctxUsage())
		return nil
	}
	switch args[0] {
	case "show":
		return ctxShowCmd(args[1:])
	case "add":
		return ctxAddCmd(args[1:])
	case "import":
		return ctxImportCmd(args[1:])
	case "fork":
		return ctxForkCmd(args[1:])
	case "ls", "list":
		return ctxListCmd(args[1:])
	case "use":
		return ctxUseCmd(args[1:])
	case "rm":
		return ctxRmCmd(args[1:])
	default:
		return fmt.Errorf("unknown ctx command %q\n%s", args[0], ctxUsage())
	}
}

func ctxUsage() string {
	return `usage: drive9 ctx <show|add|import|fork|ls|use|rm>
  show [--json] [--reveal]                            show current context
  add --api-key <key> [--name <n>] [--server <url>]   add owner context
  import --from-file <path>                           add delegated context from file (must be mode 0600)
  import --from-file -                                add delegated context from stdin explicitly
  import                                              add delegated context from stdin (default when stdin is a pipe)
  fork [<new>] [--from <ctx>] [--json]                create a copy-on-write fork context
  ls [-l|--json] [--type <kind>|--scoped]             list contexts (filter by type: owner|delegated|fs_scoped)
  use [--] <name>                                     activate a context
  rm <name>                                           remove a local context name (does NOT revoke server-side credential)`
}

func ctxShowUsage() string { return "usage: drive9 ctx show [--json] [--reveal]" }

func ctxAddUsage() string {
	return "usage: drive9 ctx add --api-key <key> [--name <n>] [--server <url>]"
}

func ctxImportUsage() string {
	return "usage: drive9 ctx import [--from-file <path|->] [--name <name>]"
}

func ctxForkUsage() string {
	return "usage: drive9 ctx fork [<new>] [--from <ctx>] [--json]"
}

func ctxListUsage() string {
	return "usage: drive9 ctx ls [-l|--json] [--type <kind>|--scoped]"
}

func ctxUseUsage() string { return "usage: drive9 ctx use [--] <name>" }

type ctxShowEntry struct {
	Name      string     `json:"name"`
	Type      string     `json:"type"`
	Server    string     `json:"server,omitempty"`
	TenantID  string     `json:"tenant_id,omitempty"`
	APIKey    string     `json:"api_key,omitempty"`
	Token     string     `json:"token,omitempty"`
	Agent     string     `json:"agent,omitempty"`
	Scope     []string   `json:"scope,omitempty"`
	Perm      string     `json:"perm,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Status    string     `json:"status,omitempty"`
	GrantID   string     `json:"grant_id,omitempty"`
	LabelHint string     `json:"label_hint,omitempty"`
	Source    string     `json:"source,omitempty"`
}

func ctxShowCmd(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, ctxShowUsage())
		return nil
	}
	asJSON := false
	reveal := false
	for _, arg := range args {
		switch arg {
		case "--json":
			asJSON = true
		case "--reveal":
			reveal = true
		default:
			if strings.HasPrefix(arg, "-") {
				return fmt.Errorf("unknown flag %q\n%s", arg, ctxShowUsage())
			}
			return fmt.Errorf("unexpected argument %q\n%s", arg, ctxShowUsage())
		}
	}

	entry := buildCtxShowEntry(loadConfig(), reveal)
	if asJSON {
		return writeCtxShowJSON(entry)
	}
	return writeCtxShowText(entry)
}

func buildCtxShowEntry(cfg *Config, reveal bool) *ctxShowEntry {
	current := cfg.currentContextEntry()
	if current == nil || cfg.CurrentContext == "" {
		return nil
	}

	entry := &ctxShowEntry{
		Name:     cfg.CurrentContext,
		Type:     string(current.Type),
		Server:   cfg.ResolveServer(),
		TenantID: tenantIDFromContext(current),
		Source:   configPath(),
	}

	switch current.Type {
	case PrincipalOwner:
		entry.APIKey = formatSecretForDisplay(current.APIKey, reveal)
	case PrincipalFSScoped:
		entry.APIKey = formatSecretForDisplay(current.APIKey, reveal)
		entry.Scope = append([]string(nil), current.Scope...)
		if !current.ExpiresAt.IsZero() {
			expiresAt := current.ExpiresAt
			entry.ExpiresAt = &expiresAt
		}
		entry.Status = ctxStatus(current, time.Now())
	case PrincipalDelegated:
		entry.Token = formatSecretForDisplay(current.Token, reveal)
		entry.Agent = current.Agent
		entry.Scope = append([]string(nil), current.Scope...)
		entry.Perm = string(current.Perm)
		if !current.ExpiresAt.IsZero() {
			expiresAt := current.ExpiresAt
			entry.ExpiresAt = &expiresAt
		}
		entry.Status = ctxStatus(current, time.Now())
		entry.GrantID = current.GrantID
		entry.LabelHint = current.LabelHint
	}

	return entry
}

func writeCtxShowJSON(entry *ctxShowEntry) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	// Keep the zero-current-context JSON contract explicit for scripts.
	// A missing active context serializes as JSON null rather than an error.
	return enc.Encode(entry)
}

func writeCtxShowText(entry *ctxShowEntry) error {
	if entry == nil {
		fmt.Println("no current context")
		return nil
	}

	fields := []struct {
		label string
		value string
	}{
		{label: "name", value: entry.Name},
		{label: "type", value: entry.Type},
		{label: "server", value: entry.Server},
	}
	if entry.TenantID != "" {
		fields = append(fields, struct {
			label string
			value string
		}{label: "tenant_id", value: entry.TenantID})
	}
	if entry.APIKey != "" {
		fields = append(fields, struct {
			label string
			value string
		}{label: "api_key", value: entry.APIKey})
	}
	if entry.Token != "" {
		fields = append(fields, struct {
			label string
			value string
		}{label: "token", value: entry.Token})
	}
	if entry.Agent != "" {
		fields = append(fields, struct {
			label string
			value string
		}{label: "agent", value: entry.Agent})
	}
	if len(entry.Scope) > 0 {
		fields = append(fields, struct {
			label string
			value string
		}{label: "scope", value: strings.Join(entry.Scope, ", ")})
	}
	if entry.Perm != "" {
		fields = append(fields, struct {
			label string
			value string
		}{label: "perm", value: entry.Perm})
	}
	if entry.ExpiresAt != nil {
		fields = append(fields, struct {
			label string
			value string
		}{label: "expires_at", value: formatExpiresAt(*entry.ExpiresAt)})
	}
	if entry.Status != "" {
		fields = append(fields, struct {
			label string
			value string
		}{label: "status", value: entry.Status})
	}
	if entry.GrantID != "" {
		fields = append(fields, struct {
			label string
			value string
		}{label: "grant_id", value: entry.GrantID})
	}
	if entry.LabelHint != "" {
		fields = append(fields, struct {
			label string
			value string
		}{label: "label_hint", value: entry.LabelHint})
	}
	if entry.Source != "" {
		fields = append(fields, struct {
			label string
			value string
		}{label: "source", value: entry.Source})
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	for _, field := range fields {
		_, _ = fmt.Fprintf(w, "%s:\t%s\n", field.label, field.value)
	}
	return w.Flush()
}

const drive9APIKeyJWTWrapperPrefix = "dat9_"

func tenantIDFromContext(ctx *Context) string {
	if ctx == nil {
		return ""
	}
	var claims *jwtClaims
	var err error
	switch ctx.Type {
	case PrincipalOwner, PrincipalFSScoped:
		claims, err = decodeDrive9APIKeyPayload(ctx.APIKey)
	case PrincipalDelegated:
		claims, err = decodeJWTPayload(ctx.Token)
	default:
		return ""
	}
	if err != nil || claims == nil {
		return ""
	}
	return claims.TenantID
}

func decodeDrive9APIKeyPayload(apiKey string) (*jwtClaims, error) {
	apiKey = strings.TrimSpace(apiKey)
	if !strings.HasPrefix(apiKey, drive9APIKeyJWTWrapperPrefix) {
		return nil, fmt.Errorf("invalid drive9 api key format")
	}
	wrapped := strings.TrimPrefix(apiKey, drive9APIKeyJWTWrapperPrefix)
	rawJWT, err := base64.RawURLEncoding.DecodeString(wrapped)
	if err != nil {
		return nil, fmt.Errorf("decode api key wrapper: %w", err)
	}
	return decodeJWTPayload(string(rawJWT))
}

func formatSecretForDisplay(secret string, reveal bool) string {
	if reveal || secret == "" {
		return secret
	}

	if strings.HasPrefix(secret, "drive9_") && len(secret) > len("drive9_")+8 {
		return secret[:len("drive9_")+4] + "..." + secret[len(secret)-4:]
	}

	if len(secret) <= 8 {
		return strings.Repeat("x", len(secret))
	}
	if len(secret) <= 12 {
		return secret[:2] + "..." + secret[len(secret)-2:]
	}
	return secret[:8] + "..." + secret[len(secret)-4:]
}

// ctxAddCmd is the user-facing `drive9 ctx add` verb. Internally it delegates
// to ctxAdd, the shared Go helper that is ALSO called by `drive9 create`.
// This keeps a single config-writer code path (no exec.Command, no cmd
// re-entry) so the invariant "exactly one place writes ~/.drive9/config" is
// preserved.
func ctxAddCmd(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, ctxAddUsage())
		return nil
	}
	var (
		apiKey string
		name   string
		server string
	)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--api-key":
			if i+1 >= len(args) {
				return fmt.Errorf("--api-key requires a value")
			}
			i++
			apiKey = args[i]
		case "--name":
			if i+1 >= len(args) {
				return fmt.Errorf("--name requires a value")
			}
			i++
			name = args[i]
		case "--server":
			if i+1 >= len(args) {
				return fmt.Errorf("--server requires a value")
			}
			i++
			server = args[i]
		default:
			return fmt.Errorf("unknown flag %q\n%s", args[i], ctxAddUsage())
		}
	}
	if apiKey == "" {
		return fmt.Errorf("--api-key is required")
	}

	cfg := loadConfig()
	if server == "" {
		server = cfg.ResolveServer()
	}
	if name == "" {
		name = randomName()
	}
	if _, err := ctxAdd(cfg, name, &Context{
		Type:   PrincipalOwner,
		Server: server,
		APIKey: apiKey,
	}); err != nil {
		return err
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf("added context %q (owner)\n", name)
	if cfg.CurrentContext == name {
		fmt.Printf("current context is now %q\n", name)
	}
	return nil
}

// ctxAdd registers a new context entry in cfg. It is the single writer for
// both `drive9 ctx add` and `drive9 create`. Collision on name is rejected;
// if cfg has no current context, the new entry becomes current.
//
// ctxAdd does NOT save cfg; callers are responsible for persistence. Returning
// the inserted *Context lets callers print per-kind success output without a
// second lookup.
func ctxAdd(cfg *Config, name string, ctx *Context) (*Context, error) {
	if name == "" {
		return nil, fmt.Errorf("context name is required")
	}
	if cfg.Contexts == nil {
		cfg.Contexts = map[string]*Context{}
	}
	if _, exists := cfg.Contexts[name]; exists {
		return nil, fmt.Errorf("context %q already exists; use a different name or run: drive9 ctx rm %s", name, name)
	}
	cfg.Contexts[name] = ctx
	if cfg.CurrentContext == "" {
		cfg.CurrentContext = name
	}
	if cfg.Server == "" && ctx.Server != "" {
		cfg.Server = ctx.Server
	}
	return ctx, nil
}

func ctxForkCmd(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, ctxForkUsage())
		return nil
	}
	newName := ""
	fromName := ""
	jsonOut := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--from":
			if i+1 >= len(args) {
				return fmt.Errorf("--from requires an argument")
			}
			i++
			fromName = args[i]
		case "--json":
			jsonOut = true
		default:
			if strings.HasPrefix(args[i], "-") {
				return fmt.Errorf("unknown flag %q\n%s", args[i], ctxForkUsage())
			}
			if newName != "" {
				return fmt.Errorf("unexpected argument %q\n%s", args[i], ctxForkUsage())
			}
			newName = args[i]
		}
	}
	cfg := loadConfig()
	if cfg.Contexts == nil {
		cfg.Contexts = map[string]*Context{}
	}
	if newName == "" {
		newName = randomName()
	}
	if _, exists := cfg.Contexts[newName]; exists {
		return fmt.Errorf("context %q already exists; use a different name", newName)
	}
	if fromName == "" {
		fromName = cfg.CurrentContext
	}
	if fromName == "" {
		return fmt.Errorf("no source context selected; use --from <ctx> or run `drive9 ctx use <ctx>`")
	}
	source := cfg.Contexts[fromName]
	if source == nil {
		return fmt.Errorf("source context %q not found; run: drive9 ctx ls", fromName)
	}
	if source.Type != PrincipalOwner || source.APIKey == "" {
		return fmt.Errorf("ctx fork requires an owner context; %q is %q", fromName, source.Type)
	}
	server := source.Server
	if server == "" {
		server = cfg.Server
	}
	if server == "" {
		return fmt.Errorf("source context %q has no server URL", fromName)
	}

	body, _ := json.Marshal(map[string]string{"name": newName})
	c := client.New(server, source.APIKey)
	resp, err := c.RawPost("/v1/fork", strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("ctx fork failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var result forkCtxResult
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		var errResp struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("ctx fork failed (HTTP %d): %s", resp.StatusCode, errResp.Error)
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode fork response: %w", err)
	}
	if result.APIKey == "" {
		return fmt.Errorf("ctx fork response missing api_key")
	}
	if _, err := ctxAdd(cfg, newName, &Context{
		Type:   PrincipalOwner,
		Server: server,
		APIKey: result.APIKey,
	}); err != nil {
		return err
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	if jsonOut {
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	fmt.Printf("Forked %s -> %s\n", fromName, newName)
	fmt.Println("Mode: copy-on-write")
	if result.Status != "" {
		fmt.Printf("Status: %s\n", result.Status)
	}
	if result.Message != "" {
		fmt.Printf("Message: %s\n", result.Message)
	}
	fmt.Printf("Use `drive9 ctx use %s` to switch when you're ready.\n", newName)
	if result.Status == "provisioning" {
		fmt.Println("The fork is still provisioning. Wait a moment, then retry a command like `drive9 fs ls /`; `fs` commands may fail until the tenant becomes active.")
	}
	return nil
}

type forkCtxResult struct {
	TenantID       string `json:"tenant_id"`
	APIKey         string `json:"api_key"`
	Status         string `json:"status"`
	Message        string `json:"message,omitempty"`
	ParentTenantID string `json:"parent_tenant_id"`
	Storage        string `json:"storage"`
}

// ctxImportCmd implements `drive9 ctx import` per spec §13.2/§13.3. Input
// modes:
//
//	--from-file <path>   read JWT from file (file MUST be mode 0600)
//	--from-file -        read JWT from stdin explicitly
//	(no args, stdin piped) read JWT from stdin (auto-detected when !isatty)
//
// Per §13.3, the JWT MUST NOT be passed as a positional argument — the
// positional form was removed because a runtime warning cannot unexpose a
// secret that has already reached shell history and /proc/<pid>/cmdline.
//
// The §19 parse-stability fork rejects before any config write:
//  1. structurally unparseable JWT                 -> command error
//  2. parseable but principal_type != "delegated"  -> command error, direct to ctx add
//  3. parseable delegated but exp already past     -> command error (§17 short-circuit #1)
//  4. parseable delegated with exp in the future   -> TOFU on iss, store
func ctxImportCmd(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, ctxImportUsage())
		return nil
	}
	var (
		fromFile string
		name     string
	)
	haveFromFile := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--from-file":
			if i+1 >= len(args) {
				return fmt.Errorf("--from-file requires a value (path or -)")
			}
			i++
			fromFile = args[i]
			haveFromFile = true
		case "--name":
			if i+1 >= len(args) {
				return fmt.Errorf("--name requires a value")
			}
			i++
			name = args[i]
		default:
			if strings.HasPrefix(args[i], "--") {
				return fmt.Errorf("unknown flag %q", args[i])
			}
			// Per §13.3: positional JWT is rejected. A token on the
			// command line would already be in shell history and
			// /proc/<pid>/cmdline by the time we see it; there is no
			// safe way to accept it.
			return fmt.Errorf("ctx import: positional JWT is not accepted (it leaks into shell history and /proc/<pid>/cmdline); use one of:\n  drive9 ctx import --from-file <path>\n  <producer> | drive9 ctx import\n  drive9 ctx import --from-file -")
		}
	}

	raw, err := readImportToken(fromFile, haveFromFile)
	if err != nil {
		return err
	}

	claims, err := decodeJWTPayload(raw)
	if err != nil {
		return fmt.Errorf("ctx import: %w (use `drive9 ctx add --api-key` for owner credentials)", err)
	}
	if claims.PrincipalType != string(PrincipalDelegated) {
		return fmt.Errorf("ctx import: token principal_type is %q, not %q; use `drive9 ctx add --api-key` for owner credentials", claims.PrincipalType, PrincipalDelegated)
	}
	exp := claims.expTime()
	if !exp.IsZero() && exp.Before(time.Now()) {
		return fmt.Errorf("ctx import: token already expired at %s", exp.Format(time.RFC3339))
	}
	if claims.Iss == "" {
		return fmt.Errorf("ctx import: token is missing the `iss` claim")
	}
	perm := Perm(claims.Perm)
	if perm != PermRead && perm != PermWrite {
		return fmt.Errorf("ctx import: token perm is %q, expected one of {read, write}", claims.Perm)
	}

	cfg := loadConfig()
	if name == "" {
		name = defaultImportName(cfg, claims)
	}
	if _, err := ctxAdd(cfg, name, &Context{
		Type:      PrincipalDelegated,
		Server:    claims.Iss, // TOFU — see Invariant #8 / §18
		Token:     raw,
		Agent:     claims.Agent,
		Scope:     append([]string(nil), claims.Scope...),
		Perm:      perm,
		ExpiresAt: exp,
		GrantID:   claims.GrantID,
		LabelHint: claims.LabelHint,
	}); err != nil {
		return err
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf("imported context %q (delegated, grant %s)\n", name, claims.GrantID)
	if cfg.CurrentContext == name {
		fmt.Printf("current context is now %q\n", name)
	}
	return nil
}

// readImportToken returns the JWT body per §13.3. The three accepted modes:
//
//  1. --from-file <path>  : read file (MUST be regular, mode 0600 — checked
//     before any read so a world-readable drop file cannot silently succeed).
//  2. --from-file -       : read stdin explicitly.
//  3. no flag, stdin piped: auto-detect (stdin is not a TTY).
//
// A bare `drive9 ctx import` with stdin attached to a TTY is refused with a
// one-line help pointing at the canonical forms. This is a client-side input
// error (EINVAL shape) — we return it before any config write.
func readImportToken(fromFile string, haveFromFile bool) (string, error) {
	switch {
	case haveFromFile && fromFile == "-":
		return readTrimmedStdin()
	case haveFromFile:
		if err := checkImportFilePerm(fromFile); err != nil {
			return "", err
		}
		data, err := os.ReadFile(fromFile)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", fromFile, err)
		}
		return strings.TrimSpace(string(data)), nil
	default:
		// No flag — auto-detect. Only accept if stdin is a pipe / not a TTY.
		piped, err := stdinIsPiped()
		if err != nil {
			return "", fmt.Errorf("ctx import: stat stdin: %w", err)
		}
		if !piped {
			return "", fmt.Errorf("ctx import: no JWT on stdin. Use one of:\n  drive9 ctx import --from-file <path>\n  <producer> | drive9 ctx import\n  drive9 ctx import --from-file -")
		}
		return readTrimmedStdin()
	}
}

func readTrimmedStdin() (string, error) {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", fmt.Errorf("read stdin: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// stdinIsPiped reports whether stdin is attached to a pipe / redirect
// (i.e. not a character device / TTY). A Stat error is surfaced.
func stdinIsPiped() (bool, error) {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false, err
	}
	// If the mode reports ModeCharDevice, the fd is a terminal.
	return (fi.Mode() & os.ModeCharDevice) == 0, nil
}

// checkImportFilePerm enforces §13.3's 0600-or-stricter rule on --from-file
// paths: any bits set outside the owner triad → refuse BEFORE reading
// contents. A JWT sitting in a world-readable drop file is already
// compromised; failing before the read surfaces the leak to the operator
// at the earliest possible point.
func checkImportFilePerm(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("ctx import: %s is not a regular file", path)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("ctx import: %s has mode %#o; JWT files MUST be mode 0600 (owner-only) — run: chmod 600 %s", path, fi.Mode().Perm(), path)
	}
	return nil
}

// defaultImportName derives the default context name from (in order):
//  1. JWT label_hint (if set and not already taken),
//  2. agent-<first-scope-root>,
//
// with a numeric suffix appended on collision.
func defaultImportName(cfg *Config, claims *jwtClaims) string {
	base := claims.LabelHint
	if base == "" {
		scopeRoot := ""
		if len(claims.Scope) > 0 {
			scopeRoot = scopeRootSegment(claims.Scope[0])
		}
		if claims.Agent != "" && scopeRoot != "" {
			base = claims.Agent + "-" + scopeRoot
		} else if claims.Agent != "" {
			base = claims.Agent
		} else if scopeRoot != "" {
			base = scopeRoot
		} else {
			base = randomName()
		}
	}
	if _, exists := cfg.Contexts[base]; !exists {
		return base
	}
	for i := 2; i < 1000; i++ {
		candidate := fmt.Sprintf("%s-%d", base, i)
		if _, exists := cfg.Contexts[candidate]; !exists {
			return candidate
		}
	}
	return base + "-" + randomName()
}

func scopeRootSegment(scope string) string {
	// Scope is of the form /n/vault/<secret>[/<key>]; return <secret>.
	parts := strings.Split(strings.Trim(scope, "/"), "/")
	if len(parts) >= 3 && parts[0] == "n" && parts[1] == "vault" {
		return parts[2]
	}
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

// ctxListCmd implements `drive9 ctx ls` per spec §13.2. Output format (table):
//
//	CURRENT   NAME  TYPE  SCOPE  PERM  EXPIRES_AT  STATUS
//
// CURRENT is a dedicated column (exactly one row holds `*`), replacing the
// pre-spec `*` marker prefix on NAME (F16).
//
// Status is computed locally from ExpiresAt at display time (§17 short-circuit).
func ctxListCmd(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, ctxListUsage())
		return nil
	}
	longForm := false
	asJSON := false
	typeFilter := ""
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-l", "--long":
			longForm = true
		case "--json":
			asJSON = true
		case "--scoped":
			// Shorthand for --type fs_scoped. The user mental model
			// is "show me the scoped tokens" — accept the shortcut
			// so they don't have to remember the type literal.
			typeFilter = string(PrincipalFSScoped)
		case "--type":
			if i+1 >= len(args) {
				return fmt.Errorf("--type requires a value (owner|delegated|fs_scoped)")
			}
			i++
			typeFilter = args[i]
		default:
			if strings.HasPrefix(args[i], "--type=") {
				typeFilter = strings.TrimPrefix(args[i], "--type=")
				continue
			}
			return fmt.Errorf("unknown flag %q\n%s", args[i], ctxListUsage())
		}
	}
	if longForm && asJSON {
		return fmt.Errorf("-l/--long and --json are mutually exclusive")
	}
	if typeFilter != "" {
		if !isKnownPrincipalType(typeFilter) {
			return fmt.Errorf("unknown --type %q; valid: owner, delegated, fs_scoped", typeFilter)
		}
	}
	cfg := loadConfig()
	if asJSON {
		return writeCtxListJSON(cfg, typeFilter)
	}
	return writeCtxListTable(cfg, longForm, typeFilter)
}

// isKnownPrincipalType validates a --type filter value against the set
// of principal types stored in local contexts. Keep this in sync with
// PrincipalOwner / PrincipalDelegated / PrincipalFSScoped in config.go.
func isKnownPrincipalType(value string) bool {
	switch PrincipalType(value) {
	case PrincipalOwner, PrincipalDelegated, PrincipalFSScoped:
		return true
	}
	return false
}

type ctxListEntry struct {
	Name      string    `json:"name"`
	Current   bool      `json:"current"`
	Type      string    `json:"type"`
	Server    string    `json:"server,omitempty"`
	TenantID  string    `json:"tenant_id,omitempty"`
	Scope     []string  `json:"scope,omitempty"`
	Perm      string    `json:"perm,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	Status    string    `json:"status"`
	Agent     string    `json:"agent,omitempty"`
	GrantID   string    `json:"grant_id,omitempty"`
}

func writeCtxListJSON(cfg *Config, typeFilter string) error {
	entries := filterCtxListEntries(buildCtxListEntries(cfg), typeFilter)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"current_context": cfg.CurrentContext,
		"contexts":        entries,
	})
}

func writeCtxListTable(cfg *Config, longForm bool, typeFilter string) error {
	entries := filterCtxListEntries(buildCtxListEntries(cfg), typeFilter)
	if len(entries) == 0 {
		if typeFilter != "" {
			fmt.Printf("no contexts of type %q configured\n", typeFilter)
			return nil
		}
		fmt.Println("no contexts configured")
		fmt.Println("run: drive9 ctx add --api-key <key>  (owner)")
		fmt.Println("     drive9 ctx import --from-file <path>  (delegated)")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	// tabwriter.Writer buffers internally; errors surface at Flush().
	_, _ = fmt.Fprintln(w, "CURRENT\tNAME\tTYPE\tTENANT_ID\tSCOPE\tPERM\tEXPIRES_AT\tSTATUS")
	for _, e := range entries {
		cur := " "
		if e.Current {
			cur = "*"
		}
		scope := renderScope(e.Scope, e.Type, longForm)
		perm := e.Perm
		if e.Type == string(PrincipalOwner) {
			perm = "rw"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			cur,
			e.Name,
			e.Type,
			e.TenantID,
			scope,
			perm,
			formatExpiresAt(e.ExpiresAt),
			e.Status,
		)
	}
	return w.Flush()
}

func buildCtxListEntries(cfg *Config) []ctxListEntry {
	names := make([]string, 0, len(cfg.Contexts))
	for n := range cfg.Contexts {
		names = append(names, n)
	}
	sort.Strings(names)
	entries := make([]ctxListEntry, 0, len(names))
	now := time.Now()
	for _, n := range names {
		c := cfg.Contexts[n]
		if c == nil {
			continue
		}
		entries = append(entries, ctxListEntry{
			Name:      n,
			Current:   n == cfg.CurrentContext,
			Type:      string(c.Type),
			Server:    c.Server,
			TenantID:  tenantIDFromContext(c),
			Scope:     c.Scope,
			Perm:      string(c.Perm),
			ExpiresAt: c.ExpiresAt,
			Status:    ctxStatus(c, now),
			Agent:     c.Agent,
			GrantID:   c.GrantID,
		})
	}
	return entries
}

// filterCtxListEntries returns the subset of entries whose Type matches
// the supplied filter. An empty filter returns the slice unchanged.
// Used by `drive9 ctx list --type <kind>` / `--scoped`.
func filterCtxListEntries(entries []ctxListEntry, typeFilter string) []ctxListEntry {
	if typeFilter == "" {
		return entries
	}
	out := entries[:0:0]
	for _, e := range entries {
		if e.Type == typeFilter {
			out = append(out, e)
		}
	}
	return out
}

func ctxStatus(c *Context, now time.Time) string {
	if (c.Type == PrincipalDelegated || c.Type == PrincipalFSScoped) && !c.ExpiresAt.IsZero() && !c.ExpiresAt.After(now) {
		return "expired"
	}
	return "active"
}

func renderScope(scope []string, kind string, longForm bool) string {
	if kind == string(PrincipalOwner) {
		return "*"
	}
	if len(scope) == 0 {
		return "—"
	}
	if longForm || len(scope) == 1 {
		return strings.Join(scope, ",")
	}
	return fmt.Sprintf("%s +%d", scope[0], len(scope)-1)
}

func formatExpiresAt(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format(time.RFC3339)
}

// ctxUseCmd implements `drive9 ctx use <name>` per spec §13.2 and §15.
// Output is the spec-pinned two-line notice (F15):
//
//	switched to context "<name>"
//	<type-specific descriptor line>
//
// Per Invariant #6, activating a context does NOT re-bind any running mount.
// The only way to rebind a mount in M1 is `umount` + `mount` again (see
// spec §12, §17). An in-process `vault reauth` verb was considered and
// deferred post-M1 (#302). This is enforced by `ctx use` doing no
// FUSE-side work; it only rewrites the active context pointer in
// ~/.drive9/config.
//
// Per §17 short-circuit, an already-expired delegated context is refused.
func ctxUseCmd(args []string) error {
	escaped := false
	if len(args) > 0 && args[0] == "--" {
		escaped = true
		args = args[1:]
	}
	if !escaped && IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, ctxUseUsage())
		return nil
	}
	if len(args) != 1 || (!escaped && strings.HasPrefix(args[0], "--")) {
		return fmt.Errorf("%s", ctxUseUsage())
	}
	name := args[0]
	cfg := loadConfig()
	c, ok := cfg.Contexts[name]
	if !ok {
		return fmt.Errorf("context %q not found; run: drive9 ctx ls", name)
	}
	if (c.Type == PrincipalDelegated || c.Type == PrincipalFSScoped) && !c.ExpiresAt.IsZero() && !c.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("context %q expired at %s; request a new grant and re-import", name, c.ExpiresAt.Format(time.RFC3339))
	}
	if cfg.CurrentContext == name {
		fmt.Printf("context %q is already active\n", name)
		fmt.Println(ctxUseDescriptor(c))
		return nil
	}
	cfg.CurrentContext = name
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf("switched to context %q\n", name)
	fmt.Println(ctxUseDescriptor(c))
	return nil
}

func ctxUseDescriptor(c *Context) string {
	switch c.Type {
	case PrincipalOwner:
		return fmt.Sprintf("  owner credentials, server %s", c.Server)
	case PrincipalFSScoped:
		scope := "—"
		if len(c.Scope) > 0 {
			scope = c.Scope[0]
			if len(c.Scope) > 1 {
				scope = fmt.Sprintf("%s +%d", scope, len(c.Scope)-1)
			}
		}
		return fmt.Sprintf("  fs_scoped: scope %s, expires %s", scope, formatExpiresAt(c.ExpiresAt))
	case PrincipalDelegated:
		scope := "—"
		if len(c.Scope) > 0 {
			scope = c.Scope[0]
			if len(c.Scope) > 1 {
				scope = fmt.Sprintf("%s +%d", scope, len(c.Scope)-1)
			}
		}
		return fmt.Sprintf("  delegated: scope %s, perm %s, expires %s", scope, c.Perm, formatExpiresAt(c.ExpiresAt))
	default:
		return ""
	}
}

// ctxRmCmd implements `drive9 ctx rm <name>` per spec §13.2.
// ctxRmCmd implements `drive9 ctx rm <name>`.
//
// SAFETY CONTRACT (per task #11 / msg `7472f701` from qiffang):
// This is a LOCAL cleanup operation. It does NOT contact the server and
// does NOT revoke any server-side credential. Three safety layers:
//
//  1. Help text (ctxRmUsage) leads with a WARNING line so the
//     scanning user reads "Local cleanup only. Does NOT revoke
//     server-side credentials." in the first screen.
//  2. Runtime: removing an owner context is unrecoverable on this
//     machine (the api_key is erased from local config; the server
//     key remains valid), so we require interactive [y/N]
//     confirmation. fs_scoped contexts do not require confirmation
//     but the post-rm output explicitly states the server token
//     remains valid until TTL or explicit revoke.
//  3. Removing the current active context is refused (per
//     @adversary-1 msg `7c8dc13c`) to prevent leaving a dangling
//     CurrentContext pointer in saved config. The user is told to
//     `ctx use <other>` first.
//
// Acceptance from #drive9:67e75a87 task #11 thread.
func ctxRmCmd(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, ctxRmUsage())
		return nil
	}
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) != 1 {
		return fmt.Errorf("%s", ctxRmUsage())
	}
	name := args[0]
	cfg := loadConfig()
	target, ok := cfg.Contexts[name]
	if !ok || target == nil {
		if IsHelpArg(name) {
			_, _ = fmt.Fprintln(os.Stdout, ctxRmUsage())
			return nil
		}
		return fmt.Errorf("context %q not found", name)
	}
	if cfg.CurrentContext == name {
		return fmt.Errorf("refusing to remove current context %q; switch first with `drive9 ctx use <other>` then retry", name)
	}
	if target.Type == PrincipalOwner {
		if !confirmCtxRmOwner(name) {
			fmt.Printf("aborted; %q was not removed\n", name)
			return nil
		}
	}
	delete(cfg.Contexts, name)
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	emitCtxRmSuccess(name, target)
	return nil
}

// ctxRmUsage returns the user-facing help text for `drive9 ctx rm`.
// Written English-only (per qiffang msg `405e0ae9` directive: no
// Chinese in CLI surface). The first screen leads with a WARNING line
// + a redirect to the correct revoke path, per @gtm-1's scan-first
// observation (msg `30bf6704`).
func ctxRmUsage() string {
	return `usage: drive9 ctx rm <name>

  WARNING: Local cleanup only. Does NOT revoke server-side credentials.
  To revoke a scoped token: drive9 token revoke <name>

  Removes the named context from local config (~/.drive9/config).
  - fs_scoped: the dat9_... token remains valid on the server until TTL
    expiry or explicit revoke.
  - owner: the api_key is erased from local config. The server key
    remains valid. You'll need it saved elsewhere to log back in.

  Removing the current active context is refused; switch with
  ` + "`drive9 ctx use <other>`" + ` first.

example:
  drive9 ctx rm smoke`
}

// confirmCtxRmOwner prompts the operator for a yes/no answer before
// removing an owner context. Returns true only if the operator types
// "y" or "yes" (case-insensitive). Any other input — including an
// empty line, "n", EOF, or read error — is treated as a refusal.
func confirmCtxRmOwner(name string) bool {
	fmt.Fprintf(os.Stderr, "WARNING: %q is an owner context. Removing it erases the owner api_key\n", name)
	fmt.Fprintf(os.Stderr, "from local config; the server key remains valid (you may lose access from this\n")
	fmt.Fprintf(os.Stderr, "machine unless the key is saved elsewhere).\n")
	fmt.Fprint(os.Stderr, "Continue? [y/N]: ")
	var answer string
	if _, err := fmt.Fscanln(os.Stdin, &answer); err != nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true
	}
	return false
}

// emitCtxRmSuccess writes the success message after a context is
// removed. For fs_scoped contexts it includes an explicit reminder
// that the server-side token is still valid, with a one-line pointer
// to the correct revoke command (per the @qiffang msg `7472f701`
// usage clarity directive).
func emitCtxRmSuccess(name string, removed *Context) {
	fmt.Printf("removed local context %q (type=%s)\n", name, removed.Type)
	if removed.Type == PrincipalFSScoped {
		fmt.Println("note: the scoped token on the server is NOT revoked.")
		fmt.Printf("      to revoke, run: drive9 token revoke -   (paste the saved api_key on stdin)\n")
		if !removed.ExpiresAt.IsZero() {
			fmt.Printf("      otherwise the token remains valid until %s\n", removed.ExpiresAt.UTC().Format(time.RFC3339))
		}
	}
}
