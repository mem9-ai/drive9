package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// Ctx dispatches drive9 ctx subcommands per spec §13.2.
//
// The 5 normative verbs are: add / import / ls / use / rm.
//
// Bare `drive9 ctx` (no verb) is retained as a non-spec compatibility
// convenience that prints the current context name. See migration call-out
// #7 in the impl PR body.
func Ctx(args []string) error {
	if len(args) == 0 {
		return ctxShow()
	}
	switch args[0] {
	case "add":
		return ctxAddCmd(args[1:])
	case "import":
		return ctxImportCmd(args[1:])
	case "ls", "list":
		return ctxListCmd(args[1:])
	case "use":
		return ctxUseCmd(args[1:])
	case "rm":
		return ctxRmCmd(args[1:])
	case "-h", "--help", "help":
		return ctxUsageErr()
	default:
		return fmt.Errorf("unknown ctx command %q\n%s", args[0], ctxUsage())
	}
}

func ctxUsage() string {
	return `usage: drive9 ctx <add|import|ls|use|rm>
  add --api-key <key> [--name <n>] [--server <url>]   add owner context
  import --from-file <path>                           add delegated context from file (must be mode 0600)
  import --from-file -                                add delegated context from stdin explicitly
  import                                              add delegated context from stdin (default when stdin is a pipe)
  ls [-l|--json]                                      list contexts
  use <name>                                          activate a context
  rm <name>                                           delete a context`
}

func ctxUsageErr() error {
	return fmt.Errorf("%s", ctxUsage())
}

func ctxShow() error {
	cfg := loadConfig()
	if cfg.CurrentContext == "" {
		fmt.Println("no current context")
		return nil
	}
	fmt.Println(cfg.CurrentContext)
	return nil
}

// ctxAddCmd is the user-facing `drive9 ctx add` verb. Internally it delegates
// to ctxAdd, the shared Go helper that is ALSO called by `drive9 create`.
// This keeps a single config-writer code path (no exec.Command, no cmd
// re-entry) so the invariant "exactly one place writes ~/.drive9/config" is
// preserved.
func ctxAddCmd(args []string) error {
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
			return fmt.Errorf("unknown flag %q\nusage: drive9 ctx add --api-key <key> [--name <n>] [--server <url>]", args[i])
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

// ctxImportCmd implements `drive9 ctx import` per spec §13.2/§13.3. Input
// modes:
//   --from-file <path>   read JWT from file (file MUST be mode 0600)
//   --from-file -        read JWT from stdin explicitly
//   (no args, stdin piped) read JWT from stdin (auto-detected when !isatty)
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
	longForm := false
	asJSON := false
	for _, arg := range args {
		switch arg {
		case "-l", "--long":
			longForm = true
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("unknown flag %q\nusage: drive9 ctx ls [-l|--json]", arg)
		}
	}
	if longForm && asJSON {
		return fmt.Errorf("-l/--long and --json are mutually exclusive")
	}
	cfg := loadConfig()
	if asJSON {
		return writeCtxListJSON(cfg)
	}
	return writeCtxListTable(cfg, longForm)
}

type ctxListEntry struct {
	Name      string    `json:"name"`
	Current   bool      `json:"current"`
	Type      string    `json:"type"`
	Server    string    `json:"server,omitempty"`
	Scope     []string  `json:"scope,omitempty"`
	Perm      string    `json:"perm,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	Status    string    `json:"status"`
	Agent     string    `json:"agent,omitempty"`
	GrantID   string    `json:"grant_id,omitempty"`
}

func writeCtxListJSON(cfg *Config) error {
	entries := buildCtxListEntries(cfg)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"current_context": cfg.CurrentContext,
		"contexts":        entries,
	})
}

func writeCtxListTable(cfg *Config, longForm bool) error {
	entries := buildCtxListEntries(cfg)
	if len(entries) == 0 {
		fmt.Println("no contexts configured")
		fmt.Println("run: drive9 ctx add --api-key <key>  (owner)")
		fmt.Println("     drive9 ctx import --from-file <path>  (delegated)")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "CURRENT\tNAME\tTYPE\tSCOPE\tPERM\tEXPIRES_AT\tSTATUS")
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
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			cur,
			e.Name,
			e.Type,
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

func ctxStatus(c *Context, now time.Time) string {
	if c.Type == PrincipalDelegated && !c.ExpiresAt.IsZero() && !c.ExpiresAt.After(now) {
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
// The only way to rebind a mount is `drive9 vault reauth`. This is enforced
// by `ctx use` doing no FUSE-side work; it only rewrites the active context
// pointer in ~/.drive9/config.
//
// Per §17 short-circuit, an already-expired delegated context is refused.
func ctxUseCmd(args []string) error {
	if len(args) != 1 || strings.HasPrefix(args[0], "--") {
		return fmt.Errorf("usage: drive9 ctx use <name>")
	}
	name := args[0]
	cfg := loadConfig()
	c, ok := cfg.Contexts[name]
	if !ok {
		return fmt.Errorf("context %q not found; run: drive9 ctx ls", name)
	}
	if c.Type == PrincipalDelegated && !c.ExpiresAt.IsZero() && !c.ExpiresAt.After(time.Now()) {
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
func ctxRmCmd(args []string) error {
	if len(args) != 1 || strings.HasPrefix(args[0], "--") {
		return fmt.Errorf("usage: drive9 ctx rm <name>")
	}
	name := args[0]
	cfg := loadConfig()
	if _, ok := cfg.Contexts[name]; !ok {
		return fmt.Errorf("context %q not found", name)
	}
	delete(cfg.Contexts, name)
	if cfg.CurrentContext == name {
		cfg.CurrentContext = ""
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf("removed context %q\n", name)
	if cfg.CurrentContext == "" {
		fmt.Println("no current context; run `drive9 ctx use <name>` to activate one")
	}
	return nil
}
