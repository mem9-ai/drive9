package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

const (
	defaultAuditLimit   = 100
	maxClientAuditLimit = 1000
)

// Secret dispatches drive9 vault subcommands.
//
// V2c hard-cut: `exec` is removed. Callers that still type `drive9 vault exec`
// fall into the `unknown vault command` branch — no bespoke rename hint, no
// alias — identical framing to any other typo. The replacement is
// `drive9 vault with <path> -- <cmd>` (spec §9). See PR body for the
// rationale on fail-loud > silent-drop.
func Secret(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage drive9 vault <set|get|put|with|ls|rm|grant|revoke|audit>")
	}
	switch args[0] {
	case "set":
		return SecretSet(args[1:])
	case "get":
		return SecretGet(args[1:])
	case "put":
		return SecretPut(args[1:])
	case "with":
		return SecretWith(args[1:])
	case "ls":
		return SecretLs(args[1:])
	case "rm":
		return SecretRm(args[1:])
	case "grant":
		return SecretGrant(args[1:])
	case "revoke":
		return SecretRevoke(args[1:])
	case "audit":
		return SecretAudit(args[1:])
	case "-h", "--help", "help":
		return fmt.Errorf("usage drive9 vault <set|get|put|with|ls|rm|grant|revoke|audit>")
	default:
		return fmt.Errorf("unknown vault command %q", args[0])
	}
}

// SecretSet creates a new secret. It deliberately does NOT update in place on
// conflict: the server-side UpdateSecret is a wholesale-replace (deletes all
// existing fields before writing the new ones), so a silent fallback here
// would drop any field the caller didn't pass on this invocation. Users who
// actually want to replace must be explicit — either `drive9 vault rm <name>`
// then re-set, or `drive9 vault put /n/vault/<name> --from <dir>` for the
// atomic wholesale-replace path.
func SecretSet(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage drive9 vault set <name> <field=value|field=@file|field=-> [more fields]")
	}
	name := args[0]
	if err := validateSecretName(name); err != nil {
		return err
	}
	fields, err := parseSecretFields(args[1:])
	if err != nil {
		return err
	}
	c, err := newVaultManagementClientFromEnv()
	if err != nil {
		return err
	}
	ctx := context.Background()
	if _, err := c.CreateVaultSecret(ctx, name, fields); err != nil {
		if errors.Is(err, client.ErrConflict) {
			return fmt.Errorf("secret %q already exists; `drive9 vault set` will not overwrite; "+
				"to replace it wholesale use `drive9 vault put /n/vault/%s --from <dir>`, "+
				"or delete first with `drive9 vault rm %s` and re-run", name, name, name)
		}
		return err
	}
	return nil
}

// SecretGet reads a whole secret or one field.
func SecretGet(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage drive9 vault get <name[/field]> [--json|--env]")
	}
	ref := args[0]
	name, field, err := parseSecretRef(ref)
	if err != nil {
		return err
	}
	asJSON := false
	asEnv := false
	for _, arg := range args[1:] {
		switch arg {
		case "--json":
			asJSON = true
		case "--env":
			asEnv = true
		default:
			return fmt.Errorf("unknown flag %q", arg)
		}
	}
	if asJSON && asEnv {
		return fmt.Errorf("--json and --env are mutually exclusive")
	}

	c, err := newVaultReadClientFromEnv()
	if err != nil {
		return err
	}
	ctx := context.Background()
	if field != "" {
		value, err := c.ReadVaultSecretField(ctx, name, field)
		if err != nil {
			return err
		}
		switch {
		case asEnv:
			envKey, err := normalizeSecretEnvKey(field)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(os.Stdout, "%s=%s\n", envKey, value)
		case asJSON:
			return writeJSON(map[string]string{field: value})
		default:
			_, _ = fmt.Fprintln(os.Stdout, value)
		}
		return nil
	}

	fields, err := c.ReadVaultSecret(ctx, name)
	if err != nil {
		return err
	}
	if asEnv {
		return printEnv(fields)
	}
	return writeJSON(fields)
}

// vaultPathPrefix is the only accepted CLI spelling for a vault secret path,
// per spec §9 L204 (`drive9 vault with /n/vault/<secret> -- <cmd>`). Bare
// secret names (`drive9 vault with aws-prod -- env`) are NOT accepted: that
// would create a second path-shape contract outside the spec, and would
// collide with the mount-path namespace (§7) when V2e ships. Rejecting
// here — loudly, at the CLI boundary — keeps the namespace single-valued.
const vaultPathPrefix = "/n/vault/"

// SecretWith implements `drive9 vault with /n/vault/<secret> -- <cmd...>`
// per spec §9. It reads the secret via the normal vault read path, parses
// the `@env` byte contract (§4.1) strictly, scrubs DRIVE9_* credential env
// vars (§9 L209 F14 scrub), then execs the child with the parsed key/value
// pairs injected. The child's exit code is propagated verbatim.
//
// Semantics that deliberately DIFFER from the removed `SecretExec`:
//   - Path shape: only `/n/vault/<secret>` is accepted; bare names rejected.
//   - Env key charset: strict `^[A-Z_][A-Z0-9_]*$` (spec §4.1 L89). No
//     coerce-normalize (no lowercase→upper, no `-`→`_`). An illegal key
//     fails the whole command with an EACCES-shaped error — no partial
//     injection (spec §4.1.3 L101).
//   - Value charset: control bytes `\x00`–`\x1f` except `\t` are rejected
//     identically (spec §4.1.3 L103).
//   - Empty ≠ missing: a secret with 0 visible keys still forks the child
//     with no injected env (§4.1.2 L96). A missing/invisible secret
//     surfaces as an error before fork (ENOENT-shaped).
//   - Credential scrub: DRIVE9_API_KEY, DRIVE9_VAULT_TOKEN, DRIVE9_SERVER
//     are removed from the child's base env unconditionally (§9 L209).
//     Only those three are scrubbed; unrelated DRIVE9_* (profiling, log
//     level) pass through untouched.
func SecretWith(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage drive9 vault with /n/vault/<secret> -- <command...>")
	}
	name, err := parseVaultPath(args[0])
	if err != nil {
		return err
	}
	// Strict argv shape: exactly one path, then `--`, then the child
	// command. Anything between the path and `--` is rejected rather
	// than silently dropped — V2c pinned a single explicit argv shape
	// for `vault with`, and accept-and-ignore here would undercut that
	// contract (reviewer blocker flagged by adv-1 on PR #306).
	if args[1] != "--" {
		return fmt.Errorf("usage drive9 vault with /n/vault/<secret> -- <command...> (unexpected argument %q before `--`)", args[1])
	}
	cmdArgs := args[2:]
	if len(cmdArgs) == 0 {
		return fmt.Errorf("usage drive9 vault with /n/vault/<secret> -- <command...>")
	}

	c, err := newVaultReadClientFromEnv()
	if err != nil {
		return err
	}
	fields, err := c.ReadVaultSecret(context.Background(), name)
	if err != nil {
		return err
	}
	envMap, err := validateVaultEnvFields(fields)
	if err != nil {
		return err
	}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = mergeEnv(scrubDrive9CredEnv(os.Environ()), envMap)
	return cmd.Run()
}

// parseVaultPath enforces G-V2c-1: the only accepted CLI spelling is
// `/n/vault/<secret>` with a non-empty, single-segment secret name. Bare
// names, subpaths (`/n/vault/a/b`), empty names, and any other path prefix
// are rejected. validateSecretName pins the `*` / empty rules so this
// stays in sync with the read-path validation used by `vault get`.
func parseVaultPath(raw string) (string, error) {
	if !strings.HasPrefix(raw, vaultPathPrefix) {
		return "", fmt.Errorf("vault path %q must start with %s (bare secret names are not accepted)", raw, vaultPathPrefix)
	}
	rest := raw[len(vaultPathPrefix):]
	if rest == "" {
		return "", fmt.Errorf("vault path %q is missing a secret name", raw)
	}
	if strings.Contains(rest, "/") {
		return "", fmt.Errorf("vault path %q must name exactly one secret; subpath selection is not supported by `vault with`", raw)
	}
	if err := validateSecretName(rest); err != nil {
		return "", err
	}
	return rest, nil
}

// validateVaultEnvFields is the CLI-side enforcement of the `@env` byte
// contract (spec §4.1 + §4.1.3). It is intentionally separate from
// buildSecretEnvMap: that function coerce-normalizes (lowercase→upper,
// non-alnum→`_`) to fit the legacy `SecretExec` shape, which directly
// contradicts §4.1.3 L101 ("does not coerce them"). `vault with` must
// reject, not coerce.
//
// Returns an error on the FIRST illegal key or value. No partial map is
// returned — §4.1.3 L101 forbids partial output, so partial injection is
// equally forbidden.
func validateVaultEnvFields(fields map[string]string) (map[string]string, error) {
	env := make(map[string]string, len(fields))
	for key, value := range fields {
		if !isValidVaultEnvKey(key) {
			return nil, fmt.Errorf("secret key %q violates @env charset [A-Z_][A-Z0-9_]*: refusing to inject (EACCES)", key)
		}
		if idx := indexOfForbiddenEnvByte(value); idx >= 0 {
			return nil, fmt.Errorf("secret value for key %q contains forbidden control byte 0x%02x at offset %d: refusing to inject (EACCES)", key, value[idx], idx)
		}
		env[key] = value
	}
	return env, nil
}

// isValidVaultEnvKey implements the strict charset `^[A-Z_][A-Z0-9_]*$`
// from spec §4.1 L89. No Unicode, no lowercase, no non-alnum punctuation.
func isValidVaultEnvKey(key string) bool {
	if key == "" {
		return false
	}
	for i := 0; i < len(key); i++ {
		ch := key[i]
		switch {
		case ch >= 'A' && ch <= 'Z', ch == '_':
			// allowed in any position
		case ch >= '0' && ch <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// indexOfForbiddenEnvByte returns the index of the first control byte
// outlawed by spec §4.1.3 L103 (`\x00`–`\x1f` except `\t`), or -1 if the
// value is clean. Bytes ≥ 0x20 and DEL (0x7f) pass through — the contract
// only singles out the C0 control range because `printf %q` is undefined
// over it. Tab (`\t`, 0x09) is explicitly carved out.
func indexOfForbiddenEnvByte(value string) int {
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b < 0x20 && b != '\t' {
			return i
		}
	}
	return -1
}

// scrubDrive9CredEnv implements the F14 scrub from spec §9 L209. Exactly
// three variables are dropped from the child's base env:
//
//	DRIVE9_API_KEY, DRIVE9_VAULT_TOKEN, DRIVE9_SERVER
//
// Any other DRIVE9_* (e.g. DRIVE9_PROF_CPU_PROFILE, DRIVE9_CLI_LOG_*) is
// preserved — profiling and logging controls are developer-side knobs that
// do not grant authority and MUST flow through to children. The scrub is
// unconditional: we drop the names whether or not they are present in
// os.Environ(), so behavior does not depend on parent state (§9 L209).
func scrubDrive9CredEnv(base []string) []string {
	out := make([]string, 0, len(base))
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			out = append(out, entry)
			continue
		}
		switch key {
		case EnvAPIKey, EnvVaultToken, EnvServer:
			continue
		}
		out = append(out, entry)
	}
	return out
}

// SecretLs lists secret names. An explicit capability token always wins over
// any ambient tenant API key/config so agents do not silently enumerate the
// whole tenant when a scoped token was intentionally provided.
func SecretLs(args []string) error {
	asJSON := false
	for _, arg := range args {
		switch arg {
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("usage drive9 vault ls [--json]")
		}
	}

	var names []string
	if currentCapabilityToken() != "" {
		c, err := newVaultReadClientFromEnv()
		if err != nil {
			return err
		}
		var errList error
		names, errList = c.ListReadableVaultSecrets(context.Background())
		if errList != nil {
			return errList
		}
	} else if c, ok := optionalVaultManagementClientFromEnv(); ok {
		secrets, err := c.ListVaultSecrets(context.Background())
		if err != nil {
			return err
		}
		names = make([]string, 0, len(secrets))
		for _, sec := range secrets {
			names = append(names, sec.Name)
		}
	} else {
		c, err := newVaultReadClientFromEnv()
		if err != nil {
			return err
		}
		var errList error
		names, errList = c.ListReadableVaultSecrets(context.Background())
		if errList != nil {
			return errList
		}
	}
	sort.Strings(names)
	if asJSON {
		return writeJSON(map[string]any{"secrets": names})
	}
	for _, name := range names {
		fmt.Println(name)
	}
	return nil
}

// SecretRm deletes a secret.
func SecretRm(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage drive9 vault rm <name>")
	}
	name := args[0]
	if err := validateSecretName(name); err != nil {
		return err
	}
	c, err := newVaultManagementClientFromEnv()
	if err != nil {
		return err
	}
	return c.DeleteVaultSecret(context.Background(), name)
}

// SecretGrant issues a scoped capability grant via /v1/vault/grants.
//
// --perm is required (one of read|write); we do not default because the spec
// does not specify one and a silent default would bake an undocumented
// contract into the CLI. Unknown values fail-closed before any request.
//
// Output:
//   - human:  token=<token>\ngrant_id=<id>\nexpires_at=<rfc3339>
//     (only the id label flips from token_id= to grant_id=; token= is unchanged)
//   - --json: full VaultGrantIssueResponse (adds scope and perm to the key set)
//   - --token-only: <token>\n  (byte-identical to the pre-V2a shape)
func SecretGrant(args []string) error {
	var (
		agentID   string
		ttlRaw    string
		permRaw   string
		asJSON    bool
		tokenOnly bool
		scope     []string
	)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "--agent":
			if i+1 >= len(args) {
				return fmt.Errorf("--agent requires a value")
			}
			i++
			agentID = args[i]
		case "--ttl":
			if i+1 >= len(args) {
				return fmt.Errorf("--ttl requires a value")
			}
			i++
			ttlRaw = args[i]
		case "--perm":
			if i+1 >= len(args) {
				return fmt.Errorf("--perm requires a value")
			}
			i++
			permRaw = args[i]
		case "--json":
			asJSON = true
		case "--token-only":
			tokenOnly = true
		default:
			if strings.HasPrefix(arg, "--") {
				return fmt.Errorf("unknown flag %q", arg)
			}
			scope = append(scope, arg)
		}
	}
	if asJSON && tokenOnly {
		return fmt.Errorf("--json and --token-only are mutually exclusive")
	}
	if agentID == "" {
		return fmt.Errorf("--agent is required")
	}
	if ttlRaw == "" {
		return fmt.Errorf("--ttl is required")
	}
	if permRaw == "" {
		return fmt.Errorf("--perm is required (read|write)")
	}
	if permRaw != "read" && permRaw != "write" {
		return fmt.Errorf("invalid --perm %q: must be one of read, write", permRaw)
	}
	if len(scope) == 0 {
		return fmt.Errorf("at least one scope entry is required")
	}
	for _, entry := range scope {
		if _, _, err := parseSecretRef(entry); err != nil {
			return fmt.Errorf("invalid scope %q: %w", entry, err)
		}
	}
	ttl, err := time.ParseDuration(ttlRaw)
	if err != nil {
		return fmt.Errorf("invalid --ttl %q: %w", ttlRaw, err)
	}
	if ttl <= 0 {
		return fmt.Errorf("--ttl must be positive")
	}
	c, err := newVaultManagementClientFromEnv()
	if err != nil {
		return err
	}
	// `--task` was removed in V2a: task_id is not part of the /v1/vault/grants
	// contract. Passing --task now fails loudly via the `unknown flag` branch
	// above — we deliberately do NOT silently accept-and-drop it, because a
	// successful return with dropped semantics is worse than a clean break.
	resp, err := c.IssueVaultGrant(context.Background(), client.VaultGrantIssueRequest{
		Agent:      agentID,
		Scope:      scope,
		Perm:       permRaw,
		TTLSeconds: int(ttl / time.Second),
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
		_, _ = fmt.Fprintf(os.Stdout, "grant_id=%s\n", resp.GrantID)
		_, _ = fmt.Fprintf(os.Stdout, "expires_at=%s\n", resp.ExpiresAt.Format(time.RFC3339))
	}
	return nil
}

// SecretRevoke revokes a capability token or grant. The id space is split:
// ids with the `grt_` prefix are grants (issued via /v1/vault/grants in V2a)
// and dispatch to RevokeVaultGrant; everything else is a legacy token id
// (still accepted by the server under /v1/vault/tokens/<id>) and dispatches
// to RevokeVaultToken. Both endpoints remain live until the legacy cleanup
// wave.
func SecretRevoke(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage drive9 vault revoke <id>")
	}
	id := args[0]
	c, err := newVaultManagementClientFromEnv()
	if err != nil {
		return err
	}
	if strings.HasPrefix(id, "grt_") {
		return c.RevokeVaultGrant(context.Background(), id, "cli", "")
	}
	return c.RevokeVaultToken(context.Background(), id)
}

// SecretAudit queries vault audit events and applies client-side filters.
func SecretAudit(args []string) error {
	var (
		secretName string
		agentID    string
		sinceRaw   string
		limit      = defaultAuditLimit
		asJSON     bool
	)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--secret":
			if i+1 >= len(args) {
				return fmt.Errorf("--secret requires a value")
			}
			i++
			secretName = args[i]
		case "--agent":
			if i+1 >= len(args) {
				return fmt.Errorf("--agent requires a value")
			}
			i++
			agentID = args[i]
		case "--since":
			if i+1 >= len(args) {
				return fmt.Errorf("--since requires a value")
			}
			i++
			sinceRaw = args[i]
		case "--limit":
			if i+1 >= len(args) {
				return fmt.Errorf("--limit requires a value")
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid --limit %q", args[i])
			}
			limit = n
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("unknown flag %q", args[i])
		}
	}
	c, err := newVaultManagementClientFromEnv()
	if err != nil {
		return err
	}
	queryLimit := limit
	if agentID != "" || sinceRaw != "" {
		queryLimit = maxClientAuditLimit
	}
	if queryLimit > maxClientAuditLimit {
		queryLimit = maxClientAuditLimit
	}
	events, err := c.QueryVaultAudit(context.Background(), secretName, queryLimit)
	if err != nil {
		return err
	}
	if sinceRaw != "" {
		d, err := time.ParseDuration(sinceRaw)
		if err != nil {
			return fmt.Errorf("invalid --since %q: %w", sinceRaw, err)
		}
		if d <= 0 {
			return fmt.Errorf("--since must be positive")
		}
		events = filterAuditEvents(events, agentID, time.Now().Add(-d))
	} else if agentID != "" {
		events = filterAuditEvents(events, agentID, time.Time{})
	}
	if len(events) > limit {
		events = events[:limit]
	}
	if asJSON {
		return writeJSON(map[string]any{"events": events})
	}
	printAudit(events)
	return nil
}

// currentCapabilityToken returns the active delegated/capability token for
// call sites that explicitly need "is there a token in play" (e.g. SecretLs's
// branch to avoid silently enumerating the whole tenant when a scoped token
// was intentionally provided). It uses the unified resolver so env > config
// priority + Unsetenv mitigation apply.
func currentCapabilityToken() string {
	r := ResolveCredentials()
	if r.Kind == CredentialDelegated {
		return r.Token
	}
	return ""
}

// optionalVaultManagementClientFromEnv returns a tenant-scoped (owner API key)
// client when one can be resolved, or false when no owner credential is in
// play. Used by SecretLs to distinguish "owner enumeration" from "delegated
// readable-only enumeration". A delegated token in env/config does NOT satisfy
// this — the caller must hold an owner credential.
func optionalVaultManagementClientFromEnv() (*client.Client, bool) {
	r := ResolveCredentials()
	if r.Kind != CredentialOwner {
		return nil, false
	}
	return client.New(r.Server, r.APIKey), true
}

func newVaultManagementClientFromEnv() (*client.Client, error) {
	c, ok := optionalVaultManagementClientFromEnv()
	if !ok {
		return nil, fmt.Errorf("missing tenant API key; set %s or run drive9 create", EnvAPIKey)
	}
	return c, nil
}

// newVaultReadClientFromEnv requires a delegated capability token (server's
// vault read path is token-gated — an owner API key alone will be rejected
// server-side with EACCES). Resolution goes through the unified resolver so
// env > config priority + Unsetenv mitigation apply uniformly.
func newVaultReadClientFromEnv() (*client.Client, error) {
	r := ResolveCredentials()
	if r.Kind != CredentialDelegated {
		return nil, fmt.Errorf("missing capability token; set %s before using drive9 vault get/with", EnvVaultToken)
	}
	return client.New(r.Server, r.Token), nil
}

func validateSecretName(name string) error {
	if name == "" {
		return fmt.Errorf("secret name is required")
	}
	if strings.Contains(name, "/") {
		return fmt.Errorf("secret name %q must be flat in v0; use <name/field> only for reads and scopes", name)
	}
	if strings.Contains(name, "*") {
		return fmt.Errorf("wildcard scope entries are not supported in v0: %q", name)
	}
	return nil
}

func parseSecretRef(raw string) (string, string, error) {
	if raw == "" {
		return "", "", fmt.Errorf("secret reference is required")
	}
	parts := strings.SplitN(raw, "/", 2)
	name := parts[0]
	if err := validateSecretName(name); err != nil {
		return "", "", err
	}
	if len(parts) == 1 {
		return name, "", nil
	}
	field := parts[1]
	if field == "" {
		return "", "", fmt.Errorf("field name is required in %q", raw)
	}
	if strings.Contains(field, "*") {
		return "", "", fmt.Errorf("wildcard scope entries are not supported in v0: %q", raw)
	}
	return name, field, nil
}

func parseSecretFields(args []string) (map[string]string, error) {
	fields := make(map[string]string, len(args))
	var stdinValue []byte
	stdinRead := false
	for _, arg := range args {
		key, valueSpec, ok := strings.Cut(arg, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("field assignment must be field=value, field=@file, or field=-: %q", arg)
		}
		var value string
		switch {
		case valueSpec == "-":
			if !stdinRead {
				data, err := io.ReadAll(os.Stdin)
				if err != nil {
					return nil, fmt.Errorf("read stdin: %w", err)
				}
				stdinValue = data
				stdinRead = true
			}
			value = string(stdinValue)
		case strings.HasPrefix(valueSpec, "@"):
			data, err := os.ReadFile(valueSpec[1:])
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", valueSpec[1:], err)
			}
			value = string(data)
		default:
			value = valueSpec
		}
		fields[key] = value
	}
	return fields, nil
}

func parseFieldAssignments(args []string) (map[string]string, error) {
	return parseSecretFields(args)
}

func writeJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printEnv(fields map[string]string) error {
	envMap, err := buildSecretEnvMap(fields)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(envMap))
	for k := range envMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%s=%s\n", key, envMap[key])
	}
	return nil
}

func printAudit(events []client.VaultAuditEvent) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TIME\tAGENT\tACTION\tSECRET\tFIELD")
	for _, ev := range events {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			ev.Timestamp.Format(time.RFC3339),
			ev.AgentID,
			ev.EventType,
			ev.SecretName,
			ev.FieldName,
		)
	}
	_ = w.Flush()
}

// Secret fields map to env vars via a stable upper-snake transformation:
// non [A-Za-z0-9_] chars become '_', and leading digits are prefixed with '_'.
// Normalization collisions are rejected instead of silently overriding values.
func buildSecretEnvMap(fields map[string]string) (map[string]string, error) {
	env := make(map[string]string, len(fields))
	owners := make(map[string]string, len(fields))
	for field, value := range fields {
		envKey, err := normalizeSecretEnvKey(field)
		if err != nil {
			return nil, err
		}
		if prevField, exists := owners[envKey]; exists {
			return nil, fmt.Errorf("secret fields %q and %q both normalize to env var %q", prevField, field, envKey)
		}
		owners[envKey] = field
		env[envKey] = value
	}
	return env, nil
}

func envMapFromSecret(fields map[string]string) map[string]string {
	env, err := buildSecretEnvMap(fields)
	if err != nil {
		panic(err)
	}
	return env
}

func filterAuditEvents(events []client.VaultAuditEvent, agentID string, since time.Time) []client.VaultAuditEvent {
	filtered := make([]client.VaultAuditEvent, 0, len(events))
	for _, ev := range events {
		if agentID != "" && ev.AgentID != agentID {
			continue
		}
		if !since.IsZero() && ev.Timestamp.Before(since) {
			continue
		}
		filtered = append(filtered, ev)
	}
	return filtered
}

func mergeEnv(base []string, overrides map[string]string) []string {
	merged := make(map[string]string, len(base)+len(overrides))
	for _, entry := range base {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			merged[key] = value
		}
	}
	for k, v := range overrides {
		merged[k] = v
	}
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, k := range keys {
		env = append(env, k+"="+merged[k])
	}
	return env
}

func normalizeSecretEnvKey(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("secret field name is required")
	}
	var b strings.Builder
	b.Grow(len(field) + 1)
	for i := 0; i < len(field); i++ {
		ch := field[i]
		switch {
		case ch >= 'a' && ch <= 'z':
			b.WriteByte(ch - ('a' - 'A'))
		case ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9', ch == '_':
			b.WriteByte(ch)
		default:
			b.WriteByte('_')
		}
	}
	key := b.String()
	if key == "" {
		return "", fmt.Errorf("secret field name is required")
	}
	if key[0] >= '0' && key[0] <= '9' {
		key = "_" + key
	}
	return key, nil
}
