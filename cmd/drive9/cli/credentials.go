package cli

import (
	"flag"
	"fmt"
	"os"
	"sync"
)

// Environment variable names recognised by the credential resolver. See spec
// §14.1. The dual-principal separation is locked — there is no single combined
// variable; DRIVE9_VAULT_TOKEN and DRIVE9_API_KEY remain distinct knobs.
//
// This exact set of three names is also the F14 scrub whitelist for
// `drive9 vault with` (spec §9 L209): the child process MUST NOT inherit
// any of these three from the parent, even when they are unset, absent, or
// identical to the current mount's credential. Other DRIVE9_* vars
// (profiling, log level, etc.) are outside the scrub — they do not grant
// authority — and flow through untouched. `scrubDrive9CredEnv` in
// secret.go imports these constants by name, so adding a new credential
// env var here requires a matching update there (and a new V2c-style
// review gate). The exactness of this list IS the contract.
const (
	EnvVaultToken = "DRIVE9_VAULT_TOKEN"
	EnvAPIKey     = "DRIVE9_API_KEY"
	EnvServer     = "DRIVE9_SERVER"
)

// CredentialKind classifies which principal the resolved credential
// authenticates as. Keeps callers from mis-routing an owner key to a vault
// read-path (or vice versa).
type CredentialKind int

const (
	CredentialNone CredentialKind = iota
	CredentialOwner
	CredentialDelegated
)

// ResolvedCredentials is the output of the unified credential resolver. Exactly
// one of APIKey / Token is non-empty when Kind != CredentialNone.
//
// Source describes where the credential and server URL came from, for error
// messages and debug tracing. It is informational; no authorization decision
// depends on Source (Invariant #7 keeps enforcement server-side).
type ResolvedCredentials struct {
	Kind         CredentialKind
	Server       string
	APIKey       string // set when Kind == CredentialOwner
	Token        string // set when Kind == CredentialDelegated
	CredSource   string // "env:DRIVE9_VAULT_TOKEN", "env:DRIVE9_API_KEY", "config:<ctx-name>", ""
	ServerSource string // "env:DRIVE9_SERVER", "config:<ctx-name>", "default"
}

// ResolveCredentials is the single source of truth for (credential, server)
// resolution across all drive9 CLI call sites. Priority matches spec §14.2:
//
//	Credential: DRIVE9_VAULT_TOKEN > DRIVE9_API_KEY > active context
//	Server:     DRIVE9_SERVER      > active-context.server > compiled-in default
//
// "First match wins" applies to *presence*, not validity. A set-but-malformed
// env var yields a distinct error at validation time; it must never silently
// fall through to a broader credential. This is the fix for the credential-
// confusion bug class called out in §14.2.
//
// Mitigation side effect: on first call, ResolveCredentials calls os.Unsetenv
// on the credential env vars so that child processes spawned by the current
// CLI invocation do not inherit credentials via /proc/<pid>/environ. This
// aligns drive9 with the "strip DRIVE9_* before exec" contract documented in
// the vault-quickstart Part 1 §6. DRIVE9_SERVER is also unset for symmetry;
// the credential-server mismatch surface is covered by §14.2.
//
// The result is cached per-process via sync.Once so that repeated calls
// within a single invocation (e.g. SecretLs calls currentCapabilityToken()
// and optionalVaultManagementClientFromEnv() back-to-back) see a consistent
// snapshot rather than losing env values after the first read-and-unset.
//
// The resolver does not validate JWT structure or API-key shape — that is
// deferred to the server (Invariant #7). It does trim leading/trailing
// whitespace to match file-based ingress (`ctx import`) behaviour.
func ResolveCredentials() ResolvedCredentials {
	credentialOnce.Do(func() {
		cachedCredentials = resolveCredentialsWithConfig(loadConfig())
	})
	return cachedCredentials
}

var (
	credentialOnce    sync.Once
	cachedCredentials ResolvedCredentials
)

// resetCredentialCacheForTest clears the per-process resolver cache. Test-
// only; callers in production would see an env-read-after-unset race.
func resetCredentialCacheForTest() {
	credentialOnce = sync.Once{}
	cachedCredentials = ResolvedCredentials{}
}

func resolveCredentialsWithConfig(cfg *Config) ResolvedCredentials {
	var r ResolvedCredentials

	// Consume all three credential-bearing env vars up-front so the Unsetenv
	// mitigation fires regardless of which one "wins" priority. Otherwise a
	// VAULT_TOKEN that wins would leave DRIVE9_API_KEY inheritable by
	// forked children — a real leak path for `secret exec`.
	envServer := consumeEnv(EnvServer)
	envToken := consumeEnv(EnvVaultToken)
	envAPIKey := consumeEnv(EnvAPIKey)

	// Server resolution is orthogonal to credential resolution (§14.2).
	if envServer != "" {
		r.Server = envServer
		r.ServerSource = "env:" + EnvServer
	}

	// Credential resolution: VAULT_TOKEN > API_KEY > active context.
	if envToken != "" {
		r.Kind = CredentialDelegated
		r.Token = envToken
		r.CredSource = "env:" + EnvVaultToken
	} else if envAPIKey != "" {
		r.Kind = CredentialOwner
		r.APIKey = envAPIKey
		r.CredSource = "env:" + EnvAPIKey
	} else if ctxName := cfg.CurrentContext; ctxName != "" {
		if ctx := cfg.Contexts[ctxName]; ctx != nil {
			switch ctx.Type {
			case PrincipalOwner:
				if ctx.APIKey != "" {
					r.Kind = CredentialOwner
					r.APIKey = ctx.APIKey
					r.CredSource = "config:" + ctxName
				}
			case PrincipalDelegated:
				if ctx.Token != "" {
					r.Kind = CredentialDelegated
					r.Token = ctx.Token
					r.CredSource = "config:" + ctxName
				}
			}
			if r.Server == "" && ctx.Server != "" {
				r.Server = ctx.Server
				r.ServerSource = "config:" + ctxName
			}
		}
	}

	if r.Server == "" {
		if cfg.Server != "" {
			r.Server = cfg.Server
			r.ServerSource = "config:server"
		} else {
			r.Server = defaultServerURL
			r.ServerSource = "default"
		}
	}

	return r
}

// consumeEnv reads an environment variable, trims surrounding whitespace, and
// if the value is non-empty unsets the variable before returning. The unset
// is what prevents a drive9 CLI invocation from leaking the credential into
// any child process it later forks (e.g. via `secret exec`).
//
// If the variable is set but contains only whitespace, it is treated as
// absent (and also unset) — this is not "malformed", it's empty-after-trim.
// True malformed-value handling (e.g. a JWT that does not decode) is the
// server's responsibility per Invariant #7 and returns EACCES via §11.
func consumeEnv(name string) string {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return ""
	}
	_ = os.Unsetenv(name)
	trimmed := trimASCIISpace(raw)
	return trimmed
}

func trimASCIISpace(s string) string {
	start := 0
	for start < len(s) && isASCIISpace(s[start]) {
		start++
	}
	end := len(s)
	for end > start && isASCIISpace(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isASCIISpace(b byte) bool {
	switch b {
	case ' ', '\t', '\r', '\n':
		return true
	}
	return false
}

const defaultServerURL = "https://api.drive9.ai"

// rejectEmptyFlag returns an error when a credential-bearing flag was passed
// with an explicit empty value (e.g. `--api-key=""`). Conflating explicit-empty
// with "unset" hides the user's intent and can silently fall through to a
// different credential; we reject at parse time per adv-2's review.
//
// Call this on each credential flag after flag.Parse.
func rejectEmptyFlag(flagName, value string, provided bool) error {
	if provided && value == "" {
		return fmt.Errorf("--%s was given an empty value; pass a non-empty credential or omit the flag", flagName)
	}
	return nil
}

// flagProvided reports whether a flag was explicitly set on the command line
// (as opposed to retaining its default value). Works for any flag.FlagSet.
func flagProvided(fs *flag.FlagSet, name string) bool {
	seen := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			seen = true
		}
	})
	return seen
}
