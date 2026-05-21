package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// fsClientWarmTimeout caps the synchronous /v1/status fetch issued by
// upload-bearing commands so a slow or unreachable server can't stall the
// CLI. On timeout the client just falls back to compiled defaults; one
// extra multipart RTT is preferable to hanging the CLI.
const fsClientWarmTimeout = 3 * time.Second

// NewFromEnv returns a tenant API-key client for the drive9 fs plane
// (drive9 fs cp/cat/ls/mv/rm/sh/grep/find/stat). Tenant API keys include
// legacy owner keys and workspace-zone fs_scoped keys; both travel through
// DRIVE9_API_KEY / owner contexts and are distinguished server-side.
//
// Delegated vault capability tokens (DRIVE9_VAULT_TOKEN) are not accepted on
// fs endpoints. If a vault token is the only resolvable credential, this
// returns a client whose API key is empty and the request fails server-side.
// Callers that need a clearer error should check Kind themselves before
// dispatch; retaining the lenient shape keeps parity with the pre-resolver
// behaviour (which also returned an empty key).
//
// Credential resolution goes through the unified resolver: env credentials
// override config credentials, while the active context's server URL wins
// over DRIVE9_SERVER. Resolver reads still use Unsetenv-after-read.
//
// NewFromEnv intentionally does NOT warm the /v1/status cache: read-only
// commands (ls/cat/stat/rm/grep/find) don't need the upload threshold and
// shouldn't pay an extra RTT (or wait on a slow server) before issuing
// their actual request. Upload-bearing commands should call
// NewFromEnvWithWarm.
func NewFromEnv() *client.Client {
	r := ResolveCredentials()
	apiKey := ""
	if r.Kind == CredentialOwner || r.Kind == CredentialFSScoped {
		apiKey = r.APIKey
	}
	return client.New(r.Server, apiKey)
}

// NewFromEnvWithWarm is NewFromEnv plus a synchronous /v1/status warm,
// bounded by fsClientWarmTimeout. Use this from commands that perform
// uploads (cp, secret put, anything that may hit the simple-PUT vs V2
// multipart split) so the very first upload picks up the server's
// configured inline_threshold instead of the local fallback.
//
// A failed warm leaves the client unflagged: subsequent client methods
// that themselves call Warm/MaxUploadBytes/SmallFileThreshold will retry,
// rather than caching the failure forever.
func NewFromEnvWithWarm() *client.Client {
	c := NewFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), fsClientWarmTimeout)
	defer cancel()
	c.Warm(ctx)
	return c
}

func newFSClientForContext(name string) (*client.Client, error) {
	cfg := loadConfig()
	ctx := cfg.Contexts[name]
	if ctx == nil {
		return nil, fmt.Errorf("context %q not found; run: drive9 ctx ls", name)
	}

	server := ctx.Server
	if server == "" {
		if cfg.Server != "" {
			server = cfg.Server
		} else {
			server = defaultServerURL
		}
	}

	switch ctx.Type {
	case PrincipalOwner, PrincipalFSScoped:
		if ctx.APIKey == "" {
			return nil, fmt.Errorf("context %q has no API key", name)
		}
		return client.New(server, ctx.APIKey), nil
	case PrincipalDelegated:
		return nil, fmt.Errorf("context %q is delegated; fs commands require an owner or fs_scoped context", name)
	default:
		return nil, fmt.Errorf("context %q has unsupported type %q", name, ctx.Type)
	}
}

func fsClientForRemoteArg(defaultClient *client.Client, raw string) (*client.Client, string, string, bool, error) {
	rp, isRemote := ParseRemote(raw)
	if !isRemote {
		return defaultClient, raw, "", false, nil
	}
	if rp.Context == "" {
		return defaultClient, rp.Path, "", true, nil
	}

	c, err := newFSClientForContext(rp.Context)
	if err != nil {
		return nil, "", "", true, err
	}
	return c, rp.Path, rp.Context, true, nil
}
