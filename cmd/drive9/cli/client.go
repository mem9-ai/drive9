package cli

import (
	"context"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// fsClientWarmTimeout caps the synchronous /v1/status fetch issued by
// upload-bearing commands so a slow or unreachable server can't stall the
// CLI. On timeout the client just falls back to compiled defaults; one
// extra multipart RTT is preferable to hanging the CLI.
const fsClientWarmTimeout = 3 * time.Second

// NewFromEnv returns an owner-scoped client for the drive9 fs plane
// (drive9 fs cp/cat/ls/mv/rm/sh/grep/find/stat). It is the single entry
// point for runFS in cmd/drive9/main.go.
//
// Invariant: the fs plane is owner-only by construction — delegated JWTs
// are not accepted on fs endpoints server-side. If a delegated credential
// is the only one resolvable, this returns a client whose API key is
// empty, and the request will fail with EACCES at the server (Invariant #7).
// Callers that need to report a clearer error should check Kind themselves
// before dispatch; retaining the lenient shape keeps parity with the pre-
// resolver behaviour (which also returned an empty key).
//
// Credential resolution goes through the unified resolver per §14.2
// (env > config, Unsetenv-after-read).
//
// NewFromEnv intentionally does NOT warm the /v1/status cache: read-only
// commands (ls/cat/stat/rm/grep/find) don't need the upload threshold and
// shouldn't pay an extra RTT (or wait on a slow server) before issuing
// their actual request. Upload-bearing commands should call
// NewFromEnvWithWarm.
func NewFromEnv() *client.Client {
	r := ResolveCredentials()
	apiKey := ""
	if r.Kind == CredentialOwner {
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
