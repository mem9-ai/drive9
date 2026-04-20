package cli

import (
	"github.com/mem9-ai/dat9/pkg/client"
)

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
func NewFromEnv() *client.Client {
	r := ResolveCredentials()
	apiKey := ""
	if r.Kind == CredentialOwner {
		apiKey = r.APIKey
	}
	return client.New(r.Server, apiKey)
}
