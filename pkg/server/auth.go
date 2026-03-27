package server

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// authMiddleware returns an HTTP handler that validates the Authorization
// header against the configured API key. If apiKey is empty, all requests
// are allowed (backward-compatible local dev mode).
func authMiddleware(apiKey string, next http.Handler) http.Handler {
	if apiKey == "" {
		return next
	}
	keyBytes := []byte(apiKey)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearerToken(r)
		if token == "" {
			errJSON(w, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}
		if subtle.ConstantTimeCompare([]byte(token), keyBytes) != 1 {
			errJSON(w, http.StatusUnauthorized, "invalid API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerToken extracts the token from "Authorization: Bearer <token>".
// Returns "" if the header is missing, not Bearer-prefixed, or the token is blank.
func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return ""
	}
	token := strings.TrimSpace(h[len(prefix):])
	return token
}
