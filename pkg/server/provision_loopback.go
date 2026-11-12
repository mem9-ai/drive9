package server

import (
	"net"
	"net/http"
	"strings"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

// provisionRequiresAuthentication reports whether POST /v1/provision must carry a
// caller credential. Anonymous provisioning is the explicit
// DRIVE9_ALLOW_ANONYMOUS_PROVISION opt-in; the local development provider is the
// only implicit exception, and it is limited to requests received over the loopback
// interface so a server bound to a network address cannot be provisioned by an
// unauthenticated remote caller.
func (s *Server) provisionRequiresAuthentication(r *http.Request, provider string) bool {
	if s.allowAnonymousProvision {
		return false
	}
	if provider != tenant.ProviderLocal {
		return true
	}
	return !requestIsLoopback(r)
}

// requestIsLoopback reports whether a request reached this process directly over the
// loopback interface.
//
// A forwarding header means it did not: a reverse proxy, ingress or sidecar in front
// of the server connects over loopback too, so the peer address alone would let a
// remote caller through a proxied hop look local. The headers are not trusted for the
// opposite reason as well — the caller controls them — so their presence is treated as
// "not a direct loopback request" and the anonymous local exception is refused. An
// operator who really wants anonymous provisioning through a proxy sets
// DRIVE9_ALLOW_ANONYMOUS_PROVISION, which bypasses this check deliberately.
func requestIsLoopback(r *http.Request) bool {
	if r == nil {
		return false
	}
	if requestLooksProxied(r) {
		return false
	}
	remote := strings.TrimSpace(r.RemoteAddr)
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// requestLooksProxied reports whether any proxy-forwarding header is present. Presence
// is what counts, not the value: an empty or whitespace-only header still means a
// proxy hop added it, and letting a caller suppress the signal by sending an empty
// value would reopen the loopback exception it exists to close.
func requestLooksProxied(r *http.Request) bool {
	// Match the keys case-insensitively rather than through Header.Values: the server
	// canonicalizes incoming names, but a header added by in-process middleware or a
	// test with a non-canonical spelling must not slip past the check.
	for name := range r.Header {
		for _, want := range [...]string{"Forwarded", "X-Forwarded-For", "X-Real-Ip", "X-Forwarded-Host", "X-Forwarded-Proto", "Via", "True-Client-IP"} {
			if strings.EqualFold(name, want) {
				return true
			}
		}
	}
	return false
}
