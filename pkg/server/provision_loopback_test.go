package server

import (
	"net/http"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

// provider=local is the one provider that provisions without a caller credential,
// and the startup guard that refuses a non-loopback bind only applies while generated
// development keys are in use. The anonymous exception is therefore scoped to the
// request: a deployment that binds a network address must not create tenants for
// unauthenticated remote callers, while loopback (dev servers, e2e on 127.0.0.1)
// keeps working.
func TestProvisionRequiresAuthenticationScopesLocalToLoopback(t *testing.T) {
	remote := func() *http.Request { return &http.Request{RemoteAddr: "203.0.113.9:5555"} }
	loopback := func() *http.Request { return &http.Request{RemoteAddr: "127.0.0.1:5555"} }
	loopbackV6 := func() *http.Request { return &http.Request{RemoteAddr: "[::1]:5555"} }

	forwarded := remote()
	forwarded.Header = http.Header{"X-Forwarded-For": []string{"127.0.0.1"}, "X-Real-Ip": []string{"127.0.0.1"}}

	cases := []struct {
		name     string
		srv      *Server
		req      *http.Request
		provider string
		want     bool
	}{
		{"local from loopback", &Server{}, loopback(), tenant.ProviderLocal, false},
		{"local from loopback v6", &Server{}, loopbackV6(), tenant.ProviderLocal, false},
		{"local from a network address", &Server{}, remote(), tenant.ProviderLocal, true},
		{"local with loopback forwarded headers only", &Server{}, forwarded, tenant.ProviderLocal, true},
		{"local when anonymous provisioning is enabled", &Server{allowAnonymousProvision: true}, remote(), tenant.ProviderLocal, false},
		{"local with an unparsable remote address", &Server{}, &http.Request{}, tenant.ProviderLocal, true},
		{"another provider from loopback", &Server{}, loopback(), tenant.ProviderTiDBZero, true},
		{"another provider from a network address", &Server{}, remote(), tenant.ProviderTiDBZero, true},
		{"another provider with anonymous provisioning enabled", &Server{allowAnonymousProvision: true}, remote(), tenant.ProviderTiDBZero, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.srv.provisionRequiresAuthentication(tc.req, tc.provider); got != tc.want {
				t.Fatalf("provisionRequiresAuthentication(%s) = %t, want %t", tc.req.RemoteAddr, got, tc.want)
			}
		})
	}
}

func TestRequestIsLoopback(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:1":          true,
		"[::1]:1":              true,
		"127.0.0.1":            true,
		"203.0.113.9:1":        false,
		"[2001:db8::1]:1":      false,
		"":                     false,
		"localhost:1":          false, // a host name is not resolved for this check
		"[::ffff:127.0.0.1]:1": true,
	}
	for addr, want := range cases {
		if got := requestIsLoopback(&http.Request{RemoteAddr: addr}); got != want {
			t.Fatalf("requestIsLoopback(%q) = %t, want %t", addr, got, want)
		}
	}
	if requestIsLoopback(nil) {
		t.Fatal("nil request must not be loopback")
	}
}

// A proxy in front of the server connects over loopback too, so the peer address
// alone would let a remote caller through: any forwarding header means the request
// did not arrive directly and loses the anonymous local exception.
func TestRequestIsLoopbackRefusesProxiedRequests(t *testing.T) {
	for _, header := range []string{"Forwarded", "X-Forwarded-For", "X-Real-Ip", "X-Forwarded-Host", "X-Forwarded-Proto", "Via", "True-Client-IP"} {
		req := &http.Request{
			RemoteAddr: "127.0.0.1:5555",
			Header:     http.Header{header: []string{"example"}},
		}
		if requestIsLoopback(req) {
			t.Fatalf("%s: a proxied request was treated as a direct loopback request", header)
		}
		if !(&Server{}).provisionRequiresAuthentication(req, tenant.ProviderLocal) {
			t.Fatalf("%s: anonymous local provisioning allowed through a proxy hop", header)
		}
	}
	// Presence is what matters: an empty or whitespace-only value still means a proxy
	// hop handled the request, and a caller must not be able to suppress the signal.
	for _, value := range []string{"", " ", "\t"} {
		req := &http.Request{
			RemoteAddr: "127.0.0.1:5555",
			Header:     http.Header{"X-Forwarded-For": []string{value}},
		}
		if requestIsLoopback(req) {
			t.Fatalf("forwarding header %q was ignored", value)
		}
		if !(&Server{}).provisionRequiresAuthentication(req, tenant.ProviderLocal) {
			t.Fatalf("forwarding header %q allowed anonymous local provisioning", value)
		}
	}
}
