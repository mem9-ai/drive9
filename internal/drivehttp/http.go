// Package drivehttp contains repository-internal HTTP wiring shared by the
// public SDK and internal clients such as the FUSE promotion coordinator.
package drivehttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// NewClient returns the standard Drive9 HTTP client. Keeping the transport in
// one internal package prevents internal protocol users from drifting from the
// public SDK's proxy, pooling, DNS fallback, and redirect-credential policy.
func NewClient() *http.Client {
	var transport *http.Transport
	if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = defaultTransport.Clone()
	} else {
		transport = &http.Transport{}
	}
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 64

	baseDialer := &net.Dialer{Timeout: 10 * time.Second}
	fallbackResolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: 5 * time.Second}
			conn, err := dialer.DialContext(ctx, "udp", "8.8.8.8:53")
			if err != nil {
				conn, err = dialer.DialContext(ctx, "udp", "1.1.1.1:53")
			}
			return conn, err
		},
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := baseDialer.DialContext(ctx, network, address)
		if err == nil {
			return conn, nil
		}
		var dnsErr *net.DNSError
		if !errors.As(err, &dnsErr) {
			return nil, err
		}
		host, port, splitErr := net.SplitHostPort(address)
		if splitErr != nil {
			return nil, err
		}
		ips, resolveErr := fallbackResolver.LookupHost(ctx, host)
		if resolveErr != nil || len(ips) == 0 {
			return nil, err
		}
		return baseDialer.DialContext(ctx, network, net.JoinHostPort(ips[0], port))
	}

	return &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > 0 && req.URL.Host != via[0].URL.Host {
				req.Header.Del("Authorization")
				req.Header.Del("X-Dat9-Actor")
			}
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}
}

// ApplyCredentials adds the standard Drive9 credential headers to req.
func ApplyCredentials(req *http.Request, credential, actor string) {
	if credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	if actor != "" {
		req.Header.Set("X-Dat9-Actor", actor)
	}
}

// DecodeErrorBody decodes both Drive9 error-envelope shapes. Callers retain
// their package-specific error types while sharing the wire interpretation.
func DecodeErrorBody(statusCode int, body []byte) (message, code string) {
	var flat struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if json.Unmarshal(body, &flat) == nil && flat.Error != "" {
		return flat.Error, flat.Code
	}
	var nested struct {
		Error struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &nested) == nil && nested.Error.Message != "" {
		return nested.Error.Message, nested.Error.Code
	}
	return fmt.Sprintf("HTTP %d: %s", statusCode, body), ""
}
