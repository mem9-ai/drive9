package slockoauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestClient(t *testing.T, h http.Handler) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(Config{
		Origin:       "https://app.slock.ai",
		APIOrigin:    srv.URL,
		ClientID:     "drive9",
		ClientSecret: "secret",
		PublicURL:    "https://drive9.example.com",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, srv
}

func TestLoginURL(t *testing.T) {
	c, err := New(Config{
		Origin:       "https://app.slock.ai/",
		APIOrigin:    "https://api.slock.ai/",
		ClientID:     "drive9",
		ClientSecret: "secret",
		PublicURL:    "https://drive9.example.com/",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := c.LoginURL()
	if !strings.HasPrefix(got, "https://app.slock.ai/login-with-slock/setup?") {
		t.Fatalf("LoginURL = %q", got)
	}
	if !strings.Contains(got, "client_id=drive9") {
		t.Fatalf("LoginURL missing client_id: %q", got)
	}
	if !strings.Contains(got, "return_to=https%3A%2F%2Fdrive9.example.com%2Fv1%2Fauth%2Fslock%2Fcallback") {
		t.Fatalf("LoginURL missing callback: %q", got)
	}
}

func TestNewTrimsClientSecret(t *testing.T) {
	reqErr := make(chan error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("drive9:secret"))
		if r.Header.Get("Authorization") != want {
			reqErr <- fmt.Errorf("Authorization = %q, want %q", r.Header.Get("Authorization"), want)
			return
		}
		reqErr <- nil
		_, _ = w.Write([]byte(`{"access_token":"tok"}`))
	}))
	t.Cleanup(srv.Close)
	c, err := New(Config{
		Origin:       "https://app.slock.ai",
		APIOrigin:    srv.URL,
		ClientID:     "drive9",
		ClientSecret: " secret ",
		PublicURL:    "https://drive9.example.com",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := c.ExchangeCode(context.Background(), "abc"); err != nil {
		t.Fatalf("ExchangeCode: %v", err)
	}
	if err := <-reqErr; err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
}

func TestExchangeCodeSuccess(t *testing.T) {
	reqErr := make(chan error, 1)
	c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/oauth/token" || r.Method != http.MethodPost {
			reqErr <- fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			return
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Basic ") {
			reqErr <- fmt.Errorf("missing basic auth")
			return
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			reqErr <- fmt.Errorf("decode body: %v", err)
			return
		}
		if body["grant_type"] != "authorization_code" || body["code"] != "abc" {
			reqErr <- fmt.Errorf("unexpected body: %#v", body)
			return
		}
		reqErr <- nil
		_, _ = w.Write([]byte(`{"access_token":"tok","token_type":"Bearer","expires_in":3600,"scope":"identity openid profile"}`))
	}))

	tok, err := c.ExchangeCode(context.Background(), "abc")
	if err != nil {
		t.Fatalf("ExchangeCode: %v", err)
	}
	if err := <-reqErr; err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if tok.AccessToken != "tok" {
		t.Fatalf("AccessToken = %q", tok.AccessToken)
	}
}

func TestExchangeCodeFailures(t *testing.T) {
	t.Run("empty code", func(t *testing.T) {
		c, _ := New(Config{Origin: "http://slock", APIOrigin: "http://api", ClientID: "drive9", ClientSecret: "secret", PublicURL: "http://drive9"})
		if _, err := c.ExchangeCode(context.Background(), " "); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("oauth error body", func(t *testing.T) {
		c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"bad secret"}`))
		}))
		_, err := c.ExchangeCode(context.Background(), "abc")
		var oe OAuthError
		if !errors.As(err, &oe) || oe.Code != "invalid_client" || oe.Status != http.StatusUnauthorized {
			t.Fatalf("error = %#v, want OAuthError invalid_client", err)
		}
	})
	t.Run("bad json", func(t *testing.T) {
		c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{`))
		}))
		if _, err := c.ExchangeCode(context.Background(), "abc"); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestUserinfoSuccess(t *testing.T) {
	reqErr := make(chan error, 1)
	c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/oauth/userinfo" || r.Method != http.MethodGet {
			reqErr <- fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			return
		}
		if r.Header.Get("Authorization") != "Bearer tok" {
			reqErr <- fmt.Errorf("Authorization = %q", r.Header.Get("Authorization"))
			return
		}
		reqErr <- nil
		_, _ = w.Write([]byte(`{"sub":"sub-1","type":"agent","client_id":"drive9","server_id":"server-1","server_slug":"dev","preferred_username":"assistant","name":"Assistant"}`))
	}))

	info, err := c.Userinfo(context.Background(), "tok")
	if err != nil {
		t.Fatalf("Userinfo: %v", err)
	}
	if err := <-reqErr; err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if info.Sub != "sub-1" || info.ServerID != "server-1" || info.Type != "agent" {
		t.Fatalf("unexpected info: %+v", info)
	}
}

func TestUserinfoValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing sub", body: `{"type":"agent","client_id":"drive9","server_id":"server-1"}`},
		{name: "missing server", body: `{"sub":"sub-1","type":"agent","client_id":"drive9"}`},
		{name: "bad type", body: `{"sub":"sub-1","type":"bot","client_id":"drive9","server_id":"server-1"}`},
		{name: "bad client", body: `{"sub":"sub-1","type":"agent","client_id":"other","server_id":"server-1"}`},
		{name: "bad json", body: `{`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			if _, err := c.Userinfo(context.Background(), "tok"); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestUserinfoHTTPError(t *testing.T) {
	c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`not json`))
	}))
	_, err := c.Userinfo(context.Background(), "tok")
	var oe OAuthError
	if !errors.As(err, &oe) || oe.Status != http.StatusBadGateway {
		t.Fatalf("error = %#v, want OAuthError", err)
	}
}
