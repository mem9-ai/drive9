// Package slockoauth implements the Login-with-Slock OAuth-like flow used by
// Drive9 to bind a Slock principal to a tenant.
package slockoauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	Origin       string
	APIOrigin    string
	ClientID     string
	ClientSecret string
	PublicURL    string
	HTTPClient   *http.Client
}

func (c Config) Validate() error {
	var missing []string
	if strings.TrimSpace(c.Origin) == "" {
		missing = append(missing, "Origin")
	}
	if strings.TrimSpace(c.APIOrigin) == "" {
		missing = append(missing, "APIOrigin")
	}
	if strings.TrimSpace(c.ClientID) == "" {
		missing = append(missing, "ClientID")
	}
	if strings.TrimSpace(c.ClientSecret) == "" {
		missing = append(missing, "ClientSecret")
	}
	if strings.TrimSpace(c.PublicURL) == "" {
		missing = append(missing, "PublicURL")
	}
	if len(missing) > 0 {
		return fmt.Errorf("slockoauth: missing config: %s", strings.Join(missing, ", "))
	}
	return nil
}

type Client struct {
	cfg  Config
	http *http.Client
}

func New(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	cfg.Origin = strings.TrimRight(strings.TrimSpace(cfg.Origin), "/")
	cfg.APIOrigin = strings.TrimRight(strings.TrimSpace(cfg.APIOrigin), "/")
	cfg.PublicURL = strings.TrimRight(strings.TrimSpace(cfg.PublicURL), "/")
	cfg.ClientID = strings.TrimSpace(cfg.ClientID)
	cfg.ClientSecret = strings.TrimSpace(cfg.ClientSecret)
	return &Client{cfg: cfg, http: httpClient}, nil
}

func (c *Client) ClientID() string { return c.cfg.ClientID }

func (c *Client) CallbackURL() string {
	return c.cfg.PublicURL + "/v1/auth/slock/callback"
}

func (c *Client) LoginURL() string {
	q := url.Values{}
	q.Set("client_id", c.cfg.ClientID)
	q.Set("return_to", c.CallbackURL())
	return c.cfg.Origin + "/login-with-slock/setup?" + q.Encode()
}

type Token struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

type UserInfo struct {
	Sub               string  `json:"sub"`
	Type              string  `json:"type"`
	Scope             string  `json:"scope"`
	ClientID          string  `json:"client_id"`
	ClientName        string  `json:"client_name"`
	ServerID          string  `json:"server_id"`
	ServerSlug        string  `json:"server_slug"`
	PreferredUsername string  `json:"preferred_username"`
	Name              string  `json:"name"`
	AvatarURL         *string `json:"avatar_url"`
	Description       *string `json:"description"`
}

type OAuthError struct {
	Code        string `json:"error"`
	Description string `json:"error_description,omitempty"`
	Status      int    `json:"-"`
}

func (e OAuthError) Error() string {
	if e.Description != "" {
		return fmt.Sprintf("slock oauth error %q: %s", e.Code, e.Description)
	}
	if e.Code != "" {
		return "slock oauth error: " + e.Code
	}
	return fmt.Sprintf("slock oauth http %d", e.Status)
}

func (c *Client) ExchangeCode(ctx context.Context, code string) (Token, error) {
	code = strings.TrimSpace(code)
	if code == "" {
		return Token{}, errors.New("slockoauth: code is required")
	}
	rawBody, err := json.Marshal(map[string]string{
		"grant_type": "authorization_code",
		"code":       code,
	})
	if err != nil {
		return Token{}, fmt.Errorf("slockoauth: encode token request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.APIOrigin+"/api/oauth/token", bytes.NewReader(rawBody))
	if err != nil {
		return Token{}, fmt.Errorf("slockoauth: build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	basic := base64.StdEncoding.EncodeToString([]byte(c.cfg.ClientID + ":" + c.cfg.ClientSecret))
	req.Header.Set("Authorization", "Basic "+basic)

	resp, err := c.http.Do(req)
	if err != nil {
		return Token{}, fmt.Errorf("slockoauth: token request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Token{}, fmt.Errorf("slockoauth: read token response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Token{}, parseOAuthError(resp.StatusCode, raw)
	}
	var tok Token
	if err := json.Unmarshal(raw, &tok); err != nil {
		return Token{}, fmt.Errorf("slockoauth: decode token response: %w", err)
	}
	if strings.TrimSpace(tok.AccessToken) == "" {
		return Token{}, errors.New("slockoauth: token response missing access_token")
	}
	return tok, nil
}

func (c *Client) Userinfo(ctx context.Context, accessToken string) (UserInfo, error) {
	accessToken = strings.TrimSpace(accessToken)
	if accessToken == "" {
		return UserInfo{}, errors.New("slockoauth: access token is required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.APIOrigin+"/api/oauth/userinfo", nil)
	if err != nil {
		return UserInfo{}, fmt.Errorf("slockoauth: build userinfo request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := c.http.Do(req)
	if err != nil {
		return UserInfo{}, fmt.Errorf("slockoauth: userinfo request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return UserInfo{}, fmt.Errorf("slockoauth: read userinfo response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return UserInfo{}, parseOAuthError(resp.StatusCode, raw)
	}
	var info UserInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return UserInfo{}, fmt.Errorf("slockoauth: decode userinfo response: %w", err)
	}
	if err := c.validateUserInfo(info); err != nil {
		return UserInfo{}, err
	}
	return info, nil
}

func (c *Client) validateUserInfo(info UserInfo) error {
	if strings.TrimSpace(info.Sub) == "" {
		return errors.New("slockoauth: userinfo missing sub")
	}
	if strings.TrimSpace(info.ServerID) == "" {
		return errors.New("slockoauth: userinfo missing server_id")
	}
	switch info.Type {
	case "human", "agent":
	default:
		return fmt.Errorf("slockoauth: unsupported userinfo type %q", info.Type)
	}
	if info.ClientID != c.cfg.ClientID {
		return fmt.Errorf("slockoauth: userinfo client_id %q does not match configured client_id", info.ClientID)
	}
	return nil
}

func parseOAuthError(status int, raw []byte) error {
	var oe OAuthError
	if err := json.Unmarshal(raw, &oe); err == nil && (oe.Code != "" || oe.Description != "") {
		oe.Status = status
		return oe
	}
	return OAuthError{Status: status}
}
