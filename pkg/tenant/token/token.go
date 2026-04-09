package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Claims struct {
	TenantID     string `json:"tenant_id"`
	TokenVersion int    `json:"token_version"`
	IssuedAt     int64  `json:"iat"`
	ExpiresAt    int64  `json:"exp,omitempty"`
}

const tokenPrefix = "dat9_"

func NewID() string { return uuid.NewString() }

func HashToken(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

func IssueToken(secret []byte, tenantID string, tokenVersion int) (string, error) {
	return IssueTokenWithExpiry(secret, tenantID, tokenVersion, time.Time{})
}

func IssueTokenWithExpiry(secret []byte, tenantID string, tokenVersion int, expiresAt time.Time) (string, error) {
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	payload := Claims{TenantID: tenantID, TokenVersion: tokenVersion, IssuedAt: time.Now().Unix()}
	if !expiresAt.IsZero() {
		payload.ExpiresAt = expiresAt.Unix()
	}

	h, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	p, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	head := base64.RawURLEncoding.EncodeToString(h)
	body := base64.RawURLEncoding.EncodeToString(p)
	msg := head + "." + body

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(msg))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	jwt := msg + "." + sig
	return tokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(jwt)), nil
}

func ParseAndVerifyToken(secret []byte, raw string) (*Claims, error) {
	return parseAndVerifyTokenAt(secret, raw, time.Now().Unix())
}

func parseAndVerifyTokenAt(secret []byte, raw string, nowUnix int64) (*Claims, error) {
	if !strings.HasPrefix(raw, tokenPrefix) {
		return nil, fmt.Errorf("invalid token format")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(raw, tokenPrefix))
	if err != nil {
		return nil, fmt.Errorf("invalid token format")
	}
	raw = string(decoded)

	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid token format")
	}
	msg := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(msg))
	expected := mac.Sum(nil)

	actual, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid token signature")
	}
	if !hmac.Equal(actual, expected) {
		return nil, fmt.Errorf("token signature mismatch")
	}

	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid token payload")
	}
	var claims Claims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, fmt.Errorf("invalid token claims")
	}
	if claims.TenantID == "" || claims.TokenVersion <= 0 || claims.IssuedAt <= 0 {
		return nil, fmt.Errorf("invalid token claims")
	}
	if claims.ExpiresAt > 0 {
		if claims.ExpiresAt <= claims.IssuedAt {
			return nil, fmt.Errorf("invalid token claims")
		}
		if nowUnix >= claims.ExpiresAt {
			return nil, fmt.Errorf("token expired")
		}
	}
	return &claims, nil
}
