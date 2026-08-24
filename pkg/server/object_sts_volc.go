package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
)

var volcengineHTTPClient = &http.Client{Timeout: 30 * time.Second}

func mintVolcengineTOSSession(ctx context.Context, backend *meta.OrgObjectBackend, secret, externalID, prefix string, write bool, ttl int) (*objectCredentialsResponse, error) {
	if strings.TrimSpace(backend.RoleARN) == "" {
		return nil, fmt.Errorf("tos mint requires role_arn (Volcengine AssumeRole)")
	}
	if strings.TrimSpace(backend.AccessKeyID) == "" || secret == "" {
		return nil, fmt.Errorf("tos mint requires access_key_id and secret_access_key to call AssumeRole")
	}
	query := url.Values{}
	query.Set("Action", "AssumeRole")
	query.Set("Version", "2018-01-01")
	query.Set("RoleTrn", backend.RoleARN)
	query.Set("RoleSessionName", "drive9-object")
	query.Set("DurationSeconds", strconv.Itoa(ttl))
	query.Set("Policy", tosSessionPolicy(backend.Bucket, prefix, write))
	if externalID != "" {
		query.Set("ExternalId", externalID)
	}

	endpoint := strings.TrimSpace(backend.STSEndpoint)
	if endpoint == "" {
		endpoint = "https://sts.volcengineapi.com"
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid tos sts_endpoint")
	}
	signedQuery := canonicalQuery(query)
	u.RawQuery = signedQuery
	region := volcengineSTSRegion(backend.Region, u.Host)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("tos assume role request: %w", err)
	}
	now := time.Now().UTC()
	payloadHash := hashSHA256Hex(nil)
	xDate, auth := signVolcengine(backend.AccessKeyID, secret, region, "sts", http.MethodGet, u.Host, "/", signedQuery, payloadHash, now)
	req.Header.Set("Host", u.Host)
	req.Header.Set("X-Date", xDate)
	req.Header.Set("Authorization", auth)
	req.Header.Set("Accept", "application/json")

	resp, err := volcengineHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tos assume role: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read tos assume role response: %w", err)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("tos assume role: http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var parsed volcAssumeRoleResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decode tos assume role response: %w", err)
	}
	if parsed.ResponseMetadata.Error.Code != "" {
		return nil, fmt.Errorf("tos assume role: %s: %s", parsed.ResponseMetadata.Error.Code, parsed.ResponseMetadata.Error.Message)
	}
	c := parsed.Result.Credentials
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return nil, fmt.Errorf("tos assume role returned empty credentials")
	}
	var exp *time.Time
	if c.ExpiredTime != "" {
		if t, err := time.Parse(time.RFC3339, c.ExpiredTime); err == nil {
			t = t.UTC()
			exp = &t
		}
	}
	return hmacSession(c.AccessKeyID, c.SecretAccessKey, c.SessionToken, exp), nil
}

type volcAssumeRoleResponse struct {
	ResponseMetadata struct {
		Error struct {
			Code    string `json:"Code"`
			Message string `json:"Message"`
		} `json:"Error"`
	} `json:"ResponseMetadata"`
	Result struct {
		Credentials struct {
			AccessKeyID     string `json:"AccessKeyId"`
			SecretAccessKey string `json:"SecretAccessKey"`
			SessionToken    string `json:"SessionToken"`
			ExpiredTime     string `json:"ExpiredTime"`
		} `json:"Credentials"`
	} `json:"Result"`
}

func volcengineSTSRegion(backendRegion, host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	if strings.HasPrefix(host, "sts.") && strings.HasSuffix(host, ".volcengineapi.com") {
		mid := strings.TrimSuffix(strings.TrimPrefix(host, "sts."), ".volcengineapi.com")
		if mid != "" && !strings.Contains(mid, ".") {
			return mid
		}
	}
	if r := strings.TrimSpace(backendRegion); r != "" {
		return r
	}
	return "cn-north-1"
}

func signVolcengine(ak, sk, region, service, method, host, canonicalURI, queryString, payloadHash string, t time.Time) (xDate, authorization string) {
	xDate = t.UTC().Format("20060102T150405Z")
	shortDate := t.UTC().Format("20060102")
	signedHeaders := "host;x-date"
	canonicalHeaders := "host:" + strings.ToLower(host) + "\n" + "x-date:" + xDate + "\n"
	canonicalRequest := strings.Join([]string{method, canonicalURI, queryString, canonicalHeaders, signedHeaders, payloadHash}, "\n")
	credentialScope := shortDate + "/" + region + "/" + service + "/request"
	stringToSign := strings.Join([]string{"HMAC-SHA256", xDate, credentialScope, hashSHA256Hex([]byte(canonicalRequest))}, "\n")
	kDate := hmacSHA256([]byte(sk), shortDate)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "request")
	sig := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
	authorization = "HMAC-SHA256 Credential=" + ak + "/" + credentialScope + ", SignedHeaders=" + signedHeaders + ", Signature=" + sig
	return xDate, authorization
}

func canonicalQuery(v url.Values) string {
	if len(v) == 0 {
		return ""
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		vals := append([]string(nil), v[k]...)
		sort.Strings(vals)
		ek := uriEncode(k, true)
		for _, val := range vals {
			parts = append(parts, ek+"="+uriEncode(val, true))
		}
	}
	return strings.Join(parts, "&")
}

func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' || (c == '/' && !encodeSlash) {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

func hashSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
