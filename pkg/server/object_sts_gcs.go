package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/credentials/downscope"
	"golang.org/x/oauth2/google"
)

// gcsDownscopeScope is the base scope requested for the service account; the
// Credential Access Boundary exchange narrows it to the one backend bucket. The
// downscope package hardcodes the identity-binding endpoint, so a
// customer-supplied credentials document cannot retarget the exchange.
const gcsDownscopeScope = "https://www.googleapis.com/auth/cloud-platform"

// gcsObjectTokenMint is a seam for tests; production exchanges the org key for
// a bucket-scoped Credential Access Boundary token.
var gcsObjectTokenMint = mintGCSObjectToken

// gcsObjectHTTPClient is the egress client for the token fetch and the
// Credential Access Boundary exchange. The credentials document is
// customer-controlled, so the exchange must not share the process default
// client (no ambient proxy/config surprises) and must have a deadline.
var gcsObjectHTTPClient = &http.Client{Timeout: 30 * time.Second}

// mintGCSObjectToken exchanges an org service-account key for an OAuth access
// token downscoped by a Credential Access Boundary to one bucket.
//
// The org service account's project-wide IAM is never handed to a tenant: an
// unscoped devstorage.read_write token lets any tenant owner read and write
// every bucket in the project, which defeats the per-backend isolation the
// object-credential endpoint assumes. A read session gets objectViewer, a
// write session objectAdmin, both limited to the backend bucket.
func mintGCSObjectToken(ctx context.Context, credentialsJSON, bucket string, write bool, httpClient *http.Client) (string, time.Time, error) {
	if strings.TrimSpace(credentialsJSON) == "" {
		return "", time.Time{}, fmt.Errorf("gcs object token mint requires a service-account JSON key")
	}
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return "", time.Time{}, fmt.Errorf("gcs object token mint requires a bucket")
	}
	// The bucket is interpolated into the boundary resource; a name carrying a
	// separator would silently build a different resource than intended.
	if strings.ContainsAny(bucket, " /\\") {
		return "", time.Time{}, fmt.Errorf("gcs object token mint received an invalid bucket name")
	}

	// The service-account document is customer-controlled. Pin token_uri so a
	// crafted key cannot make the control plane POST its signed assertion to
	// IMDS, loopback, or an attacker-controlled URL.
	pinned, err := pinGCSServiceAccountTokenURL([]byte(credentialsJSON))
	if err != nil {
		return "", time.Time{}, err
	}
	base, err := credentials.NewCredentialsFromJSON(credentials.ServiceAccount, pinned, &credentials.DetectOptions{
		Scopes: []string{gcsDownscopeScope},
		Client: httpClient,
	})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("gcs object token base credentials: %w", err)
	}
	tok, err := gcsExchangeDownscopedToken(ctx, base, gcsObjectDownscopeRules(bucket, write), httpClient)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("gcs object token downscope: %w", err)
	}
	if tok == nil || tok.Value == "" {
		return "", time.Time{}, fmt.Errorf("gcs object token downscope returned an empty token")
	}
	return tok.Value, tok.Expiry, nil
}

// pinGCSServiceAccountTokenURL rewrites the customer-supplied token_uri to
// Google's JWT token endpoint before the credential is constructed.
func pinGCSServiceAccountTokenURL(raw []byte) ([]byte, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse gcs service account json: %w", err)
	}
	if doc == nil {
		return nil, fmt.Errorf("gcs service account json must be an object")
	}
	tokenURL, err := json.Marshal(google.JWTTokenURL)
	if err != nil {
		return nil, fmt.Errorf("encode gcs token url: %w", err)
	}
	doc["token_uri"] = tokenURL
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encode gcs service account json: %w", err)
	}
	return out, nil
}

// gcsObjectDownscopeRules builds the bucket-wide Credential Access Boundary for
// one object session.
func gcsObjectDownscopeRules(bucket string, write bool) []downscope.AccessBoundaryRule {
	permission := "inRole:roles/storage.objectViewer"
	if write {
		permission = "inRole:roles/storage.objectAdmin"
	}
	return []downscope.AccessBoundaryRule{{
		AvailableResource:    "//storage.googleapis.com/projects/_/buckets/" + bucket,
		AvailablePermissions: []string{permission},
	}}
}

// gcsExchangeDownscopedToken performs the Credential Access Boundary exchange.
func gcsExchangeDownscopedToken(ctx context.Context, base *auth.Credentials, rules []downscope.AccessBoundaryRule, client *http.Client) (*auth.Token, error) {
	creds, err := downscope.NewCredentials(&downscope.Options{Credentials: base, Rules: rules, Client: client})
	if err != nil {
		return nil, err
	}
	return creds.Token(ctx)
}
