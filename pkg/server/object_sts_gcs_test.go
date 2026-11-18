package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2/google"
)

func TestGCSPinServiceAccountTokenURL(t *testing.T) {
	raw := []byte(`{"type":"service_account","client_email":"a@b.iam.gserviceaccount.com","private_key":"x","token_uri":"http://169.254.169.254/latest/meta-data"}`)
	pinned, err := pinGCSServiceAccountTokenURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(pinned, &doc); err != nil {
		t.Fatal(err)
	}
	if got := doc["token_uri"]; got != google.JWTTokenURL {
		t.Fatalf("token_uri = %v, want %s", got, google.JWTTokenURL)
	}
	if doc["client_email"] != "a@b.iam.gserviceaccount.com" {
		t.Fatalf("client_email was not preserved: %v", doc)
	}
}

func TestGCSPinServiceAccountTokenURLRejectsNonObject(t *testing.T) {
	if _, err := pinGCSServiceAccountTokenURL([]byte(`"str"`)); err == nil {
		t.Fatal("expected an error for a non-object credentials document")
	}
	if _, err := pinGCSServiceAccountTokenURL([]byte(`not json`)); err == nil {
		t.Fatal("expected an error for malformed JSON")
	}
}

// The boundary must name the backend bucket, not the project: that is what
// stops one tenant's session from reading another tenant's bucket.
func TestGCSObjectDownscopeRulesAreBucketScoped(t *testing.T) {
	read := gcsObjectDownscopeRules("tenant-a-bucket", false)
	if len(read) != 1 {
		t.Fatalf("rules = %d, want 1", len(read))
	}
	if got, want := read[0].AvailableResource, "//storage.googleapis.com/projects/_/buckets/tenant-a-bucket"; got != want {
		t.Fatalf("resource = %q, want %q", got, want)
	}
	if len(read[0].AvailablePermissions) != 1 || read[0].AvailablePermissions[0] != "inRole:roles/storage.objectViewer" {
		t.Fatalf("read permissions = %v", read[0].AvailablePermissions)
	}
	write := gcsObjectDownscopeRules("tenant-a-bucket", true)
	if len(write[0].AvailablePermissions) != 1 || write[0].AvailablePermissions[0] != "inRole:roles/storage.objectAdmin" {
		t.Fatalf("write permissions = %v", write[0].AvailablePermissions)
	}
	if strings.Contains(write[0].AvailableResource, "projects/my-project") {
		t.Fatal("boundary must not be project-wide")
	}
}

func TestMintGCSSessionUsesDownscopedToken(t *testing.T) {
	orig := gcsObjectTokenMint
	t.Cleanup(func() { gcsObjectTokenMint = orig })
	var gotBucket string
	var gotWrite bool
	expiry := time.Now().Add(30 * time.Minute).UTC().Truncate(time.Second)
	gcsObjectTokenMint = func(_ context.Context, secret, bucket string, write bool, _ *http.Client) (string, time.Time, error) {
		if secret != "sa-json" {
			t.Fatalf("secret = %q", secret)
		}
		gotBucket, gotWrite = bucket, write
		return "downscoped-token", expiry, nil
	}

	resp, err := mintGCSSession(context.Background(), "sa-json", "tenant-b-bucket", true)
	if err != nil {
		t.Fatal(err)
	}
	if resp.AccessToken != "downscoped-token" {
		t.Fatalf("token = %q", resp.AccessToken)
	}
	if gotBucket != "tenant-b-bucket" || !gotWrite {
		t.Fatalf("mint called with bucket=%q write=%v", gotBucket, gotWrite)
	}
	if resp.Expiration != expiry.Format(time.RFC3339) {
		t.Fatalf("expiration = %q, want %q", resp.Expiration, expiry.Format(time.RFC3339))
	}
}

// A failed exchange must surface as an error: falling back to the raw service
// account token would restore the project-wide credential this fix removes.
func TestMintGCSSessionDoesNotFallBackToRawToken(t *testing.T) {
	orig := gcsObjectTokenMint
	t.Cleanup(func() { gcsObjectTokenMint = orig })
	gcsObjectTokenMint = func(context.Context, string, string, bool, *http.Client) (string, time.Time, error) {
		return "", time.Time{}, errors.New("sts unavailable")
	}
	resp, err := mintGCSSession(context.Background(), "sa-json", "bucket", false)
	if err == nil {
		t.Fatalf("expected an error, got response %+v", resp)
	}
	if resp != nil {
		t.Fatal("failed mint returned credentials")
	}
}

func TestMintGCSObjectTokenRequiresInputs(t *testing.T) {
	if _, _, err := mintGCSObjectToken(context.Background(), "", "b", false, nil); err == nil {
		t.Fatal("expected an error for a missing credentials document")
	}
	if _, _, err := mintGCSObjectToken(context.Background(), "{}", "", false, nil); err == nil {
		t.Fatal("expected an error for a missing bucket")
	}
}

func TestMintGCSObjectTokenRejectsInvalidBucketName(t *testing.T) {
	for _, bucket := range []string{"a/b", "a b", "a\\b"} {
		if _, _, err := mintGCSObjectToken(context.Background(), "{}", bucket, false, nil); err == nil {
			t.Fatalf("bucket %q accepted", bucket)
		}
	}
}
