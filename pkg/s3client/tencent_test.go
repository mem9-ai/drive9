package s3client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func camCredentialsJSON(expiry time.Time) string {
	return `{"TmpSecretId":"tmp-id","TmpSecretKey":"tmp-key","Token":"tmp-token","Expiration":"` + expiry.UTC().Format(time.RFC3339Nano) + `"}`
}

func newTestCAMProvider(server *httptest.Server) *camProvider {
	provider := newCAMProvider()
	provider.client = &http.Client{Timeout: 20 * time.Millisecond}
	provider.metadataURL = server.URL + "/security-credentials/"
	provider.maxAttempts = 2
	provider.retryBackoff = 0
	return provider
}

func isCAMRoleListRequest(req *http.Request) bool {
	return strings.HasSuffix(req.URL.Path, "/security-credentials/")
}

func TestCAMProviderRetrievesCredentials(t *testing.T) {
	expiry := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if isCAMRoleListRequest(req) {
			_, _ = w.Write([]byte("drive9-cos-role"))
			return
		}
		_, _ = w.Write([]byte(camCredentialsJSON(expiry)))
	}))
	defer server.Close()
	provider := newTestCAMProvider(server)

	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if creds.AccessKeyID != "tmp-id" || creds.SecretAccessKey != "tmp-key" || creds.SessionToken != "tmp-token" {
		t.Fatalf("Retrieve() credentials = %#v", creds)
	}
	if !creds.Expires.Equal(expiry) {
		t.Fatalf("Retrieve() expiry = %v, want %v", creds.Expires, expiry)
	}
}

func TestCAMProviderRetriesTransientCredentialTimeout(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	var credentialCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if isCAMRoleListRequest(req) {
			_, _ = w.Write([]byte("drive9-cos-role"))
			return
		}
		if credentialCalls.Add(1) == 1 {
			<-req.Context().Done()
			return
		}
		_, _ = w.Write([]byte(camCredentialsJSON(expiry)))
	}))
	defer server.Close()
	provider := newTestCAMProvider(server)

	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if creds.AccessKeyID != "tmp-id" {
		t.Fatalf("Retrieve() AccessKeyID = %q, want tmp-id", creds.AccessKeyID)
	}
	if got := credentialCalls.Load(); got != 2 {
		t.Fatalf("credential metadata calls = %d, want 2", got)
	}
}

func TestCAMProviderFallsBackToSafelyUnexpiredCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		<-req.Context().Done()
	}))
	defer server.Close()
	oldCreds := aws.Credentials{
		AccessKeyID:     "old-id",
		SecretAccessKey: "old-key",
		SessionToken:    "old-token",
		Source:          "TencentCAMRole",
		CanExpire:       true,
		Expires:         time.Now().Add(2 * time.Minute),
	}
	provider := newTestCAMProvider(server)
	provider.cached = oldCreds
	provider.expiresAt = oldCreds.Expires

	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if creds.AccessKeyID != oldCreds.AccessKeyID {
		t.Fatalf("Retrieve() AccessKeyID = %q, want %q", creds.AccessKeyID, oldCreds.AccessKeyID)
	}
}

func TestCAMCredentialsCacheFallbackPreservesActualExpiry(t *testing.T) {
	expiry := time.Now().Add(10 * time.Minute)
	var metadataCalls atomic.Int32
	var metadataUnavailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		metadataCalls.Add(1)
		if metadataUnavailable.Load() {
			<-req.Context().Done()
			return
		}
		if isCAMRoleListRequest(req) {
			_, _ = w.Write([]byte("drive9-cos-role"))
			return
		}
		_, _ = w.Write([]byte(camCredentialsJSON(expiry)))
	}))
	defer server.Close()
	provider := newTestCAMProvider(server)
	provider.client.Timeout = 20 * time.Millisecond
	provider.maxAttempts = 1
	credentials := newCAMCredentialsProvider(provider, provider.refreshAhead)

	initialCreds, err := credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("initial Retrieve() error = %v", err)
	}
	if want := expiry.Add(-provider.refreshAhead); !initialCreds.Expires.Equal(want) {
		t.Fatalf("initial expiry = %v, want early refresh at %v", initialCreds.Expires, want)
	}
	metadataUnavailable.Store(true)
	provider.refreshAhead = 11 * time.Minute
	credentials.Invalidate()
	creds, err := credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("fallback Retrieve() error = %v", err)
	}
	if creds.AccessKeyID != "tmp-id" {
		t.Fatalf("fallback AccessKeyID = %q, want tmp-id", creds.AccessKeyID)
	}
	if !creds.Expires.Equal(expiry) {
		t.Fatalf("fallback expiry = %v, want actual expiry %v", creds.Expires, expiry)
	}
	callsAfterFallback := metadataCalls.Load()
	if _, err := credentials.Retrieve(context.Background()); err != nil {
		t.Fatalf("cached fallback Retrieve() error = %v", err)
	}
	if got := metadataCalls.Load(); got != callsAfterFallback {
		t.Fatalf("metadata calls after cached fallback = %d, want %d", got, callsAfterFallback)
	}
}

func TestCAMProviderRejectsNearExpiryFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		<-req.Context().Done()
	}))
	defer server.Close()
	provider := newTestCAMProvider(server)
	provider.cached = aws.Credentials{AccessKeyID: "near-expiry-id"}
	provider.expiresAt = time.Now().Add(camCredentialFallbackMin / 2)

	creds, err := provider.Retrieve(context.Background())
	if err == nil {
		t.Fatal("Retrieve() error = nil, want metadata timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Retrieve() error = %v, want deadline exceeded", err)
	}
	if !errors.Is(err, ErrCAMMetadataUnavailable) {
		t.Fatalf("Retrieve() error = %v, want CAM metadata unavailable", err)
	}
	if creds.AccessKeyID != "" {
		t.Fatalf("Retrieve() AccessKeyID = %q, want empty", creds.AccessKeyID)
	}
}

func TestCAMProviderRejectsExpiredFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		<-req.Context().Done()
	}))
	defer server.Close()
	provider := newTestCAMProvider(server)
	provider.cached = aws.Credentials{AccessKeyID: "expired-id"}
	provider.expiresAt = time.Now().Add(-time.Second)

	creds, err := provider.Retrieve(context.Background())
	if err == nil {
		t.Fatal("Retrieve() error = nil, want metadata timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Retrieve() error = %v, want deadline exceeded", err)
	}
	if !errors.Is(err, ErrCAMMetadataUnavailable) {
		t.Fatalf("Retrieve() error = %v, want CAM metadata unavailable", err)
	}
	if creds.AccessKeyID != "" {
		t.Fatalf("Retrieve() AccessKeyID = %q, want empty", creds.AccessKeyID)
	}
}

func TestCAMProviderDoesNotRetryPermanentMetadataStatus(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()
	provider := newTestCAMProvider(server)
	provider.cached = aws.Credentials{AccessKeyID: "old-id"}
	provider.expiresAt = time.Now().Add(2 * time.Minute)

	creds, err := provider.Retrieve(context.Background())
	if err == nil {
		t.Fatal("Retrieve() error = nil, want status error")
	}
	if creds.AccessKeyID != "" {
		t.Fatalf("Retrieve() AccessKeyID = %q, want no fallback", creds.AccessKeyID)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("metadata calls = %d, want 1", got)
	}
}

func TestCAMProviderDeduplicatesConcurrentRefresh(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	var listCalls atomic.Int32
	var credentialCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if isCAMRoleListRequest(req) {
			listCalls.Add(1)
			_, _ = w.Write([]byte("drive9-cos-role"))
			return
		}
		credentialCalls.Add(1)
		_, _ = w.Write([]byte(camCredentialsJSON(expiry)))
	}))
	defer server.Close()
	provider := newTestCAMProvider(server)

	const callers = 8
	start := make(chan struct{})
	errCh := make(chan error, callers)
	var waitGroup sync.WaitGroup
	for range callers {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			_, err := provider.Retrieve(context.Background())
			errCh <- err
		}()
	}
	close(start)
	waitGroup.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent Retrieve() error = %v", err)
		}
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("role-list metadata calls = %d, want 1", got)
	}
	if got := credentialCalls.Load(); got != 1 {
		t.Fatalf("credential metadata calls = %d, want 1", got)
	}
}

func TestCAMMetadataResult(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "timeout", err: &camMetadataUnavailableError{err: context.DeadlineExceeded}, want: "timeout"},
		{name: "throttled", err: &camMetadataStatusError{statusCode: http.StatusTooManyRequests}, want: "throttled"},
		{name: "server error", err: &camMetadataStatusError{statusCode: http.StatusBadGateway}, want: "server_error"},
		{name: "rejected", err: &camMetadataStatusError{statusCode: http.StatusNotFound}, want: "rejected"},
		{name: "unavailable", err: errors.New("connection reset"), want: "unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := camMetadataResult(test.err); got != test.want {
				t.Fatalf("camMetadataResult() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestCredentialsForTencentWrapsCAMProviderWithCache(t *testing.T) {
	t.Setenv("TENCENTCLOUD_SECRET_ID", "")
	t.Setenv("TENCENTCLOUD_SECRETID", "")
	t.Setenv("TENCENTCLOUD_SECRET_KEY", "")
	t.Setenv("TENCENTCLOUD_SECRETKEY", "")
	t.Setenv("TENCENTCLOUD_SECURITY_TOKEN", "")

	provider, err := credentialsForTencent(AWSConfig{})
	if err != nil {
		t.Fatalf("credentialsForTencent() error = %v", err)
	}
	if _, ok := provider.(*aws.CredentialsCache); !ok {
		t.Fatalf("credentialsForTencent() provider = %T, want *aws.CredentialsCache", provider)
	}
}
