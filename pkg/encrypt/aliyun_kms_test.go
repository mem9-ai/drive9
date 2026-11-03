package encrypt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/kms"
)

func TestAliyunKMSInstanceID(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
	}{
		{"kst-sgp6a250827p1oq0yrdoy.cryptoservice.kms.aliyuncs.com", "kst-sgp6a250827p1oq0yrdoy"},
		{"https://kst-abc.cryptoservice.kms.aliyuncs.com/", "kst-abc"},
		{"kms.example.com", ""},
		{"kst-abc.example.com:8443", "kst-abc"},
		{"kst-abc:8443", ""},
		{"kst-abc/xyz", ""},
		{"", ""},
	}
	for _, tt := range tests {
		if got := aliyunKMSInstanceID(tt.endpoint); got != tt.want {
			t.Errorf("aliyunKMSInstanceID(%q) = %q, want %q", tt.endpoint, got, tt.want)
		}
	}
}

func TestAliyunKMSFetchCA(t *testing.T) {
	const ca = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
	srv := newAliyunKMSTestTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"KmsInstance": map[string]string{"CaCertificateChainPem": ca},
		})
	}))
	client := newAliyunKMSTestClient(t, srv)
	got, err := fetchAliyunKMSCAChainWithClient(client, "kst-test", strings.TrimPrefix(srv.URL, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != ca {
		t.Fatalf("chain = %q, want %q", got, ca)
	}
}

func TestAliyunKMSFetchCARejectsMissingChain(t *testing.T) {
	srv := newAliyunKMSTestTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"KmsInstance":{}}`))
	}))
	client := newAliyunKMSTestClient(t, srv)
	if _, err := fetchAliyunKMSCAChainWithClient(client, "kst-test", strings.TrimPrefix(srv.URL, "https://")); err == nil {
		t.Fatal("expected missing CA chain error")
	}
}

func TestAliyunKMSFetchCARejectsHTTPError(t *testing.T) {
	srv := newAliyunKMSTestTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	client := newAliyunKMSTestClient(t, srv)
	if _, err := fetchAliyunKMSCAChainWithClient(client, "kst-test", strings.TrimPrefix(srv.URL, "https://")); err == nil {
		t.Fatal("expected control-plane HTTP error")
	}
}

func TestAliyunKMSCertPoolRejectsInvalidPEM(t *testing.T) {
	if _, err := aliyunKMSCertPool([]byte("not a certificate")); err == nil {
		t.Fatal("expected invalid PEM error")
	}
}

func TestAliyunKMSRejectsUnderivableInstanceID(t *testing.T) {
	_, err := NewAliyunKMSEncryptor("cn-hangzhou", "test-key", "kms.example.com")
	if err == nil || !strings.Contains(err.Error(), "cannot derive KMS instance ID") {
		t.Fatalf("err = %v, want underivable instance ID error", err)
	}
}

// The transport trusts exactly the instance CA passed in: it accepts the
// matching endpoint and rejects an endpoint signed by a different CA.
func TestAliyunKMSHTTPTransportVerifiesInstanceCA(t *testing.T) {
	trusted := newAliyunKMSTestTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	other := newAliyunKMSTestTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	pool := x509.NewCertPool()
	pool.AddCert(trusted.Certificate())
	client := &http.Client{Transport: aliyunKMSHTTPTransport(pool), Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	if _, err := client.Get(trusted.URL); err != nil {
		t.Fatalf("GET trusted endpoint: %v", err)
	}
	if _, err := client.Get(other.URL); err == nil {
		t.Fatal("GET endpoint signed by a different CA succeeded")
	}
}

func TestAliyunKMSCustomEndpointEncryptsOverVerifiedTLS(t *testing.T) {
	wantPlaintext := []byte("secret")
	wantCiphertext := []byte("ciphertext")
	srv := newAliyunKMSTestTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"RequestId":      "test",
			"CiphertextBlob": base64.StdEncoding.EncodeToString(wantCiphertext),
			"Plaintext":      base64.StdEncoding.EncodeToString(wantPlaintext),
		})
	}))

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	cfg := sdk.NewConfig()
	cfg.Scheme = "HTTPS"
	cfg.HttpTransport = aliyunKMSHTTPTransport(pool)
	client, err := kms.NewClientWithOptions("cn-hangzhou", cfg, credentials.NewAccessKeyCredential("id", "key"))
	if err != nil {
		t.Fatal(err)
	}
	enc := &AliyunKMSEncryptor{client: client, keyID: "test-key", endpoint: strings.TrimPrefix(srv.URL, "https://")}

	gotCiphertext, err := enc.Encrypt(context.Background(), wantPlaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if string(gotCiphertext) != string(wantCiphertext) {
		t.Fatalf("ciphertext = %q, want %q", gotCiphertext, wantCiphertext)
	}
	gotPlaintext, err := enc.Decrypt(context.Background(), wantCiphertext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(gotPlaintext) != string(wantPlaintext) {
		t.Fatalf("plaintext = %q, want %q", gotPlaintext, wantPlaintext)
	}
}

func newAliyunKMSTestClient(t *testing.T, srv *httptest.Server) *kms.Client {
	t.Helper()
	cfg := sdk.NewConfig()
	cfg.Scheme = "HTTPS"
	cfg.HttpTransport = srv.Client().Transport.(*http.Transport)
	client, err := kms.NewClientWithOptions("cn-hangzhou", cfg, credentials.NewAccessKeyCredential("id", "key"))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newAliyunKMSTestTLSServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}
