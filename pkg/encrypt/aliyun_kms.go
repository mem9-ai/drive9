package encrypt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/responses"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/kms"
)

type AliyunKMSEncryptor struct {
	client   *kms.Client
	keyID    string
	endpoint string // optional custom endpoint; set as request Domain when non-empty
}

func NewAliyunKMSEncryptor(region, keyID, endpoint string) (*AliyunKMSEncryptor, error) {
	if region == "" {
		return nil, fmt.Errorf("aliyun kms region is required")
	}
	if keyID == "" {
		return nil, fmt.Errorf("aliyun kms key id is required")
	}

	config := sdk.NewConfig()
	config.Scheme = "HTTPS"

	if endpoint != "" {
		// Dedicated/VPC KMS endpoints present a certificate signed by an Aliyun
		// private CA that is not in the system trust store. Fetch the instance
		// CA chain with the ambient credentials and verify against it.
		pool, err := aliyunKMSRootPool(region, endpoint)
		if err != nil {
			return nil, err
		}
		config.HttpTransport = aliyunKMSHTTPTransport(pool)
	}

	client, err := kms.NewClientWithOptions(region, config, nil)
	if err != nil {
		return nil, fmt.Errorf("create aliyun kms client: %w", err)
	}
	return &AliyunKMSEncryptor{client: client, keyID: keyID, endpoint: endpoint}, nil
}

func aliyunKMSRootPool(region, endpoint string) (*x509.CertPool, error) {
	chain, err := fetchAliyunKMSCAChain(region, endpoint)
	if err != nil {
		return nil, fmt.Errorf("aliyun kms custom endpoint TLS trust: %w", err)
	}
	pool, err := aliyunKMSCertPool(chain)
	if err != nil {
		return nil, fmt.Errorf("aliyun kms instance %s: %w", aliyunKMSInstanceID(endpoint), err)
	}
	return pool, nil
}

func aliyunKMSCertPool(chain []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(chain) {
		return nil, fmt.Errorf("no usable CA certificates")
	}
	return pool, nil
}

// aliyunKMSInstanceID derives the instance ID from the dedicated endpoint,
// e.g. kst-sgp6a250827p1oq0yrdoy.cryptoservice.kms.aliyuncs.com.
func aliyunKMSInstanceID(endpoint string) string {
	host := strings.TrimPrefix(strings.TrimSpace(endpoint), "https://")
	host = strings.TrimSuffix(host, "/")
	if i := strings.IndexByte(host, '.'); i >= 0 {
		host = host[:i]
	}
	if !strings.HasPrefix(host, "kst-") || strings.ContainsAny(host, ":/") {
		return ""
	}
	return host
}

func fetchAliyunKMSCAChain(region, endpoint string) ([]byte, error) {
	instanceID := aliyunKMSInstanceID(endpoint)
	if instanceID == "" {
		return nil, fmt.Errorf("cannot derive KMS instance ID from endpoint %q", endpoint)
	}

	config := sdk.NewConfig()
	config.Scheme = "HTTPS"
	config.Timeout = 10 * time.Second
	client, err := kms.NewClientWithOptions(region, config, nil)
	if err != nil {
		return nil, fmt.Errorf("create aliyun kms control client: %w", err)
	}
	return fetchAliyunKMSCAChainWithClient(client, instanceID, "")
}

// fetchAliyunKMSCAChainWithClient uses CommonResponse instead of the typed
// GetKmsInstance response, whose model rejects the current API payload
// (VswitchIds is an array but is modeled as a string).
func fetchAliyunKMSCAChainWithClient(client *kms.Client, instanceID, domain string) ([]byte, error) {
	req := kms.CreateGetKmsInstanceRequest()
	req.KmsInstanceId = instanceID
	if domain != "" {
		req.Domain = domain
	}
	resp := responses.NewCommonResponse()
	if err := client.DoAction(req, resp); err != nil {
		return nil, fmt.Errorf("get aliyun kms instance %s: %w", instanceID, err)
	}
	var out struct {
		KmsInstance struct {
			CaCertificateChainPem string `json:"CaCertificateChainPem"`
		} `json:"KmsInstance"`
	}
	if err := json.Unmarshal(resp.GetHttpContentBytes(), &out); err != nil {
		return nil, fmt.Errorf("decode aliyun kms instance %s response: %w", instanceID, err)
	}
	if out.KmsInstance.CaCertificateChainPem == "" {
		return nil, fmt.Errorf("aliyun kms instance %s returned no CA certificate chain", instanceID)
	}
	return []byte(out.KmsInstance.CaCertificateChainPem), nil
}

// aliyunKMSHTTPTransport returns a transport that trusts the instance CA while
// starting from the default transport's proxy and HTTP/2 settings. The SDK sets
// InsecureSkipVerify=false and its own per-request dial timeout; it preserves
// this RootCAs pool.
func aliyunKMSHTTPTransport(pool *x509.CertPool) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}
	return transport
}

func (e *AliyunKMSEncryptor) Encrypt(_ context.Context, plaintext []byte) ([]byte, error) {
	req := kms.CreateEncryptRequest()
	req.KeyId = e.keyID
	req.Plaintext = base64.StdEncoding.EncodeToString(plaintext)
	if e.endpoint != "" {
		req.Domain = e.endpoint
	}

	resp, err := e.client.Encrypt(req)
	if err != nil {
		return nil, fmt.Errorf("aliyun kms encrypt: %w", err)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(resp.CiphertextBlob)
	if err != nil {
		return nil, fmt.Errorf("aliyun kms encrypt: decode ciphertext: %w", err)
	}
	return ciphertext, nil
}

func (e *AliyunKMSEncryptor) Decrypt(_ context.Context, ciphertext []byte) ([]byte, error) {
	req := kms.CreateDecryptRequest()
	req.CiphertextBlob = base64.StdEncoding.EncodeToString(ciphertext)
	if e.endpoint != "" {
		req.Domain = e.endpoint
	}

	resp, err := e.client.Decrypt(req)
	if err != nil {
		return nil, fmt.Errorf("aliyun kms decrypt: %w", err)
	}
	plain, err := base64.StdEncoding.DecodeString(resp.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("aliyun kms decrypt: decode plaintext: %w", err)
	}
	return plain, nil
}
