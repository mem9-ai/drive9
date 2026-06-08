package encrypt

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
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
		// VPC/dedicated-gateway certificates are signed by an Aliyun internal CA
		// not present in the system trust store; skip verification for custom endpoints.
		config.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		}
	}

	client, err := kms.NewClientWithOptions(region, config, nil)
	if err != nil {
		return nil, fmt.Errorf("create aliyun kms client: %w", err)
	}
	return &AliyunKMSEncryptor{client: client, keyID: keyID, endpoint: endpoint}, nil
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
