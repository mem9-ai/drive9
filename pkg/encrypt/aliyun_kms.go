package encrypt

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials/providers"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/kms"
)

type AliyunKMSEncryptor struct {
	client *kms.Client
	keyID  string
}

func NewAliyunKMSEncryptor(region, keyID string) (*AliyunKMSEncryptor, error) {
	if region == "" {
		return nil, fmt.Errorf("aliyun kms region is required")
	}
	if keyID == "" {
		return nil, fmt.Errorf("aliyun kms key id is required")
	}

	chain := providers.NewChainProvider(
		[]providers.Provider{
			providers.NewEnvCredentialProvider(),
			providers.NewInstanceMetadataProvider(),
		},
	)
	cred, err := chain.Retrieve()
	if err != nil {
		return nil, fmt.Errorf("aliyun kms credentials: %w", err)
	}

	client, err := kms.NewClientWithOptions(region, nil, cred)
	if err != nil {
		return nil, fmt.Errorf("create aliyun kms client: %w", err)
	}
	return &AliyunKMSEncryptor{client: client, keyID: keyID}, nil
}

func (e *AliyunKMSEncryptor) Encrypt(ctx context.Context, plaintext []byte) ([]byte, error) {
	req := kms.CreateEncryptRequest()
	req.KeyId = e.keyID
	req.Plaintext = base64.StdEncoding.EncodeToString(plaintext)

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

func (e *AliyunKMSEncryptor) Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	req := kms.CreateDecryptRequest()
	req.CiphertextBlob = base64.StdEncoding.EncodeToString(ciphertext)

	resp, err := e.client.Decrypt(req)
	if err != nil {
		return nil, fmt.Errorf("aliyun kms decrypt: %w", err)
	}
	plaintext, err := base64.StdEncoding.DecodeString(resp.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("aliyun kms decrypt: decode plaintext: %w", err)
	}
	return plaintext, nil
}
