package encrypt

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

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
	accessKeyID := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	accessKeySecret := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	if accessKeyID == "" {
		return nil, fmt.Errorf("ALIBABA_CLOUD_ACCESS_KEY_ID is required")
	}
	if accessKeySecret == "" {
		return nil, fmt.Errorf("ALIBABA_CLOUD_ACCESS_KEY_SECRET is required")
	}
	client, err := kms.NewClientWithAccessKey(region, accessKeyID, accessKeySecret)
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
	return base64.StdEncoding.DecodeString(resp.CiphertextBlob)
}

func (e *AliyunKMSEncryptor) Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	req := kms.CreateDecryptRequest()
	req.CiphertextBlob = base64.StdEncoding.EncodeToString(ciphertext)

	resp, err := e.client.Decrypt(req)
	if err != nil {
		return nil, fmt.Errorf("aliyun kms decrypt: %w", err)
	}
	return base64.StdEncoding.DecodeString(resp.Plaintext)
}
