package encrypt

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	kms "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/kms/v20190118"
)

type TencentKMSEncryptor struct {
	client *kms.Client
	keyID  string
}

func NewTencentKMSEncryptor(region, keyID string) (*TencentKMSEncryptor, error) {
	if region == "" {
		return nil, fmt.Errorf("tencent kms region is required")
	}
	if keyID == "" {
		return nil, fmt.Errorf("tencent kms key id is required")
	}

	secretID := os.Getenv("TENCENTCLOUD_SECRET_ID")
	secretKey := os.Getenv("TENCENTCLOUD_SECRET_KEY")
	if secretID == "" {
		secretID = os.Getenv("TENCENTCLOUD_SECRETID")
	}

	credential := common.NewCredential(secretID, secretKey)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Scheme = "HTTPS"

	client, err := kms.NewClient(credential, region, cpf)
	if err != nil {
		return nil, fmt.Errorf("create tencent kms client: %w", err)
	}
	return &TencentKMSEncryptor{client: client, keyID: keyID}, nil
}

func (e *TencentKMSEncryptor) Encrypt(_ context.Context, plaintext []byte) ([]byte, error) {
	req := kms.NewEncryptRequest()
	req.KeyId = &e.keyID
	plain := base64.StdEncoding.EncodeToString(plaintext)
	req.Plaintext = &plain

	resp, err := e.client.Encrypt(req)
	if err != nil {
		return nil, fmt.Errorf("tencent kms encrypt: %w", err)
	}
	if resp == nil || resp.Response == nil || resp.Response.CiphertextBlob == nil {
		return nil, fmt.Errorf("tencent kms encrypt: empty ciphertext in response")
	}
	ciphertext, err := base64.StdEncoding.DecodeString(*resp.Response.CiphertextBlob)
	if err != nil {
		return nil, fmt.Errorf("tencent kms encrypt: decode ciphertext: %w", err)
	}
	return ciphertext, nil
}

func (e *TencentKMSEncryptor) Decrypt(_ context.Context, ciphertext []byte) ([]byte, error) {
	req := kms.NewDecryptRequest()
	cipher := base64.StdEncoding.EncodeToString(ciphertext)
	req.CiphertextBlob = &cipher

	resp, err := e.client.Decrypt(req)
	if err != nil {
		return nil, fmt.Errorf("tencent kms decrypt: %w", err)
	}
	if resp == nil || resp.Response == nil || resp.Response.Plaintext == nil {
		return nil, fmt.Errorf("tencent kms decrypt: empty plaintext in response")
	}
	plaintext, err := base64.StdEncoding.DecodeString(*resp.Response.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("tencent kms decrypt: decode plaintext: %w", err)
	}
	return plaintext, nil
}
