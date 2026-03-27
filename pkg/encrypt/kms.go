package encrypt

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

type KMSEncryptor struct {
	client *kms.Client
	keyID  string
}

func NewKMSEncryptor(client *kms.Client, keyID string) (*KMSEncryptor, error) {
	if client == nil {
		return nil, fmt.Errorf("kms client is nil")
	}
	if keyID == "" {
		return nil, fmt.Errorf("kms key id is required")
	}
	return &KMSEncryptor{client: client, keyID: keyID}, nil
}

func (e *KMSEncryptor) Encrypt(ctx context.Context, plaintext []byte) ([]byte, error) {
	out, err := e.client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(e.keyID),
		Plaintext: plaintext,
	})
	if err != nil {
		return nil, err
	}
	return out.CiphertextBlob, nil
}

func (e *KMSEncryptor) Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	out, err := e.client.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: ciphertext})
	if err != nil {
		return nil, err
	}
	return out.Plaintext, nil
}
