package encrypt

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

type Type string

const (
	TypeLocalAES Type = "local_aes"
	TypeKMS      Type = "kms"
)

type Config struct {
	Type   Type
	Key    string // hex master key for local_aes, KMS key id/alias for kms
	Region string // aws region for kms
}

func New(ctx context.Context, cfg Config) (Encryptor, error) {
	switch strings.ToLower(string(cfg.Type)) {
	case string(TypeLocalAES), "":
		if cfg.Key == "" {
			return nil, fmt.Errorf("local_aes requires key")
		}
		mk, err := hex.DecodeString(cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("decode local_aes key: %w", err)
		}
		return NewLocalAESEncryptor(mk)
	case string(TypeKMS):
		if cfg.Key == "" {
			return nil, fmt.Errorf("kms requires key id/alias")
		}
		acfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
		if err != nil {
			return nil, err
		}
		return NewKMSEncryptor(kms.NewFromConfig(acfg), cfg.Key)
	default:
		return nil, fmt.Errorf("unsupported encrypt type: %s", cfg.Type)
	}
}
