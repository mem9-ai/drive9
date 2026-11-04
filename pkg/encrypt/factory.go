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
	TypeLocalAES   Type = "local_aes"
	TypeKMS        Type = "kms"
	TypeAliyunKMS  Type = "aliyun_kms"
	TypeTencentKMS Type = "tencent_kms"
)

type Config struct {
	Type   Type
	Key    string // hex master key for local_aes, KMS key id/alias for kms/aliyun_kms
	Region string // aws/aliyun region for kms
	// PreviousKeys lists retired local_aes master keys (hex). Writes always use
	// Key; reads also try these, so a master key can be rotated without losing
	// the values it wrapped.
	PreviousKeys []string
	// AliyunKMSEndpoint overrides the default Aliyun KMS endpoint, e.g. a VPC or
	// dedicated-instance endpoint. Leave empty to use the default public endpoint
	// for the region. When non-empty, the instance CA chain is fetched from the
	// KMS API so the dedicated endpoint is verified without a local CA file.
	AliyunKMSEndpoint string
}

func New(ctx context.Context, cfg Config) (Encryptor, error) {
	enc, err := newEncryptor(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return newInstrumentedEncryptor(enc), nil
}

func newEncryptor(ctx context.Context, cfg Config) (Encryptor, error) {
	switch strings.ToLower(string(cfg.Type)) {
	case string(TypeLocalAES), "":
		if cfg.Key == "" {
			return nil, fmt.Errorf("local_aes requires key")
		}
		mk, err := hex.DecodeString(cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("decode local_aes key: %w", err)
		}
		if len(cfg.PreviousKeys) == 0 {
			return NewLocalAESEncryptor(mk)
		}
		previous := make([][]byte, 0, len(cfg.PreviousKeys))
		for _, raw := range cfg.PreviousKeys {
			key, err := hex.DecodeString(strings.TrimSpace(raw))
			if err != nil {
				return nil, fmt.Errorf("decode previous local_aes key: %w", err)
			}
			previous = append(previous, key)
		}
		return NewLocalAESEncryptorWithPrevious(mk, previous)
	case string(TypeKMS):
		if cfg.Key == "" {
			return nil, fmt.Errorf("kms requires key id/alias")
		}
		acfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
		if err != nil {
			return nil, err
		}
		return NewKMSEncryptor(kms.NewFromConfig(acfg), cfg.Key)
	case string(TypeAliyunKMS):
		if cfg.Key == "" {
			return nil, fmt.Errorf("aliyun_kms requires key id")
		}
		if cfg.Region == "" {
			return nil, fmt.Errorf("aliyun_kms requires region")
		}
		return NewAliyunKMSEncryptor(cfg.Region, cfg.Key, cfg.AliyunKMSEndpoint)
	case string(TypeTencentKMS):
		if cfg.Key == "" {
			return nil, fmt.Errorf("tencent_kms requires key id")
		}
		if cfg.Region == "" {
			return nil, fmt.Errorf("tencent_kms requires region")
		}
		return NewTencentKMSEncryptor(cfg.Region, cfg.Key)
	default:
		return nil, fmt.Errorf("unsupported encrypt type: %s", cfg.Type)
	}
}
