package encrypt

import (
	"context"
	"errors"
	"time"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

// instrumentedEncryptor wraps an Encryptor to record drive9_service_operations_total
// and drive9_service_operation_duration_seconds under component="encrypt". The
// KMS-backed encryptors (AWS/Aliyun/Tencent) make network calls whose failures
// (KMS unreachable, throttling, key disabled/rotated) would otherwise be invisible
// until a tenant's DB password fails to decrypt and the tenant becomes unusable.
type instrumentedEncryptor struct {
	inner Encryptor
}

func newInstrumentedEncryptor(inner Encryptor) Encryptor {
	if inner == nil {
		return nil
	}
	return &instrumentedEncryptor{inner: inner}
}

func (e *instrumentedEncryptor) Encrypt(ctx context.Context, plaintext []byte) ([]byte, error) {
	start := time.Now()
	out, err := e.inner.Encrypt(ctx, plaintext)
	metrics.RecordOperation("encrypt", "encrypt", encryptResult(err), time.Since(start))
	return out, err
}

func (e *instrumentedEncryptor) Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	start := time.Now()
	out, err := e.inner.Decrypt(ctx, ciphertext)
	metrics.RecordOperation("encrypt", "decrypt", encryptResult(err), time.Since(start))
	return out, err
}

func encryptResult(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "error"
	}
}
