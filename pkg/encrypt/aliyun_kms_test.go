package encrypt

import (
	"testing"
)

func TestAliyunKMSRequiresRegion(t *testing.T) {
	_, err := NewAliyunKMSEncryptor("", "test-key")
	if err == nil {
		t.Fatal("expected region required error")
	}
}

func TestAliyunKMSRequiresKey(t *testing.T) {
	_, err := NewAliyunKMSEncryptor("cn-hangzhou", "")
	if err == nil {
		t.Fatal("expected key required error")
	}
}

func TestAliyunKMSRequiresCredentials(t *testing.T) {
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
	_, err := NewAliyunKMSEncryptor("cn-hangzhou", "test-key")
	if err == nil {
		t.Fatal("expected credential error")
	}
}

func TestAliyunKMSCredentialValidation(t *testing.T) {
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "test-id")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
	_, err := NewAliyunKMSEncryptor("cn-hangzhou", "test-key")
	if err == nil {
		t.Fatal("expected missing secret error")
	}

	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "test-secret")
	_, err = NewAliyunKMSEncryptor("cn-hangzhou", "test-key")
	if err == nil {
		t.Fatal("expected missing access key error")
	}
}
