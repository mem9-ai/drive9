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
