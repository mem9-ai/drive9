package encrypt

import (
	"testing"
)

func TestTencentKMSRequiresRegion(t *testing.T) {
	_, err := NewTencentKMSEncryptor("", "test-key")
	if err == nil {
		t.Fatal("expected region required error")
	}
}

func TestTencentKMSRequiresKey(t *testing.T) {
	_, err := NewTencentKMSEncryptor("ap-guangzhou", "")
	if err == nil {
		t.Fatal("expected key required error")
	}
}
