package encrypt

import "testing"

func TestKMSEncryptorValidation(t *testing.T) {
	if _, err := NewKMSEncryptor(nil, "alias/dat9-dev-db-password"); err == nil {
		t.Fatal("expected nil client error")
	}
}
