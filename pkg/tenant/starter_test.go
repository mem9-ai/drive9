package tenant

import "testing"

// Compile-time check: TiDBStarterProvisioner implements Provisioner.
var _ Provisioner = (*TiDBStarterProvisioner)(nil)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		input    string
		wantHost string
		wantPort int
	}{
		{"gateway01.us-east-1.prod.aws.tidbcloud.com:4000", "gateway01.us-east-1.prod.aws.tidbcloud.com", 4000},
		{"host.example.com:3306", "host.example.com", 3306},
		{"host.example.com", "host.example.com", 4000},
		{"", "", 4000},
	}
	for _, tt := range tests {
		host, port := parseEndpoint(tt.input)
		if host != tt.wantHost || port != tt.wantPort {
			t.Errorf("parseEndpoint(%q) = (%q, %d), want (%q, %d)", tt.input, host, port, tt.wantHost, tt.wantPort)
		}
	}
}

func TestGenerateRandomPassword(t *testing.T) {
	pw, err := generateRandomPassword(16)
	if err != nil {
		t.Fatal(err)
	}
	if len(pw) != 32 { // 16 bytes → 32 hex chars
		t.Errorf("expected 32 chars, got %d: %q", len(pw), pw)
	}

	// Should be unique
	pw2, _ := generateRandomPassword(16)
	if pw == pw2 {
		t.Error("two passwords should not be equal")
	}
}
