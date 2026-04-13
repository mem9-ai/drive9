package vault

import (
	"testing"
)

func TestCheckScope(t *testing.T) {
	scope := []string{"aws-prod", "db-prod/password", "github-token"}

	tests := []struct {
		name      string
		secret    string
		field     string
		want      bool
	}{
		{"full secret access", "aws-prod", "", true},
		{"full secret with field", "aws-prod", "access-key", true},
		{"field-level access", "db-prod", "password", true},
		{"wrong field", "db-prod", "host", false},
		{"no field specified for field-level", "db-prod", "", false},
		{"not in scope", "unknown", "", false},
		{"not in scope with field", "unknown", "key", false},
		{"github-token full access", "github-token", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CheckScope(scope, tt.secret, tt.field)
			if got != tt.want {
				t.Errorf("CheckScope(%q, %q) = %v, want %v", tt.secret, tt.field, got, tt.want)
			}
		})
	}
}

func TestScopedSecretNames(t *testing.T) {
	scope := []string{"aws-prod", "db-prod/password", "db-prod/host", "github-token"}
	names := ScopedSecretNames(scope)

	expected := map[string]bool{"aws-prod": true, "db-prod": true, "github-token": true}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d: %v", len(expected), len(names), names)
	}
	for _, n := range names {
		if !expected[n] {
			t.Errorf("unexpected name: %s", n)
		}
	}
}

func TestAllowedFields(t *testing.T) {
	scope := []string{"aws-prod", "db-prod/password", "db-prod/host"}

	// Full access.
	allFields, fields := AllowedFields(scope, "aws-prod")
	if !allFields {
		t.Error("expected full access for aws-prod")
	}
	if fields != nil {
		t.Error("expected nil fields for full access")
	}

	// Field-level access.
	allFields, fields = AllowedFields(scope, "db-prod")
	if allFields {
		t.Error("expected field-level access for db-prod")
	}
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}

	// No access.
	allFields, fields = AllowedFields(scope, "unknown")
	if allFields {
		t.Error("expected no access for unknown")
	}
	if len(fields) != 0 {
		t.Errorf("expected 0 fields, got %d", len(fields))
	}
}
