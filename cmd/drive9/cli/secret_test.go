package cli

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestParseSecretRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in        string
		wantName  string
		wantField string
		wantErr   bool
	}{
		{in: "aws-prod", wantName: "aws-prod"},
		{in: "db-prod/password", wantName: "db-prod", wantField: "password"},
		{in: "", wantErr: true},
		{in: "/password", wantErr: true},
		{in: "db-prod/", wantErr: true},
	}

	for _, tt := range tests {
		name, field, err := parseSecretRef(tt.in)
		if (err != nil) != tt.wantErr {
			t.Fatalf("parseSecretRef(%q) err=%v wantErr=%v", tt.in, err, tt.wantErr)
		}
		if err == nil && (name != tt.wantName || field != tt.wantField) {
			t.Fatalf("parseSecretRef(%q) = (%q,%q), want (%q,%q)", tt.in, name, field, tt.wantName, tt.wantField)
		}
	}
}

func TestParseFieldAssignmentsReadsLiteralAndFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	if err := os.WriteFile(certPath, []byte("CERTDATA"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	fields, err := parseFieldAssignments([]string{"access_key=AKIA", "cert=@" + certPath})
	if err != nil {
		t.Fatalf("parseFieldAssignments: %v", err)
	}
	want := map[string]string{
		"access_key": "AKIA",
		"cert":       "CERTDATA",
	}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("fields = %#v, want %#v", fields, want)
	}
}

func TestEnvMapFromSecretUppercasesKeys(t *testing.T) {
	t.Parallel()

	got := envMapFromSecret(map[string]string{
		"secret_key": "b",
		"access_key": "a",
	})
	want := map[string]string{
		"ACCESS_KEY": "a",
		"SECRET_KEY": "b",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("env = %#v, want %#v", got, want)
	}
}

func TestEnvMapFromSecretNormalizesUnsafeFieldNames(t *testing.T) {
	t.Parallel()

	got := envMapFromSecret(map[string]string{
		"db-password": "pw",
		"9token":      "tok",
	})
	want := map[string]string{
		"DB_PASSWORD": "pw",
		"_9TOKEN":     "tok",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("env = %#v, want %#v", got, want)
	}
}

func TestBuildSecretEnvMapRejectsNormalizationCollisions(t *testing.T) {
	t.Parallel()

	_, err := buildSecretEnvMap(map[string]string{
		"db-password": "a",
		"db_password": "b",
	})
	if err == nil {
		t.Fatal("expected collision error")
	}
}

func TestMergeEnvOverridesExistingValues(t *testing.T) {
	t.Parallel()

	got := mergeEnv([]string{"PATH=/bin", "ACCESS_KEY=old"}, map[string]string{
		"ACCESS_KEY": "new",
		"SECRET_KEY": "sec",
	})
	want := []string{
		"ACCESS_KEY=new",
		"PATH=/bin",
		"SECRET_KEY=sec",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeEnv = %#v, want %#v", got, want)
	}
}
