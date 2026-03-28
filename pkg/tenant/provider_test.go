package tenant

import "testing"

func TestCurrentSchemaVersionByProvider(t *testing.T) {
	tests := []struct {
		provider string
		want     int
	}{
		{ProviderDB9, 1},
		{ProviderTiDBZero, 2},
		{ProviderTiDBCloudStarter, 1},
		{"unknown", 1},
	}

	for _, tt := range tests {
		if got := CurrentSchemaVersion(tt.provider); got != tt.want {
			t.Fatalf("CurrentSchemaVersion(%q)=%d, want %d", tt.provider, got, tt.want)
		}
	}
}
