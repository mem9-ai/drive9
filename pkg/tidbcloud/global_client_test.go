package tidbcloud

import "testing"

func TestParseClusterIDUint64(t *testing.T) {
	tests := []struct {
		input   string
		want    uint64
		wantErr bool
	}{
		{"12345", 12345, false},
		{"0", 0, false},
		{"18446744073709551615", 18446744073709551615, false}, // max uint64
		{"", 0, true},
		{"abc", 0, true},
		{"-1", 0, true},
		{"12345678901234567890123", 0, true}, // overflow
	}

	for _, tt := range tests {
		got, err := ParseClusterIDUint64(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("ParseClusterIDUint64(%q): expected error, got %d", tt.input, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseClusterIDUint64(%q): unexpected error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Fatalf("ParseClusterIDUint64(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
