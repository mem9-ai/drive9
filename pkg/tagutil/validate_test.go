package tagutil

import (
	"strings"
	"testing"
)

func TestValidateTagsMapRejectsInvalidKeysOrValues(t *testing.T) {
	tests := []struct {
		name string
		tags map[string]string
		want string
	}{
		{
			name: "key too long",
			tags: map[string]string{strings.Repeat("k", 256): "v"},
			want: "key exceeds 255 characters",
		},
		{
			name: "value too long",
			tags: map[string]string{"owner": strings.Repeat("v", 256)},
			want: "value exceeds 255 characters",
		},
		{
			name: "key contains equals",
			tags: map[string]string{"owner=id": "alice"},
			want: "contains '='",
		},
		{
			name: "key contains control chars",
			tags: map[string]string{"owner\n": "alice"},
			want: "contains control characters",
		},
		{
			name: "key has leading or trailing whitespace",
			tags: map[string]string{" owner ": "alice"},
			want: "leading or trailing whitespace",
		},
		{
			name: "value contains control chars",
			tags: map[string]string{"owner": "alice\t"},
			want: "contains control characters",
		},
		{
			name: "value has leading or trailing whitespace",
			tags: map[string]string{"owner": " alice "},
			want: "leading or trailing whitespace",
		},
		{
			name: "key contains invalid utf8",
			tags: map[string]string{string([]byte{0xff}): "alice"},
			want: "invalid UTF-8",
		},
		{
			name: "value contains invalid utf8",
			tags: map[string]string{"owner": string([]byte{0xff})},
			want: "invalid UTF-8",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateMap(tc.tags); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateTagsMap(%v) err = %v, want %q", tc.tags, err, tc.want)
			}
		})
	}
}
