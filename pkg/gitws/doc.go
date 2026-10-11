// JSON document helpers: every document under /.drive9/git-workspaces/
// must survive unknown keys — a writer that does not know a newer field
// may not strip it on rewrite. The extra-keys plumbing below is shared
// by every document type in this package.
package gitws

import (
	"encoding/json"
	"fmt"
)

// extraKeys collects the raw keys of doc that known does not list.
func extraKeys(doc map[string]json.RawMessage, known ...string) map[string]json.RawMessage {
	knownSet := make(map[string]bool, len(known))
	for _, k := range known {
		knownSet[k] = true
	}
	var extra map[string]json.RawMessage
	for k, v := range doc {
		if knownSet[k] {
			continue
		}
		if extra == nil {
			extra = map[string]json.RawMessage{}
		}
		extra[k] = v
	}
	return extra
}

// mergeExtra folds preserved unknown keys back into the encoded output.
// Fields explicitly set to the empty string in fields are omitted.
func mergeExtra(fields map[string]any, extra map[string]json.RawMessage) ([]byte, error) {
	out := make(map[string]json.RawMessage, len(fields)+len(extra))
	for k, v := range extra {
		out[k] = v
	}
	for k, v := range fields {
		if s, ok := v.(string); ok && s == "" {
			continue
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("gitws: encode field %s: %w", k, err)
		}
		out[k] = b
	}
	return json.Marshal(out)
}

// decodeString reads one string field of a raw document.
func decodeString(doc map[string]json.RawMessage, key string) (string, error) {
	v, ok := doc[key]
	if !ok {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return "", fmt.Errorf("gitws: field %s: %w", key, err)
	}
	return s, nil
}
