package vault

import (
	"fmt"
	"strings"
)

// ValidateScope checks that all scope entries are well-formed. Returns an error
// for entries containing wildcards (*) or empty segments.
func ValidateScope(scope []string) error {
	for _, entry := range scope {
		if entry == "" {
			return fmt.Errorf("empty scope entry")
		}
		if strings.Contains(entry, "*") {
			return fmt.Errorf("wildcard scope entries are not supported: %q", entry)
		}
		parts := strings.SplitN(entry, "/", 2)
		if parts[0] == "" {
			return fmt.Errorf("empty secret name in scope entry: %q", entry)
		}
		if len(parts) == 2 && parts[1] == "" {
			return fmt.Errorf("empty field name in scope entry: %q", entry)
		}
	}
	return nil
}

// CheckScope verifies that a requested secret/field is within the token's scope.
// Scope entries:
//   - "aws-prod" → access all fields of aws-prod
//   - "db-prod/password" → access only the password field of db-prod
func CheckScope(scope []string, secretName, fieldName string) bool {
	for _, entry := range scope {
		if entry == secretName {
			return true // full secret access
		}
		if fieldName != "" {
			// Check field-level scope: "secret/field"
			if entry == secretName+"/"+fieldName {
				return true
			}
		}
	}
	return false
}

// ScopedSecretNames returns the set of secret names the token can access.
func ScopedSecretNames(scope []string) []string {
	seen := make(map[string]bool)
	var names []string
	for _, entry := range scope {
		name := entry
		if idx := strings.IndexByte(entry, '/'); idx >= 0 {
			name = entry[:idx]
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return names
}

// HasFullSecretAccess returns true if the scope grants access to all fields.
func HasFullSecretAccess(scope []string, secretName string) bool {
	for _, entry := range scope {
		if entry == secretName {
			return true
		}
	}
	return false
}

// AllowedFields returns the set of field names allowed by scope for a given secret.
// If the scope grants full secret access, returns nil (meaning all fields allowed).
func AllowedFields(scope []string, secretName string) (allFields bool, fields []string) {
	for _, entry := range scope {
		if entry == secretName {
			return true, nil
		}
		if strings.HasPrefix(entry, secretName+"/") {
			fields = append(fields, entry[len(secretName)+1:])
		}
	}
	return false, fields
}
