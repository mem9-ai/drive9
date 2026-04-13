package vault

import "strings"

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
