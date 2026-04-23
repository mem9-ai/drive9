// Package tagutil validates file tag keys and values.
package tagutil

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// MaxLen is the maximum allowed rune length for tag keys and values.
const MaxLen = 255

// ValidateEntry validates one tag key/value pair for header-style usage.
func ValidateEntry(key, value string) error {
	if key == "" {
		return fmt.Errorf("invalid tag key %q: empty key", key)
	}
	return validateNonEmptyEntry(key, value)
}

// ValidateMap validates a full tag map for JSON request bodies.
func ValidateMap(tags map[string]string) error {
	for key, value := range tags {
		if err := ValidateEntry(key, value); err != nil {
			return err
		}
	}
	return nil
}

func validateNonEmptyEntry(key, value string) error {
	// key constraints
	if !utf8.ValidString(key) {
		return fmt.Errorf("invalid tag key %q: contains invalid UTF-8", key)
	}
	if containsControlChars(key) {
		return fmt.Errorf("invalid tag key %q: contains control characters", key)
	}
	if key != strings.TrimSpace(key) {
		return fmt.Errorf("invalid tag key %q: must not have leading or trailing whitespace", key)
	}
	if strings.Contains(key, "=") {
		return fmt.Errorf("invalid tag key %q: contains '='", key)
	}
	// value constraints
	if !utf8.ValidString(value) {
		return fmt.Errorf("invalid tag value for key %q: contains invalid UTF-8", key)
	}
	if containsControlChars(value) {
		return fmt.Errorf("invalid tag value for key %q: contains control characters", key)
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("invalid tag value for key %q: must not have leading or trailing whitespace", key)
	}
	// length constraints
	if utf8.RuneCountInString(key) > MaxLen {
		return fmt.Errorf("invalid tags: key exceeds %d characters", MaxLen)
	}
	if utf8.RuneCountInString(value) > MaxLen {
		return fmt.Errorf("invalid tags: value exceeds %d characters", MaxLen)
	}
	return nil
}

func containsControlChars(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}
