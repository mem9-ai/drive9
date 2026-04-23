// Package schemaspec provides shared helpers for parsing trusted MySQL/TiDB
// schema statements and classifying safe repair operations.
package schemaspec

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strings"

	mysql "github.com/go-sql-driver/mysql"
)

// NormalizeSQLFragment lowercases SQL, removes identifier quoting and TiDB's
// _utf8mb4 marker, and collapses repeated whitespace for stable comparisons.
func NormalizeSQLFragment(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "`", "")
	s = strings.ReplaceAll(s, "_utf8mb4", "")
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}

// CanonicalStatementForHash normalizes formatting-only differences that remain
// after whitespace collapsing so schema-version hashing is stable across
// cosmetic edits.
func CanonicalStatementForHash(s string) string {
	normalized := NormalizeSQLFragment(s)
	for _, replacement := range [][2]string{
		{" (", "("},
		{"( ", "("},
		{" )", ")"},
		{") ", ")"},
		{", ", ","},
		{" ,", ","},
	} {
		normalized = strings.ReplaceAll(normalized, replacement[0], replacement[1])
	}
	return normalized
}

// SQLSnippet truncates a statement for logging and error messages.
func SQLSnippet(stmt string) string {
	snippet := stmt
	if len(snippet) > 80 {
		snippet = snippet[:80]
	}
	return snippet
}

// ParseCreateTableStatement extracts the table name and the top-level column /
// constraint definition list from trusted CREATE TABLE SQL or SHOW CREATE TABLE
// output. The scanner is intentionally lightweight and is not intended for
// arbitrary user-supplied SQL.
func ParseCreateTableStatement(stmt string) (tableName string, definitions string, ok bool, err error) {
	lower := strings.ToLower(stmt)
	prefixes := []string{"create table if not exists", "create table"}
	start := -1
	prefixLen := 0
	for _, prefix := range prefixes {
		if idx := strings.Index(lower, prefix); idx >= 0 {
			start = idx
			prefixLen = len(prefix)
			break
		}
	}
	if start < 0 {
		return "", "", false, nil
	}
	i := start + prefixLen
	for i < len(stmt) && (stmt[i] == ' ' || stmt[i] == '\n' || stmt[i] == '\t' || stmt[i] == '\r') {
		i++
	}
	if i >= len(stmt) {
		return "", "", false, fmt.Errorf("parse create table: missing table name")
	}
	nameStart := i
	if stmt[i] == '`' {
		i++
		for i < len(stmt) && stmt[i] != '`' {
			i++
		}
		if i >= len(stmt) {
			return "", "", false, fmt.Errorf("parse create table: unterminated quoted table name")
		}
		tableName = strings.ToLower(stmt[nameStart+1 : i])
		i++
	} else {
		for i < len(stmt) && stmt[i] != ' ' && stmt[i] != '\n' && stmt[i] != '\t' && stmt[i] != '(' {
			i++
		}
		tableName = strings.ToLower(strings.TrimSpace(stmt[nameStart:i]))
	}
	for i < len(stmt) && stmt[i] != '(' {
		i++
	}
	if i >= len(stmt) || stmt[i] != '(' {
		return "", "", false, fmt.Errorf("parse create table %s: missing opening parenthesis", tableName)
	}
	bodyStart := i + 1
	depth := 1
	inSingle := false
	inDouble := false
	inBacktick := false
	for i = bodyStart; i < len(stmt); i++ {
		ch := stmt[i]
		switch ch {
		case '\\':
			i++
			continue
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case '(':
			if !inSingle && !inDouble && !inBacktick {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBacktick {
				depth--
				if depth == 0 {
					return tableName, stmt[bodyStart:i], true, nil
				}
			}
		}
	}
	return "", "", false, fmt.Errorf("parse create table %s: unbalanced parentheses", tableName)
}

// SplitTopLevelComma splits a comma-delimited definition list while ignoring
// commas inside nested parentheses or quoted strings / identifiers.
func SplitTopLevelComma(definitions string) []string {
	parts := make([]string, 0)
	start := 0
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(definitions); i++ {
		ch := definitions[i]
		switch ch {
		case '\\':
			i++
			continue
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case '(':
			if !inSingle && !inDouble && !inBacktick {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBacktick && depth > 0 {
				depth--
			}
		case ',':
			if !inSingle && !inDouble && !inBacktick && depth == 0 {
				parts = append(parts, strings.TrimSpace(definitions[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(definitions) {
		parts = append(parts, strings.TrimSpace(definitions[start:]))
	}
	return parts
}

// SplitIdentifierAndRest extracts the first bare or backtick-quoted identifier
// from a trusted schema fragment and returns the remaining suffix.
func SplitIdentifierAndRest(s string) (identifier string, rest string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	if s[0] == '`' {
		end := strings.Index(s[1:], "`")
		if end < 0 {
			return "", ""
		}
		id := s[1 : 1+end]
		return id, strings.TrimSpace(s[1+end+1:])
	}
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r' {
			return s[:i], strings.TrimSpace(s[i+1:])
		}
	}
	return s, ""
}

// ParseColumnType returns the column type prefix from a trusted column
// definition, stopping before common trailing clauses such as NULL / DEFAULT /
// COLLATE / CHECK / STORAGE.
func ParseColumnType(rest string) string {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return ""
	}
	keywords := []string{
		" not ",
		" null",
		" default ",
		" generated ",
		" as ",
		" primary ",
		" unique ",
		" comment ",
		" references ",
		" auto_increment",
		" on update",
		" character set ",
		" collate ",
		" check ",
		" storage ",
		" column_format ",
	}
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(rest); i++ {
		ch := rest[i]
		switch ch {
		case '\\':
			i++
			continue
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case '(':
			if !inSingle && !inDouble && !inBacktick {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBacktick && depth > 0 {
				depth--
			}
		}
		if inSingle || inDouble || inBacktick || depth > 0 {
			continue
		}
		suffix := " " + strings.ToLower(rest[i:])
		for _, kw := range keywords {
			if strings.HasPrefix(suffix, kw) {
				return strings.TrimSpace(rest[:i])
			}
		}
	}
	return strings.TrimSpace(rest)
}

// IsSafeAddColumnRepairSQL identifies ALTER TABLE ... ADD COLUMN statements that
// are safe to auto-apply to live schemas.
func IsSafeAddColumnRepairSQL(sqlText string) bool {
	n := NormalizeSQLFragment(sqlText)
	if !strings.HasPrefix(n, "alter table ") || !strings.Contains(n, " add column ") {
		return false
	}
	if isGeneratedColumnAddSQL(n) {
		return false
	}
	if strings.Contains(n, " not null") && !strings.Contains(n, " default ") {
		return false
	}
	return true
}

func isGeneratedColumnAddSQL(normalizedSQL string) bool {
	if strings.Contains(normalizedSQL, " generated ") {
		return true
	}
	if !strings.Contains(normalizedSQL, " as (") {
		return false
	}
	return strings.Contains(normalizedSQL, " stored") || strings.Contains(normalizedSQL, " virtual")
}

// IsIgnorableMySQLError reports whether a DDL error is benign for idempotent
// schema repair paths.
func IsIgnorableMySQLError(err error) bool {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1050, 1060, 1061:
			return true
		}
		msg := strings.ToLower(me.Message)
		return strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate")
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate")
}

// CRC32Version hashes normalized schema statements and always returns a
// non-zero integer so callers can use it as a stable schema version marker.
func CRC32Version(stmts []string) int {
	h := crc32.ChecksumIEEE([]byte(strings.Join(stmts, "\n")))
	if h == 0 {
		h = 1
	}
	return int(h)
}
