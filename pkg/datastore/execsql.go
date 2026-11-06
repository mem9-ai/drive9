package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
)

var (
	errExecSQLOnlySupported = errors.New("only SELECT queries and INSERT/UPDATE/DELETE on file_tags are allowed")
	errExecSQLMultiTable    = errors.New("multi-table DML not allowed; single-table statements on file_tags only")
	// errExecSQLUnparseable rejects statements whose lexical structure this
	// classifier cannot follow. Guessing on them would risk classifying a
	// multi-table write as a tag write, so they fail closed.
	errExecSQLUnparseable = errors.New("unsupported SQL")
	// errExecSQLMultiStatement rejects a statement chain. The tenant DSN forbids
	// multiStatements, but a single-statement classifier must not rely on that
	// alone: if the driver ever allowed a chain, everything after the first `;`
	// would execute under the tag-write authorization.
	errExecSQLMultiStatement = errors.New("multiple statements are not allowed")
	// errExecSQLUnsafeSQLMode rejects a tenant database whose sql_mode changes how
	// a string literal terminates. The classifier's lexical model assumes
	// backslash escapes active and ANSI_QUOTES off; guessing under the other modes
	// risks misclassifying a multi-table write, so SQL fails closed.
	errExecSQLUnsafeSQLMode = errors.New("exec SQL is disabled: tenant sql_mode must not set NO_BACKSLASH_ESCAPES, ANSI_QUOTES or ANSI")
	// ErrExecSQLRejected marks a policy refusal: the statement was not executed.
	// Callers use it to keep their own logging at warn level — a client sending a
	// rejected statement is not a server fault.
	ErrExecSQLRejected = errors.New("exec SQL statement rejected by policy")
	// errExecSQLSystemRead rejects statements that read outside the tenant's own
	// tables: system variables (`@@GLOBAL.x` exposes operator-supplied provider
	// credentials such as the auto-embedding API key) and the server's own
	// schemas, whose contents describe every other database on the instance.
	errExecSQLSystemRead = errors.New("reading system variables and server schemas is not allowed")
)

// execSQLKind is the classification ExecSQL acts on.
type execSQLKind int

const (
	execSQLRejected execSQLKind = iota
	execSQLSelect
	execSQLTagWrite
)

// systemSchemas are the server-maintained schemas a tenant statement must not
// reach. On a shared MySQL instance they expose every other database's metadata.
var systemSchemas = map[string]struct{}{
	"INFORMATION_SCHEMA": {},
	"MYSQL":              {},
	"PERFORMANCE_SCHEMA": {},
	"SYS":                {},
	"METRICS_SCHEMA":     {},
}

// classifyExecSQL decides whether a statement is a read (SELECT / WITH ...
// SELECT) or a single-table write on file_tags.
//
// This is a denylist, so it is deliberately conservative: it blanks comments and
// quoted segments before looking for join separators, and it rejects lexical
// forms it cannot follow instead of guessing. It is still not an authorization
// boundary — the durable boundary is the database grant on the account SQL runs
// as. The classifier exists so an agent's raw SQL cannot mutate anything except
// tags, and cannot read server state the tenant does not own.
func classifyExecSQL(query string) (execSQLKind, string, error) {
	norm, identifiers, err := sanitizeSQLWithIdentifiers(query)
	if err != nil {
		return execSQLRejected, "", err
	}
	// A single trailing semicolon is a normal statement terminator; any other
	// semicolon starts a second statement.
	if strings.Contains(strings.TrimSuffix(norm, ";"), ";") {
		return execSQLRejected, norm, errExecSQLMultiStatement
	}
	if err := rejectSystemReads(norm, identifiers); err != nil {
		return execSQLRejected, norm, err
	}

	isSelect := strings.HasPrefix(norm, "SELECT")
	if strings.HasPrefix(norm, "WITH") {
		hasDML := strings.Contains(norm, "INSERT") ||
			strings.Contains(norm, "UPDATE") ||
			strings.Contains(norm, "DELETE") ||
			strings.Contains(norm, "DROP") ||
			strings.Contains(norm, "ALTER") ||
			strings.Contains(norm, "TRUNCATE")
		if !hasDML {
			isSelect = true
		}
	}
	isTagWrite := hasKeywordPrefix(norm, "INSERT INTO FILE_TAGS") ||
		hasKeywordPrefix(norm, "UPDATE FILE_TAGS") ||
		hasKeywordPrefix(norm, "DELETE FROM FILE_TAGS")

	if !isTagWrite {
		if !isSelect {
			return execSQLRejected, norm, errExecSQLOnlySupported
		}
		return execSQLSelect, norm, nil
	}

	// A tag write may name only file_tags. MySQL joins are separated by a join
	// keyword (JOIN / STRAIGHT_JOIN / USING) or, in the UPDATE/DELETE table list,
	// by a comma; matching those as tokens after comments and literals are blanked
	// is what keeps `JOIN/**/` and `STRAIGHT_JOIN` from slipping through a plain
	// `" JOIN "` substring test.
	if strings.HasPrefix(norm, "UPDATE") || strings.HasPrefix(norm, "DELETE") {
		tokens := sqlTokens(norm)
		if containsSQLToken(tokens, "JOIN") ||
			containsSQLToken(tokens, "STRAIGHT_JOIN") ||
			containsSQLToken(tokens, "USING") {
			return execSQLRejected, norm, errExecSQLMultiTable
		}
		if strings.HasPrefix(norm, "UPDATE") && commaBeforeSQLToken(tokens, "SET") {
			return execSQLRejected, norm, errExecSQLMultiTable
		}
		if strings.HasPrefix(norm, "DELETE") && commaInDeleteTableRefs(tokens) {
			return execSQLRejected, norm, errExecSQLMultiTable
		}
	}
	return execSQLTagWrite, norm, nil
}

// rejectSystemReads refuses statements that touch server state: system variables
// (`@@GLOBAL.TIDB_EXP_EMBED_OPENAI_API_KEY` holds the operator's shared embedding
// provider key) and the server's own schemas.
//
// It runs on the blanked structural view, so a literal that merely contains those
// words is not a reference. `@` is rejected wholesale: a tenant statement has no
// legitimate use for a server or session variable, and rejecting the token avoids
// modelling the `@@`, `@` and `@@session.` spellings separately.
func rejectSystemReads(norm string, identifiers []string) error {
	if strings.Contains(norm, "@") {
		return errExecSQLSystemRead
	}
	for _, token := range sqlTokens(norm) {
		if _, ok := systemSchemas[token]; ok {
			return errExecSQLSystemRead
		}
	}
	// Quoted identifiers are blanked out of the structural view, so they are
	// checked on their own: ` + "`information_schema`" + ` is the same reference as the
	// bare word.
	for _, identifier := range identifiers {
		if strings.Contains(identifier, "@") {
			return errExecSQLSystemRead
		}
		for _, token := range sqlTokens(identifier) {
			if _, ok := systemSchemas[token]; ok {
				return errExecSQLSystemRead
			}
		}
	}
	return nil
}

// execSQLPolicyError marks a classifier rejection while keeping its message
// byte-identical: the API returns these errors to the client verbatim.
type execSQLPolicyError struct{ err error }

func (e *execSQLPolicyError) Error() string { return e.err.Error() }
func (e *execSQLPolicyError) Unwrap() error { return e.err }
func (e *execSQLPolicyError) Is(target error) bool {
	return target == ErrExecSQLRejected
}

func markExecSQLPolicyError(err error) error {
	if err == nil || errors.Is(err, ErrExecSQLRejected) {
		return err
	}
	return &execSQLPolicyError{err: err}
}

func (s *Store) ExecSQL(ctx context.Context, query string) (out []map[string]interface{}, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "exec_sql", start, &err)

	q := strings.TrimSpace(query)

	// A nil Store/DB is the classifier tests' harness; a closed store reaches
	// connection acquisition below. Classify first so a policy rejection keeps its
	// precise message even without a database.
	if s == nil || s.db == nil {
		if _, _, policyErr := classifyExecSQL(q); policyErr != nil {
			return nil, policyErr
		}
		err = fmt.Errorf("database is closed")
		logger.Error(ctx, "datastore_exec_sql_closed", zap.Error(err))
		return nil, err
	}

	// Classify before touching the database: a rejected statement needs no
	// connection, and it should not cost a sql_mode round trip.
	if _, _, policyErr := classifyExecSQL(q); policyErr != nil {
		err = markExecSQLPolicyError(policyErr)
		logger.Warn(ctx, "datastore_exec_sql_rejected", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}

	// The timeout bounds connection acquisition, the sql_mode probe, and the
	// statement together.
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Pin one connection: sql_mode is a session property, so the probe must run on
	// the same session that executes the statement. A pooled probe could check a
	// different connection than the one that lexes the literal.
	conn, err := s.db.Conn(timeoutCtx)
	if err != nil {
		logger.Error(timeoutCtx, "datastore_exec_sql_conn_failed", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	if modeErr := checkExecSQLMode(timeoutCtx, conn); modeErr != nil {
		if errors.Is(modeErr, errExecSQLUnsafeSQLMode) {
			// A permanently misconfigured tenant must not turn every request into an
			// ERROR log: warn, and let the response/metric carry the rejection. The
			// refusal is also marked as a policy error so upstream logging and the
			// failure series treat it as client input.
			err = markExecSQLPolicyError(modeErr)
			logger.Warn(timeoutCtx, "datastore_exec_sql_sql_mode_rejected", zap.Int("query_len", len(q)), zap.Error(err))
			return nil, err
		}
		err = modeErr
		logger.Error(timeoutCtx, "datastore_exec_sql_sql_mode_rejected", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}

	kind, _, err := classifyExecSQL(q)
	if err != nil {
		// Unreachable in practice (classified above); kept so the statement that
		// executes is always the statement that was classified.
		logger.Warn(timeoutCtx, "datastore_exec_sql_rejected", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, markExecSQLPolicyError(err)
	}

	if kind == execSQLTagWrite {
		res, err := conn.ExecContext(timeoutCtx, q)
		if err != nil {
			logger.Error(timeoutCtx, "datastore_exec_sql_tag_write_failed", zap.Int("query_len", len(q)), zap.Error(err))
			return nil, err
		}
		affected, _ := res.RowsAffected()
		return []map[string]interface{}{{"rows_affected": affected}}, nil
	}

	rows, err := conn.QueryContext(timeoutCtx, q)
	if err != nil {
		logger.Error(timeoutCtx, "datastore_exec_sql_query_failed", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	const maxRows = 1000
	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		if len(result) >= maxRows {
			break
		}
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			v := vals[i]
			if b, ok := v.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = v
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		logger.Error(timeoutCtx, "datastore_exec_sql_scan_failed", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}
	return result, nil
}

// checkExecSQLMode reads @@SESSION.sql_mode on the pinned connection that will
// execute the statement. sql_mode is a session property and can change after a
// connection is created, so the verdict is not cached across connections: the
// probe and the statement must agree.
func checkExecSQLMode(ctx context.Context, conn *sql.Conn) error {
	var mode string
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&mode); err != nil {
		return fmt.Errorf("read tenant sql_mode: %w", err)
	}
	if !safeExecSQLMode(mode) {
		return errExecSQLUnsafeSQLMode
	}
	return nil
}

// safeExecSQLMode reports whether mode keeps the classifier's literal model:
// backslash escapes active in quoted strings and double quotes treated as a string
// delimiter (ANSI_QUOTES off). MySQL's ANSI combo alias expands to ANSI_QUOTES,
// but comma-separated tokens (and the alias) are compared rather than substrings,
// so a value that merely contains a flag name cannot confuse it.
func safeExecSQLMode(mode string) bool {
	for _, raw := range strings.Split(mode, ",") {
		switch strings.ToUpper(strings.TrimSpace(raw)) {
		case "ANSI", "ANSI_QUOTES", "NO_BACKSLASH_ESCAPES":
			return false
		}
	}
	return true
}

// sanitizeSQL returns an upper-cased structural view of query: comments, string
// literals, and quoted identifiers are blanked, MySQL whitespace is collapsed, and
// runs of spaces are trimmed. Because blanked segments become separators, the
// result can only gain token boundaries relative to the original — it can never
// merge two tokens — which is the safe direction for a denylist that looks for
// separator keywords.
//
// The statement sent to the database is always the caller's original text.
//
// The tenant sql_mode is checked on the pinned connection before classification
// (checkExecSQLMode); this scanner relies on that check: backslash escapes active
// in string literals and ANSI_QUOTES off.
func sanitizeSQL(query string) (string, error) {
	norm, _, err := sanitizeSQLWithIdentifiers(query)
	return norm, err
}

// sanitizeSQLWithIdentifiers also returns the content of every backtick-quoted
// identifier, upper-cased.
//
// Blanking a quoted identifier is what keeps `JOIN` or a comma inside one from
// looking like structure, but it also hides a schema reference: `SELECT 1 FROM
// ` + "`information_schema`" + `.tables` would otherwise lose the token that the
// server-state check looks for. The structural view stays blanked; the identifier
// contents are checked separately.
func sanitizeSQLWithIdentifiers(query string) (string, []string, error) {
	var identifiers []string
	var b strings.Builder
	b.Grow(len(query))
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case isMySQLSpace(c):
			b.WriteByte(' ')
			i++
		case c == '#':
			// MySQL: '#' comments out the rest of the line.
			for i < len(query) && query[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
		case c == '-' && i+1 < len(query) && query[i+1] == '-' &&
			(i+2 == len(query) || isMySQLSpace(query[i+2])):
			// MySQL: '--' starts a comment only when followed by whitespace or end
			// of input; otherwise it is two minus operators.
			for i < len(query) && query[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
		case c == '/' && i+1 < len(query) && query[i+1] == '*':
			// Executable comment dialects cannot be treated as inert noise: MySQL
			// runs /*! ... */ and /*!50700 ... */, TiDB runs /*T! ... */ and
			// /*T![feature] ... */, and MariaDB runs /*M! ... */. Blanking one would
			// hide a JOIN from the classifier while the server still executes it.
			if i+2 < len(query) && isExecutableCommentStart(query[i+2:]) {
				return "", nil, fmt.Errorf("%w: executable comment", errExecSQLUnparseable)
			}
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				return "", nil, fmt.Errorf("%w: unterminated comment", errExecSQLUnparseable)
			}
			i += 2 + end + 2
			b.WriteByte(' ')
		case c == '\'' || c == '"':
			end, ok := skipSQLQuoted(query, i, c, true)
			if !ok {
				return "", nil, fmt.Errorf("%w: unterminated string literal", errExecSQLUnparseable)
			}
			b.WriteByte(' ')
			i = end
			b.WriteByte(' ')
		case c == '`':
			// Backtick identifiers escape only by doubling the backtick; a
			// backslash inside one is literal, and treating it as an escape would
			// let the scanner swallow following SQL as an identifier.
			end, ok := skipSQLQuoted(query, i, c, false)
			if !ok {
				return "", nil, fmt.Errorf("%w: unterminated quoted identifier", errExecSQLUnparseable)
			}
			if content := strings.TrimSpace(query[i+1 : end-1]); content != "" {
				identifiers = append(identifiers, strings.ToUpper(content))
			}
			b.WriteByte(' ')
			i = end
			b.WriteByte(' ')
		default:
			if c >= 'a' && c <= 'z' {
				c -= 'a' - 'A'
			}
			b.WriteByte(c)
			i++
		}
	}
	return strings.Join(strings.Fields(b.String()), " "), identifiers, nil
}

// skipSQLQuoted returns the index just past the closing quote of the literal or
// identifier starting at start. A doubled quote escapes itself; when
// backslashEscapes is set a backslash escapes the next byte (string literals
// only — never backtick identifiers).
func skipSQLQuoted(query string, start int, quote byte, backslashEscapes bool) (int, bool) {
	for i := start + 1; i < len(query); i++ {
		if backslashEscapes && query[i] == '\\' {
			i++
			continue
		}
		if query[i] != quote {
			continue
		}
		if i+1 < len(query) && query[i+1] == quote {
			i++
			continue
		}
		return i + 1, true
	}
	return 0, false
}

// isExecutableCommentStart reports whether the text right after `/*` opens a
// comment the server executes rather than ignores: `!`, or a vendor prefix letter
// followed by `!` (TiDB's `T!`, MariaDB's `M!`).
func isExecutableCommentStart(rest string) bool {
	if rest == "" {
		return false
	}
	if rest[0] == '!' {
		return true
	}
	if len(rest) >= 2 && rest[1] == '!' {
		switch rest[0] {
		case 't', 'T', 'm', 'M':
			return true
		}
	}
	return false
}

func isMySQLSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	default:
		return false
	}
}

// hasKeywordPrefix reports whether norm starts with prefix and the keyword ends
// there. Without the boundary check `UPDATE file_tags_shadow` would pass as a tag
// write.
func hasKeywordPrefix(norm, prefix string) bool {
	if !strings.HasPrefix(norm, prefix) {
		return false
	}
	if len(norm) == len(prefix) {
		return true
	}
	switch norm[len(prefix)] {
	case ' ', '(', ',', ';':
		return true
	default:
		return false
	}
}

// sqlTokens splits an upper-cased sanitized statement into word tokens and
// single-byte punctuation tokens. Only structure matters here, so quoted content
// has already been blanked.
func sqlTokens(s string) []string {
	tokens := make([]string, 0, 16)
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ':
			i++
		case isSQLWordByte(c):
			j := i
			for j < len(s) && isSQLWordByte(s[j]) {
				j++
			}
			tokens = append(tokens, s[i:j])
			i = j
		default:
			tokens = append(tokens, string(c))
			i++
		}
	}
	return tokens
}

func isSQLWordByte(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '$'
}

func containsSQLToken(tokens []string, want string) bool {
	for _, token := range tokens {
		if token == want {
			return true
		}
	}
	return false
}

// commaBeforeSQLToken reports whether a comma appears before the first occurrence
// of token. In an UPDATE the table references come before SET, so a comma there
// means the multi-table form.
func commaBeforeSQLToken(tokens []string, token string) bool {
	for _, got := range tokens {
		if got == token {
			return false
		}
		if got == "," {
			return true
		}
	}
	return false
}

// commaInDeleteTableRefs reports whether the table-reference list of a DELETE
// contains a comma. The list ends at the first clause keyword; commas after it
// belong to WHERE/ORDER BY expressions, not to the table list.
func commaInDeleteTableRefs(tokens []string) bool {
	// The delete-target list can also precede FROM: DELETE t1, t2 FROM ...
	if commaBeforeSQLToken(tokens, "FROM") {
		return true
	}
	seenFrom := false
	for _, token := range tokens {
		if !seenFrom {
			if token == "FROM" {
				seenFrom = true
			}
			continue
		}
		switch token {
		case "WHERE", "ORDER", "LIMIT", "PARTITION", ";":
			return false
		case ",":
			return true
		}
	}
	return false
}
