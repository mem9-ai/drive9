package datastore

import (
	"context"
	"strings"
	"testing"
)

// The classifier is the statement half of the SQL boundary; the durable half is
// the tenant's database grant. It has to reject structural forms it cannot follow
// rather than guess, and it must not read server state.
func TestClassifyExecSQLStructuralForms(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr string
	}{
		{name: "select allowed", query: "SELECT 1"},
		{name: "select with join allowed", query: "SELECT fn.path FROM file_nodes fn JOIN files f ON fn.file_id = f.file_id"},
		{name: "with cte allowed", query: "WITH cte AS (SELECT 1) SELECT * FROM cte"},
		{name: "insert into file_tags allowed", query: "INSERT INTO file_tags (file_id, tag_key, tag_value) VALUES ('a', 'b', 'c')"},
		{name: "update file_tags allowed", query: "UPDATE file_tags SET tag_value = 'x' WHERE tag_key = 'y'"},
		{name: "delete from file_tags allowed", query: "DELETE FROM file_tags WHERE file_id = 'a'"},

		{name: "drop rejected", query: "DROP TABLE file_tags", wantErr: "only SELECT"},
		{name: "truncate rejected", query: "TRUNCATE TABLE file_tags", wantErr: "only SELECT"},
		{name: "update files rejected", query: "UPDATE files SET status = 'DELETED'", wantErr: "only SELECT"},
		{name: "delete from files rejected", query: "DELETE FROM files WHERE file_id = 'x'", wantErr: "only SELECT"},

		{name: "multi-table update join rejected", query: "UPDATE file_tags ft JOIN files f ON f.file_id = ft.file_id SET f.status='DELETED'", wantErr: "multi-table"},
		{name: "multi-table update comma rejected", query: "UPDATE file_tags ft, files f SET f.status='DELETED' WHERE ft.file_id = f.file_id", wantErr: "multi-table"},
		{name: "multi-table update comma no alias rejected", query: "UPDATE file_tags,files SET files.status='DELETED'", wantErr: "multi-table"},
		{name: "multi-table update newline rejected", query: "UPDATE file_tags, \n files SET status='DELETED'", wantErr: "multi-table"},
		{name: "multi-table delete using rejected", query: "DELETE FROM file_tags USING file_tags JOIN files ON files.file_id = file_tags.file_id", wantErr: "multi-table"},
		{name: "multi-table delete comma rejected", query: "DELETE FROM file_tags, files WHERE file_tags.file_id = files.file_id", wantErr: "multi-table"},
		{name: "straight_join rejected", query: "UPDATE file_tags STRAIGHT_JOIN files f ON f.file_id = file_tags.file_id SET f.status='DELETED'", wantErr: "multi-table"},
		{name: "join behind comment rejected", query: "UPDATE file_tags JOIN/**/files f ON f.file_id = file_tags.file_id SET f.status='DELETED'", wantErr: "multi-table"},
		{name: "line comment before join rejected", query: "UPDATE file_tags -- x\nJOIN files f ON f.file_id = file_tags.file_id SET f.status='DELETED'", wantErr: "multi-table"},

		{name: "statement chain rejected", query: "UPDATE file_tags SET tag_value = 'x'; DROP TABLE files", wantErr: "multiple statements"},
		{name: "trailing semicolon allowed", query: "UPDATE file_tags SET tag_value = 'x';"},
		{name: "executable comment rejected", query: "UPDATE file_tags /*!50000 JOIN files */ SET a = 1", wantErr: "unsupported SQL"},
		// TiDB runs /*T! ... */ and /*T![feature] ... */; MariaDB runs /*M! ... */.
		{name: "tidb executable comment rejected", query: "UPDATE file_tags /*T! JOIN files f ON 1=1 */ SET a = 1", wantErr: "unsupported SQL"},
		{name: "tidb feature comment rejected", query: "UPDATE file_tags /*T![50000] JOIN files */ SET a = 1", wantErr: "unsupported SQL"},
		{name: "mariadb executable comment rejected", query: "UPDATE file_tags /*M! JOIN files */ SET a = 1", wantErr: "unsupported SQL"},
		{name: "unterminated literal rejected", query: "UPDATE file_tags SET tag_value = 'x", wantErr: "unsupported SQL"},
		{name: "shadow table rejected", query: "UPDATE file_tags_shadow SET tag_value = 'x'", wantErr: "only SELECT"},

		// Keywords inside literals are not references.
		{name: "join in literal allowed", query: "UPDATE file_tags SET tag_value = 'a join b' WHERE tag_key = 'k'"},
		{name: "comma in update set allowed", query: "UPDATE file_tags SET tag_key = 'k', tag_value = 'v' WHERE file_id = 'a'"},
	}
	store := &Store{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.ExecSQL(context.Background(), tt.query)
			if tt.wantErr == "" {
				if err != nil && !strings.Contains(err.Error(), "database is closed") {
					t.Fatalf("unexpected rejection: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ExecSQL(%q) was accepted", tt.query)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

// A tenant statement must not read server state: system variables carry the
// operator's provider credentials and the server schemas describe every other
// database on the instance.
func TestClassifyExecSQLRejectsServerState(t *testing.T) {
	for _, query := range []string{
		"SELECT @@GLOBAL.TIDB_EXP_EMBED_OPENAI_API_KEY",
		"SELECT @@session.sql_mode",
		"SELECT SYSTEM_VAR FROM anything WHERE x = @@GLOBAL.tidb_exp_embed_openai_api_key",
		"SELECT table_schema FROM information_schema.tables",
		"SELECT user FROM mysql.user",
		"SELECT * FROM performance_schema.session_variables",
		"SELECT * FROM sys.x",
		"SELECT 1 FROM metrics_schema.tidb_query_duration",
		// A quoted identifier is the same reference; sanitisation blanks it out of
		// the structural view, so it is checked separately.
		"SELECT 1 FROM `information_schema`.tables",
		"SELECT 1 FROM `mysql`.`user`",
		"SELECT 1 FROM `performance_schema`.session_variables",
	} {
		kind, _, err := classifyExecSQL(query)
		if err == nil {
			t.Fatalf("classifyExecSQL(%q) = kind %v, want a rejection", query, kind)
		}
		if kind != execSQLRejected {
			t.Fatalf("classifyExecSQL(%q) kind = %v, want rejected", query, kind)
		}
		if !strings.Contains(err.Error(), "system variables") {
			t.Fatalf("classifyExecSQL(%q) error = %v", query, err)
		}
	}
	// A literal that merely contains those words is not a reference.
	if _, _, err := classifyExecSQL("SELECT 'information_schema @@x' AS note"); err != nil {
		t.Fatalf("literal content was treated as a reference: %v", err)
	}
}

func TestSafeExecSQLMode(t *testing.T) {
	for _, mode := range []string{"STRICT_TRANS_TABLES", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES", ""} {
		if !safeExecSQLMode(mode) {
			t.Fatalf("safeExecSQLMode(%q) = false, want true", mode)
		}
	}
	for _, mode := range []string{"NO_BACKSLASH_ESCAPES", "STRICT_TRANS_TABLES,ANSI_QUOTES", "ANSI"} {
		if safeExecSQLMode(mode) {
			t.Fatalf("safeExecSQLMode(%q) = true, want false", mode)
		}
	}
}

func TestSanitizeSQL(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "comments become separators", input: "update/*x*/file_tags -- y\nset a='b'#z\n", want: "UPDATE FILE_TAGS SET A="},
		{name: "literal content is blanked", input: "UPDATE file_tags SET tag_value = 'a join b' WHERE k = \"c\\\"d\"", want: "UPDATE FILE_TAGS SET TAG_VALUE = WHERE K ="},
		{name: "backtick identifier is blanked", input: "UPDATE `file_tags` SET `tag_value` = 1", want: "UPDATE SET = 1"},
		{name: "minus minus without space is not a comment", input: "UPDATE file_tags SET a = 1--1", want: "UPDATE FILE_TAGS SET A = 1--1"},
		{name: "executable comment rejected", input: "UPDATE file_tags /*!50000 JOIN files */ SET a = 1", wantErr: true},
		{name: "unterminated comment rejected", input: "UPDATE file_tags /* x", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sanitizeSQL(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("sanitizeSQL(%q) = %q, want error", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("sanitizeSQL(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("sanitizeSQL(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
