package datastore

import (
	"context"
	"strings"
	"testing"
)

func TestNormalizeSQL(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"  SELECT  1  ", "SELECT 1"},
		{"SELECT\n\t1", "SELECT 1"},
		{"UPDATE\n  file_tags\n  SET  tag_value='x'", "UPDATE file_tags SET tag_value='x'"},
	}
	for _, tt := range tests {
		got := normalizeSQL(tt.input)
		if got != tt.want {
			t.Errorf("normalizeSQL(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestExecSQLClassifier(t *testing.T) {
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
		{name: "alter rejected", query: "ALTER TABLE files ADD COLUMN x INT", wantErr: "only SELECT"},
		{name: "truncate rejected", query: "TRUNCATE TABLE file_tags", wantErr: "only SELECT"},
		{name: "insert into files rejected", query: "INSERT INTO files (file_id) VALUES ('x')", wantErr: "only SELECT"},
		{name: "update files rejected", query: "UPDATE files SET status = 'DELETED'", wantErr: "only SELECT"},
		{name: "delete from files rejected", query: "DELETE FROM files WHERE file_id = 'x'", wantErr: "only SELECT"},

		{name: "with+delete rejected", query: "WITH t AS (SELECT 1) DELETE FROM file_tags WHERE file_id = 'x'", wantErr: "only SELECT"},
		{name: "with+update rejected", query: "WITH t AS (SELECT 1) UPDATE file_tags SET tag_value = 'x'", wantErr: "only SELECT"},
		{name: "with+insert rejected", query: "WITH t AS (SELECT 1) INSERT INTO file_tags VALUES ('a','b','c')", wantErr: "only SELECT"},

		{name: "multi-table update join rejected", query: "UPDATE file_tags ft JOIN files f ON f.file_id = ft.file_id SET f.status='DELETED'", wantErr: "multi-table"},
		{name: "multi-table update comma rejected", query: "UPDATE file_tags ft, files f SET f.status='DELETED' WHERE ft.file_id = f.file_id", wantErr: "multi-table"},
		{name: "multi-table delete using rejected", query: "DELETE FROM file_tags USING file_tags JOIN files ON files.file_id = file_tags.file_id", wantErr: "multi-table"},
		{name: "multi-table delete comma rejected", query: "DELETE FROM file_tags, files WHERE file_tags.file_id = files.file_id", wantErr: "multi-table"},

		{name: "update newline join rejected", query: "UPDATE file_tags ft\nJOIN files f ON f.file_id = ft.file_id\nSET f.status='DELETED'", wantErr: "multi-table"},
		{name: "delete tab using rejected", query: "DELETE FROM file_tags\tUSING file_tags JOIN files ON files.file_id = file_tags.file_id", wantErr: "multi-table"},
		{name: "delete no-where comma rejected", query: "DELETE FROM file_tags,files", wantErr: "multi-table"},
	}

	s := &Store{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.ExecSQL(context.Background(), tt.query)
			if tt.wantErr == "" {
				if err != nil && !strings.Contains(err.Error(), "invalid memory") && !strings.Contains(err.Error(), "nil pointer") && !strings.Contains(err.Error(), "database is closed") {
					t.Errorf("unexpected classification error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.wantErr)
				} else if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.wantErr)) {
					t.Errorf("expected error containing %q, got: %v", tt.wantErr, err)
				}
			}
		})
	}
}
