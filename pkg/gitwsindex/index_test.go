package gitwsindex

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestIndexPreservesUnknownFields(t *testing.T) {
	in := `{
  "version": 1,
  "updated_at": "old",
  "schema_note": "keep-top",
  "workspaces": [
    {"workspace_id": "ws1", "root_path": "/a/", "workspace_kind": "main", "alias": "one"},
    {"workspace_id": "ws2", "root_path": "/b/", "alias": "two"}
  ]
}`
	idx, err := Parse([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	// Replace ws1; keep ws2 extras; keep top-level extra.
	out := make([]Entry, 0, len(idx.Workspaces))
	for _, e := range idx.Workspaces {
		if e.WorkspaceID == "ws1" {
			continue
		}
		out = append(out, e)
	}
	out = append(out, Entry{WorkspaceID: "ws1", RootPath: "/a2/", WorkspaceKind: "main"})
	idx.Workspaces = out
	idx.UpdatedAt = "new"
	body, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	raw := string(body)
	if !strings.Contains(raw, `"schema_note":"keep-top"`) && !strings.Contains(raw, `"schema_note": "keep-top"`) {
		t.Fatalf("missing top-level extra: %s", raw)
	}
	if !strings.Contains(raw, `"alias":"two"`) && !strings.Contains(raw, `"alias": "two"`) {
		t.Fatalf("missing per-entry extra on ws2: %s", raw)
	}
	var again Index
	if err := json.Unmarshal(body, &again); err != nil {
		t.Fatal(err)
	}
	if again.UpdatedAt != "new" || len(again.Workspaces) != 2 {
		t.Fatalf("round-trip index = %+v", again)
	}
}

func TestParseRejectsOversizedEntryCount(t *testing.T) {
	entries := make([]Entry, MaxEntries+1)
	for i := range entries {
		entries[i] = Entry{WorkspaceID: "w", RootPath: "/r/"}
	}
	body, err := json.Marshal(Index{Version: 1, Workspaces: entries})
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(body)) > MaxBytes {
		t.Skip("fixture exceeded byte cap")
	}
	if _, err := Parse(body); err == nil {
		t.Fatal("Parse oversized entry count succeeded")
	}
}
