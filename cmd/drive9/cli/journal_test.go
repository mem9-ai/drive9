package cli

import (
	"flag"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/journal"
)

func TestParseJournalAssignmentsKeepsRepeatedKeys(t *testing.T) {
	labels, err := parseJournalAssignments([]string{"env=prod", "env=us-east"})
	if err != nil {
		t.Fatalf("parseJournalAssignments: %v", err)
	}
	if len(labels) != 2 {
		t.Fatalf("labels = %#v, want 2 repeated-key labels", labels)
	}
	if labels[0].Key != "env" || labels[0].Value != "prod" || labels[1].Key != "env" || labels[1].Value != "us-east" {
		t.Fatalf("labels = %#v", labels)
	}
}

func TestParseJournalAssignmentsRejectsMalformedMetadata(t *testing.T) {
	if _, err := parseJournalAssignments([]string{"env"}); err == nil {
		t.Fatal("parseJournalAssignments accepted malformed metadata")
	}
}

func TestReadJournalEntriesFromStdinLineNumbers(t *testing.T) {
	input := "\n\ninvalid-json\n"
	_, err := readJournalEntriesFromStdin(strings.NewReader(input), false)
	if err == nil {
		t.Fatal("readJournalEntriesFromStdin accepted invalid JSON")
	}
	want := "decode JSONL at line 3:"
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error = %q, want substring %q", err.Error(), want)
	}
}

func TestReadJournalEntriesFromStdinMissingType(t *testing.T) {
	input := `{"event":"test"}` + "\n"
	entries, err := readJournalEntriesFromStdin(strings.NewReader(input), false)
	if err != nil {
		t.Fatalf("readJournalEntriesFromStdin: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0].Type != "" {
		t.Errorf("entries[0].Type = %q, want empty", entries[0].Type)
	}
}

func TestApplyDefaultJournalEntryTypes(t *testing.T) {
	entries := []journal.EntryInput{{Type: ""}, {Type: "task"}, {Type: ""}}
	applyDefaultJournalEntryTypes(entries, "note")
	if entries[0].Type != "note" {
		t.Errorf("entries[0].Type = %q, want note", entries[0].Type)
	}
	if entries[1].Type != "task" {
		t.Errorf("entries[1].Type = %q, want task", entries[1].Type)
	}
	if entries[2].Type != "note" {
		t.Errorf("entries[2].Type = %q, want note", entries[2].Type)
	}
}

func TestValidateJournalEntryTypes(t *testing.T) {
	tests := []struct {
		name       string
		entries    []journal.EntryInput
		jsonArray  bool
		wantErr    bool
		wantSubstr string
	}{
		{
			name:      "all entries have type",
			entries:   []journal.EntryInput{{Type: "note"}, {Type: "task"}},
			jsonArray: false,
			wantErr:   false,
		},
		{
			name:       "missing type JSONL",
			entries:    []journal.EntryInput{{Type: ""}},
			jsonArray:  false,
			wantErr:    true,
			wantSubstr: "provide --type/-t",
		},
		{
			name:       "missing type JSON array",
			entries:    []journal.EntryInput{{Type: ""}},
			jsonArray:  true,
			wantErr:    true,
			wantSubstr: "JSON array item",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateJournalEntryTypes(tt.entries, tt.jsonArray)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validateJournalEntryTypes: expected error")
				}
				if !strings.Contains(err.Error(), tt.wantSubstr) {
					t.Errorf("error = %q, want substring %q", err.Error(), tt.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Errorf("validateJournalEntryTypes: %v", err)
			}
		})
	}
}

func TestNormalizeHelpFlags(t *testing.T) {
	// Create a FlagSet matching journal append flags.
	fs := flag.NewFlagSet("journal append", flag.ContinueOnError)
	fs.String("type", "", "default entry type")
	fs.String("t", "", "default entry type")
	fs.String("source", "", "entry source")
	fs.String("idempotency-key", "", "append id")
	fs.Bool("json-array", false, "read JSON array")

	tests := []struct {
		input []string
		want  []string
	}{
		{[]string{"-h"}, []string{"-help"}},
		{[]string{"--help"}, []string{"--help"}},
		{[]string{"-type", "foo", "-h"}, []string{"-type", "foo", "-help"}},
		{[]string{"journal-id"}, []string{"journal-id"}},
		// -h as a value for a non-bool flag should NOT be rewritten.
		{[]string{"--type", "-h"}, []string{"--type", "-h"}},
		{[]string{"-type", "-h"}, []string{"-type", "-h"}},
		{[]string{"-t", "-h"}, []string{"-t", "-h"}},
		// Everything after -- should NOT be rewritten.
		{[]string{"--", "-h"}, []string{"--", "-h"}},
	}
	for _, tt := range tests {
		got := normalizeHelpFlags(tt.input, fs)
		if len(got) != len(tt.want) {
			t.Errorf("normalizeHelpFlags(%v) = %v, want %v", tt.input, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("normalizeHelpFlags(%v) = %v, want %v", tt.input, got, tt.want)
				break
			}
		}
	}
}
