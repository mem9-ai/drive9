package cli

import "testing"

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
