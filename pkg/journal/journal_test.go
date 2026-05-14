package journal

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNormalizeCreateRequestPreservesMetaAndRepeatedLabels(t *testing.T) {
	req, err := NormalizeCreateRequest(CreateRequest{
		JournalID: "jrn_labels",
		Meta:      map[string]string{"agent": "codex"},
		Labels: []Label{
			{Key: "env", Value: "prod"},
			{Key: "env", Value: "us-east"},
		},
	})
	if err != nil {
		t.Fatalf("NormalizeCreateRequest: %v", err)
	}
	if len(req.Labels) != 3 {
		t.Fatalf("labels = %#v, want 3 labels", req.Labels)
	}
	if req.Meta["agent"] != "codex" || req.Meta["env"] == "" {
		t.Fatalf("meta = %#v, want compatibility map populated from labels", req.Meta)
	}
	genesisRaw, err := MarshalCanonical(GenesisDocument("tenant", req, NormalizeTime(testTime())))
	if err != nil {
		t.Fatalf("MarshalCanonical genesis: %v", err)
	}
	labels, err := LabelsFromGenesis(genesisRaw)
	if err != nil {
		t.Fatalf("LabelsFromGenesis: %v", err)
	}
	if len(labels) != len(req.Labels) {
		t.Fatalf("genesis labels = %#v, want %#v", labels, req.Labels)
	}
}

func TestCanonicalJSONRawIsDeterministicForNestedObjects(t *testing.T) {
	rawA := json.RawMessage(`{
		"z": [{"b":2,"a":1}, true],
		"a": {"nested":{"d":4,"c":3}, "list":[{"y":null,"x":"ok"}]},
		"n": 1.0
	}`)
	rawB := json.RawMessage(`{"n":1.0,"a":{"list":[{"x":"ok","y":null}],"nested":{"c":3,"d":4}},"z":[{"a":1,"b":2},true]}`)
	want := `{"a":{"list":[{"x":"ok","y":null}],"nested":{"c":3,"d":4}},"n":1.0,"z":[{"a":1,"b":2},true]}`

	gotA, err := CanonicalJSONRaw(rawA)
	if err != nil {
		t.Fatalf("CanonicalJSONRaw A: %v", err)
	}
	gotB, err := CanonicalJSONRaw(rawB)
	if err != nil {
		t.Fatalf("CanonicalJSONRaw B: %v", err)
	}
	if string(gotA) != want {
		t.Fatalf("canonical A = %s, want %s", gotA, want)
	}
	if string(gotB) != want {
		t.Fatalf("canonical B = %s, want %s", gotB, want)
	}
	for i := 0; i < 20; i++ {
		got, err := CanonicalJSONRaw(rawA)
		if err != nil {
			t.Fatalf("CanonicalJSONRaw repeat %d: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("canonical repeat %d = %s, want %s", i, got, want)
		}
	}
}

func TestJournalHotPathLimits(t *testing.T) {
	labels := make([]Label, MaxLabelsPerJournal+1)
	for i := range labels {
		labels[i] = Label{Key: "k" + strings.Repeat("x", i%4), Value: string(rune('a' + i))}
	}
	if err := ValidateLabels(NormalizeLabels(labels)); err == nil {
		t.Fatal("ValidateLabels accepted too many labels")
	} else if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("ValidateLabels too many labels err = %v, want ErrPayloadTooLarge", err)
	}
	if err := ValidateLabels([]Label{{Key: "repo", Value: strings.Repeat("x", MaxLabelValueBytes+1)}}); err == nil {
		t.Fatal("ValidateLabels accepted an oversized label value")
	} else if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("ValidateLabels oversized value err = %v, want ErrPayloadTooLarge", err)
	}

	if _, err := NormalizeEntryInput(EntryInput{
		Type:    "tool.call.completed",
		Summary: []byte(`{"x":"` + strings.Repeat("a", MaxInlineSummaryBytes) + `"}`),
	}, "", nil); err == nil {
		t.Fatal("NormalizeEntryInput accepted an oversized inline summary")
	} else if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("NormalizeEntryInput oversized summary err = %v, want ErrPayloadTooLarge", err)
	}
	if _, err := NormalizeEntryInput(EntryInput{
		Type:     "tool.call.completed",
		Subjects: []string{"file:" + strings.Repeat("a", MaxSubjectValueBytes+1)},
	}, "", nil); err == nil {
		t.Fatal("NormalizeEntryInput accepted an oversized subject id")
	} else if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("NormalizeEntryInput oversized subject err = %v, want ErrPayloadTooLarge", err)
	}

	subjects := make([]string, MaxSubjectsPerEntry+1)
	for i := range subjects {
		subjects[i] = "file:/tmp/" + string(rune('a'+i/26)) + string(rune('a'+i%26))
	}
	if _, err := NormalizeEntryInput(EntryInput{Type: "tool.call.completed", Subjects: subjects}, "", nil); err == nil {
		t.Fatal("NormalizeEntryInput accepted too many subjects")
	} else if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("NormalizeEntryInput too many subjects err = %v, want ErrPayloadTooLarge", err)
	}

	entries := make([]EntryInput, 80)
	mediumSummary := []byte(`{"x":"` + strings.Repeat("a", 60<<10) + `"}`)
	for i := range entries {
		n, err := NormalizeEntryInput(EntryInput{Type: "tool.call.completed", Summary: mediumSummary}, "", nil)
		if err != nil {
			t.Fatalf("NormalizeEntryInput medium summary: %v", err)
		}
		entries[i] = n
	}
	if err := ValidateAppendBatch(entries); err == nil {
		t.Fatal("ValidateAppendBatch accepted an oversized batch")
	} else if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("ValidateAppendBatch oversized batch err = %v, want ErrPayloadTooLarge", err)
	}
}

func TestNormalizeLabelsPreservesMalformedKeysForValidation(t *testing.T) {
	labels := NormalizeLabels([]Label{{Key: "", Value: "prod"}})
	if len(labels) != 1 {
		t.Fatalf("NormalizeLabels dropped malformed label: %#v", labels)
	}
	if err := ValidateLabels(labels); err == nil {
		t.Fatal("ValidateLabels accepted an empty label key")
	}
}

func TestNormalizeEntryInputPreservesRequestedSource(t *testing.T) {
	entry, err := NormalizeEntryInput(EntryInput{
		Type:   "tool.call.completed",
		Source: SourceGateway,
	}, "", nil)
	if err != nil {
		t.Fatalf("NormalizeEntryInput gateway source: %v", err)
	}
	if entry.Source != SourceGateway {
		t.Fatalf("source = %q, want %q", entry.Source, SourceGateway)
	}
	if _, err := NormalizeEntryInput(EntryInput{
		Type:   "tool.call.completed",
		Source: "made_up",
	}, "", nil); err == nil {
		t.Fatal("NormalizeEntryInput accepted invalid source")
	}
}

func testTime() time.Time {
	return time.Date(2026, 5, 12, 1, 2, 3, 0, time.UTC)
}
