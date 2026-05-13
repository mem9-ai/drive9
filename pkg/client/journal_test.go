package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/journal"
)

func TestJournalClientMethods(t *testing.T) {
	var sawAppendKey string
	var sawSearchMeta []string
	var sawSearchInclude string
	var sawSearchSince string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/journals":
			_ = json.NewEncoder(w).Encode(journal.Journal{JournalID: "jrn_client", Kind: "agent"})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/journals/jrn_client/entries":
			sawAppendKey = r.Header.Get("Idempotency-Key")
			_ = json.NewEncoder(w).Encode(journal.AppendResponse{JournalID: "jrn_client", AppendID: sawAppendKey, FirstSeq: 1, LastSeq: 1, Count: 1, HeadHash: "sha256:abc", Idempotent: true})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/journals/jrn_client/entries":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"journal_id":"jrn_client","seq":1,"entry_id":"jre_1","type":"tool.call.completed","schema_version":1,"occurred_at":"2026-05-12T00:00:00Z","observed_at":"2026-05-12T00:00:00Z","source":"self_reported","prev_hash":"sha256:gen","entry_hash":"sha256:abc"}` + "\n"))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/journal-entries":
			sawSearchMeta = append([]string(nil), r.URL.Query()["meta"]...)
			sawSearchInclude = r.URL.Query().Get("include")
			sawSearchSince = r.URL.Query().Get("since")
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"journal_id":"jrn_client","seq":1,"type":"tool.call.completed","cursor":"cur"}` + "\n"))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/journals/jrn_client/verify":
			_ = json.NewEncoder(w).Encode(journal.VerifyResult{OK: true, JournalID: "jrn_client", Entries: 1, HeadHash: "sha256:abc", HashChainOK: true})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	c := New(ts.URL, "api-key")
	ctx := context.Background()
	if got, err := c.CreateJournal(ctx, journal.CreateRequest{JournalID: "jrn_client"}); err != nil || got.JournalID != "jrn_client" {
		t.Fatalf("CreateJournal = %#v, %v", got, err)
	}
	if got, err := c.AppendJournalEntries(ctx, "jrn_client", "app_client", []journal.EntryInput{{Type: "tool.call.completed"}}); err != nil || got.AppendID != "app_client" {
		t.Fatalf("AppendJournalEntries = %#v, %v", got, err)
	}
	if sawAppendKey != "app_client" {
		t.Fatalf("Idempotency-Key = %q, want app_client", sawAppendKey)
	}
	if got, err := c.ReadJournalEntries(ctx, "jrn_client", 0, 10); err != nil || len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("ReadJournalEntries = %#v, %v", got, err)
	}
	if got, err := c.SearchJournal(ctx, journal.SearchRequest{Type: "tool.call.completed"}); err != nil || len(got) != 1 || got[0].Cursor == "" {
		t.Fatalf("SearchJournal = %#v, %v", got, err)
	}
	if got, err := c.SearchJournal(ctx, journal.SearchRequest{
		Labels: []journal.Label{{Key: "env", Value: "prod"}, {Key: "env", Value: "us-east"}},
	}); err != nil || len(got) != 1 {
		t.Fatalf("SearchJournal repeated labels = %#v, %v", got, err)
	}
	if len(sawSearchMeta) != 2 || sawSearchMeta[0] != "env=prod" || sawSearchMeta[1] != "env=us-east" {
		t.Fatalf("search meta query = %#v, want repeated env labels", sawSearchMeta)
	}
	if _, err := c.SearchJournal(ctx, journal.SearchRequest{
		Labels:  []journal.Label{{Key: "env", Value: "prod"}},
		Entries: true,
	}); err != nil {
		t.Fatalf("SearchJournal label entries: %v", err)
	}
	if sawSearchInclude != "entry" {
		t.Fatalf("search include = %q, want entry", sawSearchInclude)
	}
	absoluteSince := time.Date(2026, 5, 12, 1, 2, 3, 0, time.UTC)
	if _, err := c.SearchJournal(ctx, journal.SearchRequest{
		Labels:   []journal.Label{{Key: "env", Value: "prod"}},
		Since:    &absoluteSince,
		SinceRaw: "1h",
	}); err != nil {
		t.Fatalf("SearchJournal raw since: %v", err)
	}
	if sawSearchSince != "1h" {
		t.Fatalf("search since = %q, want raw relative bound", sawSearchSince)
	}
	if got, err := c.VerifyJournal(ctx, "jrn_client"); err != nil || !got.OK {
		t.Fatalf("VerifyJournal = %#v, %v", got, err)
	}
}
