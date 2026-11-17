package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The revision a block plan represents is delivered in the response HEADER.
//
// The server writes setFileReadHeaders (ETag / X-Dat9-Resource-ID /
// X-Dat9-Revision) and then encodes the plan body, which carries only
// size/blocks/next_offset — there is no revision field in the JSON. A client
// that only decodes the body therefore sees Revision == 0 on every plan, and
// any freshness decision keyed on it silently accepts stale answers.
func TestGetExtentReadPlanReadsRevisionFromHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Dat9-Content-Layout", "extent")
		w.Header().Set("X-Dat9-Revision", "17")
		// No revision in the body, exactly as the server sends it.
		_ = json.NewEncoder(w).Encode(ExtentReadPlan{
			Size:   8192,
			Blocks: []ExtentReadPlanBlock{{FileOffset: 0, Length: 8192, Zero: true}},
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	plan, err := c.GetExtentReadPlan(context.Background(), "/t.db", 0, 8191)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Revision != 17 {
		t.Fatalf("plan.Revision = %d, want 17 from X-Dat9-Revision", plan.Revision)
	}
}

// A body that DOES carry a revision wins: the header is a fallback for the
// wire shape the server actually emits, not an override.
func TestGetExtentReadPlanBodyRevisionWins(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Dat9-Content-Layout", "extent")
		w.Header().Set("X-Dat9-Revision", "17")
		_ = json.NewEncoder(w).Encode(ExtentReadPlan{Size: 4096, Revision: 23})
	}))
	defer srv.Close()

	plan, err := New(srv.URL, "").GetExtentReadPlan(context.Background(), "/t.db", 0, 4095)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Revision != 23 {
		t.Fatalf("plan.Revision = %d, want the body's 23", plan.Revision)
	}
}

// A server that reports no revision at all leaves it zero; callers must treat
// that as "cannot be judged", not as revision 0.
func TestGetExtentReadPlanWithoutRevisionStaysZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Dat9-Content-Layout", "extent")
		_ = json.NewEncoder(w).Encode(ExtentReadPlan{Size: 4096})
	}))
	defer srv.Close()

	plan, err := New(srv.URL, "").GetExtentReadPlan(context.Background(), "/t.db", 0, 4095)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Revision != 0 {
		t.Fatalf("plan.Revision = %d, want 0 when the server reports none", plan.Revision)
	}
}
