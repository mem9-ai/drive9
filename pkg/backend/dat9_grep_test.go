package backend

import (
	"testing"

	"github.com/mem9-ai/dat9/pkg/datastore"
)

func ptrFloat64(v float64) *float64 {
	return &v
}

func TestMergeVectorResults(t *testing.T) {
	cases := []struct {
		name string
		a, b []datastore.SearchResult
		want []datastore.SearchResult
	}{
		{
			name: "empty_both",
			a:    nil,
			b:    nil,
			want: nil,
		},
		{
			name: "empty_a",
			a:    nil,
			b: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.9)},
			},
			want: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.9)},
			},
		},
		{
			name: "empty_b",
			a: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.8)},
			},
			b: nil,
			want: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.8)},
			},
		},
		{
			name: "same_path_b_wins",
			a: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.7)},
			},
			b: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.9)},
			},
			want: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.9)},
			},
		},
		{
			name: "same_path_a_wins",
			a: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.9)},
			},
			b: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.7)},
			},
			want: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.9)},
			},
		},
		{
			name: "disjoint_sorted",
			a: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.8)},
				{Path: "/f2", Score: ptrFloat64(0.6)},
			},
			b: []datastore.SearchResult{
				{Path: "/f3", Score: ptrFloat64(0.9)},
				{Path: "/f4", Score: ptrFloat64(0.5)},
			},
			want: []datastore.SearchResult{
				{Path: "/f3", Score: ptrFloat64(0.9)},
				{Path: "/f1", Score: ptrFloat64(0.8)},
				{Path: "/f2", Score: ptrFloat64(0.6)},
				{Path: "/f4", Score: ptrFloat64(0.5)},
			},
		},
		{
			name: "nil_score_skipped",
			a: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.8)},
			},
			b: []datastore.SearchResult{
				{Path: "/f1", Score: nil},
			},
			want: []datastore.SearchResult{
				{Path: "/f1", Score: ptrFloat64(0.8)},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mergeVectorResults(tc.a, tc.b)
			if len(got) != len(tc.want) {
				t.Fatalf("len(got)=%d, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i].Path != tc.want[i].Path {
					t.Fatalf("got[%d].Path=%q, want %q", i, got[i].Path, tc.want[i].Path)
				}
				if got[i].Score == nil || tc.want[i].Score == nil {
					if got[i].Score != tc.want[i].Score {
						t.Fatalf("got[%d].Score nil mismatch", i)
					}
					continue
				}
				if *got[i].Score != *tc.want[i].Score {
					t.Fatalf("got[%d].Score=%v, want %v", i, *got[i].Score, *tc.want[i].Score)
				}
			}
		})
	}
}

func TestGrepMerge(t *testing.T) {
	b := &Dat9Backend{}

	cases := []struct {
		name        string
		ftsRows     []datastore.SearchResult
		vecRows     []datastore.SearchResult
		vecDescRows []datastore.SearchResult
		limit       int
		wantPaths   []string
	}{
		{
			name:      "fts_only",
			ftsRows:   []datastore.SearchResult{{Path: "/a", Score: ptrFloat64(0.9)}},
			limit:     10,
			wantPaths: []string{"/a"},
		},
		{
			name:      "content_vector_only",
			vecRows:   []datastore.SearchResult{{Path: "/b", Score: ptrFloat64(0.8)}},
			limit:     10,
			wantPaths: []string{"/b"},
		},
		{
			name:        "description_vector_only",
			vecDescRows: []datastore.SearchResult{{Path: "/c", Score: ptrFloat64(0.85)}},
			limit:       10,
			wantPaths:   []string{"/c"},
		},
		{
			name:        "both_vectors_same_path_a_wins",
			vecRows:     []datastore.SearchResult{{Path: "/d", Score: ptrFloat64(0.9)}},
			vecDescRows: []datastore.SearchResult{{Path: "/d", Score: ptrFloat64(0.7)}},
			limit:       10,
			wantPaths:   []string{"/d"},
		},
		{
			name:        "both_vectors_same_path_desc_wins",
			vecRows:     []datastore.SearchResult{{Path: "/e", Score: ptrFloat64(0.6)}},
			vecDescRows: []datastore.SearchResult{{Path: "/e", Score: ptrFloat64(0.95)}},
			limit:       10,
			wantPaths:   []string{"/e"},
		},
		{
			name:    "fts_and_vectors_rrf_merge",
			ftsRows: []datastore.SearchResult{{Path: "/f1", Score: ptrFloat64(0.9)}, {Path: "/f2", Score: ptrFloat64(0.7)}},
			vecRows: []datastore.SearchResult{{Path: "/f2", Score: ptrFloat64(0.85)}, {Path: "/f3", Score: ptrFloat64(0.8)}},
			limit:   10,
			// RRF: /f2 appears in both lists (rank 1 fts + rank 0 vec) → highest combined score
			wantPaths: []string{"/f2", "/f1", "/f3"},
		},
		{
			name:        "empty_all_fallback",
			ftsRows:     nil,
			vecRows:     nil,
			vecDescRows: nil,
			limit:       10,
			wantPaths:   nil,
		},
		{
			name:      "limit_respected",
			ftsRows:   []datastore.SearchResult{{Path: "/a", Score: ptrFloat64(0.9)}, {Path: "/b", Score: ptrFloat64(0.8)}, {Path: "/c", Score: ptrFloat64(0.7)}},
			limit:     2,
			wantPaths: []string{"/a", "/b"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := b.grepMerge(tc.ftsRows, tc.vecRows, tc.vecDescRows, tc.limit)
			if tc.wantPaths == nil {
				if got != nil {
					t.Fatalf("expected nil (fallback), got %v", got)
				}
				return
			}
			if len(got) != len(tc.wantPaths) {
				t.Fatalf("len(got)=%d, want %d", len(got), len(tc.wantPaths))
			}
			for i := range got {
				if got[i].Path != tc.wantPaths[i] {
					t.Fatalf("got[%d].Path=%q, want %q", i, got[i].Path, tc.wantPaths[i])
				}
			}
		})
	}
}
