package treebuilder

import (
	"encoding/json"
	"testing"

	"github.com/mem9-ai/drive9/pkg/parser"
)

func TestEncodeRelations(t *testing.T) {
	data, err := EncodeRelations([]parser.Relation{
		{
			Target:      "/data/imagenet/",
			Type:        "derived_from",
			Description: "Training subset extracted from ImageNet",
		},
	})
	if err != nil {
		t.Fatalf("EncodeRelations() error = %v", err)
	}

	var got RelationsFile
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if len(got.Relations) != 1 {
		t.Fatalf("len(got.Relations) = %d, want 1", len(got.Relations))
	}

	if got.Relations[0].Target != "/data/imagenet/" {
		t.Fatalf("got target %q, want %q", got.Relations[0].Target, "/data/imagenet/")
	}

	if got.Relations[0].Type != "derived_from" {
		t.Fatalf("got type %q, want %q", got.Relations[0].Type, "derived_from")
	}
}
