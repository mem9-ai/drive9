package treebuilder

import (
	"context"
	"encoding/json"

	"github.com/mem9-ai/drive9/pkg/parser"
)

// RelationsFileName is the advisory sidecar name reserved by the design doc.
const RelationsFileName = ".relations.json"

// Request carries the original source path plus the parser result that should
// be placed into the namespace.
type Request struct {
	SourcePath string
	MediaType  string
	Parsed     *parser.Result
	Metadata   map[string]string
}

// Artifact is a future write target produced by a TreeBuilder plan.
type Artifact struct {
	Path      string
	MediaType string
	Content   []byte
	Metadata  map[string]string
}

// Plan describes the namespace mutations needed after parsing.
// OriginalPath preserves the uploaded source file; Derived contains section
// files and any sidecars that should be written afterward.
type Plan struct {
	OriginalPath string
	Derived      []Artifact
	Relations    []parser.Relation
}

// RelationsFile matches the on-disk .relations.json sidecar format described in
// the design document.
type RelationsFile struct {
	Relations []parser.Relation `json:"relations"`
}

// EncodeRelations renders the advisory relation sidecar payload.
func EncodeRelations(relations []parser.Relation) ([]byte, error) {
	return json.MarshalIndent(RelationsFile{Relations: relations}, "", "  ")
}

// TreeBuilder decides where parsed artifacts should live in the dat9 namespace.
// It does not move bytes itself; callers apply the returned plan through the
// filesystem/backend layer.
type TreeBuilder interface {
	Build(ctx context.Context, req Request) (*Plan, error)
}
