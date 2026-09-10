package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strconv"
	"testing"
)

// telemetryDispatchSources maps each command dispatcher to its position in the
// telemetry tree. The tree is hand-maintained, so this test exists to make drift
// loud: adding a command or subcommand to a dispatcher without teaching
// telemetry about it fails here instead of silently dropping its events.
var telemetryDispatchSources = []struct {
	file     string
	function string
	path     []string
}{
	{file: "main.go", function: "dispatch"},
	{file: "main.go", function: "runFS", path: []string{"fs"}},
	{file: "cli/mount.go", function: "MountCmd", path: []string{"mount"}},
	{file: "cli/ctx.go", function: "Ctx", path: []string{"ctx"}},
	{file: "cli/secret.go", function: "Secret", path: []string{"vault"}},
	{file: "cli/token.go", function: "Token", path: []string{"token"}},
	{file: "cli/journal.go", function: "Journal", path: []string{"journal"}},
	{file: "cli/git.go", function: "Git", path: []string{"git"}},
	{file: "cli/git.go", function: "gitWorktree", path: []string{"git", "worktree"}},
	{file: "cli/region.go", function: "Region", path: []string{"region"}},
	{file: "cli/profile.go", function: "Profile", path: []string{"profile"}},
	{file: "cli/doctor.go", function: "runDoctor", path: []string{"doctor"}},
	{file: "cli/layer.go", function: "Layer", path: []string{"fs", "layer"}},
	{file: "cli/admin.go", function: "Admin", path: []string{"admin"}},
	{file: "cli/admin.go", function: "adminTenant", path: []string{"admin", "tenant"}},
	{file: "cli/admin.go", function: "adminTenantPool", path: []string{"admin", "tenant", "pool"}},
	{file: "cli/admin_object_backend.go", function: "adminObjectBackend", path: []string{"admin", "object-backend"}},
	{file: "cli/admin_extract_config.go", function: "adminTenantExtractConfig", path: []string{"admin", "tenant", "extract-config"}},
	{file: "cli/admin_embedding_config.go", function: "adminTenantEmbeddingConfig", path: []string{"admin", "tenant", "embedding-config"}},
	{file: "cli/admin_object_backend.go", function: "adminTenantObjectNamespace", path: []string{"admin", "tenant", "object-namespace"}},
}

// telemetryDispatcherCommandPattern matches command tokens only; flag labels
// ("--json"), modes, and platform names never look like this.
var telemetryDispatcherCommandPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

func TestTelemetryTreeCoversEveryDispatchedCommand(t *testing.T) {
	for _, source := range telemetryDispatchSources {
		tokens, err := dispatcherCaseLabels(source.file, source.function)
		if err != nil {
			t.Fatalf("%s: %v", source.file, err)
		}
		if len(tokens) == 0 {
			t.Fatalf("%s: dispatcher %s has no case labels; the test mapping is stale", source.file, source.function)
		}
		node := telemetryCommands
		for _, segment := range source.path {
			name, ok := node.childName(segment)
			if !ok {
				t.Fatalf("telemetry tree is missing the %q namespace", segment)
			}
			node = node.lookupChild(name)
		}
		for _, tok := range tokens {
			// help/version are namespace tokens: any token a namespace does not
			// know resolves to ineligible, so the tree does not list them.
			if tok == "help" || tok == "version" {
				continue
			}
			if _, ok := node.childName(tok); !ok {
				t.Errorf("telemetry tree %v is missing the %q command; add it to telemetryCommands or exclude it explicitly",
					append(append([]string{"drive9"}, source.path...), tok), tok)
			}
		}
	}
}

// dispatcherCaseLabels returns the string literals of every case clause in the
// named function.
func dispatcherCaseLabels(file, function string) ([]string, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		return nil, err
	}
	var labels []string
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != function || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			clause, ok := n.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expr := range clause.List {
				literal, ok := expr.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil || !telemetryDispatcherCommandPattern.MatchString(value) {
					continue
				}
				labels = append(labels, value)
			}
			return true
		})
	}
	return labels, nil
}
