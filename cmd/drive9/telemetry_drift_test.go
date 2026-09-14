package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
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

// TestTelemetryKnownFlagNamesCoverEveryDefinedFlag keeps the closed flag-name
// allowlist in sync with the CLI. Telemetry records flag names from argv, so an
// unknown name must be dropped (it is user input); this test makes sure a real
// flag is never dropped silently — add it to telemetryKnownFlagNames instead.
//
// The set is derived from source because flags are declared in two styles: a
// flag.FlagSet per command, and hand-rolled parsers that compare argv tokens
// against string literals.
func TestTelemetryKnownFlagNamesCoverEveryDefinedFlag(t *testing.T) {
	defined := map[string]string{}
	for _, file := range telemetryFlagDefinitionFiles(t) {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				if name, ok := flagSetDefinitionName(node); ok {
					defined[name] = file
				}
			case *ast.CaseClause:
				for _, expr := range node.List {
					if name, ok := dashPrefixedLiteral(expr); ok {
						defined[name] = file
					}
				}
			case *ast.BinaryExpr:
				if node.Op != token.EQL && node.Op != token.NEQ {
					return true
				}
				for _, expr := range []ast.Expr{node.X, node.Y} {
					if name, ok := dashPrefixedLiteral(expr); ok {
						defined[name] = file
					}
				}
			}
			return true
		})
	}
	if len(defined) < 100 {
		t.Fatalf("only %d flag names were derived; the scan is broken", len(defined))
	}
	var missing, stale []string
	for name, file := range defined {
		if _, known := telemetryKnownFlagNames[name]; !known {
			missing = append(missing, name+" ("+file+")")
		}
	}
	for name := range telemetryKnownFlagNames {
		if _, ok := defined[name]; !ok {
			stale = append(stale, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 {
		t.Errorf("flags are defined but missing from telemetryKnownFlagNames (their names would be dropped): %s", strings.Join(missing, ", "))
	}
	if len(stale) > 0 {
		t.Errorf("telemetryKnownFlagNames lists names no flag defines any more (they could record user input): %s", strings.Join(stale, ", "))
	}
	for name := range telemetryKnownFlagNames {
		if !telemetryFlagNamePattern.MatchString(name) {
			t.Errorf("telemetryKnownFlagNames entry %q cannot match the runtime pattern", name)
		}
	}
}

// telemetryFlagDefinitionFiles lists the non-test Go sources that can declare a
// flag or parse one by hand.
func telemetryFlagDefinitionFiles(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	cliFiles, err := filepath.Glob("cli/*.go")
	if err != nil {
		t.Fatal(err)
	}
	all := append(files, cliFiles...)
	kept := make([]string, 0, len(all))
	for _, file := range all {
		if !strings.HasSuffix(file, "_test.go") {
			kept = append(kept, file)
		}
	}
	return kept
}

// flagSetDefinitionName recognizes a flag.FlagSet definition such as
// fs.String("json", false, "..."), fs.DurationVar(&d, "timeout", ...), or
// fs.Var(v, "tag", "..."), by method name, arity, and a variable receiver (a
// package-qualified call like zap.String("command", ...) is not a flag).
func flagSetDefinitionName(call *ast.CallExpr) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	if _, ok := selector.X.(*ast.Ident); !ok {
		return "", false
	}
	wantArgs, ok := telemetryFlagDefinitionMethods[selector.Sel.Name]
	if !ok || len(call.Args) != wantArgs {
		return "", false
	}
	nameIndex := 0
	if selector.Sel.Name == "Var" || strings.HasSuffix(selector.Sel.Name, "Var") {
		nameIndex = 1
	}
	literal, ok := call.Args[nameIndex].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		return "", false
	}
	return strings.ToLower(value), true
}

// telemetryFlagDefinitionMethods maps a flag-defining method to its arity.
var telemetryFlagDefinitionMethods = map[string]int{
	"String": 3, "Bool": 3, "Int": 3, "Int64": 3, "Uint": 3, "Uint64": 3,
	"Duration": 3, "Float64": 3, "Text": 3, "Var": 3, "Func": 2,
	"StringVar": 4, "BoolVar": 4, "IntVar": 4, "Int64Var": 4, "UintVar": 4,
	"Uint64Var": 4, "DurationVar": 4, "Float64Var": 4, "TextVar": 4,
}

// dashPrefixedLiteral normalizes a "-flag" / "--flag" / "--flag=value" string
// literal the same way telemetry does at runtime.
func dashPrefixedLiteral(expr ast.Expr) (string, bool) {
	literal, ok := expr.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil || !strings.HasPrefix(value, "-") {
		return "", false
	}
	name, _, _ := strings.Cut(strings.TrimLeft(value, "-"), "=")
	name = strings.ToLower(name)
	if !telemetryFlagNamePattern.MatchString(name) {
		return "", false
	}
	return name, true
}
