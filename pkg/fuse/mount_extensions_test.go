package fuse

import (
	"reflect"
	"testing"
)

// TestParseExtensionOrder locks the --extensions contract: empty input is
// the default full set, the given order is preserved verbatim (the chain
// assembly reverses it), unknown names and duplicates are errors.
func TestParseExtensionOrder(t *testing.T) {
	got, err := parseExtensionOrder(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, defaultExtensionOrder) {
		t.Fatalf("empty input = %v, want default", got)
	}
	if len(defaultExtensionOrder) != 12 || defaultExtensionOrder[0] != "core" {
		t.Fatalf("default order = %v", defaultExtensionOrder)
	}

	got, err = parseExtensionOrder([]string{"core", "writeback", "cache"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"core", "writeback", "cache"}) {
		t.Fatalf("order not preserved: %v", got)
	}

	// sloppy spacing and trailing commas are tolerated
	got, err = parseExtensionOrder([]string{" core ", "", "sse"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"core", "sse"}) {
		t.Fatalf("trimmed = %v", got)
	}

	// "bulkfile" is an accepted deprecated no-op: pre-upgrade explicit
	// --extensions values and profiles that still name it must keep
	// mounting, on the chain without it.
	got, err = parseExtensionOrder([]string{"core", "writeback", "bulkfile", "cache"})
	if err != nil {
		t.Fatalf("pre-upgrade list with bulkfile rejected: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"core", "writeback", "cache"}) {
		t.Fatalf("pre-upgrade list dropped to %v, want the chain without bulkfile", got)
	}

	if _, err := parseExtensionOrder([]string{"core", "writebak"}); err == nil {
		t.Fatal("unknown extension accepted")
	}
	if _, err := parseExtensionOrder([]string{"core", "core"}); err == nil {
		t.Fatal("duplicate extension accepted")
	}
	if _, err := parseExtensionOrder([]string{"sse", "writeback"}); err == nil {
		t.Fatal("selection without core accepted")
	}
}

// TestReverseAssemblyOrder pins that the chain is assembled as the reverse
// of the flag order (outermost = last flag entry, core terminal first-flag).
func TestReverseAssemblyOrder(t *testing.T) {
	extOrder := []string{"core", "sse", "writeback"}
	var chain []string
	for i := len(extOrder) - 1; i >= 0; i-- {
		chain = append(chain, extOrder[i])
	}
	if !reflect.DeepEqual(chain, []string{"writeback", "sse", "core"}) {
		t.Fatalf("chain = %v", chain)
	}
}
