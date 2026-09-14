package extent

import "testing"

func TestCapJuiceCompactOrigin(t *testing.T) {
	if got := capJuiceCompactOrigin(nil); len(got) != 0 {
		t.Fatalf("nil origin len=%d", len(got))
	}
	small := make([]byte, 10*sliceBytes)
	if len(capJuiceCompactOrigin(small)) != len(small) {
		t.Fatal("short origin must be unchanged")
	}
	fat := make([]byte, (juiceMaxCompactSlices+7)*sliceBytes)
	got := capJuiceCompactOrigin(fat)
	if len(got) != juiceMaxCompactSlices*sliceBytes {
		t.Fatalf("capped slices=%d, want JuiceFS maxCompactSlices=%d", len(got)/sliceBytes, juiceMaxCompactSlices)
	}
}
