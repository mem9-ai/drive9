package client

import "testing"

func TestParseAndDispatchResetIncludesStructuralFields(t *testing.T) {
	data := `{"seq":7,"reason":"structural_change","path":"/old","op":"rename","actor":"actor1"}`

	var got *ResetEvent
	parseAndDispatch("reset", data, func(change *ChangeEvent, reset *ResetEvent) {
		if change != nil {
			t.Fatalf("change=%+v, want nil", change)
		}
		got = reset
	})

	if got == nil {
		t.Fatal("reset event was not dispatched")
	}
	if got.Seq != 7 {
		t.Errorf("Seq=%d, want 7", got.Seq)
	}
	if got.Reason != "structural_change" {
		t.Errorf("Reason=%q, want structural_change", got.Reason)
	}
	if got.Path != "/old" {
		t.Errorf("Path=%q, want /old", got.Path)
	}
	if got.Op != "rename" {
		t.Errorf("Op=%q, want rename", got.Op)
	}
	if got.Actor != "actor1" {
		t.Errorf("Actor=%q, want actor1", got.Actor)
	}
}
