package viewprotocol

import (
	"errors"
	"net/http"
	"reflect"
	"testing"
)

func TestBindingRoundTripAndMainReset(t *testing.T) {
	for _, b := range []Binding{{}, {LayerID: "layer-A", WorkID: "work_1", Fence: 1}, {LayerID: "child", WorkID: "validation", Fence: 17, Validation: true}} {
		h := http.Header{ViewHeader: []string{"stale"}, "Authorization": []string{"test"}}
		if err := b.Apply(h); err != nil {
			t.Fatal(err)
		}
		got, err := Parse(h)
		if err != nil || got != b {
			t.Fatalf("round trip %+v %v", got, err)
		}
		if h.Get("Authorization") != "test" {
			t.Fatal("modified unrelated header")
		}
		if err := (Binding{}).Apply(h); err != nil {
			t.Fatal(err)
		}
		got, err = Parse(h)
		if err != nil || got != (Binding{}) {
			t.Fatalf("main reset %+v %v", got, err)
		}
	}
	if got, err := Parse(http.Header{ViewHeader: []string{"main"}}); err != nil || got != (Binding{}) {
		t.Fatalf("explicit main %+v %v", got, err)
	}
}

func TestBindingRejectsIncompleteAndAmbiguousHeaders(t *testing.T) {
	valid := http.Header{}
	_ = (Binding{LayerID: "a", WorkID: "w", Fence: 1}).Apply(valid)
	for _, tt := range []struct {
		key    string
		values []string
	}{
		{ViewHeader, nil}, {ViewHeader, []string{""}}, {ViewHeader, []string{"main"}}, {ViewHeader, []string{"MAIN"}}, {ViewHeader, []string{"a", "b"}}, {ViewHeader, []string{"a,b"}}, {ViewHeader, []string{"a/b"}},
		{WorkHeader, nil}, {WorkHeader, []string{"w", "x"}}, {WorkHeader, []string{"w\n"}},
		{FenceHeader, nil}, {FenceHeader, []string{"0"}}, {FenceHeader, []string{"-1"}}, {FenceHeader, []string{"+1"}}, {FenceHeader, []string{"01"}}, {FenceHeader, []string{"9223372036854775808"}},
		{ValidationHeader, []string{"1"}}, {ValidationHeader, []string{"TRUE"}}, {ValidationHeader, []string{"true", "false"}},
	} {
		h := valid.Clone()
		if tt.values == nil {
			h.Del(tt.key)
		} else {
			h[tt.key] = tt.values
		}
		if _, err := Parse(h); !errors.Is(err, ErrBinding) {
			t.Fatalf("accepted %s=%q: %v", tt.key, tt.values, err)
		}
	}
	for _, key := range []string{WorkHeader, FenceHeader, ValidationHeader} {
		if _, err := Parse(http.Header{key: []string{"1"}}); !errors.Is(err, ErrBinding) {
			t.Fatalf("orphan %s: %v", key, err)
		}
	}
}

func TestInvalidApplyLeavesHeadersUnchanged(t *testing.T) {
	for _, b := range []Binding{{WorkID: "w"}, {Validation: true}, {Fence: 1}, {LayerID: "main", WorkID: "w", Fence: 1}, {LayerID: "a", WorkID: "w", Fence: -1}, {LayerID: "a"}} {
		h := http.Header{"Authorization": []string{"test"}, ViewHeader: []string{"existing"}}
		before := h.Clone()
		if err := b.Apply(h); !errors.Is(err, ErrBinding) || !reflect.DeepEqual(h, before) {
			t.Fatalf("invalid apply %v %+v", err, h)
		}
	}
}
