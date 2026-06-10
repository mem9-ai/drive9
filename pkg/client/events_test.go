package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseAndDispatchResetIncludesStructuralFields(t *testing.T) {
	data := `{"seq":7,"reason":"structural_change","path":"/old","op":"rename","actor":"actor1"}`

	var got *ResetEvent
	parseAndDispatch("reset", data, func(change *ChangeEvent, reset *ResetEvent) {
		if change != nil {
			t.Fatalf("change=%+v, want nil", change)
		}
		got = reset
	}, nil)

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

func TestParseAndDispatchHeartbeatMarksStreamCurrent(t *testing.T) {
	var gotSeq uint64
	var handlerCalled bool

	parseAndDispatch("heartbeat", `{"seq":42}`, func(*ChangeEvent, *ResetEvent) {
		handlerCalled = true
	}, func(seq uint64) {
		gotSeq = seq
	})

	if handlerCalled {
		t.Fatal("heartbeat should not dispatch a filesystem event")
	}
	if gotSeq != 42 {
		t.Fatalf("current seq = %d, want 42", gotSeq)
	}
}

func TestStreamEventsParsesMultilineDataAndComments(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(": comment\n"))
		_, _ = w.Write([]byte("event: file_changed\n"))
		_, _ = w.Write([]byte(`data: {"seq":9,` + "\n"))
		_, _ = w.Write([]byte(`data: "path":"/multi.txt","op":"write","ts":1}` + "\n\n"))
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	var got *ChangeEvent
	err := c.streamEvents(context.Background(), 0, "", func(change *ChangeEvent, reset *ResetEvent) {
		if reset != nil {
			t.Fatalf("reset=%+v, want nil", reset)
		}
		got = change
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("change event was not dispatched")
	}
	if got.Seq != 9 || got.Path != "/multi.txt" || got.Op != "write" {
		t.Fatalf("change=%+v, want multiline write event", got)
	}
}
