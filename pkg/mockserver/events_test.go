package mockserver

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	drive9client "github.com/mem9-ai/drive9/pkg/client"
)

func TestSSEStreamStaysConnectedAndDeliversChanges(t *testing.T) {
	s, ts, _ := newTestServer(t)
	key := provision(t, ts)

	changes := make(chan *drive9client.ChangeEvent, 8)
	c := drive9client.New(ts.URL, key)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connected := make(chan struct{}, 1)
	go func() {
		c.WatchEventsWithLifecycle(ctx, "actor-x", func(change *drive9client.ChangeEvent, reset *drive9client.ResetEvent) {
			if change != nil {
				select {
				case changes <- change:
				default:
				}
			}
		}, drive9client.EventLifecycle{
			OnCurrent: func(seq uint64) {
				select {
				case connected <- struct{}{}:
				default:
				}
			},
		})
	}()

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("client never reached a current seq (stream not established)")
	}

	// publish a change through the fs domain and expect delivery
	c2 := drive9client.New(ts.URL, key)
	if err := c2.Mkdir("/events-probe/"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	select {
	case ev := <-changes:
		if ev.Path != "/events-probe/" {
			t.Fatalf("event path = %q", ev.Path)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("file_changed event not delivered within 5s")
	}

	// the stream must still be alive: another change must also arrive
	if err := c2.Mkdir("/events-probe-2/"); err != nil {
		t.Fatalf("mkdir2: %v", err)
	}
	select {
	case <-changes:
	case <-time.After(5 * time.Second):
		t.Fatal("second event not delivered — stream dropped")
	}
	_ = httptest.NewServer(nil)
	_ = s
}
