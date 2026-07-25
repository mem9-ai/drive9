package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestShutdownWaitsForInflightRequests pins the graceful-shutdown contract
// the server binaries rely on: Serve/ListenAndServe returns as soon as
// Shutdown closes the listener, but Shutdown itself returns only after
// in-flight handlers complete. The server mains join the shutdown goroutine
// before calling srv.Close() (final notify flush, worker teardown), so
// teardown cannot race a still-running handler.
func TestShutdownWaitsForInflightRequests(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	handlerStarted := make(chan struct{})
	finishHandler := make(chan struct{})
	s := &Server{}
	s.httpSrv = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		<-finishHandler
		w.WriteHeader(http.StatusNoContent)
	})}
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.httpSrv.Serve(ln) }()

	respErr := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String() + "/")
		if err == nil {
			_ = resp.Body.Close()
		}
		respErr <- err
	}()
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	shutdownReturned := make(chan struct{})
	go func() {
		if err := s.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		close(shutdownReturned)
	}()

	// The handler is still blocked: Shutdown must not have returned yet.
	select {
	case <-shutdownReturned:
		t.Fatal("Shutdown returned while a request was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	close(finishHandler)
	select {
	case <-shutdownReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after the in-flight request completed")
	}
	if err := <-respErr; err != nil {
		t.Fatalf("in-flight request failed during graceful shutdown: %v", err)
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("serve: %v", err)
	}
}
