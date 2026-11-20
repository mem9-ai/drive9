package server

import (
	"errors"
	"io"
	"net/http"
	"time"
)

// serverReadStallTimeout bounds how long one request may go without sending a byte of its
// body. The clock restarts on every read that returns data, so a sender that keeps making
// progress is allowed to finish however long that takes, while one that stops is cut off —
// the difference from a total http.Server ReadTimeout, which kills a large upload that is
// still arriving. It is deliberately generous: legitimate JSON and inline PUTs finish in
// milliseconds, and large uploads use presigned S3 URLs rather than this listener.
//
// It also seeds http.Server.ReadTimeout, which stays as the total bound for request bodies
// when per-request deadlines are unavailable (a ResponseWriter that cannot reach the
// connection, or HTTP/2 without the deadline callbacks): there the timeout is not renewable,
// so an S3-less deployment streaming a large inline PUT through this listener needs roughly
// maxUploadBytes/600 bytes per second sustained — about 17 MiB/s at the default 10 GiB
// maximum — and the constant has to be raised if that deployment shape is supported.
const serverReadStallTimeout = 10 * time.Minute

// readStallTimeout is the per-server override; tests use a short one. A non-positive value
// means the package default.
func (s *Server) readStallTimeout() time.Duration {
	if s.requestReadStall > 0 {
		return s.requestReadStall
	}
	return serverReadStallTimeout
}

// withReadStall puts an idle deadline on the request body that is pushed out every time bytes
// arrive. A body that never starts arriving hits the same bound as one that stops, so a
// stalled sender cannot hold a connection (and the memory behind a body that has not finished
// arriving) for longer than the bound. Requests without a body are returned untouched.
func (s *Server) withReadStall(w http.ResponseWriter, r *http.Request) *http.Request {
	if r.Body == nil || r.Body == http.NoBody {
		return r
	}
	stall := s.readStallTimeout()
	rc := http.NewResponseController(w)
	if err := rc.SetReadDeadline(time.Now().Add(stall)); err != nil {
		// This ResponseWriter cannot reach the connection's read deadline. Leave the request
		// alone: http.Server.ReadTimeout is the total bound that still applies.
		return r
	}
	r.Body = &readStallBody{body: r.Body, rc: rc, stall: stall}
	return r
}

// readStallBody renews the connection's read deadline as bytes arrive. Close (and the end of
// the body) clears the deadline so a kept-alive connection does not inherit this request's
// clock on its next one.
type readStallBody struct {
	body  io.ReadCloser
	rc    *http.ResponseController
	stall time.Duration
}

func (b *readStallBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if n > 0 {
		// Progress: the sender has earned another full window.
		_ = b.rc.SetReadDeadline(time.Now().Add(b.stall))
	}
	switch {
	case err == nil:
	case errors.Is(err, io.EOF):
		// The body finished cleanly: clear the deadline so a kept-alive connection does not
		// inherit this request's clock.
		_ = b.rc.SetReadDeadline(time.Time{})
	default:
		// The sender stalled or the connection broke. Expire the deadline now rather than
		// clearing it: net/http drains whatever is left of an unread body before it reuses or
		// closes the socket, and that drain must not block on a peer that has stopped sending.
		_ = b.rc.SetReadDeadline(time.Now())
	}
	return n, err
}

func (b *readStallBody) Close() error {
	err := b.body.Close()
	_ = b.rc.SetReadDeadline(time.Time{})
	return err
}
