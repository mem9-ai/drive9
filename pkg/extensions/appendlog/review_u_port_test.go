package appendlog

// Review round U ports: a reader serving another fd's pending image picks
// the object's NEWEST pending image (not an arbitrary dirty sibling), and
// waits for a busy dirty writer instead of silently regressing to the
// remote's pre-write bytes.

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
)

// Two O_APPEND writers append X then Y; a third, read-only fd must always
// see abcdefXY — never the superseded abcdefX of the earlier writer.
func TestPortReaderSeesNewestPendingImageAcrossAppenders(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	w1 := openFlags(t, ch, "/app.log", syscall.O_RDWR|syscall.O_APPEND)
	w2 := openFlags(t, ch, "/app.log", syscall.O_RDWR|syscall.O_APPEND)
	reader := openLog(t, ch, "/app.log")
	if err := w1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := w2.Write(ctx, 0, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if got := mustRead(t, reader, ctx, 0, 16); got != "abcdefXY" {
			t.Fatalf("read %d = %q, want abcdefXY (an older pending image was served)", i, got)
		}
	}
}

// rangeStart parses the start offset of a "bytes=start-end" Range header.
func rangeStart(h string) int64 {
	h = strings.TrimPrefix(h, "bytes=")
	if i := strings.IndexByte(h, '-'); i > 0 {
		h = h[:i]
	}
	n, err := strconv.ParseInt(h, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// A writer that confirmed X at offset 0 and is now blocked (under its own
// lock) loading the second 8 MiB part for a later write: a reader of the
// same object must wait for it and answer X, not skip the only dirty source
// and regress to the remote's A.
func TestPortReaderWaitsForBusyWriterInsteadOfRegressing(t *testing.T) {
	const size = 9 << 20
	b := testbackend.NewBackend()
	b.Seed("/big.log", bytes.Repeat([]byte("A"), size))
	gateHit := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	intercept := func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/big.log") && rangeStart(r.Header.Get("Range")) >= 8<<20 {
			once.Do(func() { gateHit <- struct{}{} })
			<-release
		}
		return false
	}
	ch := newWrappedStack(t, wrapBackend(b, intercept), "**/*.log")
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseGate) // a failing assertion must not leave the server handler parked
	ctx := context.Background()

	writer := openLog(t, ch, "/big.log")
	reader := openLog(t, ch, "/big.log")
	if err := writer.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	// The second write blocks in the part-2 load while holding the writer.
	writeDone := make(chan error, 1)
	go func() { writeDone <- writer.Write(ctx, 8<<20, []byte("Z")) }()
	select {
	case <-gateHit:
	case <-time.After(15 * time.Second):
		t.Fatal("the part-2 load never reached the gate")
	}
	readDone := make(chan string, 1)
	go func() {
		buf := make([]byte, 1)
		n, err := reader.Read(ctx, 0, buf)
		if err != nil {
			readDone <- "ERR:" + err.Error()
			return
		}
		readDone <- string(buf[:n])
	}()
	select {
	case got := <-readDone:
		t.Fatalf("read answered %q while the dirty writer was busy, want it to wait for the confirmed X", got)
	case <-time.After(500 * time.Millisecond):
	}
	releaseGate()
	if err := <-writeDone; err != nil {
		t.Fatalf("second write = %v", err)
	}
	select {
	case got := <-readDone:
		if got != "X" {
			t.Fatalf("read after the writer finished = %q, want X", got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the read never completed")
	}
}
