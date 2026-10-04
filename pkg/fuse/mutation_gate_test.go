package fuse

import (
	"context"
	"errors"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type mutationGateTestFS struct {
	gofuse.RawFileSystem
	write   func()
	release func()
}

func (fs *mutationGateTestFS) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	fs.release()
}

func (fs *mutationGateTestFS) Write(
	cancel <-chan struct{},
	input *gofuse.WriteIn,
	data []byte,
) (uint32, gofuse.Status) {
	fs.write()
	return uint32(len(data)), gofuse.OK
}

func waitForGateQuiescing(t *testing.T, gate *workspaceMutationGate) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		quiescing := gate.quiescing
		gate.mu.Unlock()
		if quiescing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("mutation gate did not enter quiescing state")
}

func TestWorkspaceMutationGateOrdersInFlightAndLaterWritesAroundCheckpoint(t *testing.T) {
	gate := newWorkspaceMutationGate()
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	writes := 0
	fake := &mutationGateTestFS{
		RawFileSystem: gofuse.NewDefaultRawFileSystem(),
		write: func() {
			writes++
			if writes == 1 {
				close(firstEntered)
				<-releaseFirst
				return
			}
			close(secondEntered)
		},
	}
	fs := newGatedRawFileSystem(fake, gate)
	firstDone := make(chan struct{})
	go func() {
		_, _ = fs.Write(nil, &gofuse.WriteIn{}, []byte("before"))
		close(firstDone)
	}()
	<-firstEntered

	exclusive := make(chan func(), 1)
	quiesceErr := make(chan error, 1)
	go func() {
		release, err := gate.quiesce(context.Background())
		if err != nil {
			quiesceErr <- err
			return
		}
		exclusive <- release
	}()
	waitForGateQuiescing(t, gate)

	secondDone := make(chan struct{})
	go func() {
		_, _ = fs.Write(nil, &gofuse.WriteIn{}, []byte("after"))
		close(secondDone)
	}()
	select {
	case <-secondEntered:
		t.Fatal("write that started after quiesce entered the filesystem")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseFirst)
	<-firstDone
	var release func()
	select {
	case err := <-quiesceErr:
		t.Fatalf("quiesce: %v", err)
	case release = <-exclusive:
	case <-time.After(time.Second):
		t.Fatal("quiesce did not wait for the in-flight write")
	}
	select {
	case <-secondEntered:
		t.Fatal("later write entered while the checkpoint held exclusive quiescence")
	case <-time.After(20 * time.Millisecond):
	}

	release()
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("later write did not resume after checkpoint release")
	}
	<-secondDone
}

func TestWorkspaceMutationGateCancellationReleasesQuiesce(t *testing.T) {
	gate := newWorkspaceMutationGate()
	if !gate.enter(nil) {
		t.Fatal("initial mutation did not enter")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := gate.quiesce(ctx)
		done <- err
	}()
	waitForGateQuiescing(t, gate)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("quiesce error = %v, want context.Canceled", err)
	}

	if !gate.enter(nil) {
		t.Fatal("canceled quiesce left the gate closed")
	}
	gate.leave()
	gate.leave()
}

func TestGatedReleaseWaitsThroughCancellationInsteadOfBeingDropped(t *testing.T) {
	gate := newWorkspaceMutationGate()
	releaseBarrier, err := gate.quiesce(context.Background())
	if err != nil {
		t.Fatalf("quiesce: %v", err)
	}
	underlyingCalled := make(chan struct{})
	fake := &mutationGateTestFS{
		RawFileSystem: gofuse.NewDefaultRawFileSystem(),
		write:         func() {},
		release:       func() { close(underlyingCalled) },
	}
	fs := newGatedRawFileSystem(fake, gate)
	cancel := make(chan struct{})
	close(cancel)
	done := make(chan struct{})
	go func() {
		fs.Release(cancel, &gofuse.ReleaseIn{})
		close(done)
	}()
	select {
	case <-underlyingCalled:
		t.Fatal("release entered while checkpoint barrier was held")
	case <-time.After(20 * time.Millisecond):
	}

	releaseBarrier()
	select {
	case <-underlyingCalled:
	case <-time.After(time.Second):
		t.Fatal("release was dropped instead of running after the barrier")
	}
	<-done
}
