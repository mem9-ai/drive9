package fuse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestInitialSyncExecutorKeepsOwnerAndDiscardsRejectedOutput(t *testing.T) {
	fs := &Dat9FS{}
	generation := fs.mountViewGeneration.Load()
	request := &initialSyncRequest{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	value, err := executeInitialSync(fs, ctx, request, generation, func(owner context.Context, fresh bool) (int, error) {
		calls++
		if owner != ctx || fresh != (calls == 2) {
			t.Fatalf("owner or fresh changed: call=%d fresh=%t", calls, fresh)
		}
		if !fresh {
			fs.resetMountViewWithInitialSync(true)
			return 99, fmt.Errorf("rejected: %w", fs.mountViewFailureFrom(generation))
		}
		if !request.retried {
			t.Fatal("budget was not consumed before fresh attempt")
		}
		return 7, nil
	})
	if err != nil || value != 7 || calls != 2 {
		t.Fatalf("value=%d err=%v calls=%d", value, err, calls)
	}
}

func TestInitialSyncExecutorExcludedFailures(t *testing.T) {
	for _, kind := range []string{"runtime", "second reset", "wrong owner", "real EAGAIN", "mixed cause", "canceled", "expired", "budget used"} {
		t.Run(kind, func(t *testing.T) {
			fs := &Dat9FS{}
			generation := fs.mountViewGeneration.Load()
			ctx, cancel := context.WithCancel(context.Background())
			if kind == "expired" {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			}
			defer cancel()
			request := &initialSyncRequest{retried: kind == "budget used"}
			calls := 0
			value, err := executeInitialSync(fs, ctx, request, generation, func(context.Context, bool) (int, error) {
				calls++
				if kind == "runtime" {
					fs.resetMountView()
				} else {
					fs.resetMountViewWithInitialSync(true)
				}
				cause := fs.mountViewFailureFrom(generation)
				switch kind {
				case "second reset":
					fs.resetMountView()
				case "wrong owner":
					cause = &mountViewFailure{generation: generation + 1, initial: true}
				case "real EAGAIN":
					cause = syscall.EAGAIN
				case "mixed cause":
					cause = errors.Join(cause, syscall.EACCES)
				case "canceled":
					cancel()
				}
				return 99, cause
			})
			if err == nil || calls != 1 {
				t.Fatalf("unexpected recovery: value=%d err=%v calls=%d", value, err, calls)
			}
			want := 0
			if kind == "real EAGAIN" || kind == "mixed cause" {
				want = 99 // semantic output accompanies a non-view error
			}
			if value != want {
				t.Fatalf("value=%d want=%d", value, want)
			}
		})
	}
}

func TestInitialSyncExecutorStageGenerationAndSharedBudget(t *testing.T) {
	fs := &Dat9FS{}
	request := &initialSyncRequest{}
	for stage := 0; stage < 2; stage++ {
		generation := fs.mountViewGeneration.Load()
		calls := 0
		_, err := executeInitialSync(fs, context.Background(), request, generation, func(context.Context, bool) (int, error) {
			calls++
			if stage == 0 {
				fs.resetMountView() // old layer rejection does not consume first budget
			} else if calls == 1 {
				fs.resetMountViewWithInitialSync(true)
			} else {
				return 1, nil
			}
			return 0, fs.mountViewFailureFrom(generation)
		})
		if stage == 0 && (err == nil || request.retried || calls != 1) {
			t.Fatalf("runtime stage err=%v retried=%t calls=%d", err, request.retried, calls)
		}
		if stage == 1 && (err != nil || !request.retried || calls != 2) {
			t.Fatalf("first stage err=%v retried=%t calls=%d", err, request.retried, calls)
		}
	}
}

func TestInitialSyncExecutorSuccessHandsOffExistingLock(t *testing.T) {
	fs := &Dat9FS{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := executeInitialSync(fs, ctx, &initialSyncRequest{}, 0, func(context.Context, bool) (int, error) {
		if err := fs.lockMountViewReadCause(0); err != nil {
			return 0, err
		}
		cancel() // accepted output remains terminal, including lock ownership
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if fs.mountViewMu.TryLock() {
		fs.mountViewMu.Unlock()
		t.Fatal("success lost the caller's publication lock")
	}
	fs.mountViewMu.RUnlock()
	if !fs.mountViewMu.TryLock() {
		t.Fatal("success added a second read lock")
	}
	fs.mountViewMu.Unlock()
}

func TestInitialSyncRecoveryDeadlineAndFlightIsolation(t *testing.T) {
	deadline := time.Now().Add(time.Second)
	ctx, cancel := capInitialSyncTimeout(context.Background(), time.Minute, deadline)
	defer cancel()
	if got, _ := ctx.Deadline(); !got.Equal(deadline) {
		t.Fatalf("recovery deadline=%s want=%s", got, deadline)
	}
	fs := &Dat9FS{}
	before := fs.initialSyncFlightKey("key", 0)
	fs.resetMountView() // runtime-only reset retains the old key
	if got := fs.initialSyncFlightKey("key", 1); got != before {
		t.Fatalf("runtime key changed: %s", got)
	}
	fs.resetMountViewWithInitialSync(true)
	after := fs.initialSyncFlightKey("key", 2)
	if after == before || fs.initialSyncFlightKey("key", 1) != before {
		t.Fatalf("first keys: before=%s after=%s", before, after)
	}
	fs.resetMountView()
	if fs.initialSyncFlightKey("key", 3) != after {
		t.Fatal("runtime reset added another flight partition")
	}
}

func TestInitialSyncRetainedReadPreservesSnapshotAndDirtyPriority(t *testing.T) {
	fs := &Dat9FS{}
	fh := &FileHandle{Path: "/gone", Unlinked: true, UnlinkedData: []byte("private"), Dirty: NewWriteBuffer("/gone", 0, 0), BaseRev: 7, DirtySeq: 9}
	input := &gofuse.ReadIn{Size: 7}
	for _, want := range []string{"private", "overlay"} {
		if want == "overlay" {
			if _, err := fh.Dirty.Write(0, []byte(want)); err != nil {
				t.Fatal(err)
			}
		}
		result, st, _, _, handled := fs.readRetainedSource(fh, input)
		if !handled || st != gofuse.OK {
			t.Fatalf("handled=%t status=%v", handled, st)
		}
		data, _ := result.Bytes(nil)
		if string(data) != want || fh.BaseRev != 7 || fh.DirtySeq != 9 {
			t.Fatalf("data=%q base=%d seq=%d", data, fh.BaseRev, fh.DirtySeq)
		}
	}
}

func TestInitialSyncMixedViewErrorKeepsConcreteHTTPStatus(t *testing.T) {
	cause := &mountViewFailure{generation: 1, initial: true}
	for _, test := range []struct {
		name string
		err  error
		want gofuse.Status
	}{
		{"pure view", cause, gofuse.EAGAIN},
		{"mixed forbidden", errors.Join(cause, &client.StatusError{StatusCode: http.StatusForbidden}), gofuse.EACCES},
		{"wrapped mixed not-found", fmt.Errorf("wrapped: %w", errors.Join(cause, &client.StatusError{StatusCode: http.StatusNotFound})), gofuse.ENOENT},
		{"mixed gateway", errors.Join(cause, &client.StatusError{StatusCode: http.StatusGatewayTimeout}), gofuse.EAGAIN},
		{"mixed real cancellation", errors.Join(cause, context.Canceled), gofuse.EAGAIN},
		{"ordinary network precedence", errors.Join(syscall.EAGAIN, &client.StatusError{StatusCode: http.StatusForbidden}), gofuse.EAGAIN},
	} {
		t.Run(test.name, func(t *testing.T) {
			if st := httpToFuseStatus(test.err); st != test.want {
				t.Fatalf("status=%v want=%v", st, test.want)
			}
		})
	}
}
