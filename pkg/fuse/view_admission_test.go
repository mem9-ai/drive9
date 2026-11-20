package fuse

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestViewAdmissionPauseDrainsAndRejectsNewWork(t *testing.T) {
	var g viewAdmission
	first, err := g.enter()
	if err != nil {
		t.Fatal(err)
	}
	second, err := g.enter()
	if err != nil {
		t.Fatal(err)
	}
	p, err := g.pause()
	if err != nil {
		t.Fatal(err)
	}
	if leave, err := g.enter(); !errors.Is(err, errViewAdmissionPaused) || leave != nil {
		t.Fatalf("new admission: %v", err)
	}
	if _, err := g.pause(); !errors.Is(err, errViewAdmissionPaused) {
		t.Fatalf("second owner: %v", err)
	}
	first()
	first()
	select {
	case <-p.idle:
		t.Fatal("drained with operation active")
	default:
	}
	second()
	if err := p.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := g.enter(); !errors.Is(err, errViewAdmissionPaused) {
		t.Fatalf("idle reopened admission: %v", err)
	}
	if err := p.resume(); err != nil {
		t.Fatal(err)
	}
	leave, err := g.enter()
	if err != nil {
		t.Fatal(err)
	}
	leave()
}

func TestViewAdmissionCanceledWaitDoesNotResume(t *testing.T) {
	var g viewAdmission
	leave, err := g.enter()
	if err != nil {
		t.Fatal(err)
	}
	p, err := g.pause()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := g.enter(); !errors.Is(err, errViewAdmissionPaused) {
		t.Fatalf("canceled wait reopened: %v", err)
	}
	leave()
	if err := p.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.resume(); err != nil {
		t.Fatal(err)
	}
}

func TestViewAdmissionOldOwnerCannotResumeNewPause(t *testing.T) {
	var g viewAdmission
	leave, err := g.enter()
	if err != nil {
		t.Fatal(err)
	}
	old, err := g.pause()
	if err != nil {
		t.Fatal(err)
	}
	if err := old.resume(); err != nil {
		t.Fatal(err)
	} // pre-decision abort while old I/O finishes
	current, err := g.pause()
	if err != nil {
		t.Fatal(err)
	}
	if err := old.resume(); !errors.Is(err, errViewPauseLost) {
		t.Fatalf("old resume: %v", err)
	}
	if err := old.wait(context.Background()); !errors.Is(err, errViewPauseLost) {
		t.Fatalf("old wait: %v", err)
	}
	if _, err := g.enter(); !errors.Is(err, errViewAdmissionPaused) {
		t.Fatalf("successor reopened: %v", err)
	}
	select {
	case <-current.idle:
		t.Fatal("lost prior active operation")
	default:
	}
	leave()
	if err := current.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := current.resume(); err != nil {
		t.Fatal(err)
	}
}

func TestViewAdmissionConcurrentCompletion(t *testing.T) {
	var g viewAdmission
	var leaves []func()
	for range 64 {
		leave, err := g.enter()
		if err != nil {
			t.Fatal(err)
		}
		leaves = append(leaves, leave)
	}
	p, err := g.pause()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, leave := range leaves {
		for range 2 {
			wg.Add(1)
			go func() { defer wg.Done(); leave() }()
		}
	}
	wg.Wait()
	if err := p.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if g.active != 0 {
		t.Fatalf("active=%d", g.active)
	}
	if err := p.resume(); err != nil {
		t.Fatal(err)
	}
	next, err := g.pause()
	if err != nil {
		t.Fatal(err)
	}
	if err := next.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := next.resume(); err != nil {
		t.Fatal(err)
	}
}

func TestViewAdmissionCleanupAfterIdleObservation(t *testing.T) {
	var g viewAdmission
	p, err := g.pause()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	leave := g.enterCleanup()
	called := false
	if err := p.atIdle(context.Background(), func() error { called = true; return nil }); !errors.Is(err, errViewCallbacksActive) || called {
		t.Fatalf("late cleanup escaped barrier: called=%v err=%v", called, err)
	}
	select {
	case <-p.idle:
		t.Fatal("late cleanup reused closed idle notification")
	default:
	}
	leave()
	if err := p.atIdle(context.Background(), func() error { called = true; return nil }); err != nil || !called {
		t.Fatalf("idle decision: called=%v err=%v", called, err)
	}
	if _, err := g.enter(); !errors.Is(err, errViewAdmissionPaused) {
		t.Fatalf("decision resumed admission: %v", err)
	}
	if err := p.resume(); err != nil {
		t.Fatal(err)
	}
}

func TestViewAdmissionIdleDecisionFailureDoesNotResume(t *testing.T) {
	var g viewAdmission
	p, err := g.pause()
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("decision failed")
	if err := p.atIdle(context.Background(), func() error { return want }); !errors.Is(err, want) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if err := p.atIdle(ctx, func() error { called = true; return nil }); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("canceled decision: called=%v err=%v", called, err)
	}
	if _, err := g.enter(); !errors.Is(err, errViewAdmissionPaused) {
		t.Fatalf("failure resumed admission: %v", err)
	}
	if err := p.resume(); err != nil {
		t.Fatal(err)
	}
	next, err := g.pause()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.atIdle(context.Background(), func() error { called = true; return nil }); !errors.Is(err, errViewPauseLost) || called {
		t.Fatalf("stale decision: called=%v err=%v", called, err)
	}
	if err := next.resume(); err != nil {
		t.Fatal(err)
	}
}

func TestViewAdmissionWaitsForQueuedForgetAndCompleteCleanup(t *testing.T) {
	current := newXattrFS(t)
	adapter := &viewRequestFS{inner: current}
	ino := current.inodes.Lookup("/queued", false, 0, time.Time{})
	current.inodes.Lookup("/queued", false, 0, time.Time{})
	pause, err := adapter.admission.pause()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	observed := make(chan int, 3)
	done := make(chan error, 1)
	go func() {
		done <- pause.waitForCleanup(ctx, func() bool {
			n := current.nonRootKernelLookupCount()
			observed <- n
			return n == 0
		})
	}()
	if n := <-observed; n != 2 {
		t.Fatalf("initial refs=%d", n)
	}
	// No callback was active at the first check. A FORGET delivered later
	// must wake the waiter without resuming ordinary I/O or losing a wakeup.
	adapter.Forget(ino, 1)
	if n := <-observed; n != 1 {
		t.Fatalf("partial forget refs=%d", n)
	}
	select {
	case err := <-done:
		t.Fatalf("accepted remaining reference: %v", err)
	default:
	}
	// Even a zero lookup count cannot complete before the whole cleanup
	// callback has exited (it may still be changing other old-view state).
	leave := adapter.admission.enterCleanup()
	current.Forget(ino, 1)
	select {
	case err := <-done:
		t.Fatalf("accepted active cleanup: %v", err)
	default:
	}
	leave()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := <-observed; n != 0 {
		t.Fatalf("final refs=%d", n)
	}
	if _, err := adapter.admission.enter(); !errors.Is(err, errViewAdmissionPaused) {
		t.Fatalf("wait resumed ordinary I/O: %v", err)
	}
	if err := pause.resume(); err != nil {
		t.Fatal(err)
	}
}

func TestViewAdmissionCleanupWaitCancellationAndOwnership(t *testing.T) {
	for _, mode := range []string{"cancel", "resume"} {
		t.Run(mode, func(t *testing.T) {
			var gate viewAdmission
			pause, err := gate.pause()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			checked, done := make(chan struct{}), make(chan error, 1)
			go func() {
				done <- pause.waitForCleanup(ctx, func() bool { close(checked); return false })
			}()
			<-checked
			want := context.Canceled
			switch mode {
			case "cancel":
				cancel()
			case "resume":
				want = errViewPauseLost
				if err := pause.resume(); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-done; !errors.Is(err, want) {
				t.Fatalf("wait=%v want=%v", err, want)
			}
			if mode != "resume" {
				if _, err := gate.enter(); !errors.Is(err, errViewAdmissionPaused) {
					t.Fatal("expired wait reopened admission")
				}
				if err := pause.resume(); err != nil {
					t.Fatal(err)
				}
			}
			if err := pause.waitForCleanup(t.Context(), func() bool { t.Fatal("stale owner checked state"); return true }); !errors.Is(err, errViewPauseLost) {
				t.Fatal(err)
			}
		})
	}
}
