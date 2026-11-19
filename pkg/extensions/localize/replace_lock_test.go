package localize

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// lockTestKeyWithBucket finds a view path whose name stripe lands in bucket want
// and whose key satisfies pred.
func lockTestKeyWithBucket(t *testing.T, o *Overlay, prefix string, want uint32, pred func(string) bool) string {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		view := prefix + strconv.Itoa(i) + ".bin"
		key := "path:" + view
		if o.nameStripeIndex(key) != want || (pred != nil && !pred(key)) {
			continue
		}
		return view
	}
	t.Fatalf("no path in %q lands in bucket %d", prefix, want)
	return ""
}

// TestReplaceLocksSameBucketPairDoesNotSelfDeadlock: two distinct names of ONE
// call can hash to the same stripe. The stripes are buckets, not keys, so the
// dedupe has to be by bucket — locking one non-reentrant mutex twice wedges the
// goroutine and, worse, keeps the stripe it already holds.
func TestReplaceLocksSameBucketPairDoesNotSelfDeadlock(t *testing.T) {
	o := &Overlay{}
	firstByBucket := make(map[uint32]string, len(o.nameLocks))
	var pair [2]string
	found := false
	for i := 0; i < 100_000 && !found; i++ {
		view := "/repo/node_modules/collide-" + strconv.Itoa(i) + ".bin"
		idx := o.nameStripeIndex("path:" + view)
		if prev, ok := firstByBucket[idx]; ok {
			pair = [2]string{prev, view}
			found = true
			break
		}
		firstByBucket[idx] = view
	}
	if !found {
		t.Fatal("no same-bucket pair found")
	}
	done := make(chan struct{})
	go func() {
		unlock := o.ReplaceLocks(pair[0], pair[1])
		unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("ReplaceLocks(%q, %q) self-deadlocked on one stripe", pair[0], pair[1])
	}
}

// TestReplaceLocksAcquireInBucketOrder: the acquisition order must be the
// BUCKET order, not the key order — otherwise two callers whose key pairs hash
// to reversed buckets take the same two mutexes in opposite orders (ABBA).
//
// The test pins the order structurally: hold the high bucket, start a call whose
// keys are lexicographically reversed, and probe the low bucket. In bucket order
// the call holds the low bucket while it waits for the high one, so the probe
// must block; in key order the call waits for the high bucket while holding
// nothing, and the probe would take the low bucket immediately.
func TestReplaceLocksAcquireInBucketOrder(t *testing.T) {
	o := &Overlay{}
	const lo, hi = uint32(5), uint32(9)
	// "zzz-" sorts after "aaa-", so the low bucket's key is the GREATER key.
	viewLo := lockTestKeyWithBucket(t, o, "/repo/node_modules/zzz-", lo, nil)
	viewHi := lockTestKeyWithBucket(t, o, "/repo/node_modules/aaa-", hi, nil)
	if ("path:" + viewLo) <= ("path:" + viewHi) {
		t.Fatalf("test keys are not reversed: %q vs %q", viewLo, viewHi)
	}
	if !strings.HasPrefix(viewLo, "/repo/node_modules/zzz-") {
		t.Fatalf("unexpected low-bucket key %q", viewLo)
	}

	o.nameLocks[hi].Lock()
	acquired := make(chan struct{})
	go func() {
		unlock := o.ReplaceLocks(viewLo, viewHi)
		close(acquired)
		unlock()
	}()
	// Probe the low bucket until a probe blocks. In bucket order the call takes
	// lo before it waits on hi, so once it has run every probe blocks; in key
	// order it waits on hi holding nothing, so every probe takes lo at once and
	// we fail. (A probe that wins before the call is scheduled proves nothing,
	// hence the retries rather than one shot.)
	blocked := false
	for attempt := 0; attempt < 20 && !blocked; attempt++ {
		probe := make(chan struct{})
		go func() {
			o.nameLocks[lo].Lock()
			close(probe)
			o.nameLocks[lo].Unlock()
		}()
		select {
		case <-probe:
			time.Sleep(50 * time.Millisecond)
		case <-time.After(100 * time.Millisecond):
			blocked = true
		}
	}
	o.nameLocks[hi].Unlock()
	if !blocked {
		t.Fatal("the low bucket was never held while the call waited on the high one: acquisition is not in bucket order")
	}
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("the call never completed after the high bucket was released")
	}
}
