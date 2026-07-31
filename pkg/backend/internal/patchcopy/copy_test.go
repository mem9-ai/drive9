package patchcopy

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

type testClient struct {
	mu sync.Mutex

	active    int
	maxActive int
	calls     []int
	ranges    map[int][2]int64

	started chan int
	release chan struct{}

	failPart int
	failErr  error

	abortCalls    int
	activeAtAbort int
	abortCtxErr   error
	abortErr      error
}

type latencyClient struct {
	delay time.Duration
}

func (c latencyClient) UploadPartCopy(
	ctx context.Context,
	_ string,
	_ string,
	_ int,
	_ string,
	_ int64,
	_ int64,
) (string, error) {
	timer := time.NewTimer(c.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return "etag", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (c *testClient) UploadPartCopy(
	ctx context.Context,
	_ string,
	_ string,
	partNumber int,
	_ string,
	startByte int64,
	endByte int64,
) (string, error) {
	c.mu.Lock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.calls = append(c.calls, partNumber)
	if c.ranges == nil {
		c.ranges = make(map[int][2]int64)
	}
	c.ranges[partNumber] = [2]int64{startByte, endByte}
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()

	if c.started != nil {
		c.started <- partNumber
	}
	if partNumber == c.failPart {
		return "", c.failErr
	}
	if c.release != nil {
		select {
		case <-c.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "etag", nil
}

func (c *testClient) AbortMultipartUpload(ctx context.Context, _ string, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.abortCalls++
	c.activeAtAbort = c.active
	c.abortCtxErr = ctx.Err()
	return c.abortErr
}

func tasks(count int) []Task {
	out := make([]Task, count)
	for i := range out {
		out[i] = Task{
			PartNumber: i + 1,
			StartByte:  int64(i * 10),
			EndByte:    int64(i*10 + 9),
		}
	}
	return out
}

func waitForStarts(t *testing.T, started <-chan int, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("started %d copies, want %d", i, count)
		}
	}
}

func TestCopyRunsWithBoundedConcurrency(t *testing.T) {
	const concurrency = 3
	client := &testClient{
		started: make(chan int, 10),
		release: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		done <- Copy(
			context.Background(),
			client,
			"dest",
			"upload",
			"source",
			tasks(10),
			concurrency,
		)
	}()

	waitForStarts(t, client.started, concurrency)
	select {
	case part := <-client.started:
		t.Fatalf("copy part %d started above concurrency limit %d", part, concurrency)
	default:
	}

	client.mu.Lock()
	maxActive := client.maxActive
	client.mu.Unlock()
	if maxActive != concurrency {
		t.Fatalf("max active copies = %d, want %d", maxActive, concurrency)
	}

	close(client.release)
	if err := <-done; err != nil {
		t.Fatalf("Copy: %v", err)
	}

	client.mu.Lock()
	calls := append([]int(nil), client.calls...)
	ranges := make(map[int][2]int64, len(client.ranges))
	for part, byteRange := range client.ranges {
		ranges[part] = byteRange
	}
	client.mu.Unlock()
	sort.Ints(calls)
	if len(calls) != 10 {
		t.Fatalf("copy calls = %v, want 10 parts", calls)
	}
	for i, part := range calls {
		if part != i+1 {
			t.Fatalf("copied parts = %v, want 1..10", calls)
		}
		wantRange := [2]int64{int64(i * 10), int64(i*10 + 9)}
		if ranges[part] != wantRange {
			t.Fatalf("part %d range = %v, want %v", part, ranges[part], wantRange)
		}
	}
}

func TestCopyCancelsWorkersOnFirstFailure(t *testing.T) {
	copyErr := errors.New("copy failed")
	client := &testClient{
		started:  make(chan int, 4),
		release:  make(chan struct{}),
		failPart: 2,
		failErr:  copyErr,
	}

	err := Copy(
		context.Background(),
		client,
		"dest",
		"upload",
		"source",
		tasks(4),
		2,
	)
	var partErr *PartError
	if !errors.As(err, &partErr) {
		t.Fatalf("Copy error = %v, want PartError", err)
	}
	if partErr.PartNumber != 2 {
		t.Fatalf("failed part = %d, want 2", partErr.PartNumber)
	}
	if !errors.Is(err, copyErr) {
		t.Fatalf("Copy error = %v, want wrapped copy error", err)
	}

	client.mu.Lock()
	calls := append([]int(nil), client.calls...)
	active := client.active
	client.mu.Unlock()
	sort.Ints(calls)
	if len(calls) > 2 {
		t.Fatalf("copy calls = %v, want no parts after the first worker batch", calls)
	}
	if len(calls) == 0 || calls[len(calls)-1] != 2 {
		t.Fatalf("copy calls = %v, want failed part 2", calls)
	}
	if active != 0 {
		t.Fatalf("active copies after return = %d, want 0", active)
	}
}

func TestCopyReturnsParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &testClient{
		started: make(chan int, 4),
		release: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		done <- Copy(
			ctx,
			client,
			"dest",
			"upload",
			"source",
			tasks(4),
			2,
		)
	}()

	waitForStarts(t, client.started, 2)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy error = %v, want context.Canceled", err)
	}

	client.mu.Lock()
	calls := append([]int(nil), client.calls...)
	active := client.active
	client.mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("copy calls = %v, want two in-flight copies only", calls)
	}
	if active != 0 {
		t.Fatalf("active copies after return = %d, want 0", active)
	}
}

func TestCopyRejectsInvalidConcurrency(t *testing.T) {
	err := Copy(
		context.Background(),
		&testClient{},
		"dest",
		"upload",
		"source",
		tasks(1),
		0,
	)
	if err == nil {
		t.Fatal("Copy error = nil, want invalid concurrency error")
	}
}

func TestCopyReturnsCancellationWithoutTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Copy(ctx, &testClient{}, "dest", "upload", "source", nil, 8)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy error = %v, want context.Canceled", err)
	}
}

func TestCopyOrAbortWaitsAndAbortsExactlyOnce(t *testing.T) {
	copyErr := errors.New("copy failed")
	client := &testClient{
		release:  make(chan struct{}),
		failPart: 2,
		failErr:  copyErr,
	}

	err := CopyOrAbort(
		context.Background(),
		client,
		"dest",
		"upload",
		"source",
		tasks(4),
		2,
		time.Second,
	)
	if !errors.Is(err, copyErr) {
		t.Fatalf("CopyOrAbort error = %v, want copy error", err)
	}

	client.mu.Lock()
	abortCalls := client.abortCalls
	activeAtAbort := client.activeAtAbort
	client.mu.Unlock()
	if abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", abortCalls)
	}
	if activeAtAbort != 0 {
		t.Fatalf("active copies when abort started = %d, want 0", activeAtAbort)
	}
}

func TestCopyOrAbortDoesNotAbortAfterSuccess(t *testing.T) {
	client := &testClient{}

	if err := CopyOrAbort(
		context.Background(),
		client,
		"dest",
		"upload",
		"source",
		tasks(4),
		2,
		time.Second,
	); err != nil {
		t.Fatalf("CopyOrAbort: %v", err)
	}

	client.mu.Lock()
	abortCalls := client.abortCalls
	client.mu.Unlock()
	if abortCalls != 0 {
		t.Fatalf("abort calls = %d, want 0", abortCalls)
	}
}

func TestCopyOrAbortUsesDetachedContextAfterParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &testClient{
		started: make(chan int, 2),
		release: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		done <- CopyOrAbort(
			ctx,
			client,
			"dest",
			"upload",
			"source",
			tasks(2),
			2,
			time.Second,
		)
	}()

	waitForStarts(t, client.started, 2)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("CopyOrAbort error = %v, want context.Canceled", err)
	}

	client.mu.Lock()
	abortCalls := client.abortCalls
	activeAtAbort := client.activeAtAbort
	abortCtxErr := client.abortCtxErr
	client.mu.Unlock()
	if abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", abortCalls)
	}
	if activeAtAbort != 0 {
		t.Fatalf("active copies when abort started = %d, want 0", activeAtAbort)
	}
	if abortCtxErr != nil {
		t.Fatalf("abort context error = %v, want nil", abortCtxErr)
	}
}

func TestCopyOrAbortReturnsAbortFailure(t *testing.T) {
	copyErr := errors.New("copy failed")
	abortErr := errors.New("abort failed")
	client := &testClient{
		failPart: 1,
		failErr:  copyErr,
		abortErr: abortErr,
	}

	err := CopyOrAbort(
		context.Background(),
		client,
		"dest",
		"upload",
		"source",
		tasks(1),
		1,
		time.Second,
	)
	if !errors.Is(err, copyErr) {
		t.Fatalf("CopyOrAbort error = %v, want copy error", err)
	}
	if !errors.Is(err, abortErr) {
		t.Fatalf("CopyOrAbort error = %v, want abort error", err)
	}
}

func TestAbortUsesDetachedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &testClient{}

	if err := Abort(ctx, client, "dest", "upload", time.Second); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	client.mu.Lock()
	abortCalls := client.abortCalls
	abortCtxErr := client.abortCtxErr
	client.mu.Unlock()
	if abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", abortCalls)
	}
	if abortCtxErr != nil {
		t.Fatalf("abort context error = %v, want nil", abortCtxErr)
	}
}

func TestAbortRejectsInvalidTimeout(t *testing.T) {
	err := Abort(context.Background(), &testClient{}, "dest", "upload", 0)
	if err == nil {
		t.Fatal("Abort error = nil, want invalid timeout error")
	}
}

func BenchmarkCopy(b *testing.B) {
	for _, concurrency := range []int{1, 8} {
		b.Run(fmt.Sprintf("concurrency_%d", concurrency), func(b *testing.B) {
			client := latencyClient{delay: time.Millisecond}
			copyTasks := tasks(36)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := Copy(
					context.Background(),
					client,
					"dest",
					"upload",
					"source",
					copyTasks,
					concurrency,
				); err != nil {
					b.Fatalf("Copy: %v", err)
				}
			}
		})
	}
}
