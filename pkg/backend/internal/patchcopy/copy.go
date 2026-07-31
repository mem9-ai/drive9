// Package patchcopy runs retained multipart copies with bounded concurrency.
package patchcopy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Copier copies an object range into one multipart part.
type Copier interface {
	UploadPartCopy(ctx context.Context, destKey, uploadID string, partNumber int, sourceKey string, startByte, endByte int64) (string, error)
}

// Aborter aborts an incomplete multipart upload.
type Aborter interface {
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error
}

// Client supports both retained-part copies and multipart cleanup.
type Client interface {
	Copier
	Aborter
}

// Task describes one retained multipart range.
type Task struct {
	PartNumber int
	StartByte  int64
	EndByte    int64
}

// PartError identifies the retained part whose copy failed.
type PartError struct {
	PartNumber int
	Err        error
}

func (e *PartError) Error() string {
	return fmt.Sprintf("copy part %d: %v", e.PartNumber, e.Err)
}

func (e *PartError) Unwrap() error {
	return e.Err
}

// Copy runs every task with at most maxConcurrency in-flight requests.
func Copy(
	ctx context.Context,
	copier Copier,
	destKey string,
	uploadID string,
	sourceKey string,
	tasks []Task,
	maxConcurrency int,
) error {
	if maxConcurrency <= 0 {
		return fmt.Errorf("patch copy concurrency must be positive")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}

	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerCount := min(maxConcurrency, len(tasks))
	jobs := make(chan Task)

	var workers sync.WaitGroup
	var failureOnce sync.Once
	var failure *PartError
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for task := range jobs {
				if copyCtx.Err() != nil {
					return
				}
				if _, err := copier.UploadPartCopy(
					copyCtx,
					destKey,
					uploadID,
					task.PartNumber,
					sourceKey,
					task.StartByte,
					task.EndByte,
				); err != nil {
					failureOnce.Do(func() {
						failure = &PartError{PartNumber: task.PartNumber, Err: err}
						cancel()
					})
					return
				}
			}
		}()
	}

feed:
	for _, task := range tasks {
		select {
		case jobs <- task:
		case <-copyCtx.Done():
			break feed
		}
	}
	close(jobs)
	workers.Wait()

	if err := ctx.Err(); err != nil {
		return err
	}
	if failure != nil {
		return failure
	}
	return nil
}

// CopyOrAbort waits for all workers to stop, then aborts once after failure.
func CopyOrAbort(
	ctx context.Context,
	client Client,
	destKey string,
	uploadID string,
	sourceKey string,
	tasks []Task,
	maxConcurrency int,
	abortTimeout time.Duration,
) error {
	copyErr := Copy(ctx, client, destKey, uploadID, sourceKey, tasks, maxConcurrency)
	if copyErr == nil {
		return nil
	}
	if abortErr := Abort(ctx, client, destKey, uploadID, abortTimeout); abortErr != nil {
		return errors.Join(copyErr, fmt.Errorf("abort patch multipart upload: %w", abortErr))
	}
	return copyErr
}

// Abort uses a detached bounded context so caller cancellation cannot skip cleanup.
func Abort(ctx context.Context, aborter Aborter, key string, uploadID string, timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("patch abort timeout must be positive")
	}
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	return aborter.AbortMultipartUpload(abortCtx, key, uploadID)
}
