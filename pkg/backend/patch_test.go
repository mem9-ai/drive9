package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

func TestPatchAndAppendRejectDBBackedFilesWithSentinel(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()

	if _, err := b.Write("/small.txt", []byte("hello"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("Write: %v", err)
	}

	t.Run("append", func(t *testing.T) {
		_, err := b.InitiateAppendUploadIfRevision(ctx, "/small.txt", 1, s3client.PartSize, -1)
		if !errors.Is(err, ErrNotS3Stored) {
			t.Fatalf("InitiateAppendUploadIfRevision error = %v, want ErrNotS3Stored", err)
		}
	})

	t.Run("patch", func(t *testing.T) {
		_, err := b.InitiatePatchUploadIfRevision(ctx, "/small.txt", 6, []int{1}, s3client.PartSize, -1)
		if !errors.Is(err, ErrNotS3Stored) {
			t.Fatalf("InitiatePatchUploadIfRevision error = %v, want ErrNotS3Stored", err)
		}
	})
}
