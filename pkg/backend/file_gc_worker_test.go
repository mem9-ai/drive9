package backend

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

type deleteRecordingS3Client struct {
	s3client.S3Client
	mu      sync.Mutex
	deletes []string
}

func (c *deleteRecordingS3Client) DeleteObject(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes = append(c.deletes, key)
	return nil
}

func (c *deleteRecordingS3Client) deletedKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deletes...)
}

func TestFileGCTaskEnqueuesOriginalStorageRefAfterPathRecreate(t *testing.T) {
	b := newTestBackend(t)
	rec := &deleteRecordingS3Client{S3Client: b.s3}
	b.s3 = rec
	fake := newFakeMetaQuotaStore()
	b.SetMetaQuotaStore(context.Background(), "tenant-a", fake)
	b.storageNamespaceID = "ns-a"

	ctx := context.Background()
	now := time.Now().UTC()
	if err := b.Store().InsertFile(ctx, &datastore.File{
		FileID:      "old-file",
		StorageType: datastore.StorageS3,
		StorageRef:  "blobs/old-file",
		ContentType: "text/plain",
		SizeBytes:   11,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().InsertNode(ctx, &datastore.FileNode{
		NodeID:     "node-old-file",
		Path:       "/x.txt",
		ParentPath: "/",
		Name:       "x.txt",
		FileID:     "old-file",
		CreatedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := b.RemoveCtx(ctx, "/x.txt"); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().InsertFile(ctx, &datastore.File{
		FileID:      "new-file",
		StorageType: datastore.StorageS3,
		StorageRef:  "blobs/new-file",
		ContentType: "text/plain",
		SizeBytes:   22,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().InsertNode(ctx, &datastore.FileNode{
		NodeID:     "node-new-file",
		Path:       "/x.txt",
		ParentPath: "/",
		Name:       "x.txt",
		FileID:     "new-file",
		CreatedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}

	processed, err := b.ProcessOneFileGCTask(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !processed {
		t.Fatal("expected one file gc task to be processed")
	}
	keys := rec.deletedKeys()
	if len(keys) != 0 {
		t.Fatalf("deleted keys = %#v, want none", keys)
	}
	if len(fake.objectGCCandidates) != 1 {
		t.Fatalf("object gc candidates = %d, want 1", len(fake.objectGCCandidates))
	}
	if got := fake.objectGCCandidates[0]; got.StorageRef != "blobs/old-file" || got.Reason != meta.ObjectGCReasonFileDelete {
		t.Fatalf("candidate = %+v", got)
	}
	nf, err := b.Store().Stat(ctx, "/x.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.File == nil || nf.File.FileID != "new-file" || nf.File.StorageRef != "blobs/new-file" {
		t.Fatalf("recreated path was changed: %+v", nf.File)
	}
	task, err := b.Store().GetFileGCTaskByFileID(ctx, "old-file")
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != datastore.FileGCTaskSucceeded {
		t.Fatalf("task status = %s, want succeeded", task.Status)
	}
}

func TestFileGCTaskEnqueuesObjectCandidateWhenNamespaceWired(t *testing.T) {
	b := newTestBackend(t)
	rec := &deleteRecordingS3Client{S3Client: b.s3}
	b.s3 = rec
	fake := newFakeMetaQuotaStore()
	b.SetMetaQuotaStore(context.Background(), "tenant-a", fake)
	b.storageNamespaceID = "ns-a"

	ctx := context.Background()
	now := time.Now().UTC()
	if err := b.Store().InsertFile(ctx, &datastore.File{
		FileID:      "old-file",
		StorageType: datastore.StorageS3,
		StorageRef:  "blobs/old-file",
		ContentType: "text/plain",
		SizeBytes:   11,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().InsertNode(ctx, &datastore.FileNode{
		NodeID:     "node-old-file",
		Path:       "/x.txt",
		ParentPath: "/",
		Name:       "x.txt",
		FileID:     "old-file",
		CreatedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveCtx(ctx, "/x.txt"); err != nil {
		t.Fatal(err)
	}

	processed, err := b.ProcessOneFileGCTask(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !processed {
		t.Fatal("expected one file gc task to be processed")
	}
	if keys := rec.deletedKeys(); len(keys) != 0 {
		t.Fatalf("deleted keys = %#v, want none", keys)
	}
	if len(fake.objectGCCandidates) != 1 {
		t.Fatalf("object gc candidates = %d, want 1", len(fake.objectGCCandidates))
	}
	got := fake.objectGCCandidates[0]
	if got.NamespaceID != "ns-a" || got.StorageRef != "blobs/old-file" || got.Reason != meta.ObjectGCReasonFileDelete {
		t.Fatalf("candidate = %+v", got)
	}
	if got.StorageRefHash != datastore.StorageRefHash("blobs/old-file") {
		t.Fatalf("candidate hash = %q", got.StorageRefHash)
	}
}

func TestOverwriteDoesNotDirectDeleteWhenObjectGCUnavailable(t *testing.T) {
	b := newTestBackend(t)
	rec := &deleteRecordingS3Client{S3Client: b.s3}
	b.s3 = rec

	ctx := context.Background()
	payload := bytes.Repeat([]byte("a"), int(DefaultInlineThreshold))
	if _, err := b.Write("/x.bin", payload, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("create s3 file: %v", err)
	}
	old, err := b.Store().Stat(ctx, "/x.bin")
	if err != nil {
		t.Fatalf("stat old file: %v", err)
	}
	if old.File.StorageType != datastore.StorageS3 || old.File.StorageRef == "" {
		t.Fatalf("old storage = %s %q, want s3 ref", old.File.StorageType, old.File.StorageRef)
	}

	replacement := bytes.Repeat([]byte("b"), int(DefaultInlineThreshold))
	if _, err := b.Write("/x.bin", replacement, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite s3 file: %v", err)
	}
	current, err := b.Store().Stat(ctx, "/x.bin")
	if err != nil {
		t.Fatalf("stat current file: %v", err)
	}
	if current.File.StorageRef == old.File.StorageRef {
		t.Fatalf("storage ref did not change: %q", current.File.StorageRef)
	}
	if keys := rec.deletedKeys(); len(keys) != 0 {
		t.Fatalf("deleted keys = %#v, want none", keys)
	}
}

func TestOverwriteDoesNotDirectDeleteWhenObjectGCCandidateEnqueueFails(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	rec := &deleteRecordingS3Client{S3Client: b.s3}
	b.s3 = rec
	b.storageNamespaceID = "ns-a"
	fake.objectGCCandidateErr = errors.New("meta unavailable")

	ctx := context.Background()
	payload := bytes.Repeat([]byte("a"), int(DefaultInlineThreshold))
	if _, err := b.Write("/x.bin", payload, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("create s3 file: %v", err)
	}
	old, err := b.Store().Stat(ctx, "/x.bin")
	if err != nil {
		t.Fatalf("stat old file: %v", err)
	}
	if old.File.StorageType != datastore.StorageS3 || old.File.StorageRef == "" {
		t.Fatalf("old storage = %s %q, want s3 ref", old.File.StorageType, old.File.StorageRef)
	}

	replacement := bytes.Repeat([]byte("b"), int(DefaultInlineThreshold))
	if _, err := b.Write("/x.bin", replacement, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite s3 file: %v", err)
	}
	if keys := rec.deletedKeys(); len(keys) != 0 {
		t.Fatalf("deleted keys = %#v, want none", keys)
	}
	if len(fake.objectGCCandidates) != 0 {
		t.Fatalf("object gc candidates = %d, want 0", len(fake.objectGCCandidates))
	}
}

func TestFileGCTaskDoesNotDirectDeleteWhenObjectGCUnavailable(t *testing.T) {
	b := newTestBackend(t)
	rec := &deleteRecordingS3Client{S3Client: b.s3}
	b.s3 = rec

	ctx := context.Background()
	now := time.Now().UTC()
	if err := b.Store().InsertFile(ctx, &datastore.File{
		FileID:      "old-file",
		StorageType: datastore.StorageS3,
		StorageRef:  "blobs/old-file",
		ContentType: "text/plain",
		SizeBytes:   11,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().InsertNode(ctx, &datastore.FileNode{
		NodeID:     "node-old-file",
		Path:       "/x.txt",
		ParentPath: "/",
		Name:       "x.txt",
		FileID:     "old-file",
		CreatedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveCtx(ctx, "/x.txt"); err != nil {
		t.Fatal(err)
	}

	processed, err := b.ProcessOneFileGCTask(ctx)
	if !processed {
		t.Fatal("expected one file gc task to be processed")
	}
	if err == nil {
		t.Fatal("expected missing object gc enqueue to keep gc task queued")
	}
	if keys := rec.deletedKeys(); len(keys) != 0 {
		t.Fatalf("deleted keys = %#v, want none", keys)
	}
	task, err := b.Store().GetFileGCTaskByFileID(ctx, "old-file")
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != datastore.FileGCTaskQueued {
		t.Fatalf("task status = %s, want queued", task.Status)
	}
}

func TestFileGCTaskRetriesWhenObjectCandidateEnqueueFails(t *testing.T) {
	b := newTestBackend(t)
	rec := &deleteRecordingS3Client{S3Client: b.s3}
	b.s3 = rec
	fake := newFakeMetaQuotaStore()
	enqueueErr := errors.New("meta unavailable")
	fake.objectGCCandidateErr = enqueueErr
	b.SetMetaQuotaStore(context.Background(), "tenant-a", fake)
	b.storageNamespaceID = "ns-a"

	ctx := context.Background()
	now := time.Now().UTC()
	if err := b.Store().InsertFile(ctx, &datastore.File{
		FileID:      "old-file",
		StorageType: datastore.StorageS3,
		StorageRef:  "blobs/old-file",
		ContentType: "text/plain",
		SizeBytes:   11,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().InsertNode(ctx, &datastore.FileNode{
		NodeID:     "node-old-file",
		Path:       "/x.txt",
		ParentPath: "/",
		Name:       "x.txt",
		FileID:     "old-file",
		CreatedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveCtx(ctx, "/x.txt"); err != nil {
		t.Fatal(err)
	}

	processed, err := b.ProcessOneFileGCTask(ctx)
	if !processed {
		t.Fatal("expected one file gc task to be processed")
	}
	if !errors.Is(err, enqueueErr) {
		t.Fatalf("process err = %v, want %v", err, enqueueErr)
	}
	if keys := rec.deletedKeys(); len(keys) != 0 {
		t.Fatalf("deleted keys = %#v, want none", keys)
	}
	if len(fake.objectGCCandidates) != 0 {
		t.Fatalf("object gc candidates = %d, want 0", len(fake.objectGCCandidates))
	}
	task, err := b.Store().GetFileGCTaskByFileID(ctx, "old-file")
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != datastore.FileGCTaskQueued {
		t.Fatalf("task status = %s, want queued", task.Status)
	}
}

func TestFileGCTaskReleasesCentralQuotaBeforeBlobRetry(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	enqueueErr := errors.New("meta temporarily unavailable")
	fake.objectGCCandidateErr = enqueueErr
	b.storageNamespaceID = "ns-a"

	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), int(DefaultInlineThreshold))
	if _, err := b.Write("/img.png", payload, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("create image: %v", err)
	}
	nf, err := b.Store().Stat(ctx, "/img.png")
	if err != nil {
		t.Fatalf("stat image: %v", err)
	}
	if nf.File.StorageType != datastore.StorageS3 {
		t.Fatalf("storage type = %s, want %s", nf.File.StorageType, datastore.StorageS3)
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != int64(len(payload)) || usage.FileCount != 1 || usage.MediaFileCount != 1 {
		t.Fatalf("usage after create = %+v", usage)
	}

	if err := b.RemoveCtx(ctx, "/img.png"); err != nil {
		t.Fatalf("remove image: %v", err)
	}
	processed, err := b.ProcessOneFileGCTask(ctx)
	if !processed {
		t.Fatal("expected one file gc task to be processed")
	}
	if !errors.Is(err, enqueueErr) {
		t.Fatalf("process err = %v, want %v", err, enqueueErr)
	}
	usage, err = fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != 0 || usage.FileCount != 0 || usage.MediaFileCount != 0 {
		t.Fatalf("usage after failed candidate enqueue = %+v", usage)
	}
	if _, err := fake.GetFileMeta(ctx, "tenant-a", nf.File.FileID); err == nil {
		t.Fatal("central file meta should be deleted before blob retry")
	}
	task, err := b.Store().GetFileGCTaskByFileID(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != datastore.FileGCTaskQueued || task.MaxAttempts != 0 {
		t.Fatalf("task after retry = %+v", task)
	}
}

func TestFileGCTaskWaitsForPendingCentralCreateMutation(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	b.storageNamespaceID = "ns-a"
	rec := &deleteRecordingS3Client{S3Client: b.s3}
	b.s3 = rec

	ctx := context.Background()
	now := time.Now().UTC()
	fileID := "pending-central-file"
	if err := b.Store().InsertFile(ctx, &datastore.File{
		FileID:      fileID,
		StorageType: datastore.StorageS3,
		StorageRef:  "blobs/pending-central-file",
		ContentType: "image/png",
		SizeBytes:   123,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().InsertNode(ctx, &datastore.FileNode{
		NodeID:     "node-pending-central-file",
		Path:       "/pending.png",
		ParentPath: "/",
		Name:       "pending.png",
		FileID:     fileID,
		CreatedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fake.InsertMutationLog(ctx, &MutationLogView{
		TenantID:     "tenant-a",
		MutationType: "file_create",
		MutationData: []byte(`{"file_id":"pending-central-file","size_bytes":123,"is_media":true}`),
	}); err != nil {
		t.Fatal(err)
	}

	if err := b.RemoveCtx(ctx, "/pending.png"); err != nil {
		t.Fatalf("remove pending file: %v", err)
	}
	opts := FileGCWorkerOptions{
		LeaseDuration: time.Second,
		RetryBase:     time.Nanosecond,
		RetryMax:      time.Nanosecond,
	}
	processed, err := b.processOneFileGCTask(ctx, opts)
	if !processed {
		t.Fatal("expected one file gc task to be processed")
	}
	if err == nil {
		t.Fatal("expected pending central mutation to keep gc task queued")
	}
	if keys := rec.deletedKeys(); len(keys) != 0 {
		t.Fatalf("deleted keys before central mutation replay = %#v", keys)
	}
	task, err := b.Store().GetFileGCTaskByFileID(ctx, fileID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != datastore.FileGCTaskQueued {
		t.Fatalf("task status after pending central mutation = %s, want queued", task.Status)
	}

	pending, err := fake.ListPendingMutations(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending mutation count = %d, want 1", len(pending))
	}
	replay := &MutationReplayWorker{store: fake}
	if err := replay.replayOne(ctx, pending[0]); err != nil {
		t.Fatalf("replay pending mutation: %v", err)
	}
	if _, err := b.Store().DB().ExecContext(ctx,
		`UPDATE file_gc_tasks SET available_at = ? WHERE file_id = ?`,
		time.Now().UTC().Add(-time.Second), fileID); err != nil {
		t.Fatal(err)
	}

	processed, err = b.ProcessOneFileGCTask(ctx)
	if err != nil {
		t.Fatalf("process gc after central replay: %v", err)
	}
	if !processed {
		t.Fatal("expected gc task after central replay")
	}
	if keys := rec.deletedKeys(); len(keys) != 0 {
		t.Fatalf("deleted keys after central replay = %#v, want none", keys)
	}
	if len(fake.objectGCCandidates) != 1 {
		t.Fatalf("object gc candidates after central replay = %d, want 1", len(fake.objectGCCandidates))
	}
	if got := fake.objectGCCandidates[0]; got.StorageRef != "blobs/pending-central-file" || got.Reason != meta.ObjectGCReasonFileDelete {
		t.Fatalf("candidate after central replay = %+v", got)
	}
	if _, err := fake.GetFileMeta(ctx, "tenant-a", fileID); err == nil {
		t.Fatal("central file meta should be deleted")
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != 0 || usage.FileCount != 0 || usage.MediaFileCount != 0 {
		t.Fatalf("usage after gc = %+v", usage)
	}
}

func TestFileGCTaskWaitsForPendingCentralOverwriteMutation(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	b.storageNamespaceID = "ns-a"
	rec := &deleteRecordingS3Client{S3Client: b.s3}
	b.s3 = rec

	ctx := context.Background()
	now := time.Now().UTC()
	fileID := "pending-overwrite-file"
	if err := b.Store().InsertFile(ctx, &datastore.File{
		FileID:      fileID,
		StorageType: datastore.StorageS3,
		StorageRef:  "blobs/pending-overwrite-file",
		ContentType: "image/png",
		SizeBytes:   123,
		Revision:    2,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().InsertNode(ctx, &datastore.FileNode{
		NodeID:     "node-pending-overwrite-file",
		Path:       "/pending-overwrite.png",
		ParentPath: "/",
		Name:       "pending-overwrite.png",
		FileID:     fileID,
		CreatedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := fake.UpsertFileMeta(ctx, &FileMetaView{
		TenantID:  "tenant-a",
		FileID:    fileID,
		SizeBytes: 50,
		IsMedia:   true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := fake.IncrStorageBytes(ctx, "tenant-a", 50); err != nil {
		t.Fatal(err)
	}
	if err := fake.IncrFileCount(ctx, "tenant-a", 1); err != nil {
		t.Fatal(err)
	}
	if err := fake.IncrMediaFileCount(ctx, "tenant-a", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := fake.InsertMutationLog(ctx, &MutationLogView{
		TenantID:     "tenant-a",
		MutationType: "file_overwrite",
		MutationData: []byte(`{"file_id":"pending-overwrite-file","old_size_bytes":50,"old_is_media":true,"new_size_bytes":123,"new_is_media":true}`),
	}); err != nil {
		t.Fatal(err)
	}

	if err := b.RemoveCtx(ctx, "/pending-overwrite.png"); err != nil {
		t.Fatalf("remove pending overwrite file: %v", err)
	}
	opts := FileGCWorkerOptions{
		LeaseDuration: time.Second,
		RetryBase:     time.Nanosecond,
		RetryMax:      time.Nanosecond,
	}
	processed, err := b.processOneFileGCTask(ctx, opts)
	if !processed {
		t.Fatal("expected one file gc task to be processed")
	}
	if err == nil {
		t.Fatal("expected pending overwrite mutation to keep gc task queued")
	}
	if keys := rec.deletedKeys(); len(keys) != 0 {
		t.Fatalf("deleted keys before central overwrite replay = %#v", keys)
	}
	fm, err := fake.GetFileMeta(ctx, "tenant-a", fileID)
	if err != nil {
		t.Fatal(err)
	}
	if fm.SizeBytes != 50 {
		t.Fatalf("central meta before replay = %+v, want old size", fm)
	}

	pending, err := fake.ListPendingMutations(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	replay := &MutationReplayWorker{store: fake}
	if err := replay.replayOne(ctx, pending[0]); err != nil {
		t.Fatalf("replay pending overwrite: %v", err)
	}
	if _, err := b.Store().DB().ExecContext(ctx,
		`UPDATE file_gc_tasks SET available_at = ? WHERE file_id = ?`,
		time.Now().UTC().Add(-time.Second), fileID); err != nil {
		t.Fatal(err)
	}

	processed, err = b.ProcessOneFileGCTask(ctx)
	if err != nil {
		t.Fatalf("process gc after central overwrite replay: %v", err)
	}
	if !processed {
		t.Fatal("expected gc task after central overwrite replay")
	}
	if keys := rec.deletedKeys(); len(keys) != 0 {
		t.Fatalf("deleted keys after central overwrite replay = %#v, want none", keys)
	}
	if len(fake.objectGCCandidates) != 1 {
		t.Fatalf("object gc candidates after central overwrite replay = %d, want 1", len(fake.objectGCCandidates))
	}
	if got := fake.objectGCCandidates[0]; got.StorageRef != "blobs/pending-overwrite-file" || got.Reason != meta.ObjectGCReasonFileDelete {
		t.Fatalf("candidate after central overwrite replay = %+v", got)
	}
	if _, err := fake.GetFileMeta(ctx, "tenant-a", fileID); err == nil {
		t.Fatal("central file meta should be deleted")
	}
	usage, err := fake.GetQuotaUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != 0 || usage.FileCount != 0 || usage.MediaFileCount != 0 {
		t.Fatalf("usage after gc = %+v", usage)
	}
}
