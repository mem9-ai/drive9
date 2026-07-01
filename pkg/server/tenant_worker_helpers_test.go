package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

type staticSemanticEmbedder struct {
	vec []float32
	err error
}

func (e staticSemanticEmbedder) EmbedText(context.Context, string) ([]float32, error) {
	if e.err != nil {
		return nil, e.err
	}
	return append([]float32(nil), e.vec...), nil
}

type staticServerImageExtractor struct {
	text string
	err  error
}

func (e staticServerImageExtractor) ExtractImageText(_ context.Context, _ backend.ImageExtractRequest) (string, backend.ImageExtractUsage, error) {
	if e.err != nil {
		return "", backend.ImageExtractUsage{}, e.err
	}
	return e.text, backend.ImageExtractUsage{}, nil
}

func waitForTaskStatus(t *testing.T, b *backend.Dat9Backend, fileID string, version int64, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = ?`, fileID, version).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var status string
	if err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = ?`, fileID, version).Scan(&status); err != nil {
		t.Fatalf("wait task status query: %v", err)
	}
	t.Fatalf("task status=%q, want %q", status, want)
}

func waitForContentTextOnServer(t *testing.T, b *backend.Dat9Backend, path, wantSubstring string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nf, err := b.Store().Stat(context.Background(), path)
		if err == nil && nf.File != nil && strings.Contains(nf.File.ContentText, wantSubstring) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	nf, err := b.Store().Stat(context.Background(), path)
	if err != nil || nf.File == nil {
		t.Fatalf("stat %s while waiting for content text: %v", path, err)
	}
	t.Fatalf("content_text=%q, want substring %q", nf.File.ContentText, wantSubstring)
}

func loadSemanticTaskRowsForResource(t *testing.T, b *backend.Dat9Backend, resourceID string) []serverSemanticTaskRow {
	t.Helper()
	rows, err := b.Store().DB().Query(`SELECT task_type, status FROM semantic_tasks WHERE resource_id = ? ORDER BY created_at, task_id`, resourceID)
	if err != nil {
		t.Fatalf("query semantic tasks for %s: %v", resourceID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []serverSemanticTaskRow
	for rows.Next() {
		var row serverSemanticTaskRow
		if err := rows.Scan(&row.TaskType, &row.Status); err != nil {
			t.Fatalf("scan semantic task for %s: %v", resourceID, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate semantic tasks for %s: %v", resourceID, err)
	}
	return out
}

func mustServerFile(t *testing.T, b *backend.Dat9Backend, path string) *datastore.File {
	t.Helper()
	nf, err := b.Store().Stat(context.Background(), path)
	if err != nil || nf.File == nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return nf.File
}

type serverSemanticTaskRow struct {
	TaskType string
	Status   string
}