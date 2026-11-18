package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

// TestExtentJointV2ServerSmoke runs the extent v2 wire flows against a LIVE
// drive9-server built from the v2 server redesign (tidbcloud/fs). Skipped
// unless DRIVE9_EXTENT_JOINT_BASE points at the server; DRIVE9_EXTENT_JOINT_KEY
// optionally reuses an existing tenant instead of provisioning one.
//
//	DRIVE9_EXTENT_JOINT_BASE=http://127.0.0.1:9410 \
//	  go test ./pkg/client/ -run TestExtentJointV2ServerSmoke -v -count=1
func TestExtentJointV2ServerSmoke(t *testing.T) {
	base := os.Getenv("DRIVE9_EXTENT_JOINT_BASE")
	if base == "" {
		t.Skip("DRIVE9_EXTENT_JOINT_BASE not set; joint smoke runs only against a live v2 server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	apiKey := os.Getenv("DRIVE9_EXTENT_JOINT_KEY")
	if apiKey == "" {
		apiKey = provisionJointTenant(t, ctx, base)
	}
	cl := New(base, apiKey)
	// Provisioning is asynchronous: poll until the tenant answers.
	for i := 0; i < 60; i++ {
		if _, err := cl.StatCtx(ctx, "/"); err == nil {
			break
		} else if i == 59 {
			t.Fatalf("tenant never became active: %v", err)
		}
		time.Sleep(time.Second)
	}

	// 1. Fresh large file through the extent sequential session.
	big := make([]byte, 5*(1<<20)+1234) // 5 MiB + tail: two 4 MiB parts, last short
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("/joint-extent/seq-%d.bin", time.Now().UnixNano())
	if err := cl.WriteExtentStreamConditional(ctx, path, bytesReaderAt(big), int64(len(big)), nil, -1); err != nil {
		t.Fatalf("extent sequential upload: %v", err)
	}
	st, err := cl.StatCtx(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if st.ContentLayout != ContentLayoutExtent {
		t.Fatalf("stat content_layout = %q, want extent (server without v2?)", st.ContentLayout)
	}
	if st.Size != int64(len(big)) {
		t.Fatalf("stat size = %d, want %d", st.Size, len(big))
	}

	// 2. Read the whole file back through the read plan.
	got, err := io.ReadAll(mustReadStream(t, ctx, cl, path))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(big) {
		t.Fatalf("roundtrip mismatch: %d bytes read, want %d", len(got), len(big))
	}

	// 3. Ranged read through the plan window.
	off := int64(3*(1<<20) + 77)
	lim := int64(1 << 20)
	rc, err := cl.ReadStreamRange(ctx, path, off, lim)
	if err != nil {
		t.Fatal(err)
	}
	window, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(window) != string(big[off:off+lim]) {
		t.Fatalf("ranged read mismatch (%d bytes)", len(window))
	}

	// 4. Incremental PATCH: overwrite a 64 KiB window, append a tail.
	patch := make([]byte, 64*(1<<10))
	if _, err := rand.Read(patch); err != nil {
		t.Fatal(err)
	}
	copy(big[2*(1<<20):], patch)
	appendTail := []byte("joint-append-tail")
	big = append(big, appendTail...)
	rev := st.Revision
	chunk, offv, _ := ChunkOffset(2 * (1 << 20))
	tailOff := int64(len(big) - len(appendTail))
	tailChunk, tailOffIn, _ := ChunkOffset(tailOff)
	if err := cl.CommitExtentPatch(ctx, path, int64(len(big)), []ExtentBlockWrite{
		{Chunk: chunk, Off: offv, Data: patch},
		{Chunk: tailChunk, Off: tailOffIn, Data: appendTail},
	}, rev); err != nil {
		t.Fatalf("extent patch: %v", err)
	}
	got, err = io.ReadAll(mustReadStream(t, ctx, cl, path))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(big) {
		t.Fatalf("post-patch roundtrip mismatch: %d bytes, want %d", len(got), len(big))
	}

	// 5. Classic sanity on the same tenant: a small file still lands inline.
	small := []byte("joint small file")
	if err := cl.WriteStream(ctx, "/joint-extent/small.txt", bytes.NewReader(small), int64(len(small)), nil); err != nil {
		t.Fatalf("classic small write: %v", err)
	}
	sst, err := cl.StatCtx(ctx, "/joint-extent/small.txt")
	if err != nil {
		t.Fatal(err)
	}
	if sst.ContentLayout == ContentLayoutExtent {
		t.Fatal("small file must stay classic")
	}
}

func mustReadStream(t *testing.T, ctx context.Context, cl *Client, path string) io.ReadCloser {
	t.Helper()
	rc, err := cl.ReadStream(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rc.Close() })
	return rc
}

func provisionJointTenant(t *testing.T, ctx context.Context, base string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/provision", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("provision: HTTP %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		APIKey   string `json:"api_key"`
		TenantID string `json:"tenant_id"`
		Status   string `json:"status"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.APIKey == "" {
		t.Fatalf("provision returned no api_key: %s", string(body))
	}
	_ = url.QueryEscape(out.TenantID)
	return out.APIKey
}
