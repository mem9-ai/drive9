package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// createOnceTestServer fakes a server whose block objects are create-once,
// the way GCS (signed if-generation-match: 0) and Azure (Create-only SAS)
// behave: a PUT to an address that is already taken is answered with
// conflictStatus instead of overwriting. Block URLs are keyed by checksum
// so repeated blocks and retried sessions collide exactly as they do
// against a real content-addressed store. Presign and PATCH mirror the
// server: a block whose object is present comes back skip_upload / exists
// — except for the first ignorePresence calls, which reproduce the race
// where the object lands between presign and PUT.
type createOnceTestServer struct {
	*httptest.Server
	mu sync.Mutex

	conflictStatus  int               // what a PUT to a taken address returns
	requiredHeaders map[string]string // PUT is rejected unless these are sent
	ignorePresence  int               // presign/PATCH calls that pretend nothing is stored yet

	objects      map[string][]byte
	putAttempts  []string // keys PUT was attempted on, in order
	patchCalls   int
	presignCalls int
	completes    []map[string]any
	aborted      []string
}

func newCreateOnceTestServer(t *testing.T, conflictStatus int, required map[string]string) *createOnceTestServer {
	t.Helper()
	ts := &createOnceTestServer{conflictStatus: conflictStatus, requiredHeaders: required, objects: map[string][]byte{}}
	mux := http.NewServeMux()

	mux.HandleFunc("/obj/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/obj/")
		if r.Method != http.MethodPut {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		ts.mu.Lock()
		defer ts.mu.Unlock()
		ts.putAttempts = append(ts.putAttempts, key)
		for k, v := range ts.requiredHeaders {
			if r.Header.Get(k) != v {
				http.Error(w, "missing signed header "+k, http.StatusBadRequest)
				return
			}
		}
		if _, taken := ts.objects[key]; taken {
			http.Error(w, "address already taken", ts.conflictStatus)
			return
		}
		ts.objects[key] = body
		w.WriteHeader(http.StatusCreated)
	})

	// present reports whether the object is stored, honoring ignorePresence.
	present := func(key string) bool {
		if ts.ignorePresence > 0 {
			return false
		}
		_, ok := ts.objects[key]
		return ok
	}
	headersCopy := func() map[string]string {
		out := make(map[string]string, len(ts.requiredHeaders))
		for k, v := range ts.requiredHeaders {
			out[k] = v
		}
		return out
	}

	mux.HandleFunc("/v1/fs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		ts.mu.Lock()
		defer ts.mu.Unlock()
		ts.patchCalls++
		dirty, _ := body["dirty_blocks"].([]any)
		blocks := make([]ExtentWritePlanBlock, 0, len(dirty))
		for i := range dirty {
			d := dirty[i].(map[string]any)
			sum := d["checksum"].(string)
			blk := ExtentWritePlanBlock{
				Chunk: uint32(d["chunk"].(float64)), Off: uint32(d["off"].(float64)),
				Size: int64(d["size"].(float64)), Checksum: sum,
			}
			if present(sum) {
				blk.Exists = true
			} else {
				blk.URL = ts.URL + "/obj/" + sum
				blk.Headers = headersCopy()
			}
			blocks = append(blocks, blk)
		}
		if ts.ignorePresence > 0 {
			ts.ignorePresence--
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(ExtentWritePlan{
			UploadID: fmt.Sprintf("patch-%d", ts.patchCalls), NewSize: int64(body["new_size"].(float64)), Blocks: blocks,
		})
	})

	mux.HandleFunc("/v2/uploads/initiate", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["layout"] != "extent" {
			http.Error(w, "expected layout=extent", http.StatusBadRequest)
			return
		}
		total := int64(body["total_size"].(float64))
		parts := int((total + (4 << 20) - 1) / (4 << 20))
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(uploadPlanV2{
			UploadID: "seq-1", PartSize: 4 << 20, TotalParts: parts,
			ChecksumContract: checksumContract{Supported: []string{"SHA-256"}, Required: true},
		})
	})

	mux.HandleFunc("/v2/uploads/", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		ts.mu.Lock()
		defer ts.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/presign-batch"):
			ts.presignCalls++
			parts, _ := body["parts"].([]any)
			out := make([]presignedPart, 0, len(parts))
			for i := range parts {
				m := parts[i].(map[string]any)
				n := int(m["part_number"].(float64))
				sum := m["checksum"].(map[string]any)["value"].(string)
				p := presignedPart{Number: n, ChecksumSHA256: sum}
				if present(sum) {
					p.SkipUpload = true
				} else {
					p.URL = ts.URL + "/obj/" + sum
					p.Headers = headersCopy()
				}
				out = append(out, p)
			}
			if ts.ignorePresence > 0 {
				ts.ignorePresence--
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": out})
		case strings.HasSuffix(r.URL.Path, "/complete"):
			ts.completes = append(ts.completes, body)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed"})
		default:
			// abort (best effort) and anything else.
			ts.aborted = append(ts.aborted, r.URL.Path)
			w.WriteHeader(http.StatusOK)
		}
	})

	ts.Server = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

// gcsLikeHeaders is what the server prescribes for a GCS block PUT: the
// signed create-once precondition. Azure prescribes x-ms-blob-type instead;
// both are opaque to the client, which must forward whatever it is handed.
var gcsLikeHeaders = map[string]string{"x-goog-if-generation-match": "0"}

// TestCommitExtentPatchSendsPrescribedHeaders proves the PATCH uploader
// forwards the plan's headers verbatim: the fake store rejects any block PUT
// that lacks the signed precondition, exactly as GCS fails the signature
// and Azure fails Put Blob without x-ms-blob-type.
func TestCommitExtentPatchSendsPrescribedHeaders(t *testing.T) {
	ts := newCreateOnceTestServer(t, http.StatusPreconditionFailed, gcsLikeHeaders)
	cl := New(ts.URL, "")
	data := []byte("patched bytes")
	if err := cl.CommitExtentPatch(context.Background(), "/f.bin", 64, []ExtentBlockWrite{{Chunk: 0, Off: 8, Data: data}}, 3); err != nil {
		t.Fatalf("CommitExtentPatch: %v", err)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if got := ts.objects[sha256Hex(data)]; !bytes.Equal(got, data) {
		t.Fatalf("stored block = %q, want the dirty bytes", got)
	}
	if ts.patchCalls != 1 || len(ts.completes) != 1 || len(ts.putAttempts) != 1 {
		t.Fatalf("patch=%d complete=%d puts=%d, want 1/1/1", ts.patchCalls, len(ts.completes), len(ts.putAttempts))
	}
}

// TestCommitExtentPatchReplansOnCreateOnceConflict reproduces the race the
// server cannot see at plan time: the block object lands (here: an earlier
// interrupted attempt) between the PATCH plan and the PUT, so the PUT is
// answered as a taken address. The client must re-plan — the server now
// reports the block exists — and complete, not abort.
func TestCommitExtentPatchReplansOnCreateOnceConflict(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"gcs-412", http.StatusPreconditionFailed},
		{"azure-403", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newCreateOnceTestServer(t, tc.status, gcsLikeHeaders)
			data := []byte("already landed by an interrupted attempt")
			ts.objects[sha256Hex(data)] = data
			ts.ignorePresence = 1 // the first plan does not know the object is there

			cl := New(ts.URL, "")
			if err := cl.CommitExtentPatch(context.Background(), "/f.bin", 4096, []ExtentBlockWrite{{Chunk: 0, Off: 0, Data: data}}, 1); err != nil {
				t.Fatalf("CommitExtentPatch after conflict: %v", err)
			}
			ts.mu.Lock()
			defer ts.mu.Unlock()
			if ts.patchCalls != 2 {
				t.Fatalf("PATCH calls = %d, want 2 (plan, conflict, re-plan)", ts.patchCalls)
			}
			if len(ts.putAttempts) != 1 {
				t.Fatalf("PUT attempts = %d, want exactly the one that collided", len(ts.putAttempts))
			}
			if len(ts.completes) != 1 {
				t.Fatalf("completes = %d, want the re-planned session completed once", len(ts.completes))
			}
			if len(ts.aborted) == 0 {
				t.Fatal("the superseded first session must be aborted best-effort")
			}
			if !bytes.Equal(ts.objects[sha256Hex(data)], data) {
				t.Fatal("the landed object must be untouched")
			}
		})
	}
}

// TestUploadExtentSequentialRepeatedBlockCreateOnce drives the real
// sequential uploader over a file that contains the same 4 MiB block twice,
// against a presign that (like a server without batch dedupe) hands out two
// URLs for the one address. The second PUT collides; the client must
// re-presign that part, learn the object is present, and complete with
// both parts referencing it.
func TestUploadExtentSequentialRepeatedBlockCreateOnce(t *testing.T) {
	ts := newCreateOnceTestServer(t, http.StatusPreconditionFailed, gcsLikeHeaders)
	ts.ignorePresence = 1 // the first presign-batch sees an empty store

	block := bytes.Repeat([]byte{0x77}, 4<<20)
	file := append(append([]byte{}, block...), block...)
	cl := New(ts.URL, "")
	if err := cl.WriteExtentStreamConditional(context.Background(), "/twice.bin", bytes.NewReader(file), int64(len(file)), nil, -1); err != nil {
		t.Fatalf("WriteExtentStreamConditional: %v", err)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	sum := sha256Hex(block)
	if len(ts.objects) != 1 || !bytes.Equal(ts.objects[sum], block) {
		t.Fatalf("store holds %d objects, want the single block", len(ts.objects))
	}
	if len(ts.putAttempts) != 2 || ts.putAttempts[0] != sum || ts.putAttempts[1] != sum {
		t.Fatalf("PUT attempts = %v, want one success and one collision on %s", ts.putAttempts, sum[:8])
	}
	if ts.presignCalls != 2 {
		t.Fatalf("presign calls = %d, want the batch plus one re-presign", ts.presignCalls)
	}
	if len(ts.completes) != 1 {
		t.Fatalf("completes = %d", len(ts.completes))
	}
	parts := ts.completes[0]["parts"].([]any)
	if len(parts) != 2 {
		t.Fatalf("complete parts = %d, want 2", len(parts))
	}
	for i, p := range parts {
		if etag := p.(map[string]any)["etag"]; etag != sum {
			t.Fatalf("part %d etag = %v, want %s", i+1, etag, sum[:8])
		}
	}
}

// TestUploadExtentSequentialResumesAfterInterruptedUpload is the retry of a
// session that landed its first block and died before completing: a fresh
// session is told nothing is stored (the race window), its first PUT is
// answered as a taken address — with Azure's 403, which also means "expired
// URL" — and the client must re-presign, skip the present block, upload the
// rest, and complete.
func TestUploadExtentSequentialResumesAfterInterruptedUpload(t *testing.T) {
	ts := newCreateOnceTestServer(t, http.StatusForbidden, map[string]string{"x-ms-blob-type": "BlockBlob"})
	first := bytes.Repeat([]byte{0x01}, 4<<20)
	second := bytes.Repeat([]byte{0x02}, 1<<20)
	ts.objects[sha256Hex(first)] = first // from the interrupted session
	ts.ignorePresence = 1

	file := append(append([]byte{}, first...), second...)
	cl := New(ts.URL, "")
	if err := cl.WriteExtentStreamConditional(context.Background(), "/resume.bin", bytes.NewReader(file), int64(len(file)), nil, -1); err != nil {
		t.Fatalf("WriteExtentStreamConditional: %v", err)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(ts.objects) != 2 || !bytes.Equal(ts.objects[sha256Hex(second)], second) {
		t.Fatalf("store holds %d objects, want both blocks", len(ts.objects))
	}
	if len(ts.putAttempts) != 2 || ts.putAttempts[0] != sha256Hex(first) || ts.putAttempts[1] != sha256Hex(second) {
		t.Fatalf("PUT attempts = %v, want [collision on first, success on second]", ts.putAttempts)
	}
	if ts.presignCalls != 2 || len(ts.completes) != 1 {
		t.Fatalf("presign=%d completes=%d, want 2/1", ts.presignCalls, len(ts.completes))
	}
}
