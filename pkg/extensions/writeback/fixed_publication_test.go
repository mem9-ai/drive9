package writeback

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func fixedPublicationFixture() fixedExtentPublication {
	return fixedExtentPublication{Version: 1, Binding: client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1}, Path: "/file", OperationID: "original", UploadID: "upload", Size: 6,
		PendingGeneration: 1, CacheGeneration: 1, Parts: []client.FixedExtentPart{{Number: 1, ETag: strings.Repeat("a", 64)}}}
}

func TestFixedPublicationPersistsBeforeSendAndReopensOriginalRequest(t *testing.T) {
	file := filepath.Join(t.TempDir(), "journal.wal")
	journal, err := staging.NewJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	original := fixedPublicationFixture()
	index, cache := fixedPublicationStores(t, original)
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/views/a/v2/uploads/upload/complete" || r.Header.Get(viewprotocol.OperationHeader) != "original" || r.Header.Get(viewprotocol.WorkHeader) != "work" {
			t.Error("publication changed identity or repeated transfer", r.Method, r.URL.Path)
		}
		var req struct{ Parts []client.FixedExtentPart }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Parts) != 1 || req.Parts[0] != original.Parts[0] {
			t.Error(req, err)
		}
		if calls.Add(1) == 1 {
			pending, err := journal.PendingFixedUploads()
			if err != nil || len(pending) != 1 || journal.DurableSeq() < pending[0].Seq {
				t.Error("publication preceded durable prepare", pending, err)
			}
			w.WriteHeader(503)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FixedUploadCompletion{Status: "completed", OperationID: "original", View: client.FixedViewState{LayerID: "a", Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 8, Size: 6})
	}))
	defer api.Close()
	c, _ := client.New(api.URL, "").WithView(original.Binding)
	if _, err := publishFixedExtent(context.Background(), c, journal, index, cache, original); err == nil {
		t.Fatal("unknown completion returned success")
	}
	// A later local image and ordinary cleanup cannot erase the old request.
	for _, entry := range []staging.JournalEntry{
		{Op: staging.JournalPendingMeta, Path: original.Path, Meta: []byte(`{"path":"/file","generation":2}`)},
		{Op: staging.JournalCommit, Path: original.Path},
	} {
		if err := journal.Append(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := staging.NewJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	fresh, _ := client.New(api.URL, "").WithView(original.Binding)
	pending, err := loadFixedExtentPublications(fresh, reopened)
	if err != nil || len(pending) != 1 || pending[0].PendingGeneration != 1 || pending[0].CacheGeneration != 1 {
		t.Fatal(pending, err)
	}
	receipt, err := pending[0].complete(context.Background(), fresh)
	if err != nil || receipt.Revision != 8 || calls.Load() != 2 {
		t.Fatal(receipt, err, calls.Load())
	}
	// Remote success does not settle local cleanup, nor authorize deleting the
	// newer local generation. The record remains until its caller settles it.
	if pending, err := loadFixedExtentPublications(fresh, reopened); err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
}

func TestFixedPublicationRejectsBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer api.Close()
	for _, failure := range []string{"no-journal", "closed-journal", "main", "work", "fence", "generation", "size", "parts", "format", "path", "no-index", "no-cache", "pending-changed", "cache-changed", "floor-save", "cache-floor-save"} {
		t.Run(failure, func(t *testing.T) {
			journal, err := staging.NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = journal.Close() }()
			p := fixedPublicationFixture()
			index, cache := fixedPublicationStores(t, p)
			originalIndex, originalCache := index, cache
			binding := p.Binding
			save := journal
			switch failure {
			case "no-journal":
				save = nil
			case "closed-journal":
				_ = journal.Close()
			case "main":
				binding = client.ViewBinding{}
			case "work":
				binding.WorkID = "other"
			case "fence":
				binding.Fence++
			case "generation":
				p.PendingGeneration, p.CacheGeneration = 0, 0
			case "size":
				p.Size = 0
			case "parts":
				p.Parts[0].Number = 2
			case "format":
				p.Version = 2
			case "path":
				p.Path = "/other/../file"
			case "no-index":
				index = nil
			case "no-cache":
				cache = nil
			case "pending-changed":
				if _, err := index.Put(p.Path, 7, staging.PendingNew); err != nil {
					t.Fatal(err)
				}
			case "cache-changed":
				if err := cache.Put(p.Path, []byte("changed"), 7, staging.PendingNew); err != nil {
					t.Fatal(err)
				}
			case "cache-floor-save":
				if err := os.Mkdir(filepath.Join(filepath.Dir(cache.MetaFile(p.Path)), "fixed-upload-generation"), 0700); err != nil {
					t.Fatal(err)
				}
			case "floor-save":
				if err := os.Mkdir(filepath.Join(index.Dir(), "fixed-upload-generation"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			c, _ := client.New(api.URL, "").WithView(binding)
			if result, err := publishFixedExtent(context.Background(), c, save, index, cache, p); err == nil || result.Revision != 0 {
				t.Fatal(result, err)
			}
			if _, ok := originalIndex.GetMeta("/file"); !ok {
				t.Fatal("rejected publication removed pending data")
			}
			if _, ok := originalCache.Get("/file"); !ok {
				t.Fatal("rejected publication removed cached data")
			}
			if failure != "closed-journal" {
				if pending, err := journal.PendingFixedUploads(); err != nil || len(pending) != 0 {
					t.Fatal("rejected publication prepared a request", pending, err)
				}
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("rejected publication reached server", calls.Load())
	}
}

func TestFixedPublicationWrongReceiptRetainsRecovery(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/views/a/v2/uploads/upload/complete" || r.Header.Get(viewprotocol.OperationHeader) != "original" {
			t.Error("recovery changed request identity", r.URL.Path)
		}
		var body struct{ Parts []client.FixedExtentPart }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Parts) != 1 || body.Parts[0] != fixedPublicationFixture().Parts[0] {
			t.Error(body, err)
		}
		size := int64(5)
		if calls.Add(1) > 1 {
			size = 6
		}
		_ = json.NewEncoder(w).Encode(client.FixedUploadCompletion{Status: "completed", OperationID: "original", View: client.FixedViewState{LayerID: "a", Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 8, Size: size})
	}))
	defer api.Close()
	journal, err := staging.NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	p := fixedPublicationFixture()
	index, cache := fixedPublicationStores(t, p)
	c, _ := client.New(api.URL, "").WithView(p.Binding)
	result, err := publishFixedExtent(context.Background(), c, journal, index, cache, p)
	var operation *client.ViewOperationError
	var completion *client.FixedExtentCompletionError
	if err == nil || result != (client.FixedUploadCompletion{}) || !client.IsCommitAttempted(err) || !errors.As(err, &operation) || operation.OperationID != p.OperationID || operation.LayerID != p.Binding.LayerID || !errors.As(err, &completion) {
		t.Fatal(result, err)
	}
	if pending, err := loadFixedExtentPublications(c, journal); err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	result, err = completion.Retry(context.Background())
	if err != nil || result.Size != 6 || result.Revision != 8 || calls.Load() != 2 {
		t.Fatal(result, err, calls.Load())
	}
	if pending, err := loadFixedExtentPublications(c, journal); err != nil || len(pending) != 1 {
		t.Fatal("remote retry prematurely settled local state", pending, err)
	}
}

func TestFixedPublicationRecoveryRejectsMismatchedRecords(t *testing.T) {
	for _, failure := range []string{"work", "outer-path", "outer-id", "format"} {
		t.Run(failure, func(t *testing.T) {
			journal, err := staging.NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = journal.Close() }()
			p := fixedPublicationFixture()
			c, _ := client.New("http://unused.invalid", "").WithView(p.Binding)
			id, name := p.OperationID, p.Path
			switch failure {
			case "work":
				p.Binding.WorkID = "other"
			case "outer-path":
				name = "/other"
			case "outer-id":
				id = "other"
			case "format":
				p.Version = 2
			}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.PrepareFixedUpload(id, name, raw); err != nil {
				t.Fatal(err)
			}
			if pending, err := loadFixedExtentPublications(c, journal); err == nil || pending != nil {
				t.Fatal(pending, err)
			}
		})
	}
}

func fixedPublicationStores(t *testing.T, p fixedExtentPublication) (*staging.PendingIndex, *staging.WriteBackCache) {
	t.Helper()
	index, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := index.Put(p.Path, p.Size, staging.PendingNew)
	if err != nil || gen != p.PendingGeneration {
		t.Fatal(gen, err)
	}
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(p.Path, []byte("source"), p.Size, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	if meta, ok := cache.GetMeta(p.Path); !ok || meta.Generation != p.CacheGeneration {
		t.Fatal(meta, ok)
	}
	return index, cache
}

// The original identity must survive metadata removal even BEFORE any cleanup
// has run. Otherwise a completion that finishes later could retire a new image
// created after an empty restart with the same numeric generation.
func TestFixedPublicationProtectsGenerationsBeforeCompletion(t *testing.T) {
	p := fixedPublicationFixture()
	index, cache := fixedPublicationStores(t, p)
	journal, err := staging.NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	priorIndex, priorCache := index, cache
	var requests atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(viewprotocol.OperationHeader) != p.OperationID {
			t.Error("changed completion identity")
		}
		if requests.Add(1) == 1 {
			// Simulate namespace removal while the result is unknown.
			priorIndex.Remove(p.Path)
			priorCache.Remove(p.Path)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FixedUploadCompletion{Status: "completed", OperationID: p.OperationID, View: client.FixedViewState{LayerID: p.Binding.LayerID, Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 8, Size: p.Size})
	}))
	defer api.Close()
	c, _ := client.New(api.URL, "").WithView(p.Binding)
	if _, err := publishFixedExtent(context.Background(), c, journal, index, cache, p); err == nil {
		t.Fatal("lost completion returned success")
	}
	index, err = staging.NewPendingIndex(index.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if err := index.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	fresh, err := index.Put(p.Path, 9, staging.PendingNew)
	if err != nil || fresh <= p.PendingGeneration {
		t.Fatal(fresh, err)
	}
	cache, err = staging.NewWriteBackCache(filepath.Dir(cache.MetaFile(p.Path)))
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(p.Path, []byte("new image"), 9, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	meta, _ := cache.GetMeta(p.Path)
	if meta.Generation <= p.CacheGeneration {
		t.Fatal(meta.Generation)
	}
	pending, err := loadFixedExtentPublications(c, journal)
	if err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	if receipt, err := pending[0].complete(context.Background(), c); err != nil || receipt.Revision != 8 || requests.Load() != 2 {
		t.Fatal(receipt, err, requests.Load())
	}
	if owned, err := index.RetireFixedUploadGeneration(p.Path, pending[0].PendingGeneration); err != nil || owned {
		t.Fatal(owned, err)
	}
	if owned, err := cache.RetireFixedUploadGeneration(p.Path, pending[0].CacheGeneration); err != nil || owned {
		t.Fatal(owned, err)
	}
	if meta, ok := index.GetMeta(p.Path); !ok || meta.Size != 9 {
		t.Fatal(meta, ok)
	}
	if data, ok := cache.Get(p.Path); !ok || string(data) != "new image" {
		t.Fatal(string(data), ok)
	}
}

func TestFixedPublicationSupportsEitherLocalStore(t *testing.T) {
	for _, kind := range []string{"pending", "cache"} {
		t.Run(kind, func(t *testing.T) {
			p := fixedPublicationFixture()
			index, cache := fixedPublicationStores(t, p)
			if kind == "pending" {
				p.CacheGeneration, cache = 0, nil
			} else {
				p.PendingGeneration, index = 0, nil
			}
			journal, err := staging.NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = journal.Close() }()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(client.FixedUploadCompletion{Status: "completed", OperationID: p.OperationID, View: client.FixedViewState{LayerID: p.Binding.LayerID, Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 8, Size: p.Size})
			}))
			defer api.Close()
			c, _ := client.New(api.URL, "").WithView(p.Binding)
			if receipt, err := publishFixedExtent(context.Background(), c, journal, index, cache, p); err != nil || receipt.Revision != 8 {
				t.Fatal(receipt, err)
			}
			if pending, err := loadFixedExtentPublications(c, journal); err != nil || len(pending) != 1 {
				t.Fatal(pending, err)
			}
		})
	}
}
