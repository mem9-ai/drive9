package writeback

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		PendingGeneration: 1, CacheGeneration: 7, Parts: []client.FixedExtentPart{{Number: 1, ETag: strings.Repeat("a", 64)}}}
}

func TestFixedPublicationPersistsBeforeSendAndReopensOriginalRequest(t *testing.T) {
	file := filepath.Join(t.TempDir(), "journal.wal")
	journal, err := staging.NewJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	original := fixedPublicationFixture()
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
	if _, err := publishFixedExtent(context.Background(), c, journal, original); err == nil {
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
	if err != nil || len(pending) != 1 || pending[0].PendingGeneration != 1 || pending[0].CacheGeneration != 7 {
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
	for _, failure := range []string{"no-journal", "closed-journal", "main", "work", "fence", "generation", "size", "parts", "format", "path"} {
		t.Run(failure, func(t *testing.T) {
			journal, err := staging.NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = journal.Close() }()
			p := fixedPublicationFixture()
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
			}
			c, _ := client.New(api.URL, "").WithView(binding)
			if result, err := publishFixedExtent(context.Background(), c, save, p); err == nil || result.Revision != 0 {
				t.Fatal(result, err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("rejected publication reached server", calls.Load())
	}
}

func TestFixedPublicationWrongReceiptRetainsRecovery(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(client.FixedUploadCompletion{Status: "completed", OperationID: "original", View: client.FixedViewState{LayerID: "a", Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 8, Size: 5})
	}))
	defer api.Close()
	journal, err := staging.NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	p := fixedPublicationFixture()
	c, _ := client.New(api.URL, "").WithView(p.Binding)
	if result, err := publishFixedExtent(context.Background(), c, journal, p); err == nil || result.Revision != 0 {
		t.Fatal(result, err)
	}
	if pending, err := loadFixedExtentPublications(c, journal); err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
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
