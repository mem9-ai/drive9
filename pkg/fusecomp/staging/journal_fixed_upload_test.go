package staging

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalFixedUploadSurvivesPathReplacementAndCompaction(t *testing.T) {
	file := filepath.Join(t.TempDir(), "journal.wal")
	j, err := NewJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	first := []byte(`{"generation":1,"parts":["original"]}`)
	second := []byte(`{"generation":2,"parts":["replacement"]}`)
	mustAppend(t, j, JournalEntry{Op: JournalFixedUpload, Path: "/a", OperationID: "first", Meta: first})
	// These ordinary path operations must not settle an unknown remote result.
	mustAppend(t, j, JournalEntry{Op: JournalPendingMeta, Path: "/a", Meta: []byte(`{"path":"/a","generation":2}`)})
	mustAppend(t, j, JournalEntry{Op: JournalCommit, Path: "/a"})
	mustAppend(t, j, JournalEntry{Op: JournalUnlink, Path: "/a"})
	mustAppend(t, j, JournalEntry{Op: JournalFixedUpload, Path: "/a", OperationID: "second", Meta: second})
	mustAppend(t, j, JournalEntry{Op: JournalFixedUpload, Path: "/b", OperationID: "other", Meta: first})
	// Only the exact settled operation can be dropped, even at the same path.
	mustAppend(t, j, JournalEntry{Op: JournalFixedUploadDone, Path: "/a", OperationID: "second"})
	if err := j.FsyncShared(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := j.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = NewJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	var pending []JournalEntry
	if err := j.Replay(func(e JournalEntry) {
		if e.Op == JournalFixedUpload {
			pending = append(pending, e)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].OperationID != "first" || pending[0].Path != "/a" || !bytes.Equal(pending[0].Meta, first) || pending[1].OperationID != "other" {
		t.Fatalf("unresolved requests changed: %+v", pending)
	}
	mustAppend(t, j, JournalEntry{Op: JournalFixedUploadDone, Path: "/a", OperationID: "first"})
	if err := j.Compact(); err != nil {
		t.Fatal(err)
	}
	pending = nil
	if err := j.Replay(func(e JournalEntry) { pending = append(pending, e) }); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].OperationID != "other" {
		t.Fatalf("settlement crossed operation: %+v", pending)
	}
}

func TestJournalFixedUploadSurvivesFailedCompaction(t *testing.T) {
	j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	mustAppend(t, j, JournalEntry{Op: JournalFixedUpload, Path: "/a", OperationID: "pending", Meta: []byte(`{"parts":[]}`)})
	mustAppend(t, j, JournalEntry{Op: JournalCommit, Path: "/a"})
	if err := j.FsyncShared(); err != nil {
		t.Fatal(err)
	}
	// Force compaction's temporary file creation to fail, leaving the original WAL.
	if err := os.Mkdir(j.path+".compact", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(j.path+".compact", "block"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := j.Compact(); err == nil {
		t.Fatal("wanted compaction failure")
	}
	var found bool
	if err := j.Replay(func(e JournalEntry) {
		if e.Op == JournalFixedUpload && e.OperationID == "pending" {
			found = true
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("failed compaction lost completion")
	}
}

func TestJournalFixedUploadDurablePreparationAndSettlement(t *testing.T) {
	file := filepath.Join(t.TempDir(), "journal.wal")
	j, err := NewJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	payload := []byte(`{"upload":"upload","generation":1}`)
	for range 2 {
		if err := j.PrepareFixedUpload("original", "/a", payload); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := j.PendingFixedUploads()
	if err != nil || len(entries) != 1 || j.DurableSeq() < entries[0].Seq {
		t.Fatal(entries, err)
	}
	if err := j.PrepareFixedUpload("original", "/other", payload); err == nil {
		t.Fatal("identity changed path")
	}
	if err := j.PrepareFixedUpload("original", "/a", []byte(`{"generation":2}`)); err == nil {
		t.Fatal("identity changed body")
	}
	entries[0].Meta[0] = '!'
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = NewJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	entries, err = j.PendingFixedUploads()
	if err != nil || len(entries) != 1 || !bytes.Equal(entries[0].Meta, payload) {
		t.Fatal(entries, err)
	}
	for range 2 {
		if err := j.SettleFixedUpload("original"); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.PrepareFixedUpload("original", "/a", payload); err == nil {
		t.Fatal("settled identity reused")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = NewJournal(file)
	if err != nil {
		t.Fatal(err)
	}
	entries, err = j.PendingFixedUploads()
	if err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
	if err := j.Compact(); err != nil {
		t.Fatal(err)
	}
	entries, err = j.PendingFixedUploads()
	if err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
}

func TestJournalFixedUploadRejectsInvalidAndFailedPreparation(t *testing.T) {
	for _, failure := range []string{"id", "path", "json", "large", "closed", "append", "sync"} {
		t.Run(failure, func(t *testing.T) {
			j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = j.Close() })
			id, path, data := "original", "/a", []byte(`{"upload":"upload"}`)
			switch failure {
			case "id":
				id = "bad id"
			case "path":
				path = ""
			case "json":
				data = []byte("{")
			case "large":
				data = bytes.Repeat([]byte(" "), (64<<10)+1)
			case "closed":
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
			case "append":
				if err := j.fd.Close(); err != nil {
					t.Fatal(err)
				}
			case "sync":
				// A pipe accepts the frame, then rejects fsync. Preparation must
				// report failure without claiming a durable sequence.
				reader, writer, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				old := j.fd
				j.fd = writer
				t.Cleanup(func() { _ = reader.Close(); _ = writer.Close(); j.fd = old })
			}
			if err := j.PrepareFixedUpload(id, path, data); err == nil {
				t.Fatal("preparation failure returned success")
			}
			if j.DurableSeq() != 0 {
				t.Fatal("failed prepare advanced durability", j.DurableSeq())
			}
		})
	}
}

func TestJournalFixedUploadRejectsCorruptionAndWrongSettlement(t *testing.T) {
	for _, failure := range []string{"crc", "body", "settlement-path", "changed-body"} {
		t.Run(failure, func(t *testing.T) {
			j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = j.Close() })
			if err := j.PrepareFixedUpload("original", "/a", []byte(`{"generation":1}`)); err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "crc":
				raw, err := os.ReadFile(j.path)
				if err != nil {
					t.Fatal(err)
				}
				raw[len(raw)-1] ^= 1
				if err := os.WriteFile(j.path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "body":
				mustAppend(t, j, JournalEntry{Op: JournalFixedUpload, Path: "/b", OperationID: "other", Meta: []byte("{")})
			case "settlement-path":
				mustAppend(t, j, JournalEntry{Op: JournalFixedUploadDone, Path: "/other", OperationID: "original"})
			case "changed-body":
				mustAppend(t, j, JournalEntry{Op: JournalFixedUpload, Path: "/a", OperationID: "original", Meta: []byte(`{"generation":2}`)})
			}
			if _, err := j.PendingFixedUploads(); err == nil {
				t.Fatal("invalid recovery returned requests")
			}
			before, err := os.ReadFile(j.path)
			if err != nil {
				t.Fatal(err)
			}
			if err := j.Compact(); err == nil {
				t.Fatal("invalid recovery was compacted")
			}
			after, err := os.ReadFile(j.path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("failed compaction changed recovery source", err)
			}
		})
	}
}
