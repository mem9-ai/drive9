package client

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// TestExtentServerAuditLive audits the v2 server's extent storage end to
// end against a live deployment: schema inventory (no v1 jfs_* tables,
// exactly the four extent layout tables), chunk/ref consistency, quota
// accounting (async, reservation-based) around create/patch/compact/
// delete, and the compaction + block-GC queues. All server-side effects
// are eventually consistent (post-commit quota mutations, the tenant
// worker's 5-minute maintenance tick), so every check polls.
//
//	DRIVE9_EXTENT_JOINT_BASE=http://127.0.0.1:9410 \
//	DRIVE9_EXTENT_JOINT_KEY=... \
//	DRIVE9_AUDIT_DSN='root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true' \
//	  go test ./pkg/client/ -run TestExtentServerAuditLive -v -count=1 -timeout 1200s
func TestExtentServerAuditLive(t *testing.T) {
	base := os.Getenv("DRIVE9_EXTENT_JOINT_BASE")
	if base == "" {
		t.Skip("DRIVE9_EXTENT_JOINT_BASE not set; server audit runs only against a live v2 server")
	}
	dsn := os.Getenv("DRIVE9_AUDIT_DSN")
	if dsn == "" {
		dsn = "root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true"
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("TiDB unreachable: %v", err)
	}

	ctx := context.Background()
	cl := New(base, os.Getenv("DRIVE9_EXTENT_JOINT_KEY"))
	for i := 0; i < 60; i++ {
		if _, err := cl.StatCtx(ctx, "/"); err == nil {
			break
		} else if i == 59 {
			t.Fatalf("tenant never became active: %v", err)
		}
		time.Sleep(time.Second)
	}
	tenantID, err := cl.GetMigrationTenantID(ctx)
	if err != nil {
		t.Fatalf("resolve tenant id: %v", err)
	}
	tenantDB := "drive9_" + strings.ReplaceAll(tenantID, "-", "")
	usage := func() int64 {
		t.Helper()
		var v sql.NullInt64
		if err := db.QueryRow(`SELECT storage_bytes FROM drive9_local.tenant_quota_usage WHERE tenant_id = ?`, tenantID).Scan(&v); err != nil {
			t.Fatalf("read quota usage: %v", err)
		}
		return v.Int64
	}
	waitUsage := func(from int64, want int64, what string) {
		t.Helper()
		for i := 0; i < 330; i++ {
			if usage()-from == want {
				t.Logf("quota %s: delta %d landed (async reservation accounting)", what, want)
				return
			}
			time.Sleep(time.Second)
		}
		t.Errorf("quota %s: usage moved %d, want delta %d", what, usage()-from, want)
	}

	// ---- step 1: schema inventory ----
	rows, err := db.Query("SHOW TABLES FROM `" + tenantDB + "`")
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var tb string
		_ = rows.Scan(&tb)
		tables = append(tables, tb)
	}
	_ = rows.Close()
	var jfs, extent []string
	for _, tb := range tables {
		if strings.HasPrefix(tb, "jfs_") || tb == "slice_compact_tasks" || tb == "block_gc_tasks" {
			jfs = append(jfs, tb)
		}
		if strings.HasPrefix(tb, "extent_") {
			extent = append(extent, tb)
		}
	}
	if len(jfs) != 0 {
		t.Errorf("v1 extent tables survived: %v", jfs)
	}
	for _, w := range []string{"extent_chunks", "extent_block_refs", "extent_block_gc", "extent_compact_tasks"} {
		found := false
		for _, e := range extent {
			if e == w {
				found = true
			}
		}
		if !found {
			t.Errorf("missing extent table %s", w)
		}
	}
	t.Logf("tenant %s: schema OK (extent tables %v, no v1 remnants)", tenantID, extent)

	// ---- step 2: create; quota delta (async) ----
	path := fmt.Sprintf("/audit-extent-%d.bin", time.Now().UnixNano())
	payload := make([]byte, 1<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	usage0 := usage()
	if err := cl.WriteExtentStreamConditional(ctx, path, bytesReaderAt(payload), int64(len(payload)), nil, -1); err != nil {
		t.Fatalf("create extent file: %v", err)
	}
	var inodeID string
	for i := 0; i < 30; i++ {
		if err := db.QueryRow("SELECT COALESCE(inode_id, file_id) FROM `"+tenantDB+"`.file_nodes WHERE path = ?", path).Scan(&inodeID); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if inodeID == "" {
		t.Fatal("file_nodes row not found for the audit file")
	}
	var sizeBytes int64
	if err := db.QueryRow("SELECT size_bytes FROM `"+tenantDB+"`.inodes WHERE inode_id = ?", inodeID).Scan(&sizeBytes); err != nil {
		t.Fatalf("inodes row: %v", err)
	}
	if sizeBytes != int64(len(payload)) {
		t.Errorf("inodes.size_bytes = %d, want %d", sizeBytes, len(payload))
	}
	waitUsage(usage0, int64(len(payload)), "create")
	checkRefs(t, db, tenantDB, inodeID)

	// ---- step 3: compaction (65+ slices on chunk 0) ----
	for i := 0; i < 70; i++ {
		st, err := cl.StatCtx(ctx, path)
		if err != nil {
			t.Fatalf("stat @patch %d: %v", i, err)
		}
		blk := make([]byte, 4096)
		blk[0] = byte(i)
		if _, err := rand.Read(blk[1:]); err != nil {
			t.Fatal(err)
		}
		chunk, offIn, _ := ChunkOffset(int64(i * 4096))
		if err := cl.CommitExtentPatch(ctx, path, int64(len(payload)), []ExtentBlockWrite{{Chunk: chunk, Off: offIn, Data: blk}}, st.Revision); err != nil {
			t.Fatalf("patch %d: %v", i, err)
		}
	}
	var compactQueued int
	if err := db.QueryRow("SELECT COUNT(*) FROM `"+tenantDB+"`.extent_compact_tasks WHERE inode_id = ?", inodeID).Scan(&compactQueued); err != nil {
		t.Fatal(err)
	}
	if compactQueued == 0 {
		t.Errorf("no compaction task enqueued after 70 slices")
	} else {
		t.Logf("compaction task enqueued; waiting for the worker (5m tick)")
	}
	completed := false
	for i := 0; i < 330 && !completed; i++ {
		var n int
		_ = db.QueryRow("SELECT COUNT(*) FROM `"+tenantDB+"`.extent_compact_tasks WHERE inode_id = ? AND status = 'COMPLETED'", inodeID).Scan(&n)
		if n > 0 {
			completed = true
			break
		}
		time.Sleep(time.Second)
	}
	if !completed {
		t.Fatalf("compaction task never completed within 330s")
	}
	// The worker may merge mid-patch (commit kicks); slices written after
	// that merge stay until the count crosses the 64-slice threshold
	// again. The invariant: a task completed and the residue sits below
	// the re-trigger threshold.
	var slices []struct {
		Key string `json:"key"`
	}
	if raw := querySlices(t, db, tenantDB, inodeID, 0); json.Unmarshal(raw, &slices) != nil || len(slices) >= 64 {
		t.Errorf("chunk 0 not merged below the compaction threshold (slices=%d)", len(slices))
	} else {
		t.Logf("compaction merged chunk 0; residue %d slice(s) below the 64 threshold", len(slices))
	}
	if d := usage() - (usage0 + int64(len(payload))); d != 0 {
		t.Errorf("quota drifted across patches+compaction: %+d bytes (logical size never changed)", d)
	} else {
		t.Logf("quota exact across patches + compaction")
	}

	// ---- step 4: delete -> tombstone, async file GC, block GC queue ----
	// NOTE: quota is tenant-wide; no other writers may run against this
	// tenant while the audit measures deltas.
	usagePreDelete := usage()
	if err := cl.RemoveAllCtx(ctx, path); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Classic semantics: the namespace row goes immediately, the inode row
	// becomes a DELETED tombstone, and the file GC task (tenant worker,
	// 5-minute tick) reclaims the extent storage.
	var nameGone bool
	for i := 0; i < 30 && !nameGone; i++ {
		var n int
		_ = db.QueryRow("SELECT COUNT(*) FROM `"+tenantDB+"`.file_nodes WHERE path = ?", path).Scan(&n)
		if n == 0 {
			nameGone = true
			break
		}
		time.Sleep(time.Second)
	}
	if !nameGone {
		t.Errorf("file_nodes row survived the delete")
	}
	reclaimed := false
	for i := 0; i < 700 && !reclaimed; i++ {
		var chunks int
		_ = db.QueryRow("SELECT COUNT(*) FROM `"+tenantDB+"`.extent_chunks WHERE inode_id = ?", inodeID).Scan(&chunks)
		var status string
		_ = db.QueryRow("SELECT status FROM `"+tenantDB+"`.file_gc_tasks WHERE task_id = ?", inodeID).Scan(&status)
		if chunks == 0 && status == "succeeded" {
			reclaimed = true
			break
		}
		time.Sleep(time.Second)
	}
	if !reclaimed {
		t.Errorf("file GC did not reclaim extent storage within 700s")
	} else {
		t.Logf("file GC succeeded; extent_chunks reclaimed (inode row stays as the classic DELETED tombstone)")
	}
	waitUsage(usagePreDelete, -int64(len(payload)), "delete")
	var gcPending int
	if err := db.QueryRow("SELECT COUNT(*) FROM `" + tenantDB + "`.extent_block_gc WHERE status IN ('PENDING','LEASED') AND available_at > NOW()").Scan(&gcPending); err != nil {
		t.Fatal(err)
	}
	t.Logf("block GC queue (24h grace pending): %d rows — objects are deleted only after the grace window", gcPending)
	if gcPending == 0 {
		t.Errorf("no blocks entered the GC queue after delete+compaction")
	}
}

func querySlices(t *testing.T, db *sql.DB, tenantDB, inodeID string, chunk uint32) []byte {
	t.Helper()
	var raw []byte
	err := db.QueryRow("SELECT slices FROM `"+tenantDB+"`.extent_chunks WHERE inode_id = ? AND chunk = ?", inodeID, chunk).Scan(&raw)
	if err != nil {
		t.Fatalf("read chunk slices: %v", err)
	}
	return raw
}

// checkRefs asserts every slice key of every chunk is referenced.
func checkRefs(t *testing.T, db *sql.DB, tenantDB, inodeID string) {
	t.Helper()
	rows, err := db.Query("SELECT chunk, slices FROM `"+tenantDB+"`.extent_chunks WHERE inode_id = ?", inodeID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	checked := 0
	for rows.Next() {
		var chunk uint32
		var raw []byte
		if err := rows.Scan(&chunk, &raw); err != nil {
			t.Fatal(err)
		}
		var slices []struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(raw, &slices); err != nil {
			t.Fatalf("chunk %d slices: %v", chunk, err)
		}
		for _, s := range slices {
			if s.Key == "" {
				continue
			}
			var refs int
			if err := db.QueryRow("SELECT refs FROM `"+tenantDB+"`.extent_block_refs WHERE block_key = ?", s.Key).Scan(&refs); err != nil {
				t.Errorf("chunk %d slice key %s missing from extent_block_refs: %v", chunk, s.Key[:12], err)
			} else if refs < 1 {
				t.Errorf("chunk %d slice key %s has refs=%d", chunk, s.Key[:12], refs)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Errorf("no slices found to check for inode %s", inodeID)
	} else {
		t.Logf("ref consistency OK: %d slice keys all present with refs>=1", checked)
	}
}
