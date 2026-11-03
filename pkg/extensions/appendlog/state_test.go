package appendlog

// Ported from v1 pkg/fuse/append_log_test.go: direct state-machine tests of
// the handle-local tail-safety, truncate fence, layout, and committed
// baseline bookkeeping, rewritten against appendLogHandleState / Handle.

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

// newStateHandle builds a bare Handle around a fresh state machine; only the
// state fields are exercised, so no extension stack is needed.
func newStateHandle(isNew bool) *Handle {
	return &Handle{a: &AppendLog{}, path: "/state.log", isNew: isNew, origSize: 100}
}

// v1 TestAppendLogHandleMutation: tail eligibility of a write sequence.
func TestPortAppendLogHandleMutation(t *testing.T) {
	tests := []struct {
		name   string
		isNew  bool
		writes [][2]int64
		want   bool
	}{
		{name: "existing pure extension", writes: [][2]int64{{100, 8}, {108, 4}}, want: true},
		{name: "existing overwrite", writes: [][2]int64{{99, 8}, {108, 4}}, want: false},
		{name: "existing gap", writes: [][2]int64{{101, 8}}, want: false},
		{name: "existing backwrite", writes: [][2]int64{{100, 8}, {101, 4}}, want: false},
		{name: "new file random assembly", isNew: true, writes: [][2]int64{{20, 4}, {0, 8}}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newStateHandle(test.isNew)
			size := h.origSize
			for _, write := range test.writes {
				h.recordUserWrite(size, write[0], write[1])
				if end := write[0] + write[1]; end > size {
					size = end
				}
			}
			if got := h.canUseTail(); got != test.want {
				t.Fatalf("canUseTail() = %t, want %t", got, test.want)
			}
		})
	}
}

// v1 TestAppendLogHandleTruncate: a truncate permanently fences the dirty
// generation of an existing file; a new file stays append-eligible.
func TestPortAppendLogHandleTruncate(t *testing.T) {
	existing := newStateHandle(false)
	existing.recordUserWrite(100, 100, 8)
	existing.recordTruncate()
	existing.recordUserWrite(0, 0, 4)
	if existing.canUseTail() {
		t.Fatal("existing truncate must permanently invalidate its dirty generation")
	}
	if !existing.st.sqliteWALTruncated {
		t.Fatal("truncate must fence the WAL generation reset")
	}

	created := newStateHandle(true)
	created.recordTruncate()
	created.recordUserWrite(0, 12, 4)
	if !created.canUseTail() {
		t.Fatal("new file truncate must remain eligible for append-log creation")
	}
	if created.st.sqliteWALTruncated {
		t.Fatal("new file truncate must not fence the generation reset")
	}
}

// v1 TestAppendLogLayout: layout learning answers only at the exact observed
// baseline; an append success establishes a clean tail-safe baseline;
// unsupported suppresses later appends without erasing the layout.
func TestPortAppendLogLayout(t *testing.T) {
	h := newStateHandle(false)
	h.observeLayoutLocked(client.ContentLayoutSingle, 4, 100)
	if got := h.layoutAtLocked(4, 100); got != client.ContentLayoutSingle {
		t.Fatalf("layout = %q, want single", got)
	}
	if got := h.layoutAtLocked(5, 100); got != "" {
		t.Fatalf("stale-revision layout = %q, want unknown", got)
	}

	// A successful append lands an append_log baseline (v1
	// appendLogMarkAppendSuccess): the new layout is observed at the fresh
	// revision and the handle becomes tail-safe again.
	h.observeLayoutLocked(client.ContentLayoutAppendLog, 6, 108)
	h.adoptCommittedBaselineLocked(6, 108)
	if got := h.layoutAtLocked(6, 108); got != client.ContentLayoutAppendLog {
		t.Fatalf("append success layout = %q, want append_log", got)
	}
	if !h.canUseTail() {
		t.Fatal("append success must establish a clean append-safe baseline")
	}

	h.st.unsupported = true // v1 appendLogMarkUnsupported
	if h.canUseTail() {
		t.Fatal("append_log_unsupported must suppress later append attempts on the handle")
	}
	if got := h.layoutAtLocked(6, 108); got != client.ContentLayoutAppendLog {
		t.Fatalf("unsupported must not overwrite known layout: got %q", got)
	}
}

// v1 TestAppendLogBaseline: adopting a committed baseline resets tail safety
// but preserves the unsupported suppression and the learned layout.
func TestPortAppendLogBaseline(t *testing.T) {
	h := newStateHandle(false)
	h.observeLayoutLocked(client.ContentLayoutAppendLog, 4, 100)
	h.recordUserWrite(100, 90, 4) // non-tail write kills append safety
	h.st.unsupported = true
	h.adoptCommittedBaselineLocked(5, 104)

	if !h.st.appendSafe {
		t.Fatal("committed baseline must reset append safety")
	}
	if !h.st.unsupported {
		t.Fatal("committed baseline must preserve append unsupported suppression")
	}
	if !h.st.initialized {
		t.Fatal("committed baseline must initialize the state machine")
	}
	if h.st.hasRewriteBase {
		t.Fatal("committed baseline must clear the rebase-learned rewrite base")
	}
	if got := h.layoutAtLocked(5, 104); got != client.ContentLayoutAppendLog {
		t.Fatalf("baseline layout = %q, want append_log", got)
	}
}

// v1 state rows (32-byte mask): WAL header coverage gating a generation
// reset — only a byte-complete [0, 32) write starting at zero may rotate a
// WAL, and any write past byte 32 fences the reset to a full image.
func TestPortAppendLogWALHeaderDirtyByteMask(t *testing.T) {
	tests := []struct {
		name     string
		writes   [][2]int64
		complete bool
		atZero   bool
		beyond   bool
	}{
		{name: "single whole header write", writes: [][2]int64{{0, 32}}, complete: true, atZero: true},
		{name: "two aligned halves", writes: [][2]int64{{0, 16}, {16, 16}}, complete: true, atZero: true},
		{name: "first half only", writes: [][2]int64{{0, 16}}, complete: false, atZero: true},
		{name: "start past zero", writes: [][2]int64{{8, 24}}, complete: false},
		{name: "write straddles the header end", writes: [][2]int64{{0, 40}}, complete: true, atZero: true, beyond: true},
		{name: "frame write only", writes: [][2]int64{{32, 24}}, complete: false, beyond: true},
		{name: "negative offset clamps to zero", writes: [][2]int64{{-4, 8}}, complete: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var st appendLogHandleState
			for _, w := range test.writes {
				st.recordSQLiteWALHeaderWrite(w[0], w[1])
			}
			if got := st.sqliteWALHeaderDirtyComplete(); got != test.complete {
				t.Fatalf("sqliteWALHeaderDirtyComplete() = %t, want %t (mask=%032b)", got, test.complete, st.sqliteWALHeaderDirtyByteMask)
			}
			if st.sqliteWALWriteAtZero != test.atZero {
				t.Fatalf("sqliteWALWriteAtZero = %t, want %t", st.sqliteWALWriteAtZero, test.atZero)
			}
			if st.sqliteWALWriteBeyondHeader != test.beyond {
				t.Fatalf("sqliteWALWriteBeyondHeader = %t, want %t", st.sqliteWALWriteBeyondHeader, test.beyond)
			}
			st.clearSQLiteWALHeaderWrite()
			if st.sqliteWALHeaderDirtyComplete() || st.sqliteWALWriteAtZero || st.sqliteWALWriteBeyondHeader {
				t.Fatal("clearSQLiteWALHeaderWrite must reset the whole dirty generation")
			}
		})
	}
}

// The salt proof of a generation reset lives in the state machine: the
// committed header captured before a write must differ from the dirty header
// a reset would publish (saltsDiffer), and an unconfirmed baseline (no
// captured proof) can never rotate a WAL.
func TestPortAppendLogGenerationResetSaltProofState(t *testing.T) {
	h0, ok0 := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	h1, ok1 := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4))
	if !ok0 || !ok1 {
		t.Fatal("fixture headers did not parse")
	}
	if !h0.saltsDiffer(h1) {
		t.Fatal("fixture salts must differ")
	}

	h := newStateHandle(false)
	if h.st.sqliteWALConfirmed {
		t.Fatal("fresh handle must not claim a WAL header proof")
	}
	h.st.sqliteWALConfirmed = true
	h.st.sqliteWALCommittedHeader = h0
	if h.st.sqliteWALCommittedHeader.saltsDiffer(h0) {
		t.Fatal("identical salts must not count as a new generation")
	}
	if !h.st.sqliteWALCommittedHeader.saltsDiffer(h1) {
		t.Fatal("changed salts must prove a new generation")
	}
}
