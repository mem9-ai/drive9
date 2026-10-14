package bulkfile

import (
	"context"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// An existing claim reader keeps serving after the writer's stream forwards
// past the memory bound (qiffang: the emptied image answered (0, nil)).
func TestClaimReaderSurvivesPassthrough(t *testing.T) {
	h := newClaimHandle(t)
	if err := h.Write(context.Background(), 0, []byte("head")); err != nil {
		t.Fatal(err)
	}
	r := &claimReadHandle{claim: h}
	buf := make([]byte, 4)
	if n, err := r.Read(context.Background(), 0, buf); err != nil || string(buf[:n]) != "head" {
		t.Fatalf("pre-passthrough read = %q n=%d err=%v", buf[:n], n, err)
	}
	if err := h.Write(context.Background(), bulkfileMaxBufferedBytes+1, []byte("x")); err != nil {
		t.Fatalf("overflow write: %v", err)
	}
	if n, err := r.Read(context.Background(), 0, buf); err != nil || string(buf[:n]) != "head" {
		t.Fatalf("post-passthrough read = %q n=%d err=%v", buf[:n], n, err)
	}
}

// A handle-based truncate after the handoff reaches the plane below instead
// of comparing against the emptied image (qiffang: size-0 skipped).
func TestPassthroughSetAttrTruncatesBelow(t *testing.T) {
	h := newClaimHandle(t)
	if err := h.Write(context.Background(), 0, []byte("head")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(context.Background(), bulkfileMaxBufferedBytes+1, []byte("x")); err != nil {
		t.Fatalf("overflow write: %v", err)
	}
	if _, err := h.applyAttr(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Size: 0, HasMode: false}); err != nil {
		t.Fatalf("passthrough truncate: %v", err)
	}
	h.mu.Lock()
	pt, img := h.passthrough, len(h.image)
	h.mu.Unlock()
	if !pt || img != 0 {
		t.Fatalf("handle state pt=%v image=%d", pt, img)
	}
	// The file's real size lives below: verify through the plane.
	if st, err := h.below.GetAttr(context.Background(), h.path); err != nil {
		t.Fatalf("below stat: %v", err)
	} else if st.Size != 0 {
		t.Fatalf("below size after truncate = %d, want 0", st.Size)
	}
}

// A handle-based SetAttr after the passthrough handoff answers with the
// requested logical size (the emptied image must not report 0), and the
// plane below holds the persisted size.
func TestPortPassthroughSetAttrReplyKeepsLogicalSize(t *testing.T) {
	h := newClaimHandle(t)
	if err := h.Write(context.Background(), 0, []byte("head")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(context.Background(), bulkfileMaxBufferedBytes+1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	a, err := h.applyAttr(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Size: 4, HasSize: true})
	if err != nil {
		t.Fatal(err)
	}
	if a.Size != 4 {
		t.Fatalf("setattr reply size = %d, want 4 (the emptied image answered 0)", a.Size)
	}
	st, err := h.below.GetAttr(context.Background(), h.path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != 4 {
		t.Fatalf("below-plane size = %d, want the persisted 4", st.Size)
	}
}
