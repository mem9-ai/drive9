package extent

import (
	"encoding/json"
	"errors"
	"syscall"
	"testing"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
)

func TestCapJuiceCompactOrigin(t *testing.T) {
	if got := capJuiceCompactOrigin(nil); len(got) != 0 {
		t.Fatalf("nil origin len=%d", len(got))
	}
	small := make([]byte, 10*sliceBytes)
	if len(capJuiceCompactOrigin(small)) != len(small) {
		t.Fatal("short origin must be unchanged")
	}
	fat := make([]byte, (juiceMaxCompactSlices+7)*sliceBytes)
	got := capJuiceCompactOrigin(fat)
	if len(got) != juiceMaxCompactSlices*sliceBytes {
		t.Fatalf("capped slices=%d, want JuiceFS maxCompactSlices=%d", len(got)/sliceBytes, juiceMaxCompactSlices)
	}
}

// compactCall is one recorded client->server RPC.
type compactCall struct {
	op   string
	body map[string]any
}

// fakeCompactTransport answers canned JSON per op and records what the client
// actually sent, so the wire contract can be asserted without a server.
type fakeCompactTransport struct {
	calls   []compactCall
	answers map[string]string
}

func (f *fakeCompactTransport) Call(ctx jfsmeta.Context, op string, req, resp any) syscall.Errno {
	body, _ := req.(map[string]any)
	f.calls = append(f.calls, compactCall{op: op, body: body})
	if ans, ok := f.answers[op]; ok {
		if err := json.Unmarshal([]byte(ans), resp); err != nil {
			return syscall.EIO
		}
	}
	return 0
}

func (f *fakeCompactTransport) last() compactCall {
	return f.calls[len(f.calls)-1]
}

// The client half of the lease fence: the claim must announce receipt
// capability (the server refuses to lease without it), carry the minted
// receipt home, and both acks must echo it — that echo is the only thing that
// makes the server's task_id+receipt+LEASED predicate match this worker's
// lease instead of a successor's.
func TestCompactClientEchoesLeaseReceipt(t *testing.T) {
	tr := &fakeCompactTransport{answers: map[string]string{
		"claim_compact":    `{"errno":0,"inode":7,"indx":3,"task_id":"t1","receipt":"r1"}`,
		"complete_compact": `{"errno":0}`,
		"requeue_compact":  `{"errno":0}`,
	}}
	ino, indx, taskID, receipt, err := ClaimNextCompact(tr)
	if err != nil || ino != 7 || indx != 3 || taskID != "t1" || receipt != "r1" {
		t.Fatalf("claim = (%d, %d, %q, %q, %v), want (7, 3, t1, r1, nil)", ino, indx, taskID, receipt, err)
	}
	if rc, _ := tr.calls[0].body["receipt_capable"].(bool); !rc {
		t.Fatalf("claim body = %v, want receipt_capable: true (the server refuses to lease without it)", tr.calls[0].body)
	}
	if err := CompleteCompact(tr, taskID, receipt); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got := tr.last(); got.op != "complete_compact" || got.body["task_id"] != "t1" || got.body["receipt"] != "r1" {
		t.Fatalf("complete body = %v, want the task and the echoed receipt", got.body)
	}
	if err := RequeueCompact(tr, taskID, receipt, errors.New("boom")); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if got := tr.last(); got.op != "requeue_compact" || got.body["task_id"] != "t1" || got.body["receipt"] != "r1" || got.body["error"] != "boom" {
		t.Fatalf("requeue body = %v, want the task, the echoed receipt, and the cause", got.body)
	}
}

// An empty receipt is tolerated and omitted from both acks: that is exactly
// the pre-upgrade-server shape, where the claim reply carried no receipt
// field, and the extra request keys would only confuse nothing.
func TestCompactClientToleratesEmptyReceipt(t *testing.T) {
	tr := &fakeCompactTransport{answers: map[string]string{
		"claim_compact":    `{"errno":0,"inode":0,"indx":0,"task_id":"","receipt":""}`,
		"complete_compact": `{"errno":0}`,
		"requeue_compact":  `{"errno":0}`,
	}}
	if _, _, taskID, receipt, err := ClaimNextCompact(tr); err != nil || taskID != "" || receipt != "" {
		t.Fatalf("empty claim = (%q, %q, %v), want all empty", taskID, receipt, err)
	}
	if err := CompleteCompact(tr, "t2", ""); err != nil {
		t.Fatalf("complete without a receipt: %v", err)
	}
	if _, ok := tr.last().body["receipt"]; ok {
		t.Fatalf("complete body = %v, want no receipt key for an empty receipt", tr.last().body)
	}
	if err := RequeueCompact(tr, "t2", "", nil); err != nil {
		t.Fatalf("requeue without a receipt: %v", err)
	}
	if _, ok := tr.last().body["receipt"]; ok {
		t.Fatalf("requeue body = %v, want no receipt key for an empty receipt", tr.last().body)
	}
}

// The server's refusal shapes surface as errnos, not as silent success: a
// receipt-less claim gate answers EINVAL, and a fenced ack (stale receipt,
// dead lease) does too.
func TestCompactRefusedRpcSurfacesErrno(t *testing.T) {
	tr := &fakeCompactTransport{answers: map[string]string{
		"claim_compact": `{"errno":22}`,
	}}
	if _, _, _, _, err := ClaimNextCompact(tr); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("refused claim err = %v, want EINVAL", err)
	}
	tr.answers["complete_compact"] = `{"errno":22}`
	if err := CompleteCompact(tr, "t3", "spent"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("fenced complete err = %v, want EINVAL", err)
	}
	tr.answers["requeue_compact"] = `{"errno":22}`
	if err := RequeueCompact(tr, "t3", "spent", nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("fenced requeue err = %v, want EINVAL", err)
	}
}
