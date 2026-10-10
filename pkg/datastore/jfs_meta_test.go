package datastore

import (
	"context"
	"encoding/json"
	"syscall"
	"testing"
)

// Every op the JuiceFS client can send must be answered by the meta engine. A
// dispatch case deleted as "dead" but actually reachable shows up only as a
// runtime ENOSYS deep inside a workload, so pin the op list here: the engine
// may reject an op with any errno, but never with ENOSYS, which is the
// dispatcher's "no such op" answer.
//
// Two intentional shapes: "compact" is served by the runCompactOp short-circuit
// before the dispatch, and "complete_compact" is the compact-task lifecycle's
// own op.
func TestExtentMetaAnswersEveryClientOp(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Provision the tenant so the ops work against a real tree.
	if _, errno, err := s.RunExtentMetaOp(ctx, "init", json.RawMessage(`{}`), nil); err != nil || errno != 0 {
		t.Fatalf("init errno=%d err=%v", errno, err)
	}

	// Bodies are deliberately minimal: any errno but ENOSYS proves the op was
	// routed somewhere that understands it.
	ops := map[string]string{
		"get_counter":         `{"name":"nextInode"}`,
		"incr_counter":        `{"name":"nextInode","value":0}`,
		"set_if_small":        `{"name":"nextInode","value":0,"diff":0}`,
		"load":                `{}`,
		"init":                `{}`,
		"new_session":         `{"sid":1,"expire":0,"info":{}}`,
		"refresh_session":     `{"sid":1,"expire":0}`,
		"lookup":              `{"parent":1,"name":"nope"}`,
		"getattr":             `{"inode":1}`,
		"setattr":             `{"inode":1,"set":0}`,
		"mknod":               `{"parent":1,"name":"x","type":1,"mode":420,"proj_path":"/x"}`,
		"readlink":            `{"inode":1}`,
		"unlink":              `{"parent":1,"name":"nope","proj_path":"/nope"}`,
		"rmdir":               `{"parent":1,"name":"nope","proj_path":"/nope/"}`,
		"rename":              `{"src_parent":1,"src_name":"a","dst_parent":1,"dst_name":"b"}`,
		"readdir":             `{"inode":1}`,
		"read":                `{"inode":1,"indx":0}`,
		"write":               `{"inode":1,"indx":0,"off":0}`,
		"truncate":            `{"inode":1,"length":0}`,
		"delete_slice":        `{"id":1,"size":4}`,
		"delete_sustained":    `{"sid":1,"inode":1}`,
		"flock":               `{"inode":1,"sid":1,"owner":1,"ltype":1}`,
		"getlk":               `{"inode":1,"sid":1,"owner":1,"ltype":1,"start":0,"end":0}`,
		"setlk":               `{"inode":1,"sid":1,"owner":1,"ltype":1,"start":0,"end":0}`,
		"find_stale_sessions": `{"limit":1}`,
		"clean_stale_session": `{"sid":1}`,
		"get_session":         `{}`,
		"claim_compact":       `{}`,
		"requeue_compact":     `{"task_id":""}`,
		"complete_compact":    `{"task_id":""}`,
		"compact":             `{"inode":1,"indx":0}`,
	}
	for op, body := range ops {
		t.Run(op, func(t *testing.T) {
			_, errno, err := s.RunExtentMetaOp(ctx, op, json.RawMessage(body), nil)
			if errno == int(syscall.ENOSYS) {
				t.Fatalf("op %q reached the dispatcher's ENOSYS fallback: it has no handler", op)
			}
			if err != nil && errno == 0 {
				t.Fatalf("op %q returned an untranslated error: %v", op, err)
			}
		})
	}

	if _, errno, _ := s.RunExtentMetaOp(ctx, "not_an_op", json.RawMessage(`{}`), nil); errno != int(syscall.ENOSYS) {
		t.Fatalf("an unknown op must report ENOSYS, got errno=%d", errno)
	}
}
