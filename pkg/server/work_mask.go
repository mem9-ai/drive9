package server

import "github.com/mem9-ai/drive9/pkg/backend"

// Work mask constants for the unified tenant notify outbox. Each bit in
// work_mask selects one work type. The poller dispatches by testing bits:
//   SSE bit  → wake local SSE bus (broadcast: all pods with subscribers)
//   Semantic/GC/Quota bits → kick unified worker (sharded: shard owner only)
//
// These mirror the backend.Work* constants in pkg/backend/dat9.go. The
// compile-time assertions below ensure the values stay in sync.
const (
	// WorkSSE (bit 0) wakes the local SSE EventBus so SSE handlers re-read
	// fs_events. Broadcast to all pods (not sharded) — any pod with subscribers
	// for the tenant must wake.
	WorkSSE = 1
	// WorkSemantic (bit 1) kicks the unified worker to drain semantic tasks
	// for this tenant. Sharded: only the shard-owner pod processes it.
	WorkSemantic = 2
	// WorkFileGC (bit 2) kicks the unified worker to drain file_gc tasks.
	// Sharded: only the shard-owner pod processes it.
	WorkFileGC = 4
	// WorkQuota (bit 3) kicks the unified worker to drain the quota outbox.
	// Sharded: only the shard-owner pod processes it.
	WorkQuota = 8
)

// Compile-time assertions that the server-side work mask constants match the
// backend-side constants. If either set changes without the other, the build
// fails here.
var (
	_ = [1]byte{}[backend.BackendWorkSSE^WorkSSE]
	_ = [1]byte{}[backend.BackendWorkSemantic^WorkSemantic]
	_ = [1]byte{}[backend.BackendWorkFileGC^WorkFileGC]
	_ = [1]byte{}[backend.BackendWorkQuota^WorkQuota]
)