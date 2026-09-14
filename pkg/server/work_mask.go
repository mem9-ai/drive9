package server

import (
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/meta"
)

// Work mask constants for the unified tenant notify outbox. Each bit in
// work_mask selects one work type. The poller dispatches by testing bits:
//
//	SSE bit  → wake local SSE bus (broadcast: all pods with subscribers)
//	Semantic/GC bits → kick unified worker (sharded: shard owner only)
//	Metrics cleanup bit → delete local tenant series (broadcast: every pod)
//
// The persisted allocation lives in pkg/meta. Backend and server aliases plus
// the compile-time assertions below keep both producer paths in sync.
const (
	// WorkSSE (bit 0) wakes the local SSE EventBus so SSE handlers re-read
	// fs_events. Broadcast to all pods (not sharded) — any pod with subscribers
	// for the tenant must wake.
	WorkSSE = meta.TenantNotifyWorkSSE
	// WorkSemantic (bit 1) kicks the unified worker to drain semantic tasks
	// for this tenant. Sharded: only the shard-owner pod processes it.
	WorkSemantic = meta.TenantNotifyWorkSemantic
	// WorkFileGC (bit 2) kicks the unified worker to drain file_gc tasks.
	// Sharded: only the shard-owner pod processes it.
	WorkFileGC = meta.TenantNotifyWorkFileGC
	// WorkMetricsCleanup (bit 3) removes all tenant-scoped metric series and
	// invalidates the tenant API-key resolve cache in the local process. It is
	// server-only because it is emitted by durable tenant lifecycle transitions,
	// not by a tenant DB write path.
	WorkMetricsCleanup = meta.TenantNotifyWorkMetricsCleanup
	// WorkAPIKeyCacheCleanup (bit 4) invalidates locally cached API-key
	// resolutions after a key revocation. It is broadcast to every pod but does
	// not touch tenant metrics for live tenants.
	WorkAPIKeyCacheCleanup = meta.TenantNotifyWorkAPIKeyCacheCleanup
	// WorkExtent (bit 5) selects the extent data-plane maintenance jobs (block
	// GC, stale-session sweep, compact fallback). It is server-local: no tenant
	// DB write path persists it and the outbox poller never dispatches it. The
	// unified worker runs those jobs on every tenant kick and re-kicks itself
	// with this bit while a job keeps completing a full batch; a distinct bit
	// means such a re-kick cannot re-enter the semantic or file_gc drains.
	WorkExtent = 1 << 5
)

// Compile-time assertions that the server-side work mask constants match the
// backend-side constants. If either set changes without the other, the build
// fails here.
var (
	_ = [1]byte{}[backend.BackendWorkSSE^WorkSSE]
	_ = [1]byte{}[backend.BackendWorkSemantic^WorkSemantic]
	_ = [1]byte{}[backend.BackendWorkFileGC^WorkFileGC]
	_ = [1]byte{}[backend.BackendWorkMetricsCleanup^WorkMetricsCleanup]
	_ = [1]byte{}[backend.BackendWorkAPIKeyCacheCleanup^WorkAPIKeyCacheCleanup]
	// WorkExtent must stay clear of every persisted bit: the outbox poller
	// would silently drop a tenant work_mask bit it does not know, and the
	// worker would then read extent work as semantic/file_gc work (or the
	// reverse). The build fails here if the two ever overlap.
	_ = [1]byte{}[WorkExtent&(WorkSSE|WorkSemantic|WorkFileGC|WorkMetricsCleanup|WorkAPIKeyCacheCleanup)]
)
