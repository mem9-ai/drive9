package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/extent"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

// extentRuntimePool keeps one JuiceFS data-plane Runtime per tenant for the
// server-side extent jobs. The compact fallback used to build a Runtime per
// claimed task, and every NewRuntime opens a JuiceFS session that nothing
// closed: jfs_session2 churned until the stale-session sweep reclaimed the
// rows, and each task paid a format load plus a session creation. The cached
// store behind a Runtime also starts a goroutine that no API can stop, so
// reusing the Runtime is the only way not to leak one per task.
//
// A Runtime is reused while the tenant backend behind it is unchanged: the
// tenant pool builds the datastore and the S3 client together and closes both
// when it evicts, replaces or invalidates an entry, so the store pointer
// identifies the backend generation. When it changes, the cached Runtime is
// closed and rebuilt against the new store; the entry keeps the old store
// alive for exactly as long as its Runtime, so a replacement store can never
// be allocated at the same address and be mistaken for the current one.
type extentRuntimePool struct {
	newRuntime   func(extent.RuntimeConfig) (*extent.Runtime, error)
	closeRuntime func(*extent.Runtime) error

	mu      sync.Mutex
	entries map[string]*extentRuntimeEntry
	// retired records tenants whose entry was dropped by forget. A worker that
	// resolved an entry just before the deletion would otherwise re-create one
	// through entryFor and park a fresh Runtime (JuiceFS session plus cache
	// goroutine) for a tenant nothing will drain again; the tombstone makes
	// withRuntime refuse instead of rebuild.
	retired map[string]struct{}
}

type extentRuntimeEntry struct {
	// mu serializes work on this tenant's Runtime and keeps it from being
	// closed while a callback drives it. It is held across the callback: a
	// Runtime's session must not be closed underneath an in-flight compaction
	// (forget on tenant deletion, closeAll on shutdown), and the tenant worker
	// runs at most one extent task per tenant (see tryClaimTenantSlot), so the
	// lock is uncontended. A slow object PUT therefore delays only this
	// tenant's next task and its own deletion, which is the correct trade.
	mu    sync.Mutex
	rt    *extent.Runtime
	store *datastore.Store
	// gone marks an entry removed from the pool. withRuntime refuses to run
	// against it, so a rebuild racing forget/closeAll cannot park a Runtime in
	// an entry nothing will ever close.
	gone bool
}

// extentFallbackCacheBytes is enough for the one chunk the fallback compactor
// reads and rewrites per task, and small enough that many tenants doing so at
// once still fit in the server process.
const extentFallbackCacheBytes = 64 << 20

func newExtentRuntimePool() *extentRuntimePool {
	return &extentRuntimePool{
		newRuntime:   extent.NewRuntime,
		closeRuntime: extent.CloseRuntime,
		entries:      make(map[string]*extentRuntimeEntry),
		retired:      make(map[string]struct{}),
	}
}

// withRuntime runs fn with the tenant's cached Runtime, creating it on first
// use and rebuilding it after the tenant backend was replaced.
//
// The entry lock covers the callback as well as acquisition. Holding it is safe
// because the tenant worker runs at most one extent task per tenant
// (tryClaimTenantSlot), and it is necessary because a Runtime's session must
// not be closed underneath work in flight: forget (tenant deletion) and closeAll
// (shutdown) take the same lock, so a slow object PUT delays only this tenant's
// next task and its own teardown.
func (p *extentRuntimePool) withRuntime(tenantID string, store *datastore.Store, s3 s3client.S3Client, fn func(*extent.Runtime) error) error {
	if p == nil || tenantID == "" || store == nil || s3 == nil {
		return fmt.Errorf("extent runtime: missing tenant, store, or s3 client")
	}
	if p.newRuntime == nil {
		return fmt.Errorf("extent runtime for %s: pool has no runtime constructor", tenantID)
	}
	for attempt := 0; ; attempt++ {
		e, fresh := p.entryFor(tenantID)
		if !fresh {
			return fmt.Errorf("extent runtime for %s: tenant has no worker", tenantID)
		}
		e.mu.Lock()
		// forget/closeAll may have retired this entry between entryFor and the
		// lock; re-resolve rather than rebuild into a dead entry.
		if e.gone {
			e.mu.Unlock()
			if attempt > 8 {
				return fmt.Errorf("extent runtime for %s: pool entry churn", tenantID)
			}
			continue
		}
		rt, err := p.runtimeLocked(tenantID, e, store, s3)
		if err != nil {
			e.mu.Unlock()
			return err
		}
		err = fn(rt)
		e.mu.Unlock()
		return err
	}
}

// runtimeLocked returns e's Runtime, creating or rebuilding it. The caller holds
// e.mu.
func (p *extentRuntimePool) runtimeLocked(tenantID string, e *extentRuntimeEntry, store *datastore.Store, s3 s3client.S3Client) (*extent.Runtime, error) {
	if e.rt == nil || e.store != store {
		if e.rt != nil {
			p.closeEntry(tenantID, e)
		}
		st, err := extent.WrapS3(s3, "t/"+tenantID+"/")
		if err != nil {
			return nil, fmt.Errorf("extent runtime storage for %s: %w", tenantID, err)
		}
		rt, err := p.newRuntime(extent.RuntimeConfig{
			// The server-side fallback compactor is trusted infrastructure:
			// its writes carry no user quota admission (quota is enforced on
			// the mount's own meta RPCs).
			Transport: extent.NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
				return store.RunExtentMetaOp(ctx, op, raw, nil)
			}),
			Storage: st,
			// This runtime only compacts, one chunk at a time, and never serves
			// a user read: a mount-sized memory cache per tenant would be
			// reserved in the server process for no benefit.
			CacheBytes: extentFallbackCacheBytes,
		})
		if err != nil {
			return nil, fmt.Errorf("extent runtime for %s: %w", tenantID, err)
		}
		e.rt, e.store = rt, store
	}
	return e.rt, nil
}

// forget closes and drops the cached Runtime of a tenant that no longer needs
// background work (deleted tenant).
func (p *extentRuntimePool) forget(tenantID string) {
	if p == nil || tenantID == "" {
		return
	}
	p.mu.Lock()
	e := p.entries[tenantID]
	delete(p.entries, tenantID)
	if p.retired == nil {
		p.retired = make(map[string]struct{})
	}
	p.retired[tenantID] = struct{}{}
	p.mu.Unlock()
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.gone = true
	p.closeEntry(tenantID, e)
}

// closeAll closes every cached Runtime; called when the tenant worker stops.
func (p *extentRuntimePool) closeAll() {
	if p == nil {
		return
	}
	p.mu.Lock()
	entries := p.entries
	p.entries = make(map[string]*extentRuntimeEntry)
	p.mu.Unlock()
	for tenantID, e := range entries {
		e.mu.Lock()
		e.gone = true
		p.closeEntry(tenantID, e)
		e.mu.Unlock()
	}
}

// entryFor returns the tenant's entry, or false when the tenant was forgotten.
func (p *extentRuntimePool) entryFor(tenantID string) (*extentRuntimeEntry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, gone := p.retired[tenantID]; gone {
		return nil, false
	}
	e := p.entries[tenantID]
	if e == nil {
		e = &extentRuntimeEntry{}
		p.entries[tenantID] = e
	}
	return e, true
}

// closeEntry closes e's Runtime and clears the entry. The caller holds e.mu and
// must have verified that no callback is still driving it.
func (p *extentRuntimePool) closeEntry(tenantID string, e *extentRuntimeEntry) {
	if e == nil || e.rt == nil {
		return
	}
	p.closeRuntimeFor(tenantID, e.rt)
	e.rt, e.store = nil, nil
}

// closeRuntimeFor closes one Runtime, reporting a failure instead of dropping
// it.
func (p *extentRuntimePool) closeRuntimeFor(tenantID string, rt *extent.Runtime) {
	if rt == nil {
		return
	}
	if err := p.closeRuntime(rt); err != nil {
		logger.Warn(context.Background(), "tenant_worker_extent_runtime_close_failed",
			zap.String("tenant_id", tenantID), zap.Error(err))
	}
}
