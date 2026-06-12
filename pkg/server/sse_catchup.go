package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"go.uber.org/zap"
)

var errNoTenantStore = errors.New("sse tenant store unavailable")

const (
	defaultSSECatchupEnabled              = true
	defaultSSECatchupPollInterval         = time.Second
	defaultSSECatchupIdleMaxInterval      = 10 * time.Second
	defaultSSECatchupBatchSize            = 1000
	defaultSSECatchupMaxConcurrentTenants = 16
	defaultSSECatchupAcquireTimeout       = 5 * time.Second
)

type SSECatchupOptions struct {
	Disabled             bool
	Enabled              bool
	PollInterval         time.Duration
	IdleMaxInterval      time.Duration
	BatchSize            int
	MaxConcurrentTenants int
}

func defaultSSECatchupOptions() SSECatchupOptions {
	return SSECatchupOptions{
		Enabled:              defaultSSECatchupEnabled,
		PollInterval:         defaultSSECatchupPollInterval,
		IdleMaxInterval:      defaultSSECatchupIdleMaxInterval,
		BatchSize:            defaultSSECatchupBatchSize,
		MaxConcurrentTenants: defaultSSECatchupMaxConcurrentTenants,
	}
}

func normalizeSSECatchupOptions(opts SSECatchupOptions) SSECatchupOptions {
	defaults := defaultSSECatchupOptions()
	if opts.Disabled {
		defaults.Enabled = false
		defaults.Disabled = true
		return defaults
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaults.PollInterval
	}
	if opts.IdleMaxInterval <= 0 {
		opts.IdleMaxInterval = defaults.IdleMaxInterval
	}
	if opts.IdleMaxInterval < opts.PollInterval {
		opts.IdleMaxInterval = opts.PollInterval
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaults.BatchSize
	}
	if opts.MaxConcurrentTenants <= 0 {
		opts.MaxConcurrentTenants = defaults.MaxConcurrentTenants
	}
	opts.Enabled = true
	return opts
}

type sseCatchupManager struct {
	fallback *backend.Dat9Backend
	meta     *meta.Store
	pool     tenantPool
	opts     SSECatchupOptions

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	tenants map[string]*sseCatchupTenant
}

type tenantPool interface {
	Acquire(ctx context.Context, t *meta.Tenant) (*backend.Dat9Backend, func(), error)
}

type sseCatchupTenant struct {
	tenantID  string
	bus       *EventBus
	cursor    uint64
	listeners int
	nextPoll  time.Time
	interval  time.Duration
	running   bool
}

type activeSSECatchupTenant struct {
	tenantID string
	bus      *EventBus
	cursor   uint64
}

func newSSECatchupManager(fallback *backend.Dat9Backend, metaStore *meta.Store, pool tenantPool, opts SSECatchupOptions) *sseCatchupManager {
	opts = normalizeSSECatchupOptions(opts)
	if !opts.Enabled {
		recordSSECatchupGauges(opts, 0, 0)
		return nil
	}
	if fallback == nil && (metaStore == nil || pool == nil) {
		recordSSECatchupGauges(opts, 0, 0)
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &sseCatchupManager{
		fallback: fallback,
		meta:     metaStore,
		pool:     pool,
		opts:     opts,
		ctx:      ctx,
		cancel:   cancel,
		tenants:  make(map[string]*sseCatchupTenant),
	}
}

func (m *sseCatchupManager) Start() {
	if m == nil {
		return
	}
	recordSSECatchupGauges(m.opts, 0, 0)
	m.wg.Add(1)
	go m.loop()
}

func (m *sseCatchupManager) Stop() {
	if m == nil {
		return
	}
	m.cancel()
	m.wg.Wait()
	recordSSECatchupGauges(m.opts, 0, 0)
}

func (m *sseCatchupManager) Register(tenantID string, bus *EventBus, cursor uint64) {
	if m == nil || bus == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	t := m.tenants[tenantID]
	if t == nil {
		t = &sseCatchupTenant{
			tenantID: tenantID,
			bus:      bus,
			cursor:   cursor,
			interval: m.opts.PollInterval,
			nextPoll: time.Now().Add(jitterDuration(m.opts.PollInterval, tenantID)),
		}
		m.tenants[tenantID] = t
	} else {
		t.bus = bus
		if cursor > t.cursor {
			t.cursor = cursor
		}
		if t.interval <= 0 {
			t.interval = m.opts.PollInterval
		}
	}
	t.listeners++
	metrics.RecordGauge("sse", "local_bus_listeners", float64(bus.ListenerCount()))
	m.recordActiveGaugesLocked()
}

func (m *sseCatchupManager) Unregister(tenantID string, bus *EventBus) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	t := m.tenants[tenantID]
	if t == nil {
		return
	}
	if t.listeners > 0 {
		t.listeners--
	}
	if bus != nil {
		metrics.RecordGauge("sse", "local_bus_listeners", float64(bus.ListenerCount()))
	}
	if t.listeners == 0 && !t.running {
		delete(m.tenants, tenantID)
	}
	m.recordActiveGaugesLocked()
}

func (m *sseCatchupManager) AdvanceCursor(tenantID string, cursor uint64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	t := m.tenants[tenantID]
	if t == nil {
		return
	}
	if cursor > t.cursor {
		t.cursor = cursor
	}
}

func (m *sseCatchupManager) loop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.opts.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.pollDueTenants(time.Now())
		}
	}
}

func (m *sseCatchupManager) pollDueTenants(now time.Time) {
	active := m.takeDueTenants(now)
	recordSSECatchupOperation("poll_tick", sseResultOK, time.Time{})
	metrics.RecordGauge("sse", "catchup_poll_tenants", float64(len(active)))
	if len(active) == 0 {
		return
	}

	sem := make(chan struct{}, m.opts.MaxConcurrentTenants)
	var wg sync.WaitGroup
	for _, tenant := range active {
		tenant := tenant
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			m.catchupTenant(tenant)
		}()
	}
	wg.Wait()
}

func (m *sseCatchupManager) takeDueTenants(now time.Time) []activeSSECatchupTenant {
	m.mu.Lock()
	defer m.mu.Unlock()

	active := make([]activeSSECatchupTenant, 0, len(m.tenants))
	for tenantID, t := range m.tenants {
		if t.listeners == 0 || t.bus == nil || t.running {
			continue
		}
		if !t.nextPoll.IsZero() && now.Before(t.nextPoll) {
			continue
		}
		if t.bus.ListenerCount() == 0 {
			delete(m.tenants, tenantID)
			continue
		}
		t.running = true
		active = append(active, activeSSECatchupTenant{
			tenantID: tenantID,
			bus:      t.bus,
			cursor:   t.cursor,
		})
	}
	m.recordActiveGaugesLocked()
	return active
}

func (m *sseCatchupManager) catchupTenant(active activeSSECatchupTenant) {
	start := time.Now()
	store, release, err := m.acquireStore(m.ctx, active.tenantID)
	if err != nil {
		recordSSECatchupOperation("catchup", sseResultError, start)
		logger.Warn(m.ctx, "sse_catchup_acquire_failed", zap.String("tenant_id", active.tenantID), zap.Error(err))
		m.finishTenant(active.tenantID, active.cursor, false)
		return
	}
	defer release()

	cursor := active.cursor
	total := 0
	for {
		events, err := store.ListFSEventsSince(m.ctx, cursor, m.opts.BatchSize)
		if err != nil {
			recordSSECatchupOperation("catchup", sseResultError, start)
			logger.Warn(m.ctx, "sse_catchup_list_failed", zap.String("tenant_id", active.tenantID), zap.Uint64("cursor", cursor), zap.Error(err))
			m.finishTenant(active.tenantID, cursor, false)
			return
		}
		if len(events) == 0 {
			recordSSECatchupOperation("catchup", sseResultOK, start)
			m.finishTenant(active.tenantID, cursor, total > 0)
			return
		}
		for _, ev := range events {
			active.bus.PublishEvent(ChangeEvent{
				Seq:   ev.Seq,
				Path:  ev.Path,
				Op:    ev.Op,
				Actor: ev.Actor,
				Ts:    ev.Ts,
			})
			if ev.Seq > cursor {
				cursor = ev.Seq
			}
		}
		total += len(events)
		recordSSECatchupOperation("catchup_batch", sseResultOK, time.Time{})
		metrics.RecordGauge("sse", "catchup_last_batch_events", float64(len(events)))
		if len(events) < m.opts.BatchSize {
			recordSSECatchupOperation("catchup", sseResultOK, start)
			m.finishTenant(active.tenantID, cursor, true)
			return
		}
	}
}

func (m *sseCatchupManager) acquireStore(ctx context.Context, tenantID string) (*datastore.Store, func(), error) {
	if tenantID == "" {
		if m.fallback == nil || m.fallback.Store() == nil {
			return nil, func() {}, errNoTenantStore
		}
		return m.fallback.Store(), func() {}, nil
	}
	if m.meta == nil || m.pool == nil {
		return nil, func() {}, errNoTenantStore
	}
	acquireCtx, cancel := context.WithTimeout(ctx, defaultSSECatchupAcquireTimeout)
	defer cancel()

	t, err := m.meta.GetTenant(acquireCtx, tenantID)
	if err != nil {
		return nil, func() {}, err
	}
	b, release, err := m.pool.Acquire(acquireCtx, t)
	if err != nil {
		return nil, func() {}, err
	}
	if b == nil || b.Store() == nil {
		release()
		return nil, func() {}, errNoTenantStore
	}
	return b.Store(), release, nil
}

func (m *sseCatchupManager) finishTenant(tenantID string, cursor uint64, hadEvents bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	t := m.tenants[tenantID]
	if t == nil {
		return
	}
	if cursor > t.cursor {
		t.cursor = cursor
	}
	t.running = false
	if hadEvents {
		t.interval = m.opts.PollInterval
	} else {
		if t.interval <= 0 {
			t.interval = m.opts.PollInterval
		}
		t.interval *= 2
		if t.interval > m.opts.IdleMaxInterval {
			t.interval = m.opts.IdleMaxInterval
		}
	}
	t.nextPoll = time.Now().Add(t.interval + jitterDuration(t.interval, tenantID))
	if t.listeners == 0 {
		delete(m.tenants, tenantID)
	}
	m.recordActiveGaugesLocked()
}

func (m *sseCatchupManager) recordActiveGaugesLocked() {
	activeTenants := 0
	listeners := 0
	for _, t := range m.tenants {
		if t.listeners > 0 {
			activeTenants++
			listeners += t.listeners
		}
	}
	metrics.RecordGauge("sse", "active_tenant_dbs", float64(activeTenants))
	metrics.RecordGauge("sse", "catchup_registered_listeners", float64(listeners))
}

func recordSSECatchupGauges(opts SSECatchupOptions, activeTenantDBs, registeredListeners int) {
	metrics.RecordGauge("sse", "catchup_enabled", boolGauge(opts.Enabled))
	metrics.RecordGauge("sse", "catchup_poll_interval_seconds", opts.PollInterval.Seconds())
	metrics.RecordGauge("sse", "catchup_idle_max_interval_seconds", opts.IdleMaxInterval.Seconds())
	metrics.RecordGauge("sse", "catchup_batch_size", float64(opts.BatchSize))
	metrics.RecordGauge("sse", "catchup_max_concurrent_tenant_dbs", float64(opts.MaxConcurrentTenants))
	metrics.RecordGauge("sse", "active_tenant_dbs", float64(activeTenantDBs))
	metrics.RecordGauge("sse", "catchup_registered_listeners", float64(registeredListeners))
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func recordSSECatchupOperation(operation string, result sseOperationResult, start time.Time) {
	recordSSEOperation(operation, result, start)
}

func jitterDuration(base time.Duration, key string) time.Duration {
	if base <= 0 || key == "" {
		return 0
	}
	var h uint64
	for i := 0; i < len(key); i++ {
		h = h*131 + uint64(key[i])
	}
	maxJitter := base / 5
	if maxJitter <= 0 {
		return 0
	}
	return time.Duration(h % uint64(maxJitter))
}
