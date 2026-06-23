// Package leader provides a TiDB/MySQL GET_LOCK-based leader election mechanism
// so that background schedulers run only on the leader pod in a multi-pod
// deployment. The leader holds a named session-level lock on the shared meta
// database; connection death (pod crash / restart) auto-releases the lock,
// allowing another pod to acquire it.
package leader

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
)

const (
	// defaultLockName is the named lock used for leader election.
	defaultLockName = "drive9:leader"
	// defaultHeartbeatInterval is how often the leader verifies it still holds
	// the lock, and how often non-leaders retry acquisition.
	defaultHeartbeatInterval = 10 * time.Second
	// keepAliveSQL keeps the dedicated connection alive between heartbeats,
	// preventing MySQL wait_timeout from closing it.
	keepAliveSQL = "SELECT 1"
	// getLockSQL acquires a named session lock. Timeout 0 means non-blocking:
	// returns 1 if acquired, 0 if held by another connection.
	getLockSQL = "SELECT GET_LOCK(?, 0)"
	// isUsedLockSQL returns the connection ID holding the named lock, or NULL
	// if no one holds it. Used by the leader to verify it still owns the lock.
	isUsedLockSQL = "SELECT IS_USED_LOCK(?)"
	// releaseLockSQL releases the named session lock.
	releaseLockSQL = "SELECT RELEASE_LOCK(?)"
)

// LeaderChecker is a minimal interface for callers that only need to query
// leadership status (e.g. the tenant pool gating per-tenant FileGCWorker).
type LeaderChecker interface {
	IsLeader() bool
}

// Manager acquires and maintains leadership via a TiDB/MySQL named lock.
// When leadership is gained, OnLead is called; when lost, OnLose is called.
// If disabled (single-pod mode), IsLeader always returns true and no lock
// is acquired.
type Manager struct {
	db       *sql.DB
	lockName string
	interval time.Duration
	disabled bool

	mu       sync.Mutex
	isLeader bool
	onLead   func()
	onLose   func()

	cancel context.CancelFunc
	done   chan struct{}
}

// Option configures a Manager.
type Option func(*Manager)

// WithLockName sets the named lock used for election.
func WithLockName(name string) Option {
	return func(m *Manager) {
		if name != "" {
			m.lockName = name
		}
	}
}

// WithHeartbeatInterval sets the heartbeat / retry interval.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.interval = d
		}
	}
}

// WithDisabled makes the manager always report IsLeader=true without acquiring
// any lock. Use for single-pod deployments (e.g. drive9-server-local).
func WithDisabled() Option {
	return func(m *Manager) {
		m.disabled = true
	}
}

// WithCallbacks sets the leadership-change callbacks. OnLead is called when
// this pod gains leadership; OnLose is called when it loses leadership.
// Callbacks are invoked from the heartbeat goroutine and must be non-blocking.
func WithCallbacks(onLead, onLose func()) Option {
	return func(m *Manager) {
		m.onLead = onLead
		m.onLose = onLose
	}
}

// SetCallbacks sets the leadership-change callbacks. OnLead is called when
// this pod gains leadership; OnLose is called when it loses leadership.
// Callbacks are invoked from the heartbeat goroutine and must be non-blocking.
// Must be called before Start.
func (m *Manager) SetCallbacks(onLead, onLose func()) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.onLead = onLead
	m.onLose = onLose
	m.mu.Unlock()
}

// NewManager creates a leader election manager. db must be a shared database
// connection (the meta/control-plane DB) visible to all pods. If db is nil,
// the manager is created in disabled mode (always leader).
func NewManager(db *sql.DB, opts ...Option) *Manager {
	m := &Manager{
		db:       db,
		lockName: defaultLockName,
		interval: defaultHeartbeatInterval,
	}
	for _, opt := range opts {
		opt(m)
	}
	if db == nil {
		m.disabled = true
	}
	return m
}

// IsLeader reports whether this pod currently holds leadership. Always true
// when the manager is disabled.
func (m *Manager) IsLeader() bool {
	if m == nil || m.disabled {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isLeader
}

// Start begins the leader election heartbeat goroutine. When disabled, it
// immediately invokes onLead and returns (no goroutine). Safe to call once;
// calling Start again after Stop is not supported.
func (m *Manager) Start(ctx context.Context) {
	if m == nil {
		return
	}
	if m.disabled {
		m.mu.Lock()
		m.isLeader = true
		cb := m.onLead
		m.mu.Unlock()
		if cb != nil {
			cb()
		}
		return
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.done = make(chan struct{})

	go m.run(workerCtx, ctx)
}

// Stop releases leadership (if held) and stops the heartbeat goroutine.
func (m *Manager) Stop() {
	if m == nil {
		return
	}
	if m.disabled {
		m.mu.Lock()
		m.isLeader = false
		cb := m.onLose
		m.mu.Unlock()
		if cb != nil {
			cb()
		}
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	if m.done != nil {
		<-m.done
	}
}

func (m *Manager) run(workerCtx context.Context, parentCtx context.Context) {
	defer close(m.done)

	for {
		select {
		case <-workerCtx.Done():
			m.releaseLeadership()
			return
		default:
		}

		acquired, conn, err := m.tryAcquire(workerCtx)
		if err != nil {
			logger.Warn(workerCtx, "leader_acquire_failed",
				zap.String("lock", m.lockName),
				zap.Error(err))
			m.sleep(workerCtx, m.interval)
			continue
		}

		if acquired {
			m.gainLeadership()
			m.holdLeadership(workerCtx, conn)
			// When holdLeadership returns, we lost the lock or ctx was cancelled.
			m.loseLeadership()
			m.releaseConn(conn)
		} else {
			// Someone else holds the lock. Release the conn and retry.
			m.releaseConn(conn)
			m.sleep(workerCtx, m.interval)
		}

		select {
		case <-workerCtx.Done():
			return
		default:
		}
	}
}

// tryAcquire attempts a non-blocking GET_LOCK. Returns (true, conn) if the
// lock was acquired, (false, conn) if someone else holds it. The conn is
// always returned (even on failure) and must be released by the caller.
func (m *Manager) tryAcquire(ctx context.Context) (bool, *sql.Conn, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return false, nil, err
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, getLockSQL, m.lockName).Scan(&got); err != nil {
		_ = conn.Close()
		return false, nil, err
	}
	if !got.Valid || got.Int64 != 1 {
		return false, conn, nil
	}
	return true, conn, nil
}

// holdLeadership periodically verifies that we still hold the named lock by
// checking IS_USED_LOCK and keeping the connection alive. Returns when the
// lock is lost or the context is cancelled.
func (m *Manager) holdLeadership(ctx context.Context, conn *sql.Conn) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Keep the connection alive and verify lock ownership in one round-trip.
			var ownerID sql.NullInt64
			if err := conn.QueryRowContext(ctx, isUsedLockSQL, m.lockName).Scan(&ownerID); err != nil {
				logger.Warn(ctx, "leader_heartbeat_failed",
					zap.String("lock", m.lockName),
					zap.Error(err))
				return
			}
			if !ownerID.Valid || ownerID.Int64 == 0 {
				logger.Info(ctx, "leader_lock_lost",
					zap.String("lock", m.lockName),
					zap.String("reason", "is_used_lock_returned_null_or_zero"))
				return
			}
			// Keep the connection alive to prevent wait_timeout from closing it.
			if _, err := conn.ExecContext(ctx, keepAliveSQL); err != nil {
				logger.Warn(ctx, "leader_keepalive_failed",
					zap.String("lock", m.lockName),
					zap.Error(err))
				return
			}
		}
	}
}

func (m *Manager) releaseConn(conn *sql.Conn) {
	if conn == nil {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var released sql.NullInt64
	_ = conn.QueryRowContext(releaseCtx, releaseLockSQL, m.lockName).Scan(&released)
	_ = conn.Close()
}

func (m *Manager) gainLeadership() {
	m.mu.Lock()
	wasLeader := m.isLeader
	m.isLeader = true
	cb := m.onLead
	m.mu.Unlock()
	if !wasLeader && cb != nil {
		logger.Info(context.Background(), "leader_gained",
			zap.String("lock", m.lockName))
		cb()
	}
}

func (m *Manager) loseLeadership() {
	m.mu.Lock()
	wasLeader := m.isLeader
	m.isLeader = false
	cb := m.onLose
	m.mu.Unlock()
	if wasLeader && cb != nil {
		logger.Info(context.Background(), "leader_lost",
			zap.String("lock", m.lockName))
		cb()
	}
}

func (m *Manager) releaseLeadership() {
	// On Stop: if we were leader, fire onLose. The conn cleanup is handled
	// by the caller (releaseConn is called in run after holdLeadership returns).
	m.mu.Lock()
	wasLeader := m.isLeader
	m.isLeader = false
	cb := m.onLose
	m.mu.Unlock()
	if wasLeader && cb != nil {
		logger.Info(context.Background(), "leader_released",
			zap.String("lock", m.lockName))
		cb()
	}
}

func (m *Manager) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}