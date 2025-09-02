package futures

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

// ErrServiceNameExists is returned by Register when the given name is already in use.
var ErrServiceNameExists = errors.New("service: name already registered")


// typeOf returns the reflect.Type of T.
func typeOf[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}

// Manager owns a shared Pool and a registry of typed Services. It provides:
//
//   • A manager-wide "enqueue window" gate so Close() can deterministically
//     stop new requests across ALL services before draining each service.
//   • A typed registration and lookup API (as top-level generic functions).
//   • Optional observability hooks and best-effort stats.
//
// Manager is safe for concurrent use.
type Manager struct {
	// Shared pool used by all registered services.
	pool     Pool
	ownsPool bool // if true, Manager.Close also closes the pool

	// gate provides unified enqueue window coordination.
	gate

	// closeOnce ensures Close() is idempotent
	closeOnce sync.Once

	mu sync.RWMutex

	// Registry of services keyed by name.
	svcs map[string]*entry

	// Optional hooks for observability/instrumentation.
	onRegister   []func(Info)
	onUnregister []func(Info)
}

// ManagerOption configures Manager construction.
type ManagerOption func(*Manager)

// WithPoolOwnership controls whether Manager.Close() calls pool.Close() after services drain.
// Default: true.
func WithPoolOwnership(owns bool) ManagerOption {
	return func(m *Manager) { m.ownsPool = owns }
}

// WithOnRegister installs a hook invoked after a service is registered.
func WithOnRegister(hook func(Info)) ManagerOption {
	return func(m *Manager) { m.onRegister = append(m.onRegister, hook) }
}

// WithOnUnregister installs a hook invoked after a service is unregistered.
func WithOnUnregister(hook func(Info)) ManagerOption {
	return func(m *Manager) { m.onUnregister = append(m.onUnregister, hook) }
}

// NewManager creates a Manager bound to a shared worker Pool.
// By default, the Manager owns the pool's lifetime (it will close the pool on Close).
func NewManager(pool Pool, opts ...ManagerOption) *Manager {
	m := &Manager{
		pool:     pool,
		ownsPool: true,
		svcs:     make(map[string]*entry),
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// Close performs a deterministic global shutdown:
//   1) Flip global closed gate (reject future requests).
//   2) Wait for all in-flight manager enqueuers to exit.
//   3) Close every registered service (each will drain and wait its inflight).
//   4) Optionally close the shared pool (if Manager owns it).
// Close is idempotent and safe for concurrent use.
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		// 1-2) Reject future enqueues and wait for enqueuers to exit.
		m.gate.CloseAndWait()

		// 3) Close all services. We do this under a snapshot to avoid holding the lock while closing.
		var toClose []*entry
		m.mu.RLock()
		for _, e := range m.svcs {
			toClose = append(toClose, e)
		}
		m.mu.RUnlock()

		var wg sync.WaitGroup
		wg.Add(len(toClose))
		for _, e := range toClose {
			// Close services in parallel to reduce tail latency on shutdown.
			go func(e *entry) {
				defer wg.Done()
				e.closeFn()
			}(e)
		}
		wg.Wait()

		// 4) Finally close the pool if we own it.
		if m.ownsPool {
			// The Pool implementation is expected to drain already-enqueued jobs.
			// At this point, services have stopped submitting new work.
			m.pool.Close()
		}
	})
}

// CloseContext runs Close() but returns early with ctx.Err() if ctx is done
// before shutdown completes. Note: the underlying Close continues in the background.
func (m *Manager) CloseContext(ctx context.Context) error {
	done := make(chan struct{})
	go func() { m.Close(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Info describes a registered service for introspection.
type Info struct {
	Name  string
	InTyp reflect.Type
	OutTyp reflect.Type
}

// ManagerStats provides coarse information about the manager state.
type ManagerStats struct {
	Closed       bool
	ServiceCount int
}

// Stats returns coarse manager stats.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return ManagerStats{
		Closed:       m.gate.IsClosed(),
		ServiceCount: len(m.svcs),
	}
}

// List returns a snapshot of registered services.
func (m *Manager) List() []Info {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Info, 0, len(m.svcs))
	for _, e := range m.svcs {
		out = append(out, Info{
			Name:  e.name,
			InTyp: e.inT,
			OutTyp: e.outT,
		})
	}
	return out
}

// ServiceStats is a best-effort snapshot of a service's state.
// Unknown values are set to -1.
type ServiceStats struct {
	Name             string
	QueueDepth       int
	QueueCapacity    int
	InflightJobs     int64
	Closed           bool
	Disabled         bool
	QueueUtilization float64
	EnqueueRate      float64
	DequeueRate      float64
	ErrorRate        float64
	LatencyP50       time.Duration
	LatencyP95       time.Duration
}

// ServiceStats returns a snapshot of the named service's current state.
// The Service provides native stats including queue depth, capacity, and in-flight jobs.
func (m *Manager) ServiceStats(name string) (ServiceStats, bool) {
	m.mu.RLock()
	e, ok := m.svcs[name]
	m.mu.RUnlock()
	if !ok {
		return ServiceStats{}, false
	}

	// Use the Service's native Stats() method.
	if st, ok := e.maybeStats(); ok {
		// Overlay manager-maintained 'Disabled' status.
		e.mu.RLock()
		disabled := e.disabled
		e.mu.RUnlock()
		st.Disabled = disabled
		return st, true
	}

	// Fallback for services that don't implement Stats().
	return ServiceStats{
		Name:             e.name,
		QueueDepth:       -1,
		QueueCapacity:    -1,
		InflightJobs:     -1,
		Closed:           false,
		Disabled:         func() bool {
			e.mu.RLock()
			defer e.mu.RUnlock()
			return e.disabled
		}(),
		QueueUtilization: -1,
		EnqueueRate:      -1,
		DequeueRate:      -1,
		ErrorRate:        -1,
		LatencyP50:       0,
		LatencyP95:       0,
	}, true
}

// Disable prevents new requests from being accepted for the named service.
// In-flight work continues. Returns false if the service doesn't exist.
func (m *Manager) Disable(name string) bool {
	m.mu.RLock()
	e, ok := m.svcs[name]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	e.mu.Lock()
	e.disabled = true
	e.mu.Unlock()
	return true
}

// Enable allows new requests for the named service after Disable().
func (m *Manager) Enable(name string) bool {
	m.mu.RLock()
	e, ok := m.svcs[name]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	e.mu.Lock()
	e.disabled = false
	e.mu.Unlock()
	return true
}

// Unregister disables, closes, and removes the named service. Returns false if not found.
func (m *Manager) Unregister(name string) bool {
	// Snapshot the entry under lock.
	m.mu.Lock()
	e, ok := m.svcs[name]
	if !ok {
		m.mu.Unlock()
		return false
	}
	delete(m.svcs, name)
	m.mu.Unlock()

	// Ensure no new requests race in via this entry.
	e.mu.Lock()
	e.disabled = true
	e.mu.Unlock()

	// Close the service.
	e.closeFn()

	// Fire hooks.
	info := Info{Name: e.name, InTyp: e.inT, OutTyp: e.outT}
	for _, hook := range m.onUnregister {
		hook(info)
	}
	return true
}

// ===== Registration (typed, via top-level generic functions) =====

// Middleware is a typed function decorator applied around a service's process.
type Middleware[I any, O any] func(next func(context.Context, I) (O, error)) func(context.Context, I) (O, error)

// RegisterOption configures a single typed service at registration time.
type RegisterOption[I any, O any] func(*svcConfig[I, O])

type svcConfig[I any, O any] struct {
	mws      []Middleware[I, O]
	deadline time.Duration // 0 = none
}

// WithMiddleware applies a typed middleware to the service's process function.
func WithMiddleware[I any, O any](mw Middleware[I, O]) RegisterOption[I, O] {
	return func(c *svcConfig[I, O]) { c.mws = append(c.mws, mw) }
}

// WithDeadline wraps the process function with context.WithTimeout(d).
func WithDeadline[I any, O any](d time.Duration) RegisterOption[I, O] {
	return func(c *svcConfig[I, O]) { c.deadline = d }
}

// Register creates a new typed Service[I,O] on the shared Pool and stores it
// in the manager under `name`. Returns a typed Handle that callers should keep.
//
// Registration fails with ErrServiceNameExists if `name` is already taken,
// or ErrClosed if the manager is in the process of shutting down.
func Register[I any, O any](
	m *Manager,
	name string,
	qsize int,
	process func(context.Context, I) (O, error),
	opts ...RegisterOption[I, O],
) (*Handle[I, O], error) {
	// Reject registration if manager is closing/closed.
	if m.gate.IsClosed() {
		return nil, ErrClosed
	}

	// Configure process with typed options (middlewares, deadline).
	cfg := new(svcConfig[I, O])
	for _, o := range opts {
		o(cfg)
	}

	proc := process
	// Apply middlewares first
	for i := len(cfg.mws) - 1; i >= 0; i-- {
		proc = cfg.mws[i](proc)
	}
	// Then wrap with deadline (now outermost) so middleware can observe deadline if desired.
	if cfg.deadline > 0 {
		next := proc
		proc = func(ctx context.Context, in I) (O, error) {
			ctx, cancel := context.WithTimeout(ctx, cfg.deadline)
			defer cancel()
			return next(ctx, in)
		}
	}

	// Construct the typed service atop the shared pool.
	svc := New(m.pool, qsize, proc, WithName[I, O](name))

	// Prepare registry entry.
	e := &entry{
		name:    name,
		inT:   typeOf[I](),
		outT:  typeOf[O](),
		svcAny:  svc,
		closeFn: svc.Close,
		statsFn: func() (ServiceStats, bool) {
			// Use the native Stats() method now available on Service.
			return svc.Stats(), true
		},
	}

	// Insert into registry (unique name).
	m.mu.Lock()
	if m.gate.IsClosed() {
		m.mu.Unlock()
		// Manager closed during construction.
		svc.Close()
		return nil, ErrClosed
	}
	if _, exists := m.svcs[name]; exists {
		m.mu.Unlock()
		svc.Close()
		return nil, ErrServiceNameExists
	}
	m.svcs[name] = e
	m.mu.Unlock()

	// Fire on-register hooks.
	info := Info{Name: e.name, InTyp: e.inT, OutTyp: e.outT}
	for _, hook := range m.onRegister {
		hook(info)
	}

	// Return a typed handle.
	return &Handle[I, O]{name: name, mgr: m, svc: svc}, nil
}

// Lookup returns a typed Handle[I,O] for a registered service `name` if the
// type parameters match the registered (I,O). Otherwise returns (nil, false).
func Lookup[I any, O any](m *Manager, name string) (*Handle[I, O], bool) {
	m.mu.RLock()
	e, ok := m.svcs[name]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if e.inT != typeOf[I]() || e.outT != typeOf[O]() {
		return nil, false
	}
	// Convert back to the typed service pointer.
	svc, ok := e.svcAny.(*Service[I, O])
	if !ok {
		// Should not happen unless registry is corrupted.
		return nil, false
	}
	return &Handle[I, O]{name: name, mgr: m, svc: svc}, true
}

// ===== Typed client façade =====

// Handle is the typed façade that callers keep to interact with a registered service.
type Handle[I any, O any] struct {
	name string
	mgr  *Manager
	svc  *Service[I, O]
}

// Name returns the registered service name.
func (h *Handle[I, O]) Name() string { return h.name }

// Request enters the manager's global enqueue window (to make Close deterministic),
// checks whether the specific service is disabled, and then delegates to the typed
// Service.Request. The returned channel is a single-value future containing exactly
// one Response[O].
func (h *Handle[I, O]) Request(ctx context.Context, in I) <-chan Response[O] {
	// Manager gate: reject if the Manager is closing/closed.
	if h.mgr.gate.IsClosed() {
		out := make(chan Response[O], 1)
		out <- Response[O]{Err: ErrClosed}
		close(out)
		return out
	}
	// Enter the manager enqueue window: ensure Close() can wait for us before draining.
	if !h.mgr.gate.Enter() {
		out := make(chan Response[O], 1)
		out <- Response[O]{Err: ErrClosed}
		close(out)
		return out
	}
	defer h.mgr.gate.Exit()

	// If the service is disabled, fail fast with ErrClosed semantics (ops-friendly).
	if e := h.mgr.getEntry(h.name); e != nil {
		e.mu.RLock()
		disabled := e.disabled
		e.mu.RUnlock()
		if disabled {
			out := make(chan Response[O], 1)
			out <- Response[O]{Err: ErrClosed}
			close(out)
			return out
		}
	}

	// Delegate to the typed service.
	return h.svc.Request(ctx, in)
}

// Call is a convenience helper that waits for the single response.
func (h *Handle[I, O]) Call(ctx context.Context, in I) (O, error) {
	resp := <-h.Request(ctx, in)
	return resp.Value, resp.Err
}

// Stats returns best-effort stats for this handle's service.
func (h *Handle[I, O]) Stats() ServiceStats {
	if st, ok := h.mgr.ServiceStats(h.name); ok {
		return st
	}
	// Unknown; fabricate a minimal record.
	return ServiceStats{
		Name:             h.name,
		QueueDepth:       -1,
		QueueCapacity:    -1,
		InflightJobs:     -1,
		Closed:           false,
		Disabled:         false,
		QueueUtilization: -1,
		EnqueueRate:      -1,
		DequeueRate:      -1,
		ErrorRate:        -1,
		LatencyP50:       0,
		LatencyP95:       0,
	}
}

// ===== Registry entry (internal) =====

type entry struct {
	name   string
	inT  reflect.Type
	outT reflect.Type

	// Untyped pointer to the concrete Service[I,O].
	svcAny any

	// Lifecycle/stat taps (avoid generics here).
	closeFn func()
	statsFn func() (ServiceStats, bool)

	// Optional soft-gating for ops: Disable/Enable.
	mu       sync.RWMutex
	disabled bool
}

func (e *entry) maybeStats() (ServiceStats, bool) {
	if e.statsFn == nil {
		return ServiceStats{}, false
	}
	return e.statsFn()
}

// getEntry safely fetches an entry by name; nil if not found.
func (m *Manager) getEntry(name string) *entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.svcs[name]
}
