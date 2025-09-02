package futures

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Response is the single reply value placed on the channel returned by Request.
// If Err is non-nil, Value should be considered invalid.
type Response[T any] struct {
	Value T
	Err   error
}

// ErrClosed is returned when a Request is made after the service has been closed
// (or while Close() is in progress after the "closed" gate is flipped).
var ErrClosed = errors.New("service closed")

// Service is a generic, worker-pool-backed request/reply service.
//
// I is the input type, O is the output type. Callers submit requests via Request(ctx, in)
// and receive a <-chan Response[O]. The service invokes the provided process function to
// compute outputs. Execution is delegated to an external worker pool, enabling multiple
// services to share a common pool.
//
// The service is safe for concurrent use.
type Service[I any, O any] struct {
	process func(context.Context, I) (O, error)
	pool    Pool

	// A typed work queue that the Service controls. Keeping this queue internal to
	// the Service (instead of sending directly to the shared pool) allows us to:
	//   • Apply per-service backpressure (qsize).
	//   • Preserve deterministic drain semantics at Close() (we can fail leftovers).
	reqCh chan request[I, O] // Intentionally never closed by Request(); drained by Close().

	// Broadcast: signals shutdown to internal dispatchers and cancels Submit waits.
	done chan struct{}

	// Tracks jobs that have been successfully submitted to the pool and will run.
	jobsWG sync.WaitGroup

	// Tracks internal dispatcher goroutines.
	dispatchWG sync.WaitGroup

	// gate provides unified enqueue window coordination.
	gate

	// closeOnce ensures Close is idempotent.
	closeOnce sync.Once

	// Optional service name for stats.
	name string

	// Atomic counter for in-flight jobs.
	inflightJobs atomic.Int64

	// Metrics for enhanced stats.
	startTime    time.Time
	enqueueCount atomic.Int64
	dequeueCount atomic.Int64
	errorCount   atomic.Int64
	latencies    []time.Duration
	latIndex     int
	maxLat       int
	metricsMu    sync.Mutex
}

type request[I any, O any] struct {
	ctx   context.Context
	in    I
	out   chan Response[O] // single-value "future" (buffered)
	start time.Time
}

// ServiceOption configures optional behavior of the service.
type ServiceOption[I any, O any] func(*Service[I, O])

// WithName sets an optional name for the service (used in stats).
func WithName[I any, O any](name string) ServiceOption[I, O] {
	return func(s *Service[I, O]) { s.name = name }
}

// New constructs a Service that uses the provided shared worker pool.
//
//   - pool:  a shared worker pool (e.g., from package `worker`) used to run work.
//   - qsize: per-service request queue capacity (>= 0; values < 0 are clamped to 0).
//   - process: the function that produces outputs from inputs (should be ctx-aware).
//   - opts: optional configuration.
//
// The Service spawns a lightweight internal dispatcher goroutine that pulls from the
// Service’s typed queue and submits jobs to the pool. Close() is idempotent.
func New[I any, O any](pool Pool, qsize int, process func(context.Context, I) (O, error), opts ...ServiceOption[I, O]) *Service[I, O] {
	if qsize < 0 {
		qsize = 0
	}

	s := &Service[I, O]{
		process: process,
		pool:    pool,
		reqCh:   make(chan request[I, O], qsize),
		done:    make(chan struct{}),
	}

	// Initialize metrics
	s.startTime = time.Now()
	s.latencies = make([]time.Duration, 0, 1000)
	s.maxLat = 1000

	for _, opt := range opts {
		opt(s)
	}

	// Start exactly one dispatcher. This is sufficient because:
	//   • The per-service queue (reqCh) provides backlog buffering.
	//   • The shared pool controls actual concurrency/parallelism.
	// If you want to increase submission concurrency under extreme pressure,
	// you can trivially add more dispatchers here (they remain race-free).
	s.dispatchWG.Add(1)
	go s.dispatch()

	return s
}

// Request enqueues a request and returns a channel that will receive exactly one Response[O].
// The returned channel is buffered (size 1) so the worker can deliver the reply even if
// the caller has already moved on (to avoid goroutine leaks).
//
// Behavior:
//   - If the service is closed (or closing), the returned channel contains {Err: ErrClosed}.
//   - If ctx is already canceled (or cancels before enqueue), the channel contains {Err: ctx.Err()}.
//   - Otherwise the request is enqueued and later dispatched to the pool; the result is delivered on the channel.
//   - The returned channel is closed after sending the single Response.
//
// ENQUEUE GATE (critical to correctness):
//   - We hold mu.RLock() to read "closed" and (if open) increment enqWG before attempting to enqueue.
//     This prevents Close() from flipping "closed" between the check and enqWG.Add(1).
//   - Close() flips "closed" under mu.Lock(), then waits for enqWG to reach zero. That guarantees
//     there are no writers racing with the subsequent drain.
func (s *Service[I, O]) Request(ctx context.Context, in I) <-chan Response[O] {
	out := make(chan Response[O], 1) // single-value future

	// Fast-path rejections: closed or already-canceled context.
	if s.gate.IsClosed() {
		out <- Response[O]{Err: ErrClosed}
		close(out)
		return out
	}
	if err := ctx.Err(); err != nil {
		out <- Response[O]{Err: err}
		close(out)
		return out
	}

	// Enter the ENQUEUE WINDOW: count ourselves so Close() can wait for us.
	if !s.gate.Enter() {
		// Close might have flipped while we raced between the previous RUnlock and this RLock.
		out <- Response[O]{Err: ErrClosed}
		close(out)
		return out
	}
	defer s.gate.Exit()

	req := request[I, O]{ctx: ctx, in: in, out: out, start: time.Now()}
	s.enqueueCount.Add(1)

	// Stage 2: attempt to enqueue or fail if shutdown/cancel occurs.
	// This select provides backpressure when the queue is full.
	select {
	case s.reqCh <- req:
		// Enqueued successfully; the dispatcher (or the drainer during Close) will deliver the reply.
	case <-s.done:
		// Shutdown began; reject this request.
		s.fail(req, ErrClosed)
	case <-ctx.Done():
		// Caller canceled before we could enqueue.
		s.fail(req, ctx.Err())
	}

	return out
}

// Close signals the service to stop and waits for all in-flight work to finish.
// Any requests still pending in the service’s queue (that were not yet submitted
// to the pool) are drained and each receives ErrClosed. Requests already submitted
// to the pool (i.e., in-flight) will finish normally.
//
// IMPORTANT: reqCh remains OPEN to callers at all times; we coordinate a safe shutdown by:
//  1. Flipping closed=true (reject future Request()).
//  2. Waiting for all in-flight enqueuers (enqWG) to finish Stage-2.
//  3. Signaling internal dispatchers via done (they stop selecting reqCh; any Submit waits get canceled).
//  4. Draining reqCh deterministically and failing leftover requests with ErrClosed.
//  5. Waiting for dispatcher(s) to exit and for any already-submitted jobs to the pool to finish.
func (s *Service[I, O]) Close() {
	s.closeOnce.Do(func() {
		// 1-2) Reject future enqueues and wait for enqueuers to exit.
		s.gate.CloseAndWait()

		// 3) Tell dispatchers to wind down (they stop picking new work ASAP).
		close(s.done)

		// 4) Drain any leftover requests deterministically.
		//    Because (a) closed=true and (b) enqWG==0, there can be no further writers to reqCh.
		for {
			select {
			case req := <-s.reqCh:
				s.fail(req, ErrClosed)
			default:
				goto drained
			}
		}
	drained:

		// 5a) Wait for dispatcher(s) to exit (ensures no more Submit attempts are in flight).
		s.dispatchWG.Wait()

		// 5b) Wait for any already-submitted jobs (to the shared pool) to finish.
		s.jobsWG.Wait()
	})
}

// dispatch pulls typed requests from reqCh and submits jobs to the shared pool.
//
// Subtlety: Submit(ctx, job) may block while the pool’s internal queue is full.
// To make Close() responsive, we pass a derived context that is canceled when
// the Service begins closing (s.done is closed). This ensures a dispatcher that’s
// stuck in Submit unblocks promptly on Close() and fails the request with ErrClosed.
func (s *Service[I, O]) dispatch() {
	defer s.dispatchWG.Done()

	for {
		select {
		case <-s.done:
			// Stop selecting reqCh; Close() will do a deterministic drain.
			return
		case req := <-s.reqCh:
			// Prepare a submission context canceled either by the caller or by Service close.
			ctxSubmit, cancel := context.WithCancel(req.ctx)
			// Ensure this helper goroutine terminates regardless of path taken.
			go func() {
				select {
				case <-s.done:
					cancel() // Cancel Submit when Service closes.
				case <-ctxSubmit.Done():
					// Caller canceled or job already submitted and completed (cancel called in job).
				}
			}()

			// Build the job that runs inside the shared pool worker.
			job := func() {
				defer cancel()               // Ensure the merge goroutine exits once the job starts.
				defer s.jobsWG.Done()        // Paired with Add(1) before successful Submit.
				defer s.inflightJobs.Add(-1) // Decrement in-flight counter.

				// Panic-safety: convert panics in user code into an error reply.
				defer func() {
					if r := recover(); r != nil {
						s.fail(req, fmt.Errorf("panic in Service.process: %v", r))
					}
				}()

				s.handle(req)
			}

			// Count the job as in-flight before submitting to ensure Close() waits for it.
			s.inflightJobs.Add(1)
			s.jobsWG.Add(1)

			// Try to submit to the pool.
			if err := s.pool.Submit(ctxSubmit, job); err != nil {
				// Submit failed, so decrement the count and handle the error.
				s.jobsWG.Done()
				s.inflightJobs.Add(-1)
				// Prefer ErrClosed if the Service is (now) closed; otherwise propagate ctx error.
				if s.isClosed() {
					s.fail(req, ErrClosed)
				} else {
					s.fail(req, err)
				}
				cancel()
				continue
			}
		}
	}
}

// handle performs the request-specific processing and delivers a single Response to req.out.
func (s *Service[I, O]) handle(req request[I, O]) {
	// If the caller's context is already canceled, fail fast.
	if err := req.ctx.Err(); err != nil {
		s.fail(req, err)
		return
	}

	// Perform the work. The process function is expected to be context-aware.
	val, err := s.process(req.ctx, req.in)

	// Compute latency
	latency := time.Since(req.start)
	s.metricsMu.Lock()
	if len(s.latencies) < s.maxLat {
		s.latencies = append(s.latencies, latency)
	} else {
		s.latencies[s.latIndex] = latency
		s.latIndex = (s.latIndex + 1) % s.maxLat
	}
	s.metricsMu.Unlock()
	s.dequeueCount.Add(1)
	if err != nil {
		s.errorCount.Add(1)
	}

	// If the context was canceled during processing and the worker didn't return an error,
	// prefer the context error for clarity.
	if req.ctx.Err() != nil && err == nil {
		err = req.ctx.Err()
		s.errorCount.Add(1) // since err was nil but now set
	}

	req.out <- Response[O]{Value: val, Err: err}
	close(req.out)
}

// fail replies to a request with the provided error.
func (s *Service[I, O]) fail(req request[I, O], err error) {
	s.errorCount.Add(1)
	req.out <- Response[O]{Err: err}
	close(req.out)
}

// isClosed reports whether the service has begun closing.
func (s *Service[I, O]) isClosed() bool {
	return s.gate.IsClosed()
}

// Stats returns a snapshot of the service's current state.
func (s *Service[I, O]) Stats() ServiceStats {
	uptime := time.Since(s.startTime).Seconds()
	if uptime == 0 {
		uptime = 1 // avoid division by zero
	}
	enqueueRate := float64(s.enqueueCount.Load()) / uptime
	dequeueRate := float64(s.dequeueCount.Load()) / uptime
	errorRate := float64(s.errorCount.Load()) / uptime

	var queueUtil float64
	if cap(s.reqCh) > 0 {
		queueUtil = float64(len(s.reqCh)) / float64(cap(s.reqCh))
	}

	s.metricsMu.Lock()
	lats := make([]time.Duration, len(s.latencies))
	copy(lats, s.latencies)
	s.metricsMu.Unlock()

	slices.Sort(lats)
	n := len(lats)
	var p50, p95 time.Duration
	if n > 0 {
		p50 = lats[n/2]
		if n >= 20 { // for 95th percentile, need at least some samples
			p95 = lats[(n*95)/100]
		}
	}

	return ServiceStats{
		Name:             s.name,
		QueueDepth:       len(s.reqCh),
		QueueCapacity:    cap(s.reqCh),
		InflightJobs:     s.inflightJobs.Load(),
		Closed:           s.gate.IsClosed(),
		Disabled:         false, // Service-level disable not supported; this is manager-level
		QueueUtilization: queueUtil,
		EnqueueRate:      enqueueRate,
		DequeueRate:      dequeueRate,
		ErrorRate:        errorRate,
		LatencyP50:       p50,
		LatencyP95:       p95,
	}
}
