package futures

import (
	"context"
	"errors"
	"sync"
)

// ErrPoolClosed is returned when Submit is called after Close() has begun,
// or once the pool is already closed.
var ErrPoolClosed = errors.New("worker pool closed")

// ErrNilJob is returned when Submit is called with a nil job function.
var ErrNilJob = errors.New("pool: nil job submitted")

// Pool defines the public API for the worker pool.
type Pool interface {
	// Submit enqueues a job to be executed by the pool.
	//
	// Behavior:
	//   - Returns ErrNilJob if job is nil.
	//   - Returns ErrPoolClosed if the pool is closed (or closing).
	//   - Returns ctx.Err() if ctx is canceled before the job can be enqueued
	//     (e.g., while waiting for queue capacity).
	//   - Otherwise returns nil and guarantees that the job will be run exactly once.
	Submit(ctx context.Context, job func()) error

	// Close gracefully shuts the pool down:
	//   1) Rejects further Submit calls.
	//   2) Waits for in-flight submitters to exit the enqueue window.
	//   3) Closes the internal queue so workers drain the remaining jobs.
	//   4) Waits for all workers to finish.
	//
	// Close is idempotent and safe for concurrent use.
	Close()
}

// PoolOption configures optional behavior of the pool.
type PoolOption func(*pool)

// WithPanicHandler installs a handler invoked with the recovered value
// of any panic that occurs while running a job.
func WithPanicHandler(h func(any)) PoolOption {
	return func(p *pool) { p.panicHandler = h }
}

// NewWorkerPool constructs a new worker pool with the given number of workers and queue size.
//
//   workers: number of worker goroutines (>= 1; values < 1 are set to 1)
//   qsize:   maximum number of queued jobs (>= 0; values < 0 are set to 0)
//
// The returned Pool is ready for use. Call Close() to shut it down gracefully.
func NewWorkerPool(workers, qsize int, opts ...PoolOption) Pool {
	if workers < 1 {
		workers = 1
	}
	if qsize < 0 {
		qsize = 0
	}

	p := &pool{
		work: make(chan func(), qsize),
	}

	for _, o := range opts {
		o(p)
	}

	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}

	return p
}

// pool is the concrete Pool implementation.
type pool struct {
	// work carries jobs to workers. It is intentionally kept open for the pool's lifetime
	// and only closed by Close() after all enqueuers leave the enqueue window.
	work chan func()

	// wg waits for worker goroutines to exit.
	wg sync.WaitGroup

	// gate provides unified enqueue window coordination.
	gate

	// Optional panic handler.
	panicHandler func(any)

	// closeOnce ensures Close is idempotent.
	closeOnce sync.Once
}

// Submit implements Pool.Submit.
func (p *pool) Submit(ctx context.Context, job func()) error {
	if job == nil {
		// Reject nil jobs to prevent silent bugs.
		return ErrNilJob
	}

	// Fast-path rejections: context already canceled.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Enter the ENQUEUE WINDOW.
	if !p.gate.Enter() {
		return ErrPoolClosed
	}
	defer p.gate.Exit()

	// Stage 2: actually try to enqueue, or fail if ctx cancels first.
	select {
	case p.work <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close implements Pool.Close.
func (p *pool) Close() {
	p.closeOnce.Do(func() {
		p.gate.CloseAndWait()

		// 3) Close the work channel so workers drain remaining jobs then exit.
		close(p.work)

		// 4) Wait for workers to finish.
		p.wg.Wait()
	})
}

// worker runs jobs until the work channel is closed and drained.
func (p *pool) worker() {
	defer p.wg.Done()
	for job := range p.work {
		p.run(job)
	}
}

// run executes a single job with panic safety.
func (p *pool) run(job func()) {
	defer func() {
		if r := recover(); r != nil {
			if p.panicHandler != nil {
				p.panicHandler(r)
			}
			// By default, swallow the panic and keep the worker alive.
		}
	}()
	job()
}
