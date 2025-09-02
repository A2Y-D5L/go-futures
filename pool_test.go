package futures

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNewWorkerPool(t *testing.T) {
	// Test with valid parameters
	p := NewWorkerPool(2, 10)
	if p == nil {
		t.Fatal("NewWorkerPool returned nil")
	}
	p.Close()

	// Test with workers < 1, should set to 1
	p = NewWorkerPool(0, 10)
	p.Close()

	// Test with qsize < 0, should set to 0
	p = NewWorkerPool(2, -1)
	p.Close()
}

func TestSubmitBasic(t *testing.T) {
	p := NewWorkerPool(1, 1)
	defer p.Close()

	executed := make(chan bool, 1)
	job := func() {
		executed <- true
	}

	err := p.Submit(context.Background(), job)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case <-executed:
		// Good
	case <-time.After(time.Second):
		t.Fatal("Job not executed within timeout")
	}
}

func TestSubmitNilJob(t *testing.T) {
	p := NewWorkerPool(1, 1)
	defer p.Close()

	err := p.Submit(context.Background(), nil)
	if err != ErrNilJob {
		t.Fatalf("Expected ErrNilJob, got %v", err)
	}
}

func TestSubmitAfterClose(t *testing.T) {
	p := NewWorkerPool(1, 1)
	p.Close()

	err := p.Submit(context.Background(), func() {})
	if err != ErrPoolClosed {
		t.Fatalf("Expected ErrPoolClosed, got %v", err)
	}
}

func TestSubmitContextDone(t *testing.T) {
	p := NewWorkerPool(1, 0) // qsize 0 to make it easier to test blocking
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := p.Submit(ctx, func() {})
	if err != context.Canceled {
		t.Fatalf("Expected context.Canceled, got %v", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	p := NewWorkerPool(1, 1)
	p.Close()
	p.Close() // Should not panic or error
}

func TestPanicHandler(t *testing.T) {
	panicCaught := make(chan any, 1)
	p := NewWorkerPool(1, 1, WithPanicHandler(func(r any) {
		panicCaught <- r
	}))
	defer p.Close()

	job := func() {
		panic("test panic")
	}

	err := p.Submit(context.Background(), job)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case r := <-panicCaught:
		if r != "test panic" {
			t.Fatalf("Expected 'test panic', got %v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("Panic not caught within timeout")
	}
}

func TestConcurrentSubmits(t *testing.T) {
	p := NewWorkerPool(5, 10)
	defer p.Close()

	const numJobs = 100
	executed := make(chan int, numJobs)
	var wg sync.WaitGroup
	var jobWG sync.WaitGroup

	for i := range numJobs {
		wg.Add(1)
		jobWG.Add(1)
		go func(id int) {
			defer wg.Done()
			job := func() {
				executed <- id
				jobWG.Done()
			}
			err := p.Submit(context.Background(), job)
			if err != nil {
				t.Errorf("Submit failed for job %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
	jobWG.Wait()
	close(executed)

	count := 0
	for range executed {
		count++
	}

	if count != numJobs {
		t.Fatalf("Expected %d jobs executed, got %d", numJobs, count)
	}
}

func TestQueueBlocking(t *testing.T) {
	p := NewWorkerPool(1, 0) // 1 worker, queue size 0
	defer p.Close()

	// Submit a job that blocks the worker
	blockingJob := make(chan bool)
	err := p.Submit(context.Background(), func() { <-blockingJob })
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Next submit should block since worker is busy and queue size 0
	done := make(chan bool)
	go func() {
		err := p.Submit(context.Background(), func() {})
		if err != nil {
			t.Errorf("Submit failed: %v", err)
		}
		done <- true
	}()

	select {
	case <-done:
		t.Fatal("Submit should have blocked")
	case <-time.After(100 * time.Millisecond):
		// Good, it's blocking
	}

	// Unblock the first job
	blockingJob <- true

	select {
	case <-done:
		// Good, now it should have proceeded
	case <-time.After(time.Second):
		t.Fatal("Submit did not proceed after unblocking")
	}
}

func TestWorkerCount(t *testing.T) {
	const workers = 3
	p := NewWorkerPool(workers, 10)
	defer p.Close()

	started := make(chan bool, workers)
	for range workers {
		err := p.Submit(context.Background(), func() {
			started <- true
			time.Sleep(100 * time.Millisecond) // Keep workers busy
		})
		if err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
	}

	// Wait for all workers to start
	for range workers {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("Not all workers started")
		}
	}
}

func TestSubmitContextCancelDuringBlock(t *testing.T) {
	p := NewWorkerPool(1, 0) // 1 worker, queue size 0
	defer p.Close()

	// Submit a job that blocks the worker
	blockingJob := make(chan bool)
	err := p.Submit(context.Background(), func() { <-blockingJob })
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Next submit should block, then cancel ctx
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		err := p.Submit(ctx, func() {})
		done <- err
	}()

	// Wait a bit to ensure it's blocking
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// Should get context.Canceled
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Submit did not return after cancel")
	}

	// Unblock the first job
	blockingJob <- true
}
