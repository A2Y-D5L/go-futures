package futures

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewService(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	s := New(pool, 5, process)
	if s == nil {
		t.Fatal("New returned nil")
	}
	s.Close()

	// Test with qsize < 0, should be clamped to 0
	s = New(pool, -1, process)
	if cap(s.reqCh) != 0 {
		t.Fatalf("Expected queue capacity 0, got %d", cap(s.reqCh))
	}
	s.Close()
}

func TestServiceRequestBasic(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "result", nil
	}

	s := New(pool, 1, process)
	defer s.Close()

	respCh := s.Request(context.Background(), 42)

	select {
	case resp := <-respCh:
		if resp.Err != nil {
			t.Fatalf("Unexpected error: %v", resp.Err)
		}
		if resp.Value != "result" {
			t.Fatalf("Expected 'result', got %s", resp.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestServiceRequestWithError(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "", errors.New("process error")
	}

	s := New(pool, 1, process)
	defer s.Close()

	respCh := s.Request(context.Background(), 42)

	select {
	case resp := <-respCh:
		if resp.Err == nil {
			t.Fatal("Expected error, got nil")
		}
		if resp.Err.Error() != "process error" {
			t.Fatalf("Expected 'process error', got %s", resp.Err.Error())
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestServiceRequestContextCancel(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "result", nil
	}

	s := New(pool, 1, process)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	respCh := s.Request(ctx, 42)

	// Cancel before processing
	cancel()

	select {
	case resp := <-respCh:
		if resp.Err != context.Canceled {
			t.Fatalf("Expected context.Canceled, got %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestServiceClose(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "result", nil
	}

	s := New(pool, 1, process)

	// Make a request
	respCh := s.Request(context.Background(), 42)

	// Close the service
	s.Close()

	// Should get ErrClosed for pending request
	select {
	case resp := <-respCh:
		if resp.Err != ErrClosed {
			t.Fatalf("Expected ErrClosed, got %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}

	// New requests after close should fail
	respCh2 := s.Request(context.Background(), 43)
	select {
	case resp := <-respCh2:
		if resp.Err != ErrClosed {
			t.Fatalf("Expected ErrClosed, got %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestServiceCloseIdempotent(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "result", nil
	}

	s := New(pool, 1, process)
	s.Close()
	s.Close() // Should not panic
}

func TestServiceConcurrentRequests(t *testing.T) {
	pool := NewWorkerPool(5, 10)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		time.Sleep(1 * time.Millisecond) // Simulate work
		return "result", nil
	}

	s := New(pool, 10, process)
	defer s.Close()

	const numRequests = 50
	responses := make(chan Response[string], numRequests)

	var wg sync.WaitGroup
	for i := range numRequests {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			respCh := s.Request(context.Background(), id)
			resp := <-respCh
			responses <- resp
		}(i)
	}

	wg.Wait()
	close(responses)

	count := 0
	for resp := range responses {
		if resp.Err != nil {
			t.Errorf("Unexpected error: %v", resp.Err)
		}
		count++
	}

	if count != numRequests {
		t.Fatalf("Expected %d responses, got %d", numRequests, count)
	}
}

func TestServiceQueueFull(t *testing.T) {
	pool := NewWorkerPool(1, 0)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		time.Sleep(10 * time.Millisecond) // Slow processing
		return "result", nil
	}

	s := New(pool, 0, process) // Queue size 0
	defer s.Close()

	// First request should succeed
	respCh1 := s.Request(context.Background(), 1)

	// Second request should block or fail due to queue full
	var respCh2 <-chan Response[string]
	done := make(chan bool)
	go func() {
		respCh2 = s.Request(context.Background(), 2)
		<-respCh2
		done <- true
	}()

	// Wait a bit to ensure it's blocking
	time.Sleep(20 * time.Millisecond)

	// First response should be received
	select {
	case resp := <-respCh1:
		if resp.Err != nil {
			t.Fatalf("Unexpected error: %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("First response not received")
	}

	// Wait for the second request to complete
	select {
	case <-done:
		// Good
	case <-time.After(time.Second):
		t.Fatal("Second request did not complete")
	}

	// Second response should be received
	select {
	case resp := <-respCh2:
		if resp.Err != nil {
			t.Fatalf("Unexpected error: %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Second response not received")
	}
}

func TestServiceStats(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "result", nil
	}

	s := New(pool, 1, process, WithName[int, string]("test-service"))
	defer s.Close()

	// Make some requests
	for i := range 5 {
		respCh := s.Request(context.Background(), i)
		<-respCh
	}

	stats := s.Stats()

	if stats.Name != "test-service" {
		t.Fatalf("Expected name 'test-service', got %s", stats.Name)
	}

	if stats.QueueDepth != 0 {
		t.Fatalf("Expected queue depth 0, got %d", stats.QueueDepth)
	}

	if stats.EnqueueRate <= 0 {
		t.Fatalf("Expected positive enqueue rate, got %f", stats.EnqueueRate)
	}

	if stats.DequeueRate <= 0 {
		t.Fatalf("Expected positive dequeue rate, got %f", stats.DequeueRate)
	}

	if stats.LatencyP50 <= 0 {
		t.Fatalf("Expected positive P50 latency, got %v", stats.LatencyP50)
	}
}

func TestServicePanicInProcess(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		panic("test panic")
	}

	s := New(pool, 1, process)
	defer s.Close()

	respCh := s.Request(context.Background(), 42)

	select {
	case resp := <-respCh:
		if resp.Err == nil {
			t.Fatal("Expected error due to panic, got nil")
		}
		if !strings.Contains(resp.Err.Error(), "panic in Service.process: test panic") {
			t.Fatalf("Expected panic error, got %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestServiceRequestAfterClose(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "result", nil
	}

	s := New(pool, 1, process)
	s.Close()

	respCh := s.Request(context.Background(), 42)

	select {
	case resp := <-respCh:
		if resp.Err != ErrClosed {
			t.Fatalf("Expected ErrClosed, got %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestServiceWithCanceledContextBeforeRequest(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "result", nil
	}

	s := New(pool, 1, process)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	respCh := s.Request(ctx, 42)

	select {
	case resp := <-respCh:
		if resp.Err != context.Canceled {
			t.Fatalf("Expected context.Canceled, got %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestServiceMultipleClose(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "result", nil
	}

	s := New(pool, 1, process)

	// Close multiple times
	s.Close()
	s.Close()
	s.Close()

	// Should still work
	respCh := s.Request(context.Background(), 42)
	select {
	case resp := <-respCh:
		if resp.Err != ErrClosed {
			t.Fatalf("Expected ErrClosed, got %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestServiceStatsAfterClose(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "result", nil
	}

	s := New(pool, 1, process)

	// Make a request
	respCh := s.Request(context.Background(), 42)
	<-respCh

	s.Close()

	stats := s.Stats()

	if !stats.Closed {
		t.Fatal("Expected service to be closed")
	}

	if stats.InflightJobs != 0 {
		t.Fatalf("Expected 0 in-flight jobs, got %d", stats.InflightJobs)
	}
}

func TestServiceErrorRate(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	callCount := 0
	process := func(ctx context.Context, in int) (string, error) {
		callCount++
		if callCount%2 == 0 {
			return "", errors.New("even number error")
		}
		return "result", nil
	}

	s := New(pool, 1, process)
	defer s.Close()

	// Make 4 requests: 2 success, 2 errors
	for i := range 4 {
		respCh := s.Request(context.Background(), i)
		<-respCh
	}

	time.Sleep(1000 * time.Millisecond)

	stats := s.Stats()

	if stats.ErrorRate <= 0 {
		t.Fatalf("Expected positive error rate, got %f", stats.ErrorRate)
	}

	// Should be around 2.0 (2 errors per second)
	expectedRate := 2.0
	tolerance := 0.5
	if stats.ErrorRate < expectedRate-tolerance || stats.ErrorRate > expectedRate+tolerance {
		t.Fatalf("Expected error rate around %f, got %f", expectedRate, stats.ErrorRate)
	}
}

func TestServiceQueueUtilization(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "result", nil
	}

	s := New(pool, 2, process)
	defer s.Close()

	// Fill the queue
	respCh1 := s.Request(context.Background(), 1)

	// Check utilization with one request
	stats := s.Stats()
	if stats.QueueUtilization != 0.5 {
		t.Fatalf("Expected queue utilization 0.5, got %f", stats.QueueUtilization)
	}

	respCh2 := s.Request(context.Background(), 2)

	// Check utilization while queue is full
	stats = s.Stats()
	if stats.QueueUtilization != 1.0 {
		t.Fatalf("Expected queue utilization 1.0, got %f", stats.QueueUtilization)
	}

	// Wait for one to complete
	<-respCh1

	// Check utilization after one completes
	stats = s.Stats()
	if stats.QueueUtilization != 0.0 {
		t.Fatalf("Expected queue utilization 0.0, got %f", stats.QueueUtilization)
	}

	<-respCh2
}

func TestServiceWithName(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	defer pool.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "result", nil
	}

	s := New(pool, 1, process, WithName[int, string]("my-service"))
	defer s.Close()

	if s.name != "my-service" {
		t.Fatalf("Expected name 'my-service', got %s", s.name)
	}

	stats := s.Stats()
	if stats.Name != "my-service" {
		t.Fatalf("Expected stats name 'my-service', got %s", stats.Name)
	}
}
