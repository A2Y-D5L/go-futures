package futures

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	// Test basic creation
	pool := NewWorkerPool(2, 10)
	m := NewManager(pool)
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
	if m.pool != pool {
		t.Fatal("Pool not set correctly")
	}
	if !m.ownsPool {
		t.Fatal("Should own pool by default")
	}
	// Don't call m.Close() here to avoid hanging

	// Test with options
	pool2 := NewWorkerPool(2, 10)
	m = NewManager(pool2, WithPoolOwnership(false))
	if m.ownsPool {
		t.Fatal("Should not own pool when option set")
	}
	// Don't call m.Close() here to avoid hanging
	pool2.Close() // So we close it manually
}

func TestManagerRegister(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	// Test successful registration
	handle, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if handle == nil {
		t.Fatal("Handle is nil")
	}
	if handle.Name() != "test-service" {
		t.Fatalf("Expected name 'test-service', got %s", handle.Name())
	}

	// Test duplicate registration
	_, err = Register(m, "test-service", 5, process)
	if err != ErrServiceNameExists {
		t.Fatalf("Expected ErrServiceNameExists, got %v", err)
	}

	// Test registration after close
	m.Close()
	_, err = Register(m, "new-service", 5, process)
	if err != ErrClosed {
		t.Fatalf("Expected ErrClosed, got %v", err)
	}
}

func TestManagerLookup(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	// Register service
	_, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Test successful lookup
	handle, ok := Lookup[int, string](m, "test-service")
	if !ok {
		t.Fatal("Lookup failed")
	}
	if handle == nil {
		t.Fatal("Handle is nil")
	}

	// Test lookup with wrong types
	handle2, ok := Lookup[string, int](m, "test-service")
	if ok || handle2 != nil {
		t.Fatal("Lookup should fail with wrong types")
	}

	// Test lookup of non-existent service
	handle3, ok := Lookup[int, string](m, "non-existent")
	if ok || handle3 != nil {
		t.Fatal("Lookup should fail for non-existent service")
	}
}

func TestHandleRequest(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	handle, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Test successful request
	respCh := handle.Request(context.Background(), 42)
	select {
	case resp := <-respCh:
		if resp.Err != nil {
			t.Fatalf("Unexpected error: %v", resp.Err)
		}
		if resp.Value != "processed" {
			t.Fatalf("Expected 'processed', got %s", resp.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}

	// Test request with error
	processErr := func(ctx context.Context, in int) (string, error) {
		return "", errors.New("process error")
	}
	handle2, _ := Register(m, "error-service", 5, processErr)
	respCh2 := handle2.Request(context.Background(), 42)
	select {
	case resp := <-respCh2:
		if resp.Err == nil {
			t.Fatal("Expected error")
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestHandleCall(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	handle, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Test successful call
	result, err := handle.Call(context.Background(), 42)
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if result != "processed" {
		t.Fatalf("Expected 'processed', got %s", result)
	}
}

func TestManagerDisableEnable(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	_, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Test disable
	if !m.Disable("test-service") {
		t.Fatal("Disable should succeed")
	}

	// Test disable non-existent
	if m.Disable("non-existent") {
		t.Fatal("Disable should fail for non-existent service")
	}

	// Test enable
	if !m.Enable("test-service") {
		t.Fatal("Enable should succeed")
	}

	// Test enable non-existent
	if m.Enable("non-existent") {
		t.Fatal("Enable should fail for non-existent service")
	}
}

func TestManagerUnregister(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	_, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Test unregister
	if !m.Unregister("test-service") {
		t.Fatal("Unregister should succeed")
	}

	// Test unregister non-existent
	if m.Unregister("non-existent") {
		t.Fatal("Unregister should fail for non-existent service")
	}

	// Test lookup after unregister
	handle, ok := Lookup[int, string](m, "test-service")
	if ok || handle != nil {
		t.Fatal("Lookup should fail after unregister")
	}
}

func TestManagerClose(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	handle, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Test close
	m.Close()

	// Test stats after close
	stats := m.Stats()
	if !stats.Closed {
		t.Fatal("Manager should be closed")
	}

	// Test registration after close
	_, err = Register(m, "new-service", 5, process)
	if err != ErrClosed {
		t.Fatalf("Expected ErrClosed, got %v", err)
	}

	// Test request after close
	respCh := handle.Request(context.Background(), 42)
	select {
	case resp := <-respCh:
		if resp.Err != ErrClosed {
			t.Fatalf("Expected ErrClosed, got %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

func TestManagerCloseContext(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))

	process := func(ctx context.Context, in int) (string, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "processed", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	handle, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Start a request that will take time to process
	go handle.Request(context.Background(), 42)

	// Give a moment for the request to start processing
	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = m.CloseContext(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, got %v", err)
	}
}

func TestManagerStats(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	_, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	stats := m.Stats()
	if stats.ServiceCount != 1 {
		t.Fatalf("Expected 1 service, got %d", stats.ServiceCount)
	}
	if stats.Closed {
		t.Fatal("Manager should not be closed")
	}
}

func TestManagerList(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	_, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	infos := m.List()
	if len(infos) != 1 {
		t.Fatalf("Expected 1 service, got %d", len(infos))
	}

	info := infos[0]
	if info.Name != "test-service" {
		t.Fatalf("Expected name 'test-service', got %s", info.Name)
	}
	if info.InTyp != reflect.TypeOf(0) {
		t.Fatal("InTyp should be int")
	}
	if info.OutTyp != reflect.TypeOf("") {
		t.Fatal("OutTyp should be string")
	}
}

func TestManagerServiceStats(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	_, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Test existing service
	stats, ok := m.ServiceStats("test-service")
	if !ok {
		t.Fatal("ServiceStats should succeed")
	}
	if stats.Name != "test-service" {
		t.Fatalf("Expected name 'test-service', got %s", stats.Name)
	}

	// Test non-existent service
	_, ok = m.ServiceStats("non-existent")
	if ok {
		t.Fatal("ServiceStats should fail for non-existent service")
	}
}

func TestHandleStats(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	handle, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	stats := handle.Stats()
	if stats.Name != "test-service" {
		t.Fatalf("Expected name 'test-service', got %s", stats.Name)
	}
}

func TestManagerOptions(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()

	var registerCalled, unregisterCalled bool

	m := NewManager(pool, WithPoolOwnership(false),
		WithOnRegister(func(info Info) {
			registerCalled = true
		}),
		WithOnUnregister(func(info Info) {
			unregisterCalled = true
		}),
	)
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	_, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registerCalled {
		t.Fatal("OnRegister hook not called")
	}

	m.Unregister("test-service")
	if !unregisterCalled {
		t.Fatal("OnUnregister hook not called")
	}
}

func TestRegisterOptions(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	// Test with middleware
	middlewareCalled := false
	mw := func(next func(context.Context, int) (string, error)) func(context.Context, int) (string, error) {
		return func(ctx context.Context, in int) (string, error) {
			middlewareCalled = true
			return next(ctx, in)
		}
	}

	handle, err := Register(m, "test-service", 5, process, WithMiddleware(mw))
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	handle.Call(context.Background(), 42)
	if !middlewareCalled {
		t.Fatal("Middleware not called")
	}

	// Test with deadline
	handle2, err := Register(m, "deadline-service", 5, func(ctx context.Context, in int) (string, error) {
		select {
		case <-time.After(200 * time.Millisecond):
			return "processed", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, WithDeadline[int, string](100*time.Millisecond))
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	_, err = handle2.Call(context.Background(), 42)
	if err != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, got %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	pool := NewWorkerPool(10, 100)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	// Register multiple services concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := Register(m, fmt.Sprintf("service-%d", id), 10, process)
			if err != nil {
				t.Errorf("Register failed: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Test concurrent requests
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			handle, ok := Lookup[int, string](m, fmt.Sprintf("service-%d", id))
			if !ok {
				t.Errorf("Lookup failed for service-%d", id)
				return
			}
			for j := 0; j < 5; j++ {
				_, err := handle.Call(context.Background(), j)
				if err != nil {
					t.Errorf("Call failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestDisabledService(t *testing.T) {
	pool := NewWorkerPool(2, 10)
	defer pool.Close()
	m := NewManager(pool, WithPoolOwnership(false))
	defer m.Close()

	process := func(ctx context.Context, in int) (string, error) {
		return "processed", nil
	}

	handle, err := Register(m, "test-service", 5, process)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Disable service
	m.Disable("test-service")

	// Request should fail
	respCh := handle.Request(context.Background(), 42)
	select {
	case resp := <-respCh:
		if resp.Err != ErrClosed {
			t.Fatalf("Expected ErrClosed, got %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}

	// Re-enable
	m.Enable("test-service")

	// Request should succeed
	respCh2 := handle.Request(context.Background(), 42)
	select {
	case resp := <-respCh2:
		if resp.Err != nil {
			t.Fatalf("Unexpected error: %v", resp.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Response not received within timeout")
	}
}

