package futures

import (
	"sync"
	"testing"
	"time"
)

// TestIsClosedInitiallyFalse tests that the gate is not closed initially.
func TestIsClosedInitiallyFalse(t *testing.T) {
	g := &gate{}
	if g.IsClosed() {
		t.Error("Expected gate to be open initially")
	}
}

// TestEnterBeforeClose tests entering the gate before closing.
func TestEnterBeforeClose(t *testing.T) {
	g := &gate{}
	if !g.Enter() {
		t.Error("Expected to enter gate before close")
	}
	g.Exit()
}

// TestEnterAfterClose tests that entering fails after closing.
func TestEnterAfterClose(t *testing.T) {
	g := &gate{}
	g.CloseAndWait()
	if g.Enter() {
		t.Error("Expected not to enter gate after close")
	}
}

// TestCloseAndWait tests closing and waiting.
func TestCloseAndWait(t *testing.T) {
	g := &gate{}
	done := make(chan bool)
	go func() {
		if g.Enter() {
			time.Sleep(10 * time.Millisecond)
			g.Exit()
		}
		done <- true
	}()
	time.Sleep(5 * time.Millisecond)
	g.CloseAndWait()
	select {
	case <-done:
		// Good, goroutine finished
	case <-time.After(100 * time.Millisecond):
		t.Error("CloseAndWait did not wait for entrant")
	}
}

// TestMultipleEnters tests multiple enters and exits.
func TestMultipleEnters(t *testing.T) {
	g := &gate{}
	for i := range 10 {
		if !g.Enter() {
			t.Errorf("Failed to enter on iteration %d", i)
		}
	}
	for range 10 {
		g.Exit()
	}
}

// TestConcurrentEnters tests concurrent enters.
func TestConcurrentEnters(t *testing.T) {
	g := &gate{}
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if g.Enter() {
				time.Sleep(1 * time.Millisecond)
				g.Exit()
			}
		})
	}
	wg.Wait()
}

// TestIdempotentClose tests that CloseAndWait is idempotent.
func TestIdempotentClose(t *testing.T) {
	g := &gate{}
	g.CloseAndWait()
	if !g.IsClosed() {
		t.Error("Gate should be closed after first close")
	}
	g.CloseAndWait() // Should not panic or hang
	if !g.IsClosed() {
		t.Error("Gate should still be closed after second close")
	}
}

// TestEnterExitCycle tests entering and exiting in a cycle.
func TestEnterExitCycle(t *testing.T) {
	g := &gate{}
	for i := range 100 {
		if !g.Enter() {
			t.Errorf("Failed to enter on cycle %d", i)
		}
		g.Exit()
	}
}

// TestCloseAfterEnters tests closing after some enters.
func TestCloseAfterEnters(t *testing.T) {
	g := &gate{}
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			if g.Enter() {
				time.Sleep(10 * time.Millisecond)
				g.Exit()
			}
		})
	}
	time.Sleep(5 * time.Millisecond)
	g.CloseAndWait()
	wg.Wait() // Should not hang
}

// TestIsClosedAfterClose tests IsClosed after close.
func TestIsClosedAfterClose(t *testing.T) {
	g := &gate{}
	if g.IsClosed() {
		t.Error("Gate should not be closed initially")
	}
	g.CloseAndWait()
	if !g.IsClosed() {
		t.Error("Gate should be closed after CloseAndWait")
	}
}

// TestConcurrentCloseAndEnter tests concurrent close and enter.
func TestConcurrentCloseAndEnter(t *testing.T) {
	g := &gate{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		g.CloseAndWait()
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			if !g.Enter() {
				return // Expected after close
			}
			g.Exit()
		}
	}()
	wg.Wait()
}
