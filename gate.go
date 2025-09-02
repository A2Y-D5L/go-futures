package futures

import "sync"

// gate provides unified "enqueue window" coordination for shutdown.
// It encapsulates the closed flag, RWMutex, enqWG, and once used to
// reject new enqueues when Close begins, while ensuring deterministic drain.
type gate struct {
	mu     sync.RWMutex
	closed bool
	enqWG  sync.WaitGroup
	once   sync.Once
}

// IsClosed returns true if the gate has been closed.
func (g *gate) IsClosed() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.closed
}

// Enter attempts to enter the enqueue window.
// Returns true if entered (caller must call Exit when done), false if closed.
func (g *gate) Enter() bool {
	g.mu.RLock()
	if g.closed {
		g.mu.RUnlock()
		return false
	}
	g.enqWG.Add(1)
	g.mu.RUnlock()
	return true
}

// Exit leaves the enqueue window.
func (g *gate) Exit() {
	g.enqWG.Done()
}

// CloseAndWait closes the gate and waits for all entrants to exit.
// It is idempotent and safe for concurrent use.
func (g *gate) CloseAndWait() {
	g.once.Do(func() {
		g.mu.Lock()
		g.closed = true
		g.mu.Unlock()
		g.enqWG.Wait()
	})
}