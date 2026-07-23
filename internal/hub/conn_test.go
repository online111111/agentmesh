package hub

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/online111111/agentmesh/internal/protocol"
)

// blockingWriter is a frameWriter whose writes block until released, so tests
// can fill the send queue deterministically.
type blockingWriter struct {
	release chan struct{}
	mu      sync.Mutex
	written [][]byte
	err     error
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{release: make(chan struct{})}
}

func (w *blockingWriter) WriteFrame(ctx context.Context, frame []byte) error {
	select {
	case <-w.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	cp := make([]byte, len(frame))
	copy(cp, frame)
	w.written = append(w.written, cp)
	return nil
}

func (w *blockingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.written)
}

// passWriter writes without blocking, recording frames.
type passWriter struct {
	mu      sync.Mutex
	written [][]byte
	done    chan struct{}
	target  int
}

func (w *passWriter) WriteFrame(_ context.Context, frame []byte) error {
	w.mu.Lock()
	cp := make([]byte, len(frame))
	copy(cp, frame)
	w.written = append(w.written, cp)
	n := len(w.written)
	w.mu.Unlock()
	if w.done != nil && n == w.target {
		close(w.done)
	}
	return nil
}

func TestConnEnqueueBackpressure(t *testing.T) {
	w := newBlockingWriter()
	// Queue capacity 100 bytes.
	c := NewConn(w, "alice-laptop", "default", 100)
	defer c.Close()

	// First frame (10 bytes) gets picked up by the write goroutine and blocks
	// there; give it a moment to leave the queue.
	frame10 := make([]byte, 10)
	if err := c.Enqueue(frame10); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// Now fill the 100-byte queue. Frames still in the queue count toward the
	// limit; the one in-flight at the writer does not.
	filled := 0
	for i := 0; i < 20; i++ {
		if err := c.Enqueue(make([]byte, 50)); err != nil {
			if err.Error() != protocol.ErrQueueFull {
				t.Fatalf("want QUEUE_FULL, got %v", err)
			}
			break
		}
		filled++
	}
	if filled == 0 {
		t.Fatal("expected some enqueues to succeed before QUEUE_FULL")
	}
	// The next enqueue must be rejected as the queue is now full.
	if err := c.Enqueue(make([]byte, 50)); err == nil || err.Error() != protocol.ErrQueueFull {
		t.Fatalf("expected QUEUE_FULL after fill, got %v", err)
	}

	// Release the writer: it drains frames, freeing byte budget.
	close(w.release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := c.Enqueue(make([]byte, 50)); err == nil {
			break // budget freed, enqueue succeeds again
		}
		if time.Now().After(deadline) {
			t.Fatal("queue never drained after writer released")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConnDeliversInOrder(t *testing.T) {
	done := make(chan struct{})
	w := &passWriter{done: done, target: 5}
	c := NewConn(w, "a", "default", 1<<20)
	defer c.Close()

	for i := 0; i < 5; i++ {
		if err := c.Enqueue([]byte{byte(i)}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not receive all 5 frames")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := 0; i < 5; i++ {
		if len(w.written[i]) != 1 || w.written[i][0] != byte(i) {
			t.Fatalf("frame %d out of order: %v", i, w.written[i])
		}
	}
}

func TestConnCloseStopsGoroutine(t *testing.T) {
	w := &passWriter{}
	c := NewConn(w, "a", "default", 1<<20)
	c.Close()
	// Close is idempotent and unblocks the write goroutine.
	c.Close()
	// Enqueue after Close must fail, not panic or block.
	if err := c.Enqueue([]byte{1}); err == nil {
		t.Fatal("Enqueue after Close should fail")
	}
	// Done() must be closed after Close.
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() not closed after Close")
	}
}

func TestConnIdentity(t *testing.T) {
	c := NewConn(&passWriter{}, "alice-laptop", "t1", 1<<20)
	defer c.Close()
	if c.AgentID() != "alice-laptop" || c.Tenant() != "t1" {
		t.Fatalf("identity mismatch: %s / %s", c.AgentID(), c.Tenant())
	}
}

func TestConnChurnNoGoroutineLeak(t *testing.T) {
	// Create and close many Conns; goroutine count should return near baseline.
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	base := runtime.NumGoroutine()

	const N = 50
	for i := 0; i < N; i++ {
		w := &passWriter{}
		c := NewConn(w, "alice-x", "default", 1<<16)
		_ = c.Enqueue([]byte("hi"))
		c.Close()
		// Wait for writeLoop to exit
		select {
		case <-c.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("writeLoop did not exit")
		}
	}
	// Allow scheduler to reap
	deadline := time.Now().Add(3 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		after = runtime.NumGoroutine()
		if after <= base+5 {
			return
		}
	}
	t.Fatalf("goroutine leak: baseline=%d after=%d (delta=%d)", base, after, after-base)
}
