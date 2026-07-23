package hub

import (
	"context"
	"errors"
	"sync"

	"github.com/online111111/agentmesh/internal/protocol"
)

// ErrQueueFull is returned by Enqueue when the per-connection byte budget is
// exhausted (backpressure). Its Error() is the stable protocol.ErrQueueFull
// code string.
var ErrQueueFull = errors.New(protocol.ErrQueueFull)

// errConnClosed is returned by Enqueue after the connection is closed.
var errConnClosed = errors.New("hub: connection closed")

// frameWriter is the transport write side. The real implementation wraps a
// WebSocket connection (Task 1.5); tests use in-memory fakes. Keeping Conn
// decoupled from a concrete transport lets 1.4 build and be tested in isolation.
type frameWriter interface {
	// WriteFrame writes one complete frame, honoring ctx cancellation.
	WriteFrame(ctx context.Context, frame []byte) error
}

// Conn is a Hub-side connection: identity (agentId, tenant), a byte-bounded
// send queue, and a single write goroutine that drains the queue to the
// transport (DESIGN §5). Enqueue applies backpressure by byte water level, not
// message count, to prevent OOM under many connections. Conn implements the
// registry's Sender interface.
type Conn struct {
	agentID   string
	tenant    string
	principal string // authenticated principal (for same-principal takeover)

	writer     frameWriter
	maxBytes   int
	ctx        context.Context
	cancel     context.CancelFunc
	writerDone chan struct{}

	mu       sync.Mutex
	queue    [][]byte
	curBytes int
	notify   chan struct{} // buffered(1) wakeup for the write goroutine
	closed   bool

	// Write coalescing (DESIGN §5): bound batch size to protect p99 latency.
	coalesceMaxBytes  int
	coalesceMaxFrames int
}

// Conn implements the registry's Sender interface.
var _ Sender = (*Conn)(nil)

// NewConn creates a Conn bound to writer with the given identity and send-queue
// byte budget, and starts its write goroutine.
func NewConn(writer frameWriter, agentID, tenant string, maxQueueBytes int) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		agentID:           agentID,
		tenant:            tenant,
		writer:            writer,
		maxBytes:          maxQueueBytes,
		ctx:               ctx,
		cancel:            cancel,
		writerDone:        make(chan struct{}),
		notify:            make(chan struct{}, 1),
		coalesceMaxBytes:  32 << 10, // 32 KiB
		coalesceMaxFrames: 16,
	}
	go c.writeLoop()
	return c
}

// AgentID returns the connection's authenticated agentId.
func (c *Conn) AgentID() string { return c.agentID }

// Tenant returns the connection's authenticated tenant.
func (c *Conn) Tenant() string { return c.tenant }

// Done is closed once the connection has fully torn down. It lets the gateway
// coordinate read/write goroutine lifetimes and registry cleanup.
func (c *Conn) Done() <-chan struct{} { return c.writerDone }

// Enqueue appends a frame to the send queue if the byte budget allows. It
// returns ErrQueueFull when adding the frame would exceed maxBytes, and an error
// once the connection is closed. It never blocks.
func (c *Conn) Enqueue(frame []byte) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errConnClosed
	}
	if c.curBytes+len(frame) > c.maxBytes {
		c.mu.Unlock()
		return ErrQueueFull
	}
	c.queue = append(c.queue, frame)
	c.curBytes += len(frame)
	c.mu.Unlock()

	// Wake the write goroutine (non-blocking; buffered size 1).
	select {
	case c.notify <- struct{}{}:
	default:
	}
	return nil
}

// writeLoop drains the queue to the transport in FIFO order. A frame in flight
// at the writer has already been removed from the byte count, so a slow
// transport does not permanently pin budget beyond the queued frames.
func (c *Conn) writeLoop() {
	defer close(c.writerDone)
	for {
		frame, ok := c.dequeue()
		if !ok {
			select {
			case <-c.ctx.Done():
				return
			case <-c.notify:
				continue
			}
		}
		// Write this frame, then drain up to coalesceMaxFrames-1 more without
		// returning to the idle select — reduces scheduling overhead while
		// preserving one WebSocket binary message per protocol frame (p99 bound).
		if err := c.writer.WriteFrame(c.ctx, frame); err != nil {
			c.Close()
			return
		}
		for i := 1; i < c.coalesceMaxFrames; i++ {
			next, ok := c.dequeue()
			if !ok {
				break
			}
			if err := c.writer.WriteFrame(c.ctx, next); err != nil {
				c.Close()
				return
			}
		}
	}
}

func (c *Conn) dequeue() ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queue) == 0 {
		return nil, false
	}
	frame := c.queue[0]
	c.queue[0] = nil
	c.queue = c.queue[1:]
	c.curBytes -= len(frame)
	return frame, true
}

// WriteDirect writes one frame on the transport immediately, bypassing the
// send queue. Used for SESSION_TAKEOVER so the notice is not lost when Close
// clears the queue. Fails if the connection is already closed.
func (c *Conn) WriteDirect(ctx context.Context, frame []byte) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errConnClosed
	}
	w := c.writer
	c.mu.Unlock()
	return w.WriteFrame(ctx, frame)
}

// Close tears down the connection: it cancels the write goroutine's context and
// marks the connection closed so further Enqueue calls fail. It is idempotent
// and safe to call from any goroutine.
func (c *Conn) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.queue = nil
	c.curBytes = 0
	c.mu.Unlock()
	c.cancel()
}
