package meshclient

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/online111111/agentmesh/internal/protocol"
)

// StreamChunk is one item from a RequestStream async iterator.
// When IsEnd is true, Status holds the STREAM_END status (ok/error/aborted)
// and Data is empty; the channel is closed after the end chunk.
type StreamChunk struct {
	Seq    int
	Data   []byte
	IsEnd  bool
	Status string
	Err    error
}

// Stream is the async iterator returned by RequestStream. Range over Chunks
// until closed. LastSeq is updated as DATA frames arrive (for tests/diagnostics).
type Stream struct {
	Corr    string
	Stream  string
	Chunks  <-chan StreamChunk
	LastSeq int

	cancel context.CancelFunc
	done   chan struct{}
}

// Close aborts local consumption (does not yet send CANCEL — Task 3.6).
func (s *Stream) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	// drain is best-effort; readLoop will stop delivering once waiter is cleared.
}

// streamWaiter multiplexes OPEN/DATA/END for one corr (and then stream id).
type streamWaiter struct {
	corr     string
	streamID string // set on OPEN
	ch       chan StreamChunk
	done     chan struct{}
}

// RequestStream sends a REQUEST and returns an async iterator of stream chunks.
// The target must reply with STREAM_OPEN→DATA*→END (not a single RESPONSE).
// ttlMs bounds how long we wait for OPEN and subsequent frames; ≤0 → 30s.
func (c *Client) RequestStream(ctx context.Context, dst string, payload []byte, ttlMs int32) (*Stream, error) {
	if dst == "" {
		return nil, errors.New("meshclient: dst is required")
	}
	if ttlMs <= 0 {
		ttlMs = 30000
	}
	corr := protocol.NewID()
	w := &streamWaiter{
		corr: corr,
		ch:   make(chan StreamChunk, 16),
		done: make(chan struct{}),
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("meshclient: closed")
	}
	if c.streamWaiters == nil {
		c.streamWaiters = make(map[string]*streamWaiter)
	}
	if c.streamByID == nil {
		c.streamByID = make(map[string]*streamWaiter)
	}
	c.streamWaiters[corr] = w
	c.mu.Unlock()

	env := protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.REQUEST,
		ID:   protocol.NewID(),
		Corr: corr,
		Src:  c.agentID,
		Dst:  dst,
		TTL:  ttlMs,
		Hops: hubDefaultMaxHops(),
		Hdr:  map[string]string{"stream": "1"},
	}
	if err := c.writeFrame(ctx, env, payload); err != nil {
		c.clearStreamWaiter(w)
		return nil, err
	}

	sctx, cancel := context.WithCancel(ctx)
	out := make(chan StreamChunk, 16)
	stream := &Stream{
		Corr:   corr,
		Chunks: out,
		cancel: cancel,
		done:   w.done,
	}

	go func() {
		defer close(out)
		defer c.clearStreamWaiter(w)
		defer close(w.done)

		timer := time.NewTimer(time.Duration(ttlMs) * time.Millisecond)
		defer timer.Stop()

		sendCancel := func() {
			_ = c.Cancel(context.Background(), dst, corr)
		}

		for {
			select {
			case chunk, ok := <-w.ch:
				if !ok {
					return
				}
				if chunk.IsEnd {
					stream.Stream = w.streamID
					out <- chunk
					return
				}
				if chunk.Err != nil {
					out <- chunk
					return
				}
				stream.LastSeq = chunk.Seq
				stream.Stream = w.streamID
				out <- chunk
			case <-timer.C:
				sendCancel()
				out <- StreamChunk{IsEnd: true, Status: "aborted", Err: &RPCError{Code: protocol.ErrTimeout, Message: "stream timed out"}}
				return
			case <-sctx.Done():
				sendCancel()
				out <- StreamChunk{IsEnd: true, Status: "aborted", Err: sctx.Err()}
				return
			case <-c.closeCh:
				out <- StreamChunk{IsEnd: true, Status: "aborted", Err: errors.New("meshclient: closed")}
				return
			}
		}
	}()
	return stream, nil
}

func (c *Client) clearStreamWaiter(w *streamWaiter) {
	c.mu.Lock()
	delete(c.streamWaiters, w.corr)
	if w.streamID != "" {
		delete(c.streamByID, w.streamID)
	}
	c.mu.Unlock()
}

// deliverStreamFrame routes STREAM_OPEN/DATA/END to the matching waiter.
// Returns true if consumed.
func (c *Client) deliverStreamFrame(env protocol.Envelope, payload []byte) bool {
	c.mu.Lock()
	var w *streamWaiter
	switch env.Type {
	case protocol.STREAM_OPEN:
		if env.Corr != "" {
			w = c.streamWaiters[env.Corr]
			if w != nil {
				w.streamID = env.Stream
				if env.Stream != "" {
					c.streamByID[env.Stream] = w
				}
			}
		}
	case protocol.STREAM_DATA, protocol.STREAM_END:
		if env.Stream != "" {
			w = c.streamByID[env.Stream]
		}
	}
	c.mu.Unlock()
	if w == nil {
		return false
	}

	switch env.Type {
	case protocol.STREAM_OPEN:
		// OPEN itself is not a chunk; wait for DATA/END.
		return true
	case protocol.STREAM_DATA:
		seq := 0
		if env.Hdr != nil {
			if s, ok := env.Hdr["seq"]; ok {
				if n, err := strconv.Atoi(s); err == nil {
					seq = n
				}
			}
		}
		cp := append([]byte(nil), payload...)
		select {
		case w.ch <- StreamChunk{Seq: seq, Data: cp}:
		default:
			// Buffer full: surface as abort to avoid silent drop (design §4.10).
			select {
			case w.ch <- StreamChunk{IsEnd: true, Status: "aborted", Err: &RPCError{Code: protocol.ErrQueueFull, Message: "stream consumer too slow"}}:
			default:
			}
		}
		return true
	case protocol.STREAM_END:
		status := "ok"
		if env.Hdr != nil && env.Hdr["status"] != "" {
			status = env.Hdr["status"]
		}
		select {
		case w.ch <- StreamChunk{IsEnd: true, Status: status}:
		default:
		}
		return true
	}
	return false
}

// Cancel sends a CANCEL frame for the given corr to dst (DESIGN §4.10).
// The Hub routes it to the target agent so it can stop work.
func (c *Client) Cancel(ctx context.Context, dst, corr string) error {
	if corr == "" {
		return errors.New("meshclient: corr is required")
	}
	env := protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.CANCEL,
		ID:   protocol.NewID(),
		Corr: corr,
		Src:  c.agentID,
		Dst:  dst,
	}
	return c.writeFrame(ctx, env, nil)
}
