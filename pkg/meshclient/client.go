// Package meshclient is the official Go SDK for AgentMesh agents.
//
//	c, err := meshclient.Dial(ctx, meshclient.Options{
//	    HubURL:  "ws://127.0.0.1:8080",
//	    Token:   "ka",
//	    AgentID: "alice-laptop",
//	    Caps:    []string{"echo"},
//	})
//	// c.Send / c.OnMessage / c.Request (later tasks)
package meshclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/online111111/agentmesh/internal/protocol"
)

// Options configures a Client Dial.
type Options struct {
	// HubURL is the Hub base URL, e.g. "http://127.0.0.1:8080" or
	// "ws://127.0.0.1:8080". http(s) is rewritten to ws(s).
	HubURL string
	// Token is the API key secret presented in HELLO.
	Token string
	// AgentID is the registrable agentId (must fall in principal namespace).
	AgentID string
	// Caps are capability labels advertised in HELLO (informational in v1).
	Caps []string
	// DialTimeout bounds the connect+HELLO handshake. Zero → 10s.
	DialTimeout time.Duration
}

// MessageHandler is invoked for every inbound application frame after HELLO.
type MessageHandler func(env protocol.Envelope, payload []byte)

// Client is a live Hub connection for one agentId.
type Client struct {
	ws      *websocket.Conn
	agentID string
	session string
	tenant  string // filled from first overwriten inbound / WELCOME path (Hub-bound)

	mu       sync.Mutex
	onMsg    MessageHandler
	closed   bool
	closeCh  chan struct{}
	readDone chan struct{}

	// pending corr → waiter for Request/RESPONSE.
	pending map[string]*corrWaiter

	// stream waiters: by corr (until OPEN) and by stream id (after OPEN).
	streamWaiters map[string]*streamWaiter
	streamByID    map[string]*streamWaiter
}

// corrWaiter is a one-shot RESPONSE waiter keyed by corr.
type corrWaiter struct {
	ch chan Response
}

// Response is a successful REQUEST reply.
type Response struct {
	From    string
	Payload []byte
	Corr    string
	Env     protocol.Envelope
}

// RPCError is a Hub/SDK application error carrying a stable protocol code
// (TIMEOUT, NO_ROUTE, CANCELLED, ...).
type RPCError struct {
	Code    string
	Message string
}

func (e *RPCError) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

// IsTimeout reports whether err is (or wraps) a TIMEOUT RPCError.
func IsTimeout(err error) bool {
	var re *RPCError
	if errors.As(err, &re) {
		return re.Code == protocol.ErrTimeout
	}
	return false
}

// IsRPCCode reports whether err is an RPCError with the given protocol code.
func IsRPCCode(err error, code string) bool {
	var re *RPCError
	if errors.As(err, &re) {
		return re.Code == code
	}
	return false
}

// Dial connects to the Hub, sends HELLO, and returns a Client after WELCOME.
// On auth/namespace failure it returns an error carrying the Hub ERROR code.
func Dial(ctx context.Context, opt Options) (*Client, error) {
	if opt.HubURL == "" || opt.Token == "" || opt.AgentID == "" {
		return nil, errors.New("meshclient: HubURL, Token, and AgentID are required")
	}
	timeout := opt.DialTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	wsURL := toWSURL(opt.HubURL)
	ws, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{},
	})
	if err != nil {
		return nil, fmt.Errorf("meshclient: dial: %w", err)
	}
	ws.SetReadLimit(1 << 20)

	// HELLO
	hp, err := protocol.MarshalHello(protocol.Hello{
		Token:   opt.Token,
		AgentID: opt.AgentID,
		Caps:    opt.Caps,
		V:       protocol.ProtocolVersion,
	})
	if err != nil {
		_ = ws.Close(websocket.StatusInternalError, "marshal hello")
		return nil, err
	}
	frame, err := protocol.EncodeFrame(protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.HELLO,
	}, hp)
	if err != nil {
		_ = ws.Close(websocket.StatusInternalError, "encode hello")
		return nil, err
	}
	if err := ws.Write(dialCtx, websocket.MessageBinary, frame); err != nil {
		_ = ws.Close(websocket.StatusInternalError, "write hello")
		return nil, fmt.Errorf("meshclient: write HELLO: %w", err)
	}

	// Expect WELCOME (or ERROR).
	typ, data, err := ws.Read(dialCtx)
	if err != nil {
		_ = ws.Close(websocket.StatusProtocolError, "read welcome")
		return nil, fmt.Errorf("meshclient: read WELCOME: %w", err)
	}
	if typ != websocket.MessageBinary {
		_ = ws.Close(websocket.StatusProtocolError, "non-binary welcome")
		return nil, errors.New("meshclient: expected binary WELCOME")
	}
	env, payload, err := protocol.DecodeFrame(data)
	if err != nil {
		_ = ws.Close(websocket.StatusProtocolError, "bad welcome")
		return nil, fmt.Errorf("meshclient: decode WELCOME: %w", err)
	}
	switch env.Type {
	case protocol.WELCOME:
		// ok
	case protocol.ERROR:
		ep, _ := protocol.UnmarshalError(payload)
		_ = ws.Close(websocket.StatusPolicyViolation, ep.Code)
		return nil, fmt.Errorf("meshclient: %s: %s", ep.Code, ep.Message)
	default:
		_ = ws.Close(websocket.StatusProtocolError, "unexpected first frame")
		return nil, fmt.Errorf("meshclient: expected WELCOME, got %s", protocol.TypeName(env.Type))
	}
	welcome, err := protocol.UnmarshalWelcome(payload)
	if err != nil {
		_ = ws.Close(websocket.StatusProtocolError, "bad welcome payload")
		return nil, fmt.Errorf("meshclient: unmarshal WELCOME: %w", err)
	}

	c := &Client{
		ws:            ws,
		agentID:       opt.AgentID,
		session:       welcome.Session,
		closeCh:       make(chan struct{}),
		readDone:      make(chan struct{}),
		pending:       make(map[string]*corrWaiter),
		streamWaiters: make(map[string]*streamWaiter),
		streamByID:    make(map[string]*streamWaiter),
	}
	go c.readLoop()
	return c, nil
}

// AgentID returns the registered agentId.
func (c *Client) AgentID() string { return c.agentID }

// Session returns the Hub-issued session id from WELCOME.
func (c *Client) Session() string { return c.session }

// OnMessage registers the inbound application-frame handler. It replaces any
// previous handler. Safe to call before or after Dial returns.
func (c *Client) OnMessage(h MessageHandler) {
	c.mu.Lock()
	c.onMsg = h
	c.mu.Unlock()
}

// Send delivers a fire-and-forget SEND frame to dst (at-most-once).
func (c *Client) Send(ctx context.Context, dst string, payload []byte) error {
	env := protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.SEND,
		ID:   protocol.NewID(),
		Src:  c.agentID,
		Dst:  dst,
	}
	return c.writeFrame(ctx, env, payload)
}

// Request sends a REQUEST with a fresh corr and waits for the matching RESPONSE
// (or ERROR / TIMEOUT). ttlMs is the relative timeout in milliseconds; values
// ≤0 default to 30s. The Hub overwrites src/tenant from the connection identity.
func (c *Client) Request(ctx context.Context, dst string, payload []byte, ttlMs int32) (*Response, error) {
	if dst == "" {
		return nil, errors.New("meshclient: dst is required")
	}
	if ttlMs <= 0 {
		ttlMs = 30000
	}
	corr := protocol.NewID()
	w := &corrWaiter{ch: make(chan Response, 1)}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("meshclient: closed")
	}
	c.pending[corr] = w
	c.mu.Unlock()
	defer c.clearPending(corr)

	env := protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.REQUEST,
		ID:   protocol.NewID(),
		Corr: corr,
		Src:  c.agentID,
		Dst:  dst,
		TTL:  ttlMs,
	}
	if err := c.writeFrame(ctx, env, payload); err != nil {
		return nil, err
	}

	timer := time.NewTimer(time.Duration(ttlMs) * time.Millisecond)
	defer timer.Stop()
	select {
	case res := <-w.ch:
		if res.Env.Type == protocol.ERROR {
			ep, _ := protocol.UnmarshalError(res.Payload)
			code := ep.Code
			if code == "" {
				code = "ERROR"
			}
			return nil, &RPCError{Code: code, Message: ep.Message}
		}
		return &res, nil
	case <-timer.C:
		return nil, &RPCError{Code: protocol.ErrTimeout, Message: "request timed out"}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closeCh:
		return nil, errors.New("meshclient: closed")
	}
}

func (c *Client) clearPending(corr string) {
	c.mu.Lock()
	delete(c.pending, corr)
	c.mu.Unlock()
}

// deliverPending hands a RESPONSE (or correlated ERROR) to a waiter. Returns
// true if a waiter consumed it (so OnMessage is not also invoked).
func (c *Client) deliverPending(env protocol.Envelope, payload []byte) bool {
	if env.Corr == "" {
		return false
	}
	c.mu.Lock()
	w, ok := c.pending[env.Corr]
	if ok {
		delete(c.pending, env.Corr)
	}
	c.mu.Unlock()
	if !ok {
		return false
	}
	cp := append([]byte(nil), payload...)
	select {
	case w.ch <- Response{From: env.Src, Payload: cp, Corr: env.Corr, Env: env}:
	default:
	}
	return true
}

// WriteFrame encodes and writes an arbitrary envelope+payload on the data plane.
// Used by the agent runtime to send RESPONSE (and later STREAM_*) frames.
// The Hub still overwrites src/tenant from the connection identity.
func (c *Client) WriteFrame(ctx context.Context, env protocol.Envelope, payload []byte) error {
	if env.V == 0 {
		env.V = protocol.ProtocolVersion
	}
	if env.Src == "" {
		env.Src = c.agentID
	}
	return c.writeFrame(ctx, env, payload)
}

// Done is closed when the read loop exits (after Close or a transport error).
func (c *Client) Done() <-chan struct{} {
	return c.readDone
}

// Close tears down the connection. It is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.closeCh)
	c.mu.Unlock()
	err := c.ws.Close(websocket.StatusNormalClosure, "bye")
	// Wait briefly for readLoop to exit so tests don't leak.
	select {
	case <-c.readDone:
	case <-time.After(2 * time.Second):
	}
	return err
}

func (c *Client) writeFrame(ctx context.Context, env protocol.Envelope, payload []byte) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return errors.New("meshclient: closed")
	}
	frame, err := protocol.EncodeFrame(env, payload)
	if err != nil {
		return err
	}
	return c.ws.Write(ctx, websocket.MessageBinary, frame)
}

func (c *Client) readLoop() {
	defer close(c.readDone)
	ctx := context.Background()
	for {
		select {
		case <-c.closeCh:
			return
		default:
		}
		typ, data, err := c.ws.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		env, payload, err := protocol.DecodeFrame(data)
		if err != nil {
			continue
		}
		// Route RESPONSE / STREAM_* / correlated ERROR to waiters first.
		switch env.Type {
		case protocol.RESPONSE:
			if c.deliverPending(env, payload) {
				continue
			}
		case protocol.STREAM_OPEN, protocol.STREAM_DATA, protocol.STREAM_END:
			if c.deliverStreamFrame(env, payload) {
				continue
			}
		case protocol.ERROR:
			// Correlated application errors (e.g. NO_ROUTE for a REQUEST) wake the waiter.
			if env.Corr != "" && c.deliverPending(env, payload) {
				continue
			}
			// Also wake stream waiters on correlated ERROR.
			if env.Corr != "" {
				c.mu.Lock()
				sw := c.streamWaiters[env.Corr]
				c.mu.Unlock()
				if sw != nil {
					ep, _ := protocol.UnmarshalError(payload)
					code := ep.Code
					if code == "" {
						code = "ERROR"
					}
					select {
					case sw.ch <- StreamChunk{IsEnd: true, Status: "aborted", Err: &RPCError{Code: code, Message: ep.Message}}:
					default:
					}
					continue
				}
			}
		}
		// Copy payload: DecodeFrame aliases the transport buffer.
		cp := append([]byte(nil), payload...)
		c.mu.Lock()
		h := c.onMsg
		c.mu.Unlock()
		if h != nil {
			h(env, cp)
		}
	}
}

// toWSURL rewrites http(s):// → ws(s):// and leaves ws(s):// alone. Paths are
// preserved; empty path becomes "/".
func toWSURL(raw string) string {
	u := strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(u, "https://"):
		u = "wss://" + strings.TrimPrefix(u, "https://")
	case strings.HasPrefix(u, "http://"):
		u = "ws://" + strings.TrimPrefix(u, "http://")
	case strings.HasPrefix(u, "ws://"), strings.HasPrefix(u, "wss://"):
		// already
	default:
		// bare host:port → ws://
		u = "ws://" + u
	}
	// httptest.Server URL has no path; Hub accepts WS at "/".
	return u
}
