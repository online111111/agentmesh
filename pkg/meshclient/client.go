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
		ws:       ws,
		agentID:  opt.AgentID,
		session:  welcome.Session,
		closeCh:  make(chan struct{}),
		readDone: make(chan struct{}),
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
