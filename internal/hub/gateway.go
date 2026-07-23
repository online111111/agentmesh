package hub

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/protocol"
)

// defaultHeartbeatMs is advertised in WELCOME as the suggested client heartbeat.
const defaultHeartbeatMs = 15000

// wsWriter adapts a *websocket.Conn to the frameWriter interface Conn expects,
// writing each frame as a single binary message.
type wsWriter struct {
	ws *websocket.Conn
}

func (w *wsWriter) WriteFrame(ctx context.Context, frame []byte) error {
	return w.ws.Write(ctx, websocket.MessageBinary, frame)
}

// Gateway upgrades HTTP requests to WebSocket, performs the HELLO/WELCOME
// authentication handshake (DESIGN §4.3/§6), and registers authenticated
// connections. Identity (src/tenant) is derived from the authenticated key and
// is the sole trust root.
type Gateway struct {
	auth          *auth.Authenticator
	registry      *Registry
	maxFrameBytes int
	queueBytes    int
}

// NewGateway constructs a Gateway. maxFrameBytes bounds the read message size;
// queueBytes is the per-connection send-queue byte budget.
func NewGateway(a *auth.Authenticator, r *Registry, maxFrameBytes, queueBytes int) *Gateway {
	return &Gateway{auth: a, registry: r, maxFrameBytes: maxFrameBytes, queueBytes: queueBytes}
}

// Registry exposes the underlying registry (used by the control plane and tests).
func (g *Gateway) Registry() *Registry { return g.registry }

// ServeWS is the http.HandlerFunc that upgrades to WebSocket and runs the
// connection lifecycle.
func (g *Gateway) ServeWS(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if err != nil {
		return // Accept already wrote an HTTP error.
	}
	// Bound the read size to the configured frame cap.
	ws.SetReadLimit(int64(g.maxFrameBytes))

	ctx := r.Context()
	conn, err := g.handshake(ctx, ws)
	if err != nil {
		// handshake already sent an ERROR frame; close and stop.
		_ = ws.Close(websocket.StatusPolicyViolation, "handshake failed")
		return
	}

	// Ensure registry cleanup and connection teardown on exit.
	defer func() {
		g.registry.Remove(conn.Tenant(), conn.AgentID(), conn)
		conn.Close()
	}()

	g.readLoop(ctx, ws, conn)
}

// handshake reads the first frame, requires it to be a valid HELLO, authenticates
// the token, binds identity, registers the connection, and replies WELCOME. On
// any failure it writes an ERROR frame and returns an error.
func (g *Gateway) handshake(ctx context.Context, ws *websocket.Conn) (*Conn, error) {
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	typ, data, err := ws.Read(readCtx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		writeError(ctx, ws, protocol.ErrAuthFailed, "first frame must be binary HELLO")
		return nil, errors.New("non-binary first frame")
	}
	env, payload, err := protocol.DecodeFrame(data)
	if err != nil {
		writeError(ctx, ws, protocol.ErrAuthFailed, "malformed first frame")
		return nil, err
	}
	if env.Type != protocol.HELLO {
		writeError(ctx, ws, protocol.ErrAuthFailed, "first frame must be HELLO")
		return nil, errors.New("first frame not HELLO")
	}

	hello, err := protocol.UnmarshalHello(payload)
	if err != nil {
		writeError(ctx, ws, protocol.ErrAuthFailed, "malformed HELLO payload")
		return nil, err
	}

	// Version negotiation (DESIGN §4.9): v1 freezes v=1.
	if hello.V != 0 && hello.V != protocol.ProtocolVersion {
		writeErrorSupported(ctx, ws, protocol.ErrUnsupportedVersion, "unsupported protocol version",
			[]uint8{protocol.ProtocolVersion})
		return nil, errors.New("unsupported version")
	}

	identity, err := g.auth.Authenticate(hello.Token)
	if err != nil {
		writeError(ctx, ws, protocol.ErrAuthFailed, "authentication failed")
		return nil, err
	}

	// agentId must fall within the principal's authorized namespace.
	if !identity.AllowsAgentID(hello.AgentID) {
		writeError(ctx, ws, protocol.ErrAgentIDForbidden, "agentId outside authorized namespace")
		return nil, errors.New("agentId forbidden")
	}

	conn := NewConn(&wsWriter{ws: ws}, hello.AgentID, identity.Tenant, g.queueBytes)

	if err := g.registry.Register(identity.Tenant, hello.AgentID, conn); err != nil {
		conn.Close()
		writeError(ctx, ws, protocol.ErrDuplicateAgentID, "agentId already registered in tenant")
		return nil, err
	}

	// Registration succeeded: reply WELCOME.
	if err := writeWelcome(ctx, conn, identity); err != nil {
		g.registry.Remove(identity.Tenant, hello.AgentID, conn)
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// readLoop reads frames from the client until the connection closes. It couples
// its read context to conn.Done() so that if the write goroutine tears the
// connection down independently (e.g. a transport write error on a half-closed
// socket), the blocked ws.Read is cancelled promptly and registry cleanup runs
// without waiting for a TCP timeout. Frame routing (SEND relay etc.) is added in
// later tasks; for now unknown frames are ignored so the connection stays alive
// after WELCOME.
func (g *Gateway) readLoop(ctx context.Context, ws *websocket.Conn, conn *Conn) {
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-conn.Done():
			cancel()
		case <-readCtx.Done():
		}
	}()
	for {
		typ, data, err := ws.Read(readCtx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		g.route(readCtx, conn, data)
	}
}

// applyIdentity overwrites the src and tenant fields of an envelope with the
// connection's authenticated identity (DESIGN §6 trust root). Client-reported
// src/tenant are always ignored, defeating spoofing: agent A can never send as
// B, and cannot forge a tenant. All other fields are preserved.
func applyIdentity(env protocol.Envelope, agentID, tenant string) protocol.Envelope {
	env.Src = agentID
	env.Tenant = tenant
	return env
}

// route dispatches a decoded frame. Every inbound frame first has its identity
// overwritten from the connection (§6), then is relayed by type. SEND is
// at-most-once point-to-point: lookup dst in the same tenant, re-encode the
// envelope with trusted identity, and Enqueue the original payload tail
// without re-encoding it (zero-copy payload). Offline targets get ERROR{NO_ROUTE}.
func (g *Gateway) route(ctx context.Context, conn *Conn, data []byte) {
	env, payload, err := protocol.DecodeFrame(data)
	if err != nil {
		return
	}
	env = applyIdentity(env, conn.AgentID(), conn.Tenant())

	switch env.Type {
	case protocol.SEND:
		g.relaySend(ctx, conn, env, payload)
	default:
		// Unknown / not-yet-implemented types are ignored so the connection stays alive.
	}
}

// relaySend implements SEND at-most-once (DESIGN §4.6): lookup the destination
// within the sender's tenant; if offline, ERROR{NO_ROUTE} back to the source.
// The payload tail is never re-encoded — only the small envelope is rewritten
// with the trusted identity, then EncodeFrame reattaches the original payload
// bytes (zero-copy of the application body).
func (g *Gateway) relaySend(ctx context.Context, conn *Conn, env protocol.Envelope, payload []byte) {
	if env.Dst == "" {
		g.replyError(conn, protocol.ErrNoRoute, "empty destination")
		return
	}
	dst, ok := g.registry.Lookup(conn.Tenant(), env.Dst)
	if !ok {
		g.replyError(conn, protocol.ErrNoRoute, "target offline or absent")
		return
	}
	frame, err := protocol.EncodeFrame(env, payload)
	if err != nil {
		return
	}
	if err := dst.Enqueue(frame); err != nil {
		// Queue full or closed: surface as QUEUE_FULL / NO_ROUTE to the source.
		code := protocol.ErrQueueFull
		if !errors.Is(err, ErrQueueFull) {
			code = protocol.ErrNoRoute
		}
		g.replyError(conn, code, err.Error())
		return
	}
	_ = ctx
}

// replyError enqueues an ERROR frame on the source connection's send queue.
func (g *Gateway) replyError(conn *Conn, code, msg string) {
	ep, err := protocol.MarshalError(protocol.ErrorPayload{Code: code, Message: msg})
	if err != nil {
		return
	}
	frame, err := protocol.EncodeFrame(protocol.Envelope{
		V:      protocol.ProtocolVersion,
		Type:   protocol.ERROR,
		Dst:    conn.AgentID(),
		Tenant: conn.Tenant(),
	}, ep)
	if err != nil {
		return
	}
	_ = conn.Enqueue(frame)
}

// writeWelcome enqueues a WELCOME frame on the connection's send queue.
func writeWelcome(_ context.Context, conn *Conn, _ *auth.Identity) error {
	wp, err := protocol.MarshalWelcome(protocol.Welcome{
		Session:     protocol.NewID(),
		HeartbeatMs: defaultHeartbeatMs,
		Features:    []string{"stream", "pubsub"},
	})
	if err != nil {
		return err
	}
	frame, err := protocol.EncodeFrame(protocol.Envelope{
		V:      protocol.ProtocolVersion,
		Type:   protocol.WELCOME,
		Dst:    conn.AgentID(),
		Tenant: conn.Tenant(),
	}, wp)
	if err != nil {
		return err
	}
	return conn.Enqueue(frame)
}

// writeError writes an ERROR frame directly to the websocket (used during
// handshake before a Conn/send-queue exists).
func writeError(ctx context.Context, ws *websocket.Conn, code, msg string) {
	writeErrorSupported(ctx, ws, code, msg, nil)
}

func writeErrorSupported(ctx context.Context, ws *websocket.Conn, code, msg string, supported []uint8) {
	ep, err := protocol.MarshalError(protocol.ErrorPayload{Code: code, Message: msg, Supported: supported})
	if err != nil {
		return
	}
	frame, err := protocol.EncodeFrame(protocol.Envelope{V: protocol.ProtocolVersion, Type: protocol.ERROR}, ep)
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = ws.Write(writeCtx, websocket.MessageBinary, frame)
}
