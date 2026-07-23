package hub

import (
	"context"
	"errors"
	"net/http"
	"sync"
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

// pendingWaiter is a one-shot waiter for a RESPONSE correlated by corr id.
// Used by the HTTP /v1/rpc control-plane path and (later) multi-hop REQUEST.
type pendingWaiter struct {
	ch chan pendingResult
}

type pendingResult struct {
	env     protocol.Envelope
	payload []byte
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

	pendingMu sync.Mutex
	pending   map[string]*pendingWaiter // corr → waiter

	// drop counters (atomic via mutex) — observability for §4.6 late/unroutable drops.
	statsMu              sync.Mutex
	droppedLateResp      uint64 // RESPONSE with unknown/already-consumed corr and empty dst
	droppedUnroutable    uint64 // RESPONSE whose dst agent is offline
	droppedDuplicateCorr uint64 // second RESPONSE for an already-delivered pending corr

	// Active streams: stream id → binding established at STREAM_OPEN.
	// STREAM_DATA/END use compact envelopes (no src/dst) and resolve via this table.
	streamsMu sync.Mutex
	streams   map[string]*streamBinding
}

// streamBinding routes compact STREAM_DATA/END frames to the initiator and
// tracks ownership so disconnect can synthesize STREAM_END{aborted} (Task 3.4).
type streamBinding struct {
	streamID string
	corr     string
	src      string // producer agentId (responder)
	dst      string // consumer agentId (initiator)
	tenant   string
}

// NewGateway constructs a Gateway. maxFrameBytes bounds the read message size;
// queueBytes is the per-connection send-queue byte budget.
func NewGateway(a *auth.Authenticator, r *Registry, maxFrameBytes, queueBytes int) *Gateway {
	return &Gateway{
		auth:          a,
		registry:      r,
		maxFrameBytes: maxFrameBytes,
		queueBytes:    queueBytes,
		pending:       make(map[string]*pendingWaiter),
		streams:       make(map[string]*streamBinding),
	}
}

// registerPending installs a waiter for corr. The caller must later either
// receive on the channel or call cancelPending to avoid leaks.
func (g *Gateway) registerPending(corr string) *pendingWaiter {
	w := &pendingWaiter{ch: make(chan pendingResult, 1)}
	g.pendingMu.Lock()
	g.pending[corr] = w
	g.pendingMu.Unlock()
	return w
}

// cancelPending removes a waiter without delivering. Safe if already delivered.
func (g *Gateway) cancelPending(corr string) {
	g.pendingMu.Lock()
	delete(g.pending, corr)
	g.pendingMu.Unlock()
}

// deliverPending delivers a RESPONSE to a registered waiter. Returns true if a
// waiter consumed it (so the frame must not also be relayed to a WS agent).
// A missing waiter for a non-empty corr is NOT counted here — the caller decides
// whether the frame is late (no dst) or should still be routed to a WS agent.
func (g *Gateway) deliverPending(env protocol.Envelope, payload []byte) bool {
	if env.Corr == "" {
		return false
	}
	g.pendingMu.Lock()
	w, ok := g.pending[env.Corr]
	if ok {
		delete(g.pending, env.Corr)
	}
	g.pendingMu.Unlock()
	if !ok {
		return false
	}
	// Copy payload: DecodeFrame may alias into a reused buffer.
	cp := append([]byte(nil), payload...)
	select {
	case w.ch <- pendingResult{env: env, payload: cp}:
	default:
		// Waiter channel already filled (should not happen with cap=1 + delete).
		g.statsMu.Lock()
		g.droppedDuplicateCorr++
		g.statsMu.Unlock()
	}
	return true
}

// DropStats is a snapshot of RESPONSE drop counters (DESIGN §4.6).
type DropStats struct {
	Late       uint64
	Unroutable uint64
	Duplicate  uint64
}

// DropStats returns a snapshot of late/duplicate/unroutable RESPONSE counters.
func (g *Gateway) DropStats() DropStats {
	g.statsMu.Lock()
	defer g.statsMu.Unlock()
	return DropStats{
		Late:       g.droppedLateResp,
		Unroutable: g.droppedUnroutable,
		Duplicate:  g.droppedDuplicateCorr,
	}
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

	// Ensure registry cleanup, synthesize STREAM_END for producer-owned open
	// streams (DESIGN §4.10), and tear down the connection on exit.
	defer func() {
		g.abortStreamsOwnedBy(conn)
		g.registry.Remove(conn.Tenant(), conn.AgentID(), conn)
		conn.Close()
	}()

	g.readLoop(ctx, ws, conn)
}

// abortStreamsOwnedBy synthesizes STREAM_END{status=aborted} for every stream
// this connection was producing, and delivers them to the consumer. STREAM_END
// is the sole terminal state (DESIGN §4.10) — prevents async iterators hanging.
func (g *Gateway) abortStreamsOwnedBy(conn *Conn) {
	g.streamsMu.Lock()
	var doomed []*streamBinding
	for id, b := range g.streams {
		if b.src == conn.AgentID() && b.tenant == conn.Tenant() {
			doomed = append(doomed, b)
			delete(g.streams, id)
		}
	}
	g.streamsMu.Unlock()

	for _, b := range doomed {
		dst, ok := g.registry.Lookup(b.tenant, b.dst)
		if !ok {
			continue
		}
		env := protocol.Envelope{
			V:      protocol.ProtocolVersion,
			Type:   protocol.STREAM_END,
			Stream: b.streamID,
			Corr:   b.corr,
			Src:    b.src,
			Dst:    b.dst,
			Tenant: b.tenant,
			Hdr:    map[string]string{"status": "aborted"},
		}
		frame, err := protocol.EncodeFrame(env, nil)
		if err != nil {
			continue
		}
		_ = dst.Enqueue(frame)
	}
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
	case protocol.REQUEST:
		g.relayRequest(ctx, conn, env, payload)
	case protocol.RESPONSE:
		g.relayResponse(ctx, conn, env, payload)
	case protocol.STREAM_OPEN:
		g.relayStreamOpen(ctx, conn, env, payload)
	case protocol.STREAM_DATA:
		g.relayStreamData(ctx, conn, env, payload)
	case protocol.STREAM_END:
		g.relayStreamEnd(ctx, conn, env, payload)
	default:
		// Unknown / not-yet-implemented types are ignored so the connection stays alive.
	}
}

// relayStreamOpen binds stream→(src,dst,tenant,corr) and forwards OPEN to dst.
// OPEN must carry dst (the original REQUEST initiator). Identity is already applied.
func (g *Gateway) relayStreamOpen(_ context.Context, conn *Conn, env protocol.Envelope, payload []byte) {
	if env.Stream == "" || env.Dst == "" {
		return
	}
	dst, ok := g.registry.Lookup(conn.Tenant(), env.Dst)
	if !ok {
		g.replyErrorCorr(conn, env.Corr, protocol.ErrNoRoute, "stream target offline")
		return
	}
	bind := &streamBinding{
		streamID: env.Stream,
		corr:     env.Corr,
		src:      conn.AgentID(),
		dst:      env.Dst,
		tenant:   conn.Tenant(),
	}
	g.streamsMu.Lock()
	// Last OPEN wins for a reused stream id (v1: stream ids are ULIDs, collisions rare).
	g.streams[env.Stream] = bind
	g.streamsMu.Unlock()

	frame, err := protocol.EncodeFrame(env, payload)
	if err != nil {
		return
	}
	if err := dst.Enqueue(frame); err != nil {
		// Binding established but delivery failed: abort binding so DATA can't orphan.
		g.streamsMu.Lock()
		delete(g.streams, env.Stream)
		g.streamsMu.Unlock()
		code := protocol.ErrQueueFull
		if !errors.Is(err, ErrQueueFull) {
			code = protocol.ErrNoRoute
		}
		g.replyErrorCorr(conn, env.Corr, code, err.Error())
	}
}

// relayStreamData forwards a compact STREAM_DATA frame using the OPEN binding.
// Envelope carries only type/stream/seq (hdr); Hub does not re-expand src/dst.
func (g *Gateway) relayStreamData(_ context.Context, conn *Conn, env protocol.Envelope, payload []byte) {
	if env.Stream == "" {
		return
	}
	g.streamsMu.Lock()
	bind, ok := g.streams[env.Stream]
	g.streamsMu.Unlock()
	if !ok {
		return // unknown stream: drop (Task 3.5 may surface as abort)
	}
	// Only the producer that opened the stream may emit DATA.
	if bind.src != conn.AgentID() || bind.tenant != conn.Tenant() {
		return
	}
	dst, ok := g.registry.Lookup(bind.tenant, bind.dst)
	if !ok {
		// Consumer offline: abort stream (full synthesize END is Task 3.4).
		return
	}
	// Preserve compact shape: do not inject src/dst into DATA (design §4.1).
	frame, err := protocol.EncodeFrame(env, payload)
	if err != nil {
		return
	}
	_ = dst.Enqueue(frame) // backpressure abort is Task 3.5
}

// relayStreamEnd forwards STREAM_END and removes the binding (sole terminal).
func (g *Gateway) relayStreamEnd(_ context.Context, conn *Conn, env protocol.Envelope, payload []byte) {
	if env.Stream == "" {
		return
	}
	g.streamsMu.Lock()
	bind, ok := g.streams[env.Stream]
	if ok {
		delete(g.streams, env.Stream)
	}
	g.streamsMu.Unlock()
	if !ok {
		return
	}
	if bind.src != conn.AgentID() || bind.tenant != conn.Tenant() {
		// Non-owner END: restore binding (we already deleted under the lock).
		g.streamsMu.Lock()
		g.streams[env.Stream] = bind
		g.streamsMu.Unlock()
		return
	}
	dst, ok := g.registry.Lookup(bind.tenant, bind.dst)
	if !ok {
		return
	}
	frame, err := protocol.EncodeFrame(env, payload)
	if err != nil {
		return
	}
	_ = dst.Enqueue(frame)
}

// relayRequest routes a REQUEST to its destination within the sender's tenant.
// Offline targets get ERROR{NO_ROUTE} with the request's corr so the initiator's
// Request waiter can complete. Used both by WS agents and by the HTTP /v1/rpc
// path (which injects frames via a synthetic source).
func (g *Gateway) relayRequest(_ context.Context, conn *Conn, env protocol.Envelope, payload []byte) {
	if env.Dst == "" {
		g.replyErrorCorr(conn, env.Corr, protocol.ErrNoRoute, "empty destination")
		return
	}
	dst, ok := g.registry.Lookup(conn.Tenant(), env.Dst)
	if !ok {
		g.replyErrorCorr(conn, env.Corr, protocol.ErrNoRoute, "target offline or absent")
		return
	}
	frame, err := protocol.EncodeFrame(env, payload)
	if err != nil {
		return
	}
	if err := dst.Enqueue(frame); err != nil {
		code := protocol.ErrQueueFull
		if !errors.Is(err, ErrQueueFull) {
			code = protocol.ErrNoRoute
		}
		g.replyErrorCorr(conn, env.Corr, code, err.Error())
	}
}

// relayResponse delivers a RESPONSE either to an in-process pending waiter
// (HTTP /v1/rpc) or to the destination agent connection. Late (unknown corr,
// empty dst), duplicate, and unroutable RESPONSEs are dropped and counted
// (DESIGN §4.6) — never forwarded to a random agent.
func (g *Gateway) relayResponse(_ context.Context, conn *Conn, env protocol.Envelope, payload []byte) {
	if g.deliverPending(env, payload) {
		return
	}
	// No in-process waiter. If corr was set, this may be a late/duplicate of a
	// completed /v1/rpc call (dst often empty or synthetic). Count and drop when
	// there is no live destination agent.
	if env.Dst == "" {
		g.statsMu.Lock()
		g.droppedLateResp++
		g.statsMu.Unlock()
		return
	}
	dst, ok := g.registry.Lookup(conn.Tenant(), env.Dst)
	if !ok {
		g.statsMu.Lock()
		g.droppedUnroutable++
		g.statsMu.Unlock()
		return
	}
	frame, err := protocol.EncodeFrame(env, payload)
	if err != nil {
		return
	}
	_ = dst.Enqueue(frame)
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
	g.replyErrorCorr(conn, "", code, msg)
}

// replyErrorCorr is like replyError but attaches corr so SDK Request waiters
// can complete on NO_ROUTE / QUEUE_FULL for a specific REQUEST.
func (g *Gateway) replyErrorCorr(conn *Conn, corr, code, msg string) {
	ep, err := protocol.MarshalError(protocol.ErrorPayload{Code: code, Message: msg})
	if err != nil {
		return
	}
	frame, err := protocol.EncodeFrame(protocol.Envelope{
		V:      protocol.ProtocolVersion,
		Type:   protocol.ERROR,
		Corr:   corr,
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
