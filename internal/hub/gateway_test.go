package hub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/protocol"
)

// newTestGateway builds a Gateway + httptest server with the given API keys.
func newTestGateway(t *testing.T, keySpec string) (*Gateway, *httptest.Server) {
	t.Helper()
	keys, err := auth.ParseKeys(keySpec)
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	g := NewGateway(auth.NewAuthenticator(keys), NewRegistry(), 1<<20, 1<<20)
	srv := httptest.NewServer(http.HandlerFunc(g.ServeWS))
	t.Cleanup(srv.Close)
	return g, srv
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// dialWS connects a raw websocket client to the gateway.
func dialWS(t *testing.T, srv *httptest.Server) (*websocket.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	c, _, err := websocket.Dial(ctx, wsURL(srv), nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(websocket.StatusNormalClosure, "") })
	return c, ctx
}

// sendFrame encodes and writes a binary frame.
func sendFrame(t *testing.T, ctx context.Context, c *websocket.Conn, env protocol.Envelope, payload []byte) {
	t.Helper()
	frame, err := protocol.EncodeFrame(env, payload)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if err := c.Write(ctx, websocket.MessageBinary, frame); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

// readFrame reads and decodes a binary frame.
func readFrame(t *testing.T, ctx context.Context, c *websocket.Conn) (protocol.Envelope, []byte) {
	t.Helper()
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if typ != websocket.MessageBinary {
		t.Fatalf("want binary frame, got %v", typ)
	}
	env, payload, err := protocol.DecodeFrame(data)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	return env, payload
}

func sendHello(t *testing.T, ctx context.Context, c *websocket.Conn, token, agentID string) {
	t.Helper()
	hp, err := protocol.MarshalHello(protocol.Hello{Token: token, AgentID: agentID, V: protocol.ProtocolVersion})
	if err != nil {
		t.Fatalf("MarshalHello: %v", err)
	}
	sendFrame(t, ctx, c, protocol.Envelope{V: protocol.ProtocolVersion, Type: protocol.HELLO}, hp)
}

func TestGatewayHelloWelcome(t *testing.T) {
	g, srv := newTestGateway(t, "a:ka:alice:default")
	c, ctx := dialWS(t, srv)

	sendHello(t, ctx, c, "ka", "alice-laptop")
	env, _ := readFrame(t, ctx, c)
	if env.Type != protocol.WELCOME {
		t.Fatalf("want WELCOME, got %s", protocol.TypeName(env.Type))
	}
	// Registered under the connection's authenticated tenant.
	if _, ok := g.Registry().Lookup("default", "alice-laptop"); !ok {
		t.Fatal("agent not registered after WELCOME")
	}
}

func TestGatewayInvalidToken(t *testing.T) {
	_, srv := newTestGateway(t, "a:ka:alice:default")
	c, ctx := dialWS(t, srv)

	sendHello(t, ctx, c, "wrong", "alice-laptop")
	env, payload := readFrame(t, ctx, c)
	if env.Type != protocol.ERROR {
		t.Fatalf("want ERROR, got %s", protocol.TypeName(env.Type))
	}
	ep, err := protocol.UnmarshalError(payload)
	if err != nil {
		t.Fatalf("UnmarshalError: %v", err)
	}
	if ep.Code != protocol.ErrAuthFailed {
		t.Fatalf("want AUTH_FAILED, got %s", ep.Code)
	}
	// Connection must be closed after ERROR.
	if _, _, err := c.Read(ctx); err == nil {
		t.Fatal("expected connection close after AUTH_FAILED")
	}
}

func TestGatewayFirstFrameNotHello(t *testing.T) {
	_, srv := newTestGateway(t, "a:ka:alice:default")
	c, ctx := dialWS(t, srv)

	// Send SEND as the first frame instead of HELLO.
	sendFrame(t, ctx, c, protocol.Envelope{V: protocol.ProtocolVersion, Type: protocol.SEND, Dst: "x"}, []byte("hi"))
	env, payload := readFrame(t, ctx, c)
	if env.Type != protocol.ERROR {
		t.Fatalf("want ERROR, got %s", protocol.TypeName(env.Type))
	}
	ep, _ := protocol.UnmarshalError(payload)
	if ep.Code != protocol.ErrAuthFailed {
		t.Fatalf("first-frame-not-HELLO should be AUTH_FAILED, got %s", ep.Code)
	}
}

func TestGatewayForbiddenAgentID(t *testing.T) {
	_, srv := newTestGateway(t, "a:ka:alice:default")
	c, ctx := dialWS(t, srv)

	// principal alice -> prefix "alice-"; "bob-x" is outside namespace.
	sendHello(t, ctx, c, "ka", "bob-x")
	env, payload := readFrame(t, ctx, c)
	if env.Type != protocol.ERROR {
		t.Fatalf("want ERROR, got %s", protocol.TypeName(env.Type))
	}
	ep, _ := protocol.UnmarshalError(payload)
	if ep.Code != protocol.ErrAgentIDForbidden {
		t.Fatalf("want AGENTID_FORBIDDEN, got %s", ep.Code)
	}
}

func TestGatewayDuplicateAgentID(t *testing.T) {
	_, srv := newTestGateway(t, "a:ka:alice:default")

	c1, ctx1 := dialWS(t, srv)
	sendHello(t, ctx1, c1, "ka", "alice-laptop")
	if env, _ := readFrame(t, ctx1, c1); env.Type != protocol.WELCOME {
		t.Fatalf("first conn want WELCOME, got %s", protocol.TypeName(env.Type))
	}

	c2, ctx2 := dialWS(t, srv)
	sendHello(t, ctx2, c2, "ka", "alice-laptop")
	env, payload := readFrame(t, ctx2, c2)
	if env.Type != protocol.ERROR {
		t.Fatalf("want ERROR on duplicate, got %s", protocol.TypeName(env.Type))
	}
	ep, _ := protocol.UnmarshalError(payload)
	if ep.Code != protocol.ErrDuplicateAgentID {
		t.Fatalf("want DUPLICATE_AGENT_ID, got %s", ep.Code)
	}
}

// connectAgent dials, sends HELLO, and drains WELCOME. Returns the live WS.
func connectAgent(t *testing.T, srv *httptest.Server, token, agentID string) (*websocket.Conn, context.Context) {
	t.Helper()
	c, ctx := dialWS(t, srv)
	sendHello(t, ctx, c, token, agentID)
	env, _ := readFrame(t, ctx, c)
	if env.Type != protocol.WELCOME {
		t.Fatalf("connectAgent %s: want WELCOME, got %s", agentID, protocol.TypeName(env.Type))
	}
	return c, ctx
}

func TestGatewaySendRelay(t *testing.T) {
	// Two agents A and B under the same key/tenant; A SEND → B receives.
	_, srv := newTestGateway(t, "a:ka:alice:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	b, ctxB := connectAgent(t, srv, "ka", "alice-b")

	payload := []byte(`{"hello":"bob"}`)
	sendFrame(t, ctxA, a, protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.SEND,
		ID:   "msg-1",
		Src:  "spoofed-should-be-ignored",
		Dst:  "alice-b",
	}, payload)

	env, got := readFrame(t, ctxB, b)
	if env.Type != protocol.SEND {
		t.Fatalf("B want SEND, got %s", protocol.TypeName(env.Type))
	}
	if env.Src != "alice-a" {
		t.Fatalf("src not overwritten by Hub: got %q, want alice-a", env.Src)
	}
	if env.Tenant != "default" {
		t.Fatalf("tenant not overwritten: got %q, want default", env.Tenant)
	}
	if env.Dst != "alice-b" {
		t.Fatalf("dst: got %q", env.Dst)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %q want %q", got, payload)
	}
}

func TestGatewaySendNoRoute(t *testing.T) {
	_, srv := newTestGateway(t, "a:ka:alice:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")

	sendFrame(t, ctxA, a, protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.SEND,
		ID:   "msg-missing",
		Dst:  "alice-nobody",
	}, []byte("x"))

	env, payload := readFrame(t, ctxA, a)
	if env.Type != protocol.ERROR {
		t.Fatalf("want ERROR, got %s", protocol.TypeName(env.Type))
	}
	ep, err := protocol.UnmarshalError(payload)
	if err != nil {
		t.Fatalf("UnmarshalError: %v", err)
	}
	if ep.Code != protocol.ErrNoRoute {
		t.Fatalf("want NO_ROUTE, got %s", ep.Code)
	}
}

func TestGatewaySendIdentitySpoofBlocked(t *testing.T) {
	// Alice tries to forge src=bob and tenant=other; B must see real identity.
	// Two principals share tenant default for routing, but keys map to different
	// namespaces so each can only register its own agentId.
	_, srv := newTestGateway(t, "a:ka:alice:default\nb:kb:bob:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-sender")
	b, ctxB := connectAgent(t, srv, "kb", "bob-receiver")

	sendFrame(t, ctxA, a, protocol.Envelope{
		V:      protocol.ProtocolVersion,
		Type:   protocol.SEND,
		Src:    "bob-receiver", // spoof
		Tenant: "evil-tenant",  // spoof
		Dst:    "bob-receiver",
	}, []byte("pwn"))

	env, _ := readFrame(t, ctxB, b)
	if env.Type != protocol.SEND {
		t.Fatalf("want SEND, got %s", protocol.TypeName(env.Type))
	}
	if env.Src != "alice-sender" {
		t.Fatalf("spoofed src leaked: got %q", env.Src)
	}
	if env.Tenant != "default" {
		t.Fatalf("spoofed tenant leaked: got %q", env.Tenant)
	}
}

func TestDropLateResponse(t *testing.T) {
	// RESPONSE with no pending waiter and empty dst is dropped and counted late.
	g, srv := newTestGateway(t, "a:ka:alice:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")

	sendFrame(t, ctxA, a, protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.RESPONSE,
		ID:   "late-1",
		Corr: "no-such-corr",
		// empty Dst → late drop
	}, []byte("stale"))

	// Give the hub a moment to process; nothing should be delivered back.
	time.Sleep(30 * time.Millisecond)
	st := g.DropStats()
	if st.Late < 1 {
		t.Fatalf("expected late drop >=1, got %+v", st)
	}
}

func TestDropUnroutableResponse(t *testing.T) {
	// RESPONSE targeting an offline agent is dropped and counted unroutable.
	g, srv := newTestGateway(t, "a:ka:alice:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")

	sendFrame(t, ctxA, a, protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.RESPONSE,
		ID:   "ur-1",
		Corr: "corr-offline",
		Dst:  "alice-nobody",
	}, []byte("x"))

	time.Sleep(30 * time.Millisecond)
	st := g.DropStats()
	if st.Unroutable < 1 {
		t.Fatalf("expected unroutable drop >=1, got %+v", st)
	}
}

func TestDropDuplicatePendingResponse(t *testing.T) {
	// First RESPONSE delivers to /v1/rpc waiter; a second with same corr and
	// empty dst is late-dropped (waiter already removed).
	g, srv := newTestGateway(t, "a:ka:alice:default")
	// Install a pending waiter as /v1/rpc would.
	corr := "corr-dup-test"
	w := g.registerPending(corr)
	defer g.cancelPending(corr)

	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	sendFrame(t, ctxA, a, protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.RESPONSE,
		ID:   "dup-1",
		Corr: corr,
		Src:  "alice-a",
	}, []byte("first"))

	select {
	case res := <-w.ch:
		if string(res.payload) != "first" {
			t.Fatalf("payload: %q", res.payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first RESPONSE not delivered to waiter")
	}

	// Second RESPONSE with same corr, empty dst → late (waiter gone).
	sendFrame(t, ctxA, a, protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.RESPONSE,
		ID:   "dup-2",
		Corr: corr,
	}, []byte("second"))
	time.Sleep(30 * time.Millisecond)
	st := g.DropStats()
	if st.Late < 1 {
		t.Fatalf("expected late drop for duplicate corr, got %+v", st)
	}
}

func TestRoutableResponseStillDelivered(t *testing.T) {
	// RESPONSE with live dst is still relayed (agent-to-agent Request path).
	_, srv := newTestGateway(t, "a:ka:alice:default\nb:kb:bob:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	b, ctxB := connectAgent(t, srv, "kb", "bob-b")

	sendFrame(t, ctxA, a, protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.RESPONSE,
		ID:   "ok-1",
		Corr: "corr-ok",
		Dst:  "bob-b",
	}, []byte("pong"))

	env, payload := readFrame(t, ctxB, b)
	if env.Type != protocol.RESPONSE {
		t.Fatalf("want RESPONSE, got %s", protocol.TypeName(env.Type))
	}
	if env.Src != "alice-a" {
		t.Fatalf("src: %q", env.Src)
	}
	if string(payload) != "pong" {
		t.Fatalf("payload: %q", payload)
	}
}

func TestStreamOpenDataEndRelay(t *testing.T) {
	// B (responder) replies to A with STREAM_OPEN→DATA→END; A receives ordered frames.
	// STREAM_DATA uses compact envelope (only stream + seq in hdr); Hub looks up binding.
	_, srv := newTestGateway(t, "a:ka:alice:default\nb:kb:bob:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	b, ctxB := connectAgent(t, srv, "kb", "bob-b")

	// A → B REQUEST (so B knows who to reply to)
	corr := "corr-stream-1"
	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.REQUEST,
		ID: "req-1", Corr: corr, Dst: "bob-b", TTL: 5000,
	}, []byte("stream-please"))

	env, _ := readFrame(t, ctxB, b)
	if env.Type != protocol.REQUEST {
		t.Fatalf("B want REQUEST, got %s", protocol.TypeName(env.Type))
	}

	streamID := "01STREAMTEST"
	// B → A STREAM_OPEN
	sendFrame(t, ctxB, b, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_OPEN,
		ID: "so-1", Corr: corr, Stream: streamID, Dst: "alice-a",
	}, nil)

	env, _ = readFrame(t, ctxA, a)
	if env.Type != protocol.STREAM_OPEN {
		t.Fatalf("A want STREAM_OPEN, got %s", protocol.TypeName(env.Type))
	}
	if env.Stream != streamID || env.Corr != corr {
		t.Fatalf("open fields: stream=%q corr=%q", env.Stream, env.Corr)
	}
	if env.Src != "bob-b" {
		t.Fatalf("open src: %q", env.Src)
	}

	// B → A STREAM_DATA compact (no src/dst; only stream + seq)
	sendFrame(t, ctxB, b, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_DATA,
		Stream: streamID, Hdr: map[string]string{"seq": "0"},
	}, []byte("tok-0"))

	env, payload := readFrame(t, ctxA, a)
	if env.Type != protocol.STREAM_DATA {
		t.Fatalf("A want STREAM_DATA, got %s", protocol.TypeName(env.Type))
	}
	if env.Stream != streamID {
		t.Fatalf("data stream: %q", env.Stream)
	}
	if env.Hdr["seq"] != "0" {
		t.Fatalf("seq: %q", env.Hdr["seq"])
	}
	if string(payload) != "tok-0" {
		t.Fatalf("payload: %q", payload)
	}

	// second DATA seq=1
	sendFrame(t, ctxB, b, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_DATA,
		Stream: streamID, Hdr: map[string]string{"seq": "1"},
	}, []byte("tok-1"))
	env, payload = readFrame(t, ctxA, a)
	if env.Type != protocol.STREAM_DATA || env.Hdr["seq"] != "1" || string(payload) != "tok-1" {
		t.Fatalf("second data: type=%s seq=%q payload=%q", protocol.TypeName(env.Type), env.Hdr["seq"], payload)
	}

	// STREAM_END
	sendFrame(t, ctxB, b, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_END,
		Stream: streamID, Hdr: map[string]string{"status": "ok"},
	}, nil)
	env, _ = readFrame(t, ctxA, a)
	if env.Type != protocol.STREAM_END {
		t.Fatalf("A want STREAM_END, got %s", protocol.TypeName(env.Type))
	}
	if env.Hdr["status"] != "ok" {
		t.Fatalf("status: %q", env.Hdr["status"])
	}
}

func TestStreamDataUnknownDropped(t *testing.T) {
	// STREAM_DATA for unknown stream is dropped (no ERROR required for v1 unknown stream).
	g, srv := newTestGateway(t, "a:ka:alice:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_DATA,
		Stream: "nope", Hdr: map[string]string{"seq": "0"},
	}, []byte("x"))
	time.Sleep(30 * time.Millisecond)
	// nothing panics; drop stats optional
	_ = g
}

func TestSynthesizeStreamEndOnProducerDisconnect(t *testing.T) {
	// B opens a stream to A, then B disconnects mid-stream → A gets STREAM_END{aborted}.
	g, srv := newTestGateway(t, "a:ka:alice:default\nb:kb:bob:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	b, ctxB := connectAgent(t, srv, "kb", "bob-b")

	corr := "corr-abort-1"
	streamID := "stream-abort-1"
	// Simulate OPEN from B to A
	sendFrame(t, ctxB, b, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_OPEN,
		ID: "so", Corr: corr, Stream: streamID, Dst: "alice-a",
	}, nil)
	env, _ := readFrame(t, ctxA, a)
	if env.Type != protocol.STREAM_OPEN {
		t.Fatalf("want OPEN, got %s", protocol.TypeName(env.Type))
	}

	// One DATA
	sendFrame(t, ctxB, b, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_DATA,
		Stream: streamID, Hdr: map[string]string{"seq": "0"},
	}, []byte("partial"))
	env, payload := readFrame(t, ctxA, a)
	if env.Type != protocol.STREAM_DATA || string(payload) != "partial" {
		t.Fatalf("data: type=%s payload=%q", protocol.TypeName(env.Type), payload)
	}

	// B disconnects (close WS)
	_ = b.Close(websocket.StatusNormalClosure, "bye")

	// A should receive synthesized STREAM_END{aborted}
	env, _ = readFrame(t, ctxA, a)
	if env.Type != protocol.STREAM_END {
		t.Fatalf("want synthesized STREAM_END, got %s", protocol.TypeName(env.Type))
	}
	if env.Stream != streamID {
		t.Fatalf("stream: %q", env.Stream)
	}
	if env.Hdr["status"] != "aborted" {
		t.Fatalf("status: %q want aborted", env.Hdr["status"])
	}

	// Binding removed
	time.Sleep(20 * time.Millisecond)
	g.streamsMu.Lock()
	_, still := g.streams[streamID]
	g.streamsMu.Unlock()
	if still {
		t.Fatal("stream binding should be removed after abort")
	}
}

func TestStreamAbortOnBackpressure(t *testing.T) {
	// Mid-stream QUEUE_FULL on consumer → whole stream aborted (no silent drop).
	keys, err := auth.ParseKeys("a:ka:alice:default\nb:kb:bob:default")
	if err != nil {
		t.Fatal(err)
	}
	g := NewGateway(auth.NewAuthenticator(keys), NewRegistry(), 1<<20, 1<<20)

	// Consumer with tiny queue + blocking writer so frames stay queued.
	bw := newBlockingWriter()
	consumer := NewConn(bw, "alice-a", "default", 80)
	defer consumer.Close()
	// Producer with normal queue (collects ERROR frames).
	pw := &passWriter{}
	producer := NewConn(pw, "bob-b", "default", 1<<20)
	defer producer.Close()

	if err := g.registry.Register("default", "alice-a", consumer); err != nil {
		t.Fatal(err)
	}
	if err := g.registry.Register("default", "bob-b", producer); err != nil {
		t.Fatal(err)
	}

	streamID := "stream-bp-1"
	corr := "corr-bp-1"
	ctx := context.Background()
	g.relayStreamOpen(ctx, producer, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_OPEN,
		ID: "so", Corr: corr, Stream: streamID, Dst: "alice-a",
		Src: "bob-b", Tenant: "default",
	}, nil)

	// Drain OPEN from consumer queue into the blocking writer path by not
	// releasing; fill remaining budget so next DATA fails.
	// OPEN frame already enqueued; pad until full.
	for i := 0; i < 20; i++ {
		if err := consumer.Enqueue(make([]byte, 40)); err != nil {
			break
		}
	}
	// Confirm full
	if err := consumer.Enqueue(make([]byte, 40)); err == nil {
		t.Fatal("expected consumer queue full before DATA")
	}

	// DATA must abort the stream, not leave a hole.
	g.relayStreamData(ctx, producer, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_DATA,
		Stream: streamID, Hdr: map[string]string{"seq": "0"},
		Src: "bob-b", Tenant: "default",
	}, []byte("token-that-wont-fit-if-nearly-full----------------"))

	// Binding removed
	g.streamsMu.Lock()
	_, still := g.streams[streamID]
	g.streamsMu.Unlock()
	if still {
		t.Fatal("stream binding should be removed after backpressure abort")
	}

	// Producer should eventually get ERROR{QUEUE_FULL} (write loop drains).
	deadline := time.Now().Add(2 * time.Second)
	var saw bool
	for time.Now().Before(deadline) {
		pw.mu.Lock()
		frames := append([][]byte(nil), pw.written...)
		pw.mu.Unlock()
		for _, f := range frames {
			env, payload, err := protocol.DecodeFrame(f)
			if err != nil {
				continue
			}
			if env.Type == protocol.ERROR {
				ep, _ := protocol.UnmarshalError(payload)
				if ep.Code == protocol.ErrQueueFull {
					saw = true
					break
				}
			}
		}
		if saw {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !saw {
		t.Fatal("producer did not receive QUEUE_FULL after stream abort")
	}

	// Subsequent DATA for same stream is ignored (no re-open).
	g.relayStreamData(ctx, producer, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.STREAM_DATA,
		Stream: streamID, Hdr: map[string]string{"seq": "1"},
		Src: "bob-b", Tenant: "default",
	}, []byte("orphan"))
}

func TestCancelRoutesToTarget(t *testing.T) {
	_, srv := newTestGateway(t, "a:ka:alice:default\nb:kb:bob:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	b, ctxB := connectAgent(t, srv, "kb", "bob-b")

	corr := "corr-cancel-1"
	// A REQUEST to B
	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.REQUEST,
		ID: "r1", Corr: corr, Dst: "bob-b", TTL: 5000,
	}, []byte("work"))
	env, _ := readFrame(t, ctxB, b)
	if env.Type != protocol.REQUEST {
		t.Fatalf("want REQUEST, got %s", protocol.TypeName(env.Type))
	}

	// A CANCEL
	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.CANCEL,
		ID: "c1", Corr: corr, Dst: "bob-b",
	}, nil)
	env, _ = readFrame(t, ctxB, b)
	if env.Type != protocol.CANCEL {
		t.Fatalf("want CANCEL, got %s", protocol.TypeName(env.Type))
	}
	if env.Corr != corr {
		t.Fatalf("corr: %q", env.Corr)
	}
	if env.Src != "alice-a" {
		t.Fatalf("src: %q", env.Src)
	}
}

func TestCancelOnInitiatorDisconnect(t *testing.T) {
	// A REQUEST to B, then A disconnects → B receives CANCEL for that corr.
	_, srv := newTestGateway(t, "a:ka:alice:default\nb:kb:bob:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	b, ctxB := connectAgent(t, srv, "kb", "bob-b")

	corr := "corr-disc-1"
	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.REQUEST,
		ID: "r1", Corr: corr, Dst: "bob-b", TTL: 30000,
	}, []byte("long-job"))
	env, _ := readFrame(t, ctxB, b)
	if env.Type != protocol.REQUEST {
		t.Fatalf("want REQUEST, got %s", protocol.TypeName(env.Type))
	}

	_ = a.Close(websocket.StatusNormalClosure, "bye")

	env, _ = readFrame(t, ctxB, b)
	if env.Type != protocol.CANCEL {
		t.Fatalf("want auto CANCEL on disconnect, got %s", protocol.TypeName(env.Type))
	}
	if env.Corr != corr {
		t.Fatalf("corr: %q", env.Corr)
	}
}
