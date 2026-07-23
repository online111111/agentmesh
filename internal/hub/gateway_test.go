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
