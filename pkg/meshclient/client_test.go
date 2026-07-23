package meshclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/hub"
	"github.com/online111111/agentmesh/internal/protocol"
)

// startTestHub starts a full Hub (HTTP+WS) with the given key spec and returns
// its base HTTP URL (http://127.0.0.1:port).
func startTestHub(t *testing.T, keySpec string) string {
	t.Helper()
	keys, err := auth.ParseKeys(keySpec)
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	a := auth.NewAuthenticator(keys)
	g := hub.NewGateway(a, hub.NewRegistry(), 1<<20, 1<<20)
	h := hub.NewHTTP(g, a)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestDialWelcome(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := Dial(ctx, Options{
		HubURL:  base,
		Token:   "ka",
		AgentID: "alice-laptop",
		Caps:    []string{"echo"},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if c.AgentID() != "alice-laptop" {
		t.Fatalf("AgentID: %q", c.AgentID())
	}
	if c.Session() == "" {
		t.Fatal("Session empty after WELCOME")
	}
}

func TestDialAuthFailed(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := Dial(ctx, Options{
		HubURL:  base,
		Token:   "wrong",
		AgentID: "alice-laptop",
	})
	if err == nil {
		t.Fatal("expected auth failure")
	}
}

func TestDialForbiddenAgentID(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := Dial(ctx, Options{
		HubURL:  base,
		Token:   "ka",
		AgentID: "bob-x", // outside alice- namespace
	})
	if err == nil {
		t.Fatal("expected AGENTID_FORBIDDEN")
	}
}

// compile-time check that httptest is referenced if needed later
var _ = http.StatusOK

func TestSendOnMessage(t *testing.T) {
	// Two clients via Hub: A SEND → B OnMessage receives with trusted src.
	base := startTestHub(t, "a:ka:alice:default\nb:kb:bob:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a, err := Dial(ctx, Options{HubURL: base, Token: "ka", AgentID: "alice-a"})
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer a.Close()
	b, err := Dial(ctx, Options{HubURL: base, Token: "kb", AgentID: "bob-b"})
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	defer b.Close()

	got := make(chan struct {
		src     string
		payload string
	}, 1)
	b.OnMessage(func(env protocol.Envelope, payload []byte) {
		if env.Type != protocol.SEND {
			return
		}
		got <- struct {
			src     string
			payload string
		}{env.Src, string(payload)}
	})

	// Give B's readLoop a moment to be ready (handler registered).
	time.Sleep(20 * time.Millisecond)

	if err := a.Send(ctx, "bob-b", []byte("ping-from-a")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-got:
		if m.src != "alice-a" {
			t.Fatalf("src: got %q want alice-a", m.src)
		}
		if m.payload != "ping-from-a" {
			t.Fatalf("payload: got %q", m.payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for OnMessage")
	}
}

func TestRequestResponse(t *testing.T) {
	// A Request → B echoes RESPONSE with same corr + payload.
	base := startTestHub(t, "a:ka:alice:default\nb:kb:bob:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a, err := Dial(ctx, Options{HubURL: base, Token: "ka", AgentID: "alice-a"})
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer a.Close()
	b, err := Dial(ctx, Options{HubURL: base, Token: "kb", AgentID: "bob-b"})
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	defer b.Close()

	// B: echo REQUEST → RESPONSE
	b.OnMessage(func(env protocol.Envelope, payload []byte) {
		if env.Type != protocol.REQUEST {
			return
		}
		resp := protocol.Envelope{
			V:    protocol.ProtocolVersion,
			Type: protocol.RESPONSE,
			ID:   protocol.NewID(),
			Corr: env.Corr,
			Src:  "bob-b",
			Dst:  env.Src,
		}
		_ = b.WriteFrame(context.Background(), resp, payload)
	})
	time.Sleep(20 * time.Millisecond)

	out, err := a.Request(ctx, "bob-b", []byte("ping-req"), 3000)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if out.From != "bob-b" {
		t.Fatalf("from: %q", out.From)
	}
	if string(out.Payload) != "ping-req" {
		t.Fatalf("payload: %q", out.Payload)
	}
	if out.Corr == "" {
		t.Fatal("corr empty")
	}
}

func TestRequestTimeout(t *testing.T) {
	// Target never replies → TIMEOUT within ttl.
	base := startTestHub(t, "a:ka:alice:default\nb:kb:bob:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a, err := Dial(ctx, Options{HubURL: base, Token: "ka", AgentID: "alice-a"})
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer a.Close()
	b, err := Dial(ctx, Options{HubURL: base, Token: "kb", AgentID: "bob-silent"})
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	defer b.Close()
	// B registers but never answers.
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	_, err = a.Request(ctx, "bob-silent", []byte("x"), 150)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected TIMEOUT")
	}
	if !IsTimeout(err) {
		t.Fatalf("want TIMEOUT error, got %v", err)
	}
	if elapsed < 100*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("timeout window unexpected: %v", elapsed)
	}
}

func TestRequestNoRoute(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a, err := Dial(ctx, Options{HubURL: base, Token: "ka", AgentID: "alice-a"})
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer a.Close()

	_, err = a.Request(ctx, "alice-missing", []byte("x"), 1000)
	if err == nil {
		t.Fatal("expected NO_ROUTE")
	}
	if !IsRPCCode(err, protocol.ErrNoRoute) {
		t.Fatalf("want NO_ROUTE, got %v", err)
	}
}
