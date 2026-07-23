package hub

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/protocol"
)

func TestTokenBucketBurst(t *testing.T) {
	b := NewTokenBucket(10, 3) // 10/s, burst 3
	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("burst token %d denied", i)
		}
	}
	if b.Allow() {
		t.Fatal("4th token should be denied")
	}
	time.Sleep(150 * time.Millisecond) // ~1.5 tokens
	if !b.Allow() {
		t.Fatal("should refill after wait")
	}
}

func TestRateLimiterPerKey(t *testing.T) {
	r := NewRateLimiter(100, 1)
	if !r.Allow("a") {
		t.Fatal("a denied")
	}
	if r.Allow("a") {
		t.Fatal("a second should deny at burst=1")
	}
	if !r.Allow("b") {
		t.Fatal("b independent key")
	}
}

func TestPerAgentRateLimit(t *testing.T) {
	// Gateway with tiny msg rate rejects excess SEND with RATE_LIMITED.
	keys, _ := auth.ParseKeys("a:ka:alice:default")
	g := NewGateway(auth.NewAuthenticator(keys), NewRegistry(), 1<<20, 1<<20)
	g.msgLimiter = NewRateLimiter(1000, 2) // burst 2
	srv := httptest.NewServer(http.HandlerFunc(g.ServeWS))
	t.Cleanup(srv.Close)

	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	// Need a peer for successful send path; offline still goes through rate limit first.
	for i := 0; i < 2; i++ {
		sendFrame(t, ctxA, a, protocol.Envelope{V: 1, Type: protocol.SEND, Dst: "alice-missing"}, []byte("x"))
		// drain ERROR (NO_ROUTE or RATE_LIMITED)
		_, _ = readFrame(t, ctxA, a)
	}
	// Third should be RATE_LIMITED (burst=2)
	sendFrame(t, ctxA, a, protocol.Envelope{V: 1, Type: protocol.SEND, Dst: "alice-missing"}, []byte("x"))
	env, payload := readFrame(t, ctxA, a)
	if env.Type != protocol.ERROR {
		t.Fatalf("want ERROR, got %s", protocol.TypeName(env.Type))
	}
	ep, _ := protocol.UnmarshalError(payload)
	if ep.Code != protocol.ErrRateLimited {
		t.Fatalf("code: %s want RATE_LIMITED", ep.Code)
	}
}
