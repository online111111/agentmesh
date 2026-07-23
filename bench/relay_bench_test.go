package bench_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/hub"
	"github.com/online111111/agentmesh/internal/protocol"
	"github.com/online111111/agentmesh/pkg/meshclient"
)

// maxRelayAllocs is the allocs/op gate for the SEND hot path (loose for v1).
const maxRelayAllocs = 200

func startHub(tb testing.TB) string {
	tb.Helper()
	keys, err := auth.ParseKeys("a:ka:alice:default\nb:kb:bob:default")
	if err != nil {
		tb.Fatal(err)
	}
	a := auth.NewAuthenticator(keys)
	g := hub.NewGateway(a, hub.NewRegistry(), 1<<20, 1<<20)
	g.SetLimiters(nil, nil)
	h := hub.NewHTTP(g, a)
	srv := httptest.NewServer(h)
	tb.Cleanup(srv.Close)
	return srv.URL
}

func BenchmarkSendRelay(b *testing.B) {
	base := startHub(b)
	ctx := context.Background()
	alice, err := meshclient.Dial(ctx, meshclient.Options{HubURL: base, Token: "ka", AgentID: "alice-bench"})
	if err != nil {
		b.Fatal(err)
	}
	defer alice.Close()
	bob, err := meshclient.Dial(ctx, meshclient.Options{HubURL: base, Token: "kb", AgentID: "bob-bench"})
	if err != nil {
		b.Fatal(err)
	}
	defer bob.Close()
	bob.OnMessage(func(env protocol.Envelope, payload []byte) {})

	payload := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := alice.Send(ctx, "bob-bench", payload); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSendRelayAllocsGate(t *testing.T) {
	base := startHub(t)
	ctx := context.Background()
	alice, err := meshclient.Dial(ctx, meshclient.Options{HubURL: base, Token: "ka", AgentID: "alice-alloc"})
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()
	bob, err := meshclient.Dial(ctx, meshclient.Options{HubURL: base, Token: "kb", AgentID: "bob-alloc"})
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Close()
	bob.OnMessage(func(env protocol.Envelope, payload []byte) {})
	payload := make([]byte, 64)
	for i := 0; i < 20; i++ {
		if err := alice.Send(ctx, "bob-alloc", payload); err != nil {
			t.Fatal(err)
		}
	}
	allocs := testing.AllocsPerRun(50, func() {
		if err := alice.Send(ctx, "bob-alloc", payload); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > maxRelayAllocs {
		t.Fatalf("allocs/op = %.1f > gate %d", allocs, maxRelayAllocs)
	}
	t.Logf("allocs/op = %.1f (gate %d)", allocs, maxRelayAllocs)
}
