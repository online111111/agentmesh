package hub

import (
	"context"
	"testing"
	"time"

	"github.com/online111111/agentmesh/internal/protocol"
)

func TestPubSubSubscribePublish(t *testing.T) {
	_, srv := newTestGateway(t, "a:ka:alice:default\nb:kb:bob:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	b, ctxB := connectAgent(t, srv, "kb", "bob-b")

	// A SUBSCRIBE topic:news → expects SUBACK
	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.SUBSCRIBE,
		ID: "sub1", Dst: "topic:news",
	}, nil)
	env, _ := readFrame(t, ctxA, a)
	if env.Type != protocol.SUBACK {
		t.Fatalf("want SUBACK, got %s", protocol.TypeName(env.Type))
	}

	// B PUBLISH to topic:news → A receives PUBLISH
	sendFrame(t, ctxB, b, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.PUBLISH,
		ID: "pub1", Dst: "topic:news",
	}, []byte(`{"n":1}`))
	env, payload := readFrame(t, ctxA, a)
	if env.Type != protocol.PUBLISH {
		t.Fatalf("A want PUBLISH, got %s", protocol.TypeName(env.Type))
	}
	if env.Src != "bob-b" {
		t.Fatalf("src: %q", env.Src)
	}
	if string(payload) != `{"n":1}` {
		t.Fatalf("payload: %q", payload)
	}
}

func TestPubSubNoSelfDelivery(t *testing.T) {
	_, srv := newTestGateway(t, "a:ka:alice:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")

	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.SUBSCRIBE,
		ID: "sub1", Dst: "topic:news",
	}, nil)
	env, _ := readFrame(t, ctxA, a)
	if env.Type != protocol.SUBACK {
		t.Fatalf("SUBACK: %s", protocol.TypeName(env.Type))
	}

	// Self publish should not be delivered back by default.
	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.PUBLISH,
		ID: "pub1", Dst: "topic:news",
	}, []byte("self"))

	// Brief wait; no frame expected. Use short read context.
	short, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, _, err := a.Read(short)
	if err == nil {
		t.Fatal("self-delivery should be suppressed by default")
	}
}

func TestPubSubUnsub(t *testing.T) {
	_, srv := newTestGateway(t, "a:ka:alice:default\nb:kb:bob:default")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	b, ctxB := connectAgent(t, srv, "kb", "bob-b")

	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.SUBSCRIBE,
		ID: "sub1", Dst: "topic:news",
	}, nil)
	_, _ = readFrame(t, ctxA, a) // SUBACK

	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.UNSUB,
		ID: "unsub1", Dst: "topic:news",
	}, nil)

	// Publish after unsub: A must not receive.
	sendFrame(t, ctxB, b, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.PUBLISH,
		ID: "pub1", Dst: "topic:news",
	}, []byte("x"))
	short, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, _, err := a.Read(short)
	if err == nil {
		t.Fatal("expected no delivery after UNSUB")
	}
}

func TestPubSubTenantIsolation(t *testing.T) {
	// alice in default, dave in other tenant — no cross-tenant publish delivery.
	_, srv := newTestGateway(t, "a:ka:alice:default\nd:kd:dave:other")
	a, ctxA := connectAgent(t, srv, "ka", "alice-a")
	d, ctxD := connectAgent(t, srv, "kd", "dave-d")

	sendFrame(t, ctxA, a, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.SUBSCRIBE,
		ID: "sub1", Dst: "topic:news",
	}, nil)
	_, _ = readFrame(t, ctxA, a)

	sendFrame(t, ctxD, d, protocol.Envelope{
		V: protocol.ProtocolVersion, Type: protocol.PUBLISH,
		ID: "pub1", Dst: "topic:news",
	}, []byte("cross"))
	short, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, _, err := a.Read(short)
	if err == nil {
		t.Fatal("cross-tenant PUBLISH must not deliver")
	}
}
