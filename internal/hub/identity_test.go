package hub

import (
	"testing"

	"github.com/online111111/agentmesh/internal/protocol"
)

func TestApplyIdentityOverwrite(t *testing.T) {
	// A client tries to spoof src and tenant; the Hub must overwrite both from
	// the connection's authenticated identity (DESIGN §6 trust root).
	env := protocol.Envelope{
		V:      protocol.ProtocolVersion,
		Type:   protocol.SEND,
		ID:     "msg-1",
		Corr:   "corr-1",
		Stream: "s-1",
		Src:    "victim-agent", // spoofed
		Tenant: "other-tenant", // spoofed
		Dst:    "bob-node",
		TTL:    5000,
		Hops:   2,
		Hdr:    map[string]string{"k": "v"},
	}
	out := applyIdentity(env, "alice-laptop", "default")
	if out.Src != "alice-laptop" {
		t.Errorf("src not overwritten: got %q, want alice-laptop", out.Src)
	}
	if out.Tenant != "default" {
		t.Errorf("tenant not overwritten: got %q, want default", out.Tenant)
	}
	// Non-identity fields are preserved.
	if out.Dst != "bob-node" || out.Type != protocol.SEND {
		t.Errorf("non-identity fields altered: %+v", out)
	}
	if out.ID != "msg-1" || out.Corr != "corr-1" || out.Stream != "s-1" {
		t.Errorf("routing metadata altered: %+v", out)
	}
	if out.TTL != 5000 || out.Hops != 2 || out.V != protocol.ProtocolVersion {
		t.Errorf("ttl/hops/v altered: %+v", out)
	}
	if out.Hdr["k"] != "v" {
		t.Errorf("hdr altered: %+v", out.Hdr)
	}
}

func TestApplyIdentityOverwriteAlways(t *testing.T) {
	// Even when the client leaves src/tenant empty, they are set from identity.
	out := applyIdentity(protocol.Envelope{Type: protocol.REQUEST, Dst: "x"}, "alice-laptop", "t7")
	if out.Src != "alice-laptop" || out.Tenant != "t7" {
		t.Fatalf("identity not applied to empty fields: %+v", out)
	}
}

func TestApplyIdentityIgnoresClientAlways(t *testing.T) {
	// Regardless of what the client claims, identity is connection-derived.
	cases := []struct {
		src, tenant string
	}{
		{"", ""},
		{"alice-laptop", "default"}, // matching still overwritten (no short-circuit)
		{"admin", "system"},
		{"topic:foo", "default"},
	}
	for _, tc := range cases {
		out := applyIdentity(protocol.Envelope{Src: tc.src, Tenant: tc.tenant, Type: protocol.SEND}, "alice-phone", "tenant-a")
		if out.Src != "alice-phone" || out.Tenant != "tenant-a" {
			t.Errorf("input src=%q tenant=%q → got src=%q tenant=%q", tc.src, tc.tenant, out.Src, out.Tenant)
		}
	}
}
