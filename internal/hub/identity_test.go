package hub

import (
	"testing"

	"github.com/online111111/agentmesh/internal/protocol"
)

func TestApplyIdentityOverwrite(t *testing.T) {
	// A client tries to spoof src and tenant; the Hub must overwrite both from
	// the connection's authenticated identity (DESIGN §6 trust root).
	env := protocol.Envelope{
		Type:   protocol.SEND,
		Src:    "victim-agent", // spoofed
		Tenant: "other-tenant", // spoofed
		Dst:    "bob-node",
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
}

func TestApplyIdentityOverwriteAlways(t *testing.T) {
	// Even when the client leaves src/tenant empty, they are set from identity.
	out := applyIdentity(protocol.Envelope{Type: protocol.REQUEST, Dst: "x"}, "alice-laptop", "t7")
	if out.Src != "alice-laptop" || out.Tenant != "t7" {
		t.Fatalf("identity not applied to empty fields: %+v", out)
	}
}
