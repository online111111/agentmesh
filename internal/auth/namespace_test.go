package auth

import "testing"

func TestIdentityAllowsAgentID(t *testing.T) {
	keys, err := ParseKeys("a:ka:alice:default")
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	a := NewAuthenticator(keys)
	id, err := a.Authenticate("ka")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	// Default prefix derived from principal is "alice-".
	if id.AgentIDPrefix != "alice-" {
		t.Fatalf("want default prefix alice-, got %q", id.AgentIDPrefix)
	}

	allow := []string{"alice-laptop", "alice-", "alice-desktop-2"}
	for _, agentID := range allow {
		if !id.AllowsAgentID(agentID) {
			t.Errorf("AllowsAgentID(%q) = false, want true", agentID)
		}
	}
	deny := []string{"bob-x", "alic", "alice", "", "carol-alice-laptop"}
	for _, agentID := range deny {
		if id.AllowsAgentID(agentID) {
			t.Errorf("AllowsAgentID(%q) = true, want false", agentID)
		}
	}
}

func TestIdentityAllowsAgentID_ExplicitPrefix(t *testing.T) {
	keys, err := ParseKeys("a:ka:alice:default:team1-")
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	id, err := NewAuthenticator(keys).Authenticate("ka")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if !id.AllowsAgentID("team1-node") {
		t.Errorf("explicit prefix: team1-node should be allowed")
	}
	if id.AllowsAgentID("alice-node") {
		t.Errorf("explicit prefix overrides principal default; alice-node should be denied")
	}
}

func TestIdentityAllowsAgentID_RejectsTopicPrefix(t *testing.T) {
	// DESIGN §4.8: agentId must never start with "topic:" to keep the pub/sub
	// namespace disjoint from routable agentIds.
	keys, _ := ParseKeys("a:ka:topic:default")
	id, err := NewAuthenticator(keys).Authenticate("ka")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	// Even though the derived prefix would be "topic-", an explicit topic: id
	// must be refused regardless of prefix match.
	if id.AllowsAgentID("topic:broadcast") {
		t.Errorf("agentId starting with topic: must be denied")
	}
}
