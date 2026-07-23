package auth

import (
	"testing"

	"github.com/online111111/agentmesh/internal/protocol"
)

func TestParseKeys_MultiLine(t *testing.T) {
	spec := "a:ka:alice:default\nb:kb:bob:default\n\n  c:kc:carol:t2  "
	keys, err := ParseKeys(spec)
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("want 3 keys, got %d", len(keys))
	}
	if keys[0].ID != "a" || keys[0].Secret != "ka" || keys[0].Principal != "alice" || keys[0].Tenant != "default" {
		t.Fatalf("key0 mismatch: %+v", keys[0])
	}
	if keys[2].ID != "c" || keys[2].Tenant != "t2" {
		t.Fatalf("key2 mismatch (trim/blank lines): %+v", keys[2])
	}
}

func TestParseKeys_OptionalAgentIDPrefix(t *testing.T) {
	// Optional 5th field overrides the default principal-derived prefix.
	keys, err := ParseKeys("a:ka:alice:default:alice-")
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	if keys[0].AgentIDPrefix != "alice-" {
		t.Fatalf("want explicit prefix alice-, got %q", keys[0].AgentIDPrefix)
	}
}

func TestParseKeys_Errors(t *testing.T) {
	cases := []string{
		"",                 // empty -> no keys is an error (production must have keys)
		"onlyid",           // too few fields
		"a:ka:alice",       // missing tenant
		"a::alice:default", // empty secret
	}
	for _, spec := range cases {
		if _, err := ParseKeys(spec); err == nil {
			t.Fatalf("ParseKeys(%q): expected error, got nil", spec)
		}
	}
}

func TestAuthenticate_Valid(t *testing.T) {
	keys, err := ParseKeys("a:ka:alice:default\nb:kb:bob:t2")
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	a := NewAuthenticator(keys)

	id, err := a.Authenticate("ka")
	if err != nil {
		t.Fatalf("Authenticate(ka): %v", err)
	}
	if id.Principal != "alice" || id.Tenant != "default" {
		t.Fatalf("identity mismatch: %+v", id)
	}

	id2, err := a.Authenticate("kb")
	if err != nil {
		t.Fatalf("Authenticate(kb): %v", err)
	}
	if id2.Principal != "bob" || id2.Tenant != "t2" {
		t.Fatalf("identity mismatch: %+v", id2)
	}
}

func TestAuthenticate_Invalid(t *testing.T) {
	keys, _ := ParseKeys("a:ka:alice:default")
	a := NewAuthenticator(keys)

	// Wrong secret and a different-length secret must both fail without panic
	// (constant-time compare handles length mismatch safely).
	for _, tok := range []string{"nope", "", "kakaka-longer"} {
		if _, err := a.Authenticate(tok); err == nil {
			t.Fatalf("Authenticate(%q): expected error", tok)
		} else if err.Error() != protocol.ErrAuthFailed {
			t.Fatalf("Authenticate(%q): want AUTH_FAILED, got %v", tok, err)
		}
	}
}
