// Package auth implements per-connection API key authentication and the
// principal->agentId namespace authorization that forms the Hub trust root
// (DESIGN §6). The connection's authenticated identity is the only trusted
// source of src/tenant/agentId; client-reported values are ignored.
package auth

import (
	"crypto/subtle"
	"errors"
	"strings"

	"github.com/online111111/agentmesh/internal/protocol"
)

// Key is one parsed API key entry. Spec format (DESIGN §9 MESH_API_KEYS):
//
//	id:secret:principal:tenant[:agentIdPrefix]
//
// The optional 5th field overrides the agentId namespace prefix; when absent it
// defaults to "<principal>-" (see NewAuthenticator).
type Key struct {
	ID            string
	Secret        string
	Principal     string
	Tenant        string
	AgentIDPrefix string
}

// Identity is the authenticated connection identity derived from a Key. It is
// the trust root: the Hub overwrites src/tenant on every frame from this
// identity, and binds registrable agentIds to its namespace.
type Identity struct {
	Principal     string
	Tenant        string
	AgentIDPrefix string
}

// ErrAuthFailed is returned when a token does not match any configured key. Its
// Error() is the stable protocol.ErrAuthFailed code string.
var ErrAuthFailed = errors.New(protocol.ErrAuthFailed)

// ParseKeys parses a MESH_API_KEYS spec of newline-separated
// id:secret:principal:tenant[:agentIdPrefix] entries. Blank lines are ignored
// and surrounding whitespace is trimmed. An empty spec or any malformed line is
// an error (production must configure at least one valid key).
func ParseKeys(spec string) ([]Key, error) {
	var keys []Key
	for _, raw := range strings.Split(spec, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) < 4 || len(parts) > 5 {
			return nil, errors.New("auth: malformed key entry: " + line)
		}
		for i := 0; i < 4; i++ {
			if parts[i] == "" {
				return nil, errors.New("auth: empty field in key entry: " + line)
			}
		}
		k := Key{
			ID:        parts[0],
			Secret:    parts[1],
			Principal: parts[2],
			Tenant:    parts[3],
		}
		if len(parts) == 5 {
			k.AgentIDPrefix = parts[4]
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil, errors.New("auth: no keys configured")
	}
	return keys, nil
}

// Authenticator holds the configured keys and authenticates tokens in constant
// time relative to the secret comparison.
type Authenticator struct {
	keys []Key
}

// NewAuthenticator builds an Authenticator from parsed keys, defaulting each
// key's AgentIDPrefix to "<principal>-" when not explicitly provided.
func NewAuthenticator(keys []Key) *Authenticator {
	cp := make([]Key, len(keys))
	copy(cp, keys)
	for i := range cp {
		if cp[i].AgentIDPrefix == "" {
			cp[i].AgentIDPrefix = cp[i].Principal + "-"
		}
	}
	return &Authenticator{keys: cp}
}

// Authenticate matches a presented token (the key secret) against configured
// keys using a constant-time comparison, returning the derived Identity. It
// scans all keys to avoid early-exit timing leaks and never panics on a
// length mismatch. On no match it returns ErrAuthFailed.
func (a *Authenticator) Authenticate(token string) (*Identity, error) {
	var matched *Key
	tokenB := []byte(token)
	for i := range a.keys {
		secret := []byte(a.keys[i].Secret)
		if subtle.ConstantTimeCompare(secret, tokenB) == 1 {
			matched = &a.keys[i]
		}
	}
	if matched == nil {
		return nil, ErrAuthFailed
	}
	return &Identity{
		Principal:     matched.Principal,
		Tenant:        matched.Tenant,
		AgentIDPrefix: matched.AgentIDPrefix,
	}, nil
}
