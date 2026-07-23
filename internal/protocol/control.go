package protocol

import "github.com/vmihailenco/msgpack/v5"

// Control-frame payloads. Unlike the opaque user payload the Hub relays without
// inspection, HELLO/WELCOME/ERROR carry a small structured payload that the Hub
// (and SDKs) parse. These are NOT the frozen positional envelope; they use
// ordinary msgpack struct encoding and may evolve with the protocol version.

// Hello is the HELLO (0x01) payload: registration request. The proposed agentId
// is validated against the connection principal's namespace; token is verified
// against configured API keys. Tenant is advisory — the Hub binds tenant from
// the authenticated identity.
type Hello struct {
	Token     string   `msgpack:"token"`
	AgentID   string   `msgpack:"agentId"`
	Caps      []string `msgpack:"caps"`
	Protocols []string `msgpack:"protocols"`
	V         uint8    `msgpack:"v"`
}

// Welcome is the WELCOME (0x02) payload: registration success. Session is an
// audit-correlation id only (no state recovery on reconnect, DESIGN §4.7).
type Welcome struct {
	Session     string   `msgpack:"session"`
	HeartbeatMs int      `msgpack:"heartbeatMs"`
	Features    []string `msgpack:"features"`
}

// ErrorPayload is the ERROR (0xFF) payload: a stable error code plus optional
// human-readable message and, for UNSUPPORTED_VERSION, the supported versions.
type ErrorPayload struct {
	Code      string  `msgpack:"code"`
	Message   string  `msgpack:"message,omitempty"`
	Supported []uint8 `msgpack:"supported,omitempty"`
}

// MarshalHello encodes a Hello payload.
func MarshalHello(h Hello) ([]byte, error) { return msgpack.Marshal(&h) }

// UnmarshalHello decodes a Hello payload.
func UnmarshalHello(b []byte) (Hello, error) {
	var h Hello
	err := msgpack.Unmarshal(b, &h)
	return h, err
}

// MarshalWelcome encodes a Welcome payload.
func MarshalWelcome(w Welcome) ([]byte, error) { return msgpack.Marshal(&w) }

// UnmarshalWelcome decodes a Welcome payload.
func UnmarshalWelcome(b []byte) (Welcome, error) {
	var w Welcome
	err := msgpack.Unmarshal(b, &w)
	return w, err
}

// MarshalError encodes an ErrorPayload.
func MarshalError(e ErrorPayload) ([]byte, error) { return msgpack.Marshal(&e) }

// UnmarshalError decodes an ErrorPayload.
func UnmarshalError(b []byte) (ErrorPayload, error) {
	var e ErrorPayload
	err := msgpack.Unmarshal(b, &e)
	return e, err
}
