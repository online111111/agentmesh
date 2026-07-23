package protocol

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestJSONRoundTrip(t *testing.T) {
	env := Envelope{
		V:      ProtocolVersion,
		Type:   REQUEST,
		ID:     "01ID",
		Corr:   "01CORR",
		Stream: "01S",
		Src:    "alice",
		Dst:    "bob",
		Tenant: "acme",
		TTL:    5000,
		Hops:   8,
		Hdr:    map[string]string{"ct": "application/x-mesh-task", "trace": "xyz"},
	}
	payload := []byte("json-debug-🚀-payload")

	text, err := EncodeJSON(env, payload)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	if !json.Valid(text) {
		t.Fatalf("EncodeJSON produced invalid JSON: %s", text)
	}

	gotEnv, gotPayload, err := DecodeJSON(text)
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if gotEnv.V != env.V || gotEnv.Type != env.Type || gotEnv.ID != env.ID ||
		gotEnv.Corr != env.Corr || gotEnv.Stream != env.Stream || gotEnv.Src != env.Src ||
		gotEnv.Dst != env.Dst || gotEnv.Tenant != env.Tenant || gotEnv.TTL != env.TTL ||
		gotEnv.Hops != env.Hops {
		t.Fatalf("envelope mismatch:\n got=%+v\nwant=%+v", gotEnv, env)
	}
	for k, v := range env.Hdr {
		if gotEnv.Hdr[k] != v {
			t.Fatalf("hdr[%q] = %q, want %q", k, gotEnv.Hdr[k], v)
		}
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload = %q, want %q", gotPayload, payload)
	}
}

func TestJSONEmptyPayload(t *testing.T) {
	env := Envelope{V: ProtocolVersion, Type: PING}
	text, err := EncodeJSON(env, nil)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	_, gotPayload, err := DecodeJSON(text)
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if len(gotPayload) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(gotPayload))
	}
}

func TestJSONPayloadBase64(t *testing.T) {
	// The payload must be base64-encoded in the JSON text form, so arbitrary
	// binary bytes survive.
	env := Envelope{V: ProtocolVersion, Type: SEND, Src: "a", Dst: "b"}
	payload := []byte{0x00, 0x01, 0xff, 0xfe, 0x80}
	text, err := EncodeJSON(env, payload)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	_, gotPayload, err := DecodeJSON(text)
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("binary payload round-trip failed: got % x want % x", gotPayload, payload)
	}
}
