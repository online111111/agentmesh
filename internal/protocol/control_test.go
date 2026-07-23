package protocol

import (
	"reflect"
	"testing"
)

func TestHelloRoundTrip(t *testing.T) {
	h := Hello{
		Token:     "ka",
		AgentID:   "alice-laptop",
		Caps:      []string{"echo", "code"},
		Protocols: []string{"mesh/1"},
		V:         ProtocolVersion,
	}
	b, err := MarshalHello(h)
	if err != nil {
		t.Fatalf("MarshalHello: %v", err)
	}
	got, err := UnmarshalHello(b)
	if err != nil {
		t.Fatalf("UnmarshalHello: %v", err)
	}
	if !reflect.DeepEqual(got, h) {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got, h)
	}
}

func TestWelcomeRoundTrip(t *testing.T) {
	w := Welcome{Session: "01H...", HeartbeatMs: 15000, Features: []string{"stream"}}
	b, err := MarshalWelcome(w)
	if err != nil {
		t.Fatalf("MarshalWelcome: %v", err)
	}
	got, err := UnmarshalWelcome(b)
	if err != nil {
		t.Fatalf("UnmarshalWelcome: %v", err)
	}
	if !reflect.DeepEqual(got, w) {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, w)
	}
}

func TestErrorPayloadRoundTrip(t *testing.T) {
	e := ErrorPayload{Code: ErrUnsupportedVersion, Message: "v2 not supported", Supported: []uint8{1}}
	b, err := MarshalError(e)
	if err != nil {
		t.Fatalf("MarshalError: %v", err)
	}
	got, err := UnmarshalError(b)
	if err != nil {
		t.Fatalf("UnmarshalError: %v", err)
	}
	if got.Code != e.Code || got.Message != e.Message || !reflect.DeepEqual(got.Supported, e.Supported) {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, e)
	}
}
