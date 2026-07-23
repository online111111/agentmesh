package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestEncodeFrameLayout(t *testing.T) {
	env := Envelope{
		V:      ProtocolVersion,
		Type:   SEND,
		ID:     "01ABCID",
		Corr:   "",
		Stream: "",
		Src:    "alice",
		Dst:    "bob",
		Tenant: "acme",
		TTL:    5000,
		Hops:   8,
		Hdr:    nil,
	}
	payload := []byte("hello-opaque-payload")

	frame, err := EncodeFrame(env, payload)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	// Byte 0 must be the type.
	if frame[0] != byte(SEND) {
		t.Fatalf("frame[0] = 0x%02X, want 0x%02X", frame[0], byte(SEND))
	}

	// Bytes 1.. is a uvarint env_len.
	envLen, n := binary.Uvarint(frame[1:])
	if n <= 0 {
		t.Fatalf("bad uvarint env_len, n=%d", n)
	}
	off := 1 + n

	// Envelope segment is exactly env_len bytes.
	if off+int(envLen) > len(frame) {
		t.Fatalf("env_len %d overruns frame len %d", envLen, len(frame))
	}
	envBytes := frame[off : off+int(envLen)]

	// Trailing payload must equal the original payload byte-for-byte (proves it
	// was appended raw, not rewritten and not inside the msgpack body).
	tail := frame[off+int(envLen):]
	if !bytes.Equal(tail, payload) {
		t.Fatalf("payload tail = %q, want %q", tail, payload)
	}

	// The envelope must decode as a msgpack ARRAY of 11 elements (positional).
	dec := msgpack.NewDecoder(bytes.NewReader(envBytes))
	arrLen, err := dec.DecodeArrayLen()
	if err != nil {
		t.Fatalf("envelope is not a msgpack array: %v", err)
	}
	if arrLen != 11 {
		t.Fatalf("envelope array len = %d, want 11 positional slots", arrLen)
	}
}

func TestEncodeFrameEmptyPayload(t *testing.T) {
	env := Envelope{V: ProtocolVersion, Type: PING}
	frame, err := EncodeFrame(env, nil)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if frame[0] != byte(PING) {
		t.Fatalf("frame[0] = 0x%02X, want 0x%02X", frame[0], byte(PING))
	}
	envLen, n := binary.Uvarint(frame[1:])
	if n <= 0 {
		t.Fatal("bad uvarint")
	}
	if 1+n+int(envLen) != len(frame) {
		t.Fatalf("empty-payload frame has trailing bytes: total=%d expected=%d", len(frame), 1+n+int(envLen))
	}
}

func TestEncodeFrameDeterministic(t *testing.T) {
	env := Envelope{
		V:    ProtocolVersion,
		Type: REQUEST,
		ID:   "id1",
		Src:  "a",
		Dst:  "b",
		Hdr:  map[string]string{"z": "1", "a": "2", "m": "3"},
	}
	a, err := EncodeFrame(env, []byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeFrame(env, []byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("EncodeFrame not deterministic across calls")
	}
}
