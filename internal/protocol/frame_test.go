package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
	"unsafe"

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

func TestDecodeFrameRoundTrip(t *testing.T) {
	env := Envelope{
		V:      ProtocolVersion,
		Type:   REQUEST,
		ID:     "01ID",
		Corr:   "01CORR",
		Stream: "01STREAM",
		Src:    "alice-laptop",
		Dst:    "bob-server",
		Tenant: "acme",
		TTL:    5000,
		Hops:   8,
		Hdr:    map[string]string{"ct": "application/x-mesh-task", "trace": "abc"},
	}
	payload := []byte("multibyte-🚀-payload")

	frame, err := EncodeFrame(env, payload)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	got, gotPayload, err := DecodeFrame(frame)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if got.V != env.V || got.Type != env.Type || got.ID != env.ID ||
		got.Corr != env.Corr || got.Stream != env.Stream || got.Src != env.Src ||
		got.Dst != env.Dst || got.Tenant != env.Tenant || got.TTL != env.TTL ||
		got.Hops != env.Hops {
		t.Fatalf("envelope mismatch:\n got=%+v\nwant=%+v", got, env)
	}
	if len(got.Hdr) != len(env.Hdr) {
		t.Fatalf("hdr len = %d, want %d", len(got.Hdr), len(env.Hdr))
	}
	for k, v := range env.Hdr {
		if got.Hdr[k] != v {
			t.Fatalf("hdr[%q] = %q, want %q", k, got.Hdr[k], v)
		}
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload = %q, want %q", gotPayload, payload)
	}
}

func TestDecodeFrameZeroCopyPayload(t *testing.T) {
	env := Envelope{V: ProtocolVersion, Type: SEND, Src: "a", Dst: "b"}
	payload := []byte("zero-copy-payload-bytes")

	frame, err := EncodeFrame(env, payload)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	_, gotPayload, err := DecodeFrame(frame)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if len(gotPayload) == 0 {
		t.Fatal("expected non-empty payload for aliasing check")
	}

	// Locate the payload offset inside frame the same way the decoder must.
	envLen, n := binary.Uvarint(frame[1:])
	off := 1 + n + int(envLen)

	// The returned payload slice must share the ORIGINAL frame backing array
	// (zero-copy tail), not be a copy.
	if unsafe.SliceData(gotPayload) != unsafe.SliceData(frame[off:]) {
		t.Fatal("payload was copied; DecodeFrame must return a slice into the original buffer")
	}
}

func TestDecodeFrameEmptyPayloadNoAlias(t *testing.T) {
	env := Envelope{V: ProtocolVersion, Type: PING}
	frame, err := EncodeFrame(env, nil)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	_, gotPayload, err := DecodeFrame(frame)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if len(gotPayload) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(gotPayload))
	}
}

func TestDecodeFrameTooBig(t *testing.T) {
	// Craft a frame whose declared env_len exceeds MaxFrameBytes without
	// actually allocating a huge buffer.
	saved := MaxFrameBytes
	MaxFrameBytes = 8
	defer func() { MaxFrameBytes = saved }()

	var buf []byte
	buf = append(buf, byte(SEND))
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(1<<20)) // 1 MiB declared env
	buf = append(buf, lenBuf[:n]...)
	buf = append(buf, 0x90) // a lone byte (nowhere near declared length)

	_, _, err := DecodeFrame(buf)
	if err == nil {
		t.Fatal("expected FRAME_TOO_BIG error")
	}
	if err.Error() != ErrFrameTooBig {
		t.Fatalf("err = %q, want %q", err.Error(), ErrFrameTooBig)
	}
}

func TestDecodeFrameMalformed(t *testing.T) {
	// Build a valid frame to derive a truncated variant.
	valid, err := EncodeFrame(Envelope{V: ProtocolVersion, Type: SEND, Src: "a", Dst: "b"}, []byte("pl"))
	if err != nil {
		t.Fatal(err)
	}

	// env_len that overruns remaining bytes.
	var overrun []byte
	overrun = append(overrun, byte(SEND))
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], 100)
	overrun = append(overrun, lb[:n]...)
	overrun = append(overrun, 0x90, 0x01) // far fewer than 100 bytes

	// Corrupted msgpack envelope: declares a small env_len of junk bytes.
	var corrupt []byte
	corrupt = append(corrupt, byte(SEND))
	n2 := binary.PutUvarint(lb[:], 3)
	corrupt = append(corrupt, lb[:n2]...)
	corrupt = append(corrupt, 0xc1, 0xc1, 0xc1) // 0xc1 is "never used" in msgpack

	// Envelope that is a valid msgpack array but wrong arity.
	var wrongArity bytes.Buffer
	enc := msgpack.NewEncoder(&wrongArity)
	_ = enc.EncodeArrayLen(3)
	_ = enc.EncodeUint(1)
	_ = enc.EncodeUint(uint64(SEND))
	_ = enc.EncodeString("id")
	var badArity []byte
	badArity = append(badArity, byte(SEND))
	n3 := binary.PutUvarint(lb[:], uint64(wrongArity.Len()))
	badArity = append(badArity, lb[:n3]...)
	badArity = append(badArity, wrongArity.Bytes()...)

	cases := []struct {
		name string
		buf  []byte
	}{
		{"empty", nil},
		{"only_type", []byte{byte(SEND)}},
		{"truncated_varint", []byte{byte(SEND), 0x80}}, // continuation bit set, no more bytes
		{"env_overrun", overrun},
		{"corrupt_envelope", corrupt},
		{"wrong_arity", badArity},
		{"truncated_env", valid[:len(valid)-3]}, // chop into envelope/payload region
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("DecodeFrame panicked on %s: %v", c.name, r)
				}
			}()
			_, _, err := DecodeFrame(c.buf)
			if err == nil {
				t.Fatalf("expected error for malformed frame %q", c.name)
			}
		})
	}
}
