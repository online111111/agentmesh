package protocol

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
)

// VectorInput is a canonical (name, envelope, payload) triple that drives the
// cross-language golden test vectors (DESIGN §7). GenerateVectors() returns the
// frozen set; the committed testvectors.json is produced from it and asserted
// byte-for-byte by testvectors_test.go and by the Python/TS spot-check.
//
// Because the wire envelope is FROZEN at 11 positional slots (B2), stream
// bookkeeping that has no dedicated slot — STREAM_DATA seq, STREAM_END
// status/usage — rides in the hdr sub-map (slot 10). This keeps the array at
// exactly 11 slots while still expressing the compact stream shapes required by
// DESIGN §4.1/§4.10.
type VectorInput struct {
	Name    string
	Env     Envelope
	Payload []byte
}

// GoldenVector is the serialized form stored in testvectors.json. It carries
// the input envelope (as JSON), the opaque payload as hex, and the canonical
// outputs: the full binary split frame as hex, and the debug JSON frame text.
type GoldenVector struct {
	Name       string          `json:"name"`
	Type       uint8           `json:"type"`
	TypeName   string          `json:"typeName"`
	Env        goldenEnv       `json:"env"`
	PayloadHex string          `json:"payloadHex"`
	FrameHex   string          `json:"frameHex"`
	EnvHex     string          `json:"envHex"`
	JSONFrame  json.RawMessage `json:"jsonFrame"`
}

// goldenEnv mirrors Envelope with stable JSON keys for the golden file so the
// Python/TS spot-check can reconstruct the same envelope independently.
type goldenEnv struct {
	V      uint8             `json:"v"`
	Type   uint8             `json:"type"`
	ID     string            `json:"id"`
	Corr   string            `json:"corr"`
	Stream string            `json:"stream"`
	Src    string            `json:"src"`
	Dst    string            `json:"dst"`
	Tenant string            `json:"tenant"`
	TTL    int32             `json:"ttl"`
	Hops   uint8             `json:"hops"`
	Hdr    map[string]string `json:"hdr"`
}

// GoldenFile is the top-level shape of testvectors.json.
type GoldenFile struct {
	Version uint8          `json:"version"`
	Slots   int            `json:"slots"`
	Vectors []GoldenVector `json:"vectors"`
}

// GenerateVectors returns the frozen canonical vector set. It MUST cover:
//   - all 21 message type values (DESIGN §4.3),
//   - the three compact stream shapes (STREAM_OPEN / STREAM_DATA / STREAM_END),
//     with STREAM_END status in {ok, error, aborted} plus a usage variant,
//   - boundary cases: empty payload, non-empty payload, hdr with >=2 keys
//     (exposes B3 key sorting), and multi-byte UTF-8 payload + agentId.
func GenerateVectors() []VectorInput {
	const v = ProtocolVersion
	return []VectorInput{
		// ---- control plane ----
		{
			Name: "hello",
			Env: Envelope{V: v, Type: HELLO, ID: "01HELLO", Src: "alice-laptop", Tenant: "acme",
				Hdr: map[string]string{"caps": "echo,code", "proto": "1"}},
			Payload: []byte(`{"token":"ka","agentId":"alice-laptop"}`),
		},
		{
			Name:    "welcome",
			Env:     Envelope{V: v, Type: WELCOME, ID: "01WELCOME", Dst: "alice-laptop", Tenant: "acme", Hdr: map[string]string{"heartbeat": "15000", "session": "S1"}},
			Payload: []byte(`{"session":"S1","features":["stream"]}`),
		},
		{
			Name:    "ping",
			Env:     Envelope{V: v, Type: PING, ID: "01PING"},
			Payload: nil,
		},
		{
			Name:    "pong",
			Env:     Envelope{V: v, Type: PONG, ID: "01PONG", Hdr: map[string]string{"rtt": "3"}},
			Payload: nil,
		},

		// ---- point-to-point / request-response ----
		{
			Name:    "send",
			Env:     Envelope{V: v, Type: SEND, ID: "01SEND", Src: "alice-laptop", Dst: "bob-server", Tenant: "acme", TTL: 0, Hops: 8},
			Payload: []byte(`{"hello":"from-b"}`),
		},
		{
			Name:    "request",
			Env:     Envelope{V: v, Type: REQUEST, ID: "01REQ", Corr: "01CORR", Src: "alice-laptop", Dst: "bob-server", Tenant: "acme", TTL: 30000, Hops: 8, Hdr: map[string]string{"ct": "application/x-mesh-task"}},
			Payload: []byte(`{"task":"summarize","stream":true}`),
		},
		{
			Name:    "response",
			Env:     Envelope{V: v, Type: RESPONSE, ID: "01RESP", Corr: "01CORR", Src: "bob-server", Dst: "alice-laptop", Tenant: "acme"},
			Payload: []byte(`{"ok":true}`),
		},
		{
			Name:    "cancel",
			Env:     Envelope{V: v, Type: CANCEL, ID: "01CANCEL", Corr: "01CORR", Src: "alice-laptop", Dst: "bob-server", Tenant: "acme"},
			Payload: nil,
		},
		{
			Name:    "ack",
			Env:     Envelope{V: v, Type: ACK, ID: "01SEND", Corr: "", Src: "bob-server", Dst: "alice-laptop", Tenant: "acme"},
			Payload: nil,
		},
		{
			Name:    "nack",
			Env:     Envelope{V: v, Type: NACK, ID: "01SEND", Src: "bob-server", Dst: "alice-laptop", Tenant: "acme", Hdr: map[string]string{"code": "QUEUE_FULL"}},
			Payload: nil,
		},

		// ---- streaming (three compact shapes) ----
		{
			// STREAM_OPEN binds to a corr and names the stream.
			Name:    "stream_open",
			Env:     Envelope{V: v, Type: STREAM_OPEN, ID: "01SO", Corr: "01CORR", Stream: "01STREAM", Src: "bob-server", Dst: "alice-laptop", Tenant: "acme"},
			Payload: nil,
		},
		{
			// STREAM_DATA compact shape: only type + stream + seq (in hdr).
			// All other slots empty to freeze the "compact == empty slots" bytes.
			Name:    "stream_data",
			Env:     Envelope{V: v, Type: STREAM_DATA, Stream: "01STREAM", Hdr: map[string]string{"seq": "0"}},
			Payload: []byte("token-0"),
		},
		{
			Name:    "stream_end_ok",
			Env:     Envelope{V: v, Type: STREAM_END, Stream: "01STREAM", Hdr: map[string]string{"status": "ok"}},
			Payload: nil,
		},
		{
			Name:    "stream_end_ok_usage",
			Env:     Envelope{V: v, Type: STREAM_END, Stream: "01STREAM", Hdr: map[string]string{"status": "ok", "usage": "42"}},
			Payload: nil,
		},
		{
			Name:    "stream_end_error",
			Env:     Envelope{V: v, Type: STREAM_END, Stream: "01STREAM", Hdr: map[string]string{"status": "error"}},
			Payload: nil,
		},
		{
			Name:    "stream_end_aborted",
			Env:     Envelope{V: v, Type: STREAM_END, Stream: "01STREAM", Hdr: map[string]string{"status": "aborted"}},
			Payload: nil,
		},

		// ---- pub/sub ----
		{
			Name:    "subscribe",
			Env:     Envelope{V: v, Type: SUBSCRIBE, ID: "01SUB", Src: "alice-laptop", Dst: "topic:news", Tenant: "acme"},
			Payload: nil,
		},
		{
			Name:    "suback",
			Env:     Envelope{V: v, Type: SUBACK, ID: "01SUB", Dst: "alice-laptop", Tenant: "acme", Hdr: map[string]string{"topic": "topic:news"}},
			Payload: nil,
		},
		{
			Name:    "unsub",
			Env:     Envelope{V: v, Type: UNSUB, ID: "01UNSUB", Src: "alice-laptop", Dst: "topic:news", Tenant: "acme"},
			Payload: nil,
		},
		{
			Name:    "publish",
			Env:     Envelope{V: v, Type: PUBLISH, ID: "01PUB", Src: "alice-laptop", Dst: "topic:news", Tenant: "acme", Hdr: map[string]string{"ct": "application/json"}},
			Payload: []byte(`{"headline":"hi"}`),
		},

		// ---- P2P (P7, frozen now for wire completeness) ----
		{
			Name:    "ticket_req",
			Env:     Envelope{V: v, Type: TICKET_REQ, ID: "01TREQ", Src: "alice-laptop", Dst: "bob-server", Tenant: "acme"},
			Payload: nil,
		},
		{
			Name:    "ticket",
			Env:     Envelope{V: v, Type: TICKET, ID: "01TICKET", Dst: "alice-laptop", Tenant: "acme", Hdr: map[string]string{"nonce": "abc", "ttl": "5000"}},
			Payload: []byte("ticket-blob"),
		},
		{
			Name:    "p2p_hello",
			Env:     Envelope{V: v, Type: P2P_HELLO, ID: "01P2P", Src: "alice-laptop", Dst: "bob-server", Tenant: "acme"},
			Payload: nil,
		},

		// ---- error ----
		{
			Name:    "error",
			Env:     Envelope{V: v, Type: ERROR, ID: "01ERR", Corr: "01CORR", Dst: "alice-laptop", Tenant: "acme", Hdr: map[string]string{"code": ErrNoRoute}},
			Payload: []byte(`{"code":"NO_ROUTE"}`),
		},

		// ---- boundary vectors ----
		{
			// hdr with >=2 keys deliberately supplied out of order to expose B3
			// (encoder MUST sort keys ascending).
			Name:    "hdr_multi_unsorted",
			Env:     Envelope{V: v, Type: SEND, ID: "01HDR", Src: "a", Dst: "b", Tenant: "acme", Hdr: map[string]string{"zeta": "1", "alpha": "2", "mid": "3"}},
			Payload: []byte("x"),
		},
		{
			// multi-byte UTF-8 in both agentId and payload.
			Name:    "utf8_multibyte",
			Env:     Envelope{V: v, Type: SEND, ID: "01UTF8", Src: "爱丽丝-🚀", Dst: "鲍勃-🛰", Tenant: "租户", TTL: 1000, Hops: 4},
			Payload: []byte("多字节-🚀-payload"),
		},
		{
			Name:    "empty_payload_full_env",
			Env:     Envelope{V: v, Type: SEND, ID: "01EMPTY", Src: "alice-laptop", Dst: "bob-server", Tenant: "acme", TTL: 5000, Hops: 8},
			Payload: nil,
		},
	}
}

// BuildGoldenFile encodes every vector and returns the GoldenFile ready to be
// marshalled into testvectors.json.
func BuildGoldenFile() (GoldenFile, error) {
	inputs := GenerateVectors()
	out := GoldenFile{Version: ProtocolVersion, Slots: envelopeSlots, Vectors: make([]GoldenVector, 0, len(inputs))}
	for _, in := range inputs {
		frame, err := EncodeFrame(in.Env, in.Payload)
		if err != nil {
			return GoldenFile{}, err
		}
		envBytes, err := encodeEnvelope(in.Env)
		if err != nil {
			return GoldenFile{}, err
		}
		jsonFrame, err := EncodeJSON(in.Env, in.Payload)
		if err != nil {
			return GoldenFile{}, err
		}
		out.Vectors = append(out.Vectors, GoldenVector{
			Name:     in.Name,
			Type:     uint8(in.Env.Type),
			TypeName: TypeName(in.Env.Type),
			Env: goldenEnv{
				V: in.Env.V, Type: uint8(in.Env.Type), ID: in.Env.ID, Corr: in.Env.Corr,
				Stream: in.Env.Stream, Src: in.Env.Src, Dst: in.Env.Dst, Tenant: in.Env.Tenant,
				TTL: in.Env.TTL, Hops: in.Env.Hops, Hdr: in.Env.Hdr,
			},
			PayloadHex: hex.EncodeToString(in.Payload),
			FrameHex:   hex.EncodeToString(frame),
			EnvHex:     hex.EncodeToString(envBytes),
			JSONFrame:  json.RawMessage(jsonFrame),
		})
	}
	return out, nil
}

// MarshalGolden returns indented JSON bytes for testvectors.json.
func MarshalGolden() ([]byte, error) {
	gf, err := BuildGoldenFile()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(gf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
