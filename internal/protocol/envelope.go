package protocol

// ProtocolVersion is the frozen wire protocol version for v1 (DESIGN §4.9).
const ProtocolVersion uint8 = 1

// Envelope is the small, fixed routing header that leads every frame after the
// type byte (DESIGN §4.2). The opaque payload is NOT part of the envelope; it is
// the raw trailing bytes of the frame (DESIGN §4.1).
//
// On the wire the envelope is encoded as a fixed-order msgpack ARRAY of 11
// slots (see frame.go) so that Go/Python/TypeScript SDKs produce byte-identical
// output. Field/slot order is frozen per DESIGN §4.2:
//
//	[0] v, [1] type, [2] id, [3] corr, [4] stream, [5] src, [6] dst,
//	[7] tenant, [8] ttl, [9] hops, [10] hdr
type Envelope struct {
	V      uint8             // [0] protocol version
	Type   MsgType           // [1] message type (§4.3)
	ID     string            // [2] message unique id (ULID)
	Corr   string            // [3] request/response correlation id
	Stream string            // [4] stream id (STREAM_* frames only)
	Src    string            // [5] source agentId (Hub overwrites from identity)
	Dst    string            // [6] target agentId or topic:<name>
	Tenant string            // [7] tenant (Hub overwrites from identity)
	TTL    int32             // [8] relative timeout in ms
	Hops   uint8             // [9] remaining hop count (loop guard)
	Hdr    map[string]string // [10] optional headers (content-type, trace, ...)
}
