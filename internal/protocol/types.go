package protocol

// MsgType is the single-byte message type/subtype that leads every frame
// (see docs/design/mesh-design.md §4.3). Each subtype has a distinct value so a
// received frame is never ambiguous (no shared 0x20 meaning OPEN-or-DATA-or-END).
type MsgType uint8

// Message type constants, frozen per DESIGN §4.3.
const (
	HELLO   MsgType = 0x01 // register: token + agentId + capabilities + protocols
	WELCOME MsgType = 0x02 // registration success: session + heartbeat + features
	PING    MsgType = 0x03 // heartbeat request
	PONG    MsgType = 0x04 // heartbeat response + RTT

	SEND     MsgType = 0x10 // point-to-point fire-and-forget
	REQUEST  MsgType = 0x11 // request with corr + ttl
	RESPONSE MsgType = 0x12 // non-streaming terminal response to REQUEST
	CANCEL   MsgType = 0x13 // initiator -> target: cancel in-flight corr/stream
	ACK      MsgType = 0x1E // SEND weak ack: enqueued to target connection
	NACK     MsgType = 0x1F // SEND weak ack failure: enqueue failed

	STREAM_OPEN MsgType = 0x20 // open stream, bound to a corr
	STREAM_DATA MsgType = 0x21 // ordered stream data chunk (seq, no silent drop)
	STREAM_END  MsgType = 0x22 // sole terminal state: status(ok/error/aborted) + usage?

	SUBSCRIBE MsgType = 0x30 // subscribe to topic
	SUBACK    MsgType = 0x31 // subscription effective confirmation
	UNSUB     MsgType = 0x32 // unsubscribe
	PUBLISH   MsgType = 0x33 // broadcast to topic (dst must be topic:*)

	TICKET_REQ MsgType = 0x40 // (P7) P2P ticket request
	TICKET     MsgType = 0x41 // (P7) P2P ticket issuance
	P2P_HELLO  MsgType = 0x42 // (P7) P2P direct handshake

	ERROR MsgType = 0xFF // stable error code (see §4.5)
)

var typeNames = map[MsgType]string{
	HELLO:       "HELLO",
	WELCOME:     "WELCOME",
	PING:        "PING",
	PONG:        "PONG",
	SEND:        "SEND",
	REQUEST:     "REQUEST",
	RESPONSE:    "RESPONSE",
	CANCEL:      "CANCEL",
	ACK:         "ACK",
	NACK:        "NACK",
	STREAM_OPEN: "STREAM_OPEN",
	STREAM_DATA: "STREAM_DATA",
	STREAM_END:  "STREAM_END",
	SUBSCRIBE:   "SUBSCRIBE",
	SUBACK:      "SUBACK",
	UNSUB:       "UNSUB",
	PUBLISH:     "PUBLISH",
	TICKET_REQ:  "TICKET_REQ",
	TICKET:      "TICKET",
	P2P_HELLO:   "P2P_HELLO",
	ERROR:       "ERROR",
}

// TypeName returns the human-readable name for a MsgType. Unknown values return
// a readable "UNKNOWN(0xNN)" form rather than the empty string.
func TypeName(t MsgType) string {
	if name, ok := typeNames[t]; ok {
		return name
	}
	return "UNKNOWN(0x" + hexByte(uint8(t)) + ")"
}

func hexByte(b uint8) string {
	const digits = "0123456789ABCDEF"
	return string([]byte{digits[b>>4], digits[b&0x0F]})
}
