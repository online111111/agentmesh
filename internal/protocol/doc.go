// Package protocol defines the AgentMesh wire protocol: the frozen split-frame
// format, message type constants, the routing envelope, error codes, and codecs.
//
// The wire format is authoritative per docs/design/mesh-design.md §4. A frame is
// physically split into: type(1 byte) + env_len(varint) + envelope(msgpack) +
// payload(opaque trailing bytes). The payload is never encoded inside the
// msgpack envelope; it is the raw tail of the frame so the Hub can relay it
// zero-copy.
package protocol
