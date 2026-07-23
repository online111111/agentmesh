# AgentMesh wire protocol (v1, frozen)

This document is the authoritative wire specification. Implementation conflicts
resolve in favor of `docs/design/mesh-design.md` §4 and the golden vectors in
`internal/protocol/testvectors.json`.

## Frame layout

Binary WebSocket message:

```
type (1 byte) | env_len (unsigned varint) | envelope (msgpack) | payload (opaque)
```

- `type` is the message subtype (see below). It is authoritative; the envelope
  also carries type at slot [1] and must match.
- `env_len` is checked against `MESH_MAX_FRAME_BYTES` (default 1 MiB) **before**
  allocating buffers (`FRAME_TOO_BIG`).
- Hub decodes only the envelope for routing; the payload tail is never
  re-encoded (zero-copy relay).

## Envelope (11-slot positional msgpack array)

**Not a map. No omitempty. Fixed order:**

| Index | Field   | Type   | Notes |
|------:|---------|--------|-------|
| 0 | v | u8 | protocol version (=1) |
| 1 | type | u8 | message type |
| 2 | id | str | message ULID |
| 3 | corr | str | request correlation id |
| 4 | stream | str | stream id (STREAM_* only) |
| 5 | src | str | source agentId (**Hub overwrites**) |
| 6 | dst | str | target agentId or `topic:<name>` |
| 7 | tenant | str | tenant (**Hub overwrites**) |
| 8 | ttl | i32 | relative timeout ms |
| 9 | hops | u8 | remaining hop budget |
| 10 | hdr | map[str]str | optional; **keys sorted ascending** on encode |

## Message types

| Value | Name | Role |
|------:|------|------|
| 0x01 | HELLO | first frame: auth + register |
| 0x02 | WELCOME | registration success |
| 0x03 / 0x04 | PING / PONG | heartbeat |
| 0x10 | SEND | fire-and-forget |
| 0x11 | REQUEST | corr + ttl request |
| 0x12 | RESPONSE | non-stream reply |
| 0x13 | CANCEL | cancel in-flight corr |
| 0x1E / 0x1F | ACK / NACK | optional delivery signals |
| 0x20 | STREAM_OPEN | open stream bound to corr |
| 0x21 | STREAM_DATA | ordered chunk (`hdr.seq`) |
| 0x22 | STREAM_END | **sole** stream terminal (`hdr.status`) |
| 0x30–0x33 | SUBSCRIBE / SUBACK / UNSUB / PUBLISH | pub/sub |
| 0x40–0x42 | TICKET_* / P2P_HELLO | P2P (deferred) |
| 0xFF | ERROR | stable error code payload |

## Control payloads

HELLO / WELCOME / ERROR use ordinary msgpack **maps** (not the 11-slot array):

- HELLO: `token`, `agentId`, `caps`, `protocols`, `v`
- WELCOME: `session`, `heartbeatMs`, `features`
- ERROR: `code`, `message?`, `supported?`

## Semantics (summary)

- **Trust root:** Hub overwrites `src`/`tenant` from connection identity every frame.
- **agentId** bound to principal namespace; `topic:` prefix forbidden on agentIds.
- **REQUEST** responses: either one RESPONSE or STREAM_OPEN→DATA*→END (not both).
- **STREAM_END** is the only stream terminal; disconnect synthesizes `status=aborted`.
- Mid-stream backpressure aborts the whole stream (no silent DATA drops).
- **CANCEL** on initiator disconnect/timeout; multi-hop decrements `hops`.
- Pub/sub: connection-level soft state, SUBACK before delivery, tenant isolated,
  snapshot fan-out, no self-delivery by default.

## Golden vectors

`internal/protocol/testvectors.json` is the cross-language parity oracle.
Go, Python, and TypeScript SDKs must re-encode envelopes to identical `envHex`.
