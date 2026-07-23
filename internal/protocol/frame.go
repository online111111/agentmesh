package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/vmihailenco/msgpack/v5"
)

// envelopeSlots is the frozen number of positional slots in the envelope array
// (DESIGN §4.2): [v, type, id, corr, stream, src, dst, tenant, ttl, hops, hdr].
const envelopeSlots = 11

// MaxFrameBytes bounds env_len + payload to prevent memory amplification. It is
// checked before allocation on decode. Default per config contract; kept as a
// package-level var so P1 server config can override it.
var MaxFrameBytes = 16 << 20 // 16 MiB

// ErrFrameTooBigErr is returned by DecodeFrame when a declared length exceeds
// MaxFrameBytes. Its Error() is the stable ErrFrameTooBig code string.
var ErrFrameTooBigErr = errors.New(ErrFrameTooBig)

// EncodeFrame produces a split frame:
//
//	type(1 byte) + env_len(uvarint) + envelope(msgpack fixed-11 array) + payload
//
// The envelope is encoded as a fixed-order positional msgpack ARRAY (never a
// string/int-keyed map, never omitempty) so that Go/Python/TypeScript SDKs
// produce byte-identical output. The hdr sub-map (slot 10) is encoded with keys
// in ascending order for determinism. The payload is appended raw as opaque
// trailing bytes and is never placed inside the msgpack body.
func EncodeFrame(env Envelope, payload []byte) ([]byte, error) {
	envBytes, err := encodeEnvelope(env)
	if err != nil {
		return nil, err
	}

	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(envBytes)))

	total := 1 + n + len(envBytes) + len(payload)
	frame := make([]byte, 0, total)
	frame = append(frame, byte(env.Type))
	frame = append(frame, lenBuf[:n]...)
	frame = append(frame, envBytes...)
	frame = append(frame, payload...)
	return frame, nil
}

// encodeEnvelope serializes the envelope as a fixed-order 11-slot msgpack array
// with deterministic integer encoding and sorted hdr keys.
func encodeEnvelope(env Envelope) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	if err := enc.EncodeArrayLen(envelopeSlots); err != nil {
		return nil, err
	}
	if err := enc.EncodeUint(uint64(env.V)); err != nil { // [0] v
		return nil, err
	}
	if err := enc.EncodeUint(uint64(env.Type)); err != nil { // [1] type
		return nil, err
	}
	if err := enc.EncodeString(env.ID); err != nil { // [2] id
		return nil, err
	}
	if err := enc.EncodeString(env.Corr); err != nil { // [3] corr
		return nil, err
	}
	if err := enc.EncodeString(env.Stream); err != nil { // [4] stream
		return nil, err
	}
	if err := enc.EncodeString(env.Src); err != nil { // [5] src
		return nil, err
	}
	if err := enc.EncodeString(env.Dst); err != nil { // [6] dst
		return nil, err
	}
	if err := enc.EncodeString(env.Tenant); err != nil { // [7] tenant
		return nil, err
	}
	if err := enc.EncodeInt(int64(env.TTL)); err != nil { // [8] ttl
		return nil, err
	}
	if err := enc.EncodeUint(uint64(env.Hops)); err != nil { // [9] hops
		return nil, err
	}
	if err := encodeHdr(enc, env.Hdr); err != nil { // [10] hdr
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeHdr encodes the optional header map with keys sorted ascending so the
// byte output is deterministic across languages. A nil/empty map encodes as an
// empty map (map16/fixmap len 0), never as nil, to keep the slot present.
func encodeHdr(enc *msgpack.Encoder, hdr map[string]string) error {
	if err := enc.EncodeMapLen(len(hdr)); err != nil {
		return err
	}
	if len(hdr) == 0 {
		return nil
	}
	keys := make([]string, 0, len(hdr))
	for k := range hdr {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := enc.EncodeString(k); err != nil {
			return err
		}
		if err := enc.EncodeString(hdr[k]); err != nil {
			return err
		}
	}
	return nil
}
