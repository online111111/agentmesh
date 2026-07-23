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

// MaxFrameBytes bounds env_len (checked before allocation on decode) to prevent
// memory amplification. It matches the MESH_MAX_FRAME_BYTES default (DESIGN §9,
// 1 MiB) and is a package-level var so P1 server config can override it at
// startup. Note: this bounds the ENVELOPE segment only. The payload tail is a
// zero-copy slice of the caller-provided buffer and is never allocated by the
// decoder; enforcing the total env_len + payload cap is the transport read
// layer's responsibility before the full frame is buffered.
var MaxFrameBytes = 1 << 20 // 1 MiB (MESH_MAX_FRAME_BYTES default, §9)

// maxHdrPrealloc caps the map size hint used when decoding the hdr slot. An
// untrusted frame can declare a huge msgpack Map32 length in very few bytes;
// trusting it as a make() hint would force a giant preallocation before the
// per-entry decode loop fails on EOF. The map still grows if a legitimately
// large (but env_len-bounded) header arrives, so this only defeats the
// amplification attack, never a valid frame.
const maxHdrPrealloc = 256

// ErrFrameTooBigErr is returned by DecodeFrame when a declared length exceeds
// MaxFrameBytes. Its Error() is the stable ErrFrameTooBig code string.
var ErrFrameTooBigErr = errors.New(ErrFrameTooBig)

// Decode errors for malformed frames. These are defensive and must never panic.
var (
	ErrShortFrame  = errors.New("protocol: short frame")
	ErrBadVarint   = errors.New("protocol: invalid env_len varint")
	ErrEnvOverrun  = errors.New("protocol: env_len overruns frame")
	ErrBadEnvelope = errors.New("protocol: malformed envelope")
	ErrBadArrayLen = errors.New("protocol: envelope array must have 11 slots")
)

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
// empty map (fixmap len 0), never as nil, to keep the slot present.
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

// DecodeFrame parses a split frame. It returns the decoded Envelope and the
// payload as a slice pointing INTO the original buf (zero-copy tail): the
// returned payload shares buf's backing array and is never copied. The
// envelope segment is decoded (it may be copied by the msgpack library, which
// is acceptable); only the payload tail must not be copied.
//
// The env_len bound is checked before any allocation to prevent memory
// amplification (DESIGN §4.1). Malformed frames return an error and never panic.
func DecodeFrame(buf []byte) (Envelope, []byte, error) {
	if len(buf) < 1 {
		return Envelope{}, nil, ErrShortFrame
	}
	typ := MsgType(buf[0])

	envLen, n := binary.Uvarint(buf[1:])
	if n <= 0 {
		return Envelope{}, nil, ErrBadVarint
	}
	off := 1 + n

	// Bound check BEFORE trusting/allocating on the declared length.
	if envLen > uint64(MaxFrameBytes) {
		return Envelope{}, nil, ErrFrameTooBigErr
	}
	if uint64(len(buf)-off) < envLen {
		return Envelope{}, nil, ErrEnvOverrun
	}

	envBytes := buf[off : off+int(envLen)]
	env, err := decodeEnvelope(envBytes)
	if err != nil {
		return Envelope{}, nil, err
	}
	// The type byte is authoritative for routing; keep envelope Type in sync.
	env.Type = typ

	// Zero-copy payload tail: slice of the original buffer.
	payload := buf[off+int(envLen):]
	return env, payload, nil
}

// decodeEnvelope parses the fixed-order 11-slot positional msgpack array.
func decodeEnvelope(envBytes []byte) (Envelope, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(envBytes))
	arrLen, err := dec.DecodeArrayLen()
	if err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	if arrLen != envelopeSlots {
		return Envelope{}, ErrBadArrayLen
	}

	var env Envelope
	v, err := dec.DecodeUint()
	if err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	env.V = uint8(v)

	typ, err := dec.DecodeUint()
	if err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	env.Type = MsgType(typ)

	if env.ID, err = dec.DecodeString(); err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	if env.Corr, err = dec.DecodeString(); err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	if env.Stream, err = dec.DecodeString(); err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	if env.Src, err = dec.DecodeString(); err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	if env.Dst, err = dec.DecodeString(); err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	if env.Tenant, err = dec.DecodeString(); err != nil {
		return Envelope{}, ErrBadEnvelope
	}

	ttl, err := dec.DecodeInt64()
	if err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	env.TTL = int32(ttl)

	hops, err := dec.DecodeUint()
	if err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	env.Hops = uint8(hops)

	env.Hdr, err = decodeHdr(dec)
	if err != nil {
		return Envelope{}, ErrBadEnvelope
	}
	return env, nil
}

// decodeHdr parses the hdr map slot. An empty map decodes to a nil map so the
// round-trip of a nil/empty hdr is stable (len == 0 either way).
func decodeHdr(dec *msgpack.Decoder) (map[string]string, error) {
	mapLen, err := dec.DecodeMapLen()
	if err != nil {
		return nil, err
	}
	if mapLen <= 0 {
		return nil, nil
	}
	hdr := make(map[string]string, min(mapLen, maxHdrPrealloc))
	for i := 0; i < mapLen; i++ {
		k, err := dec.DecodeString()
		if err != nil {
			return nil, err
		}
		val, err := dec.DecodeString()
		if err != nil {
			return nil, err
		}
		hdr[k] = val
	}
	return hdr, nil
}
