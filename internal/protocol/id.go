package protocol

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"
)

// id.go provides compact, sortable unique identifiers for messages, correlation
// ids, streams, and session/audit correlation. The format is a 26-char
// Crockford base32 ULID: 48-bit millisecond timestamp + 80 bits of randomness,
// so ids are time-ordered and collision-resistant without external deps.

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var (
	idMu       sync.Mutex
	lastMs     uint64
	lastRand   [10]byte
	randReader = rand.Read
)

// NewID returns a new ULID string. It is safe for concurrent use and guarantees
// strict monotonicity within the same millisecond by incrementing the random
// component, so ids generated in a burst still sort in creation order.
func NewID() string {
	idMu.Lock()
	defer idMu.Unlock()

	ms := uint64(time.Now().UnixMilli())
	if ms == lastMs {
		incrementRand(&lastRand)
	} else {
		lastMs = ms
		_, _ = randReader(lastRand[:])
	}

	var buf [16]byte
	// 48-bit timestamp big-endian in the first 6 bytes.
	buf[0] = byte(ms >> 40)
	buf[1] = byte(ms >> 32)
	buf[2] = byte(ms >> 24)
	buf[3] = byte(ms >> 16)
	buf[4] = byte(ms >> 8)
	buf[5] = byte(ms)
	copy(buf[6:], lastRand[:])

	return encodeCrockford(buf)
}

// incrementRand adds 1 to the 80-bit random field (big-endian) for intra-ms
// monotonicity.
func incrementRand(r *[10]byte) {
	for i := len(r) - 1; i >= 0; i-- {
		r[i]++
		if r[i] != 0 {
			return
		}
	}
}

// encodeCrockford encodes a 128-bit value as a 26-char Crockford base32 string
// (the canonical ULID text encoding).
func encodeCrockford(b [16]byte) string {
	// Interpret as a 128-bit big-endian number, emit 26 base32 chars.
	hi := binary.BigEndian.Uint64(b[0:8])
	lo := binary.BigEndian.Uint64(b[8:16])
	out := make([]byte, 26)
	// 26 * 5 = 130 bits; the top char carries only the highest 2 bits.
	for i := 25; i >= 0; i-- {
		out[i] = crockford[lo&0x1f]
		lo = (lo >> 5) | (hi << 59)
		hi >>= 5
	}
	return string(out)
}
