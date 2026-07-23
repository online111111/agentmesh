package protocol

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"testing"
)

// updateGolden regenerates testvectors.json from GenerateVectors when set:
//
//	go test ./internal/protocol/ -run TestGoldenFileUpToDate -update
var updateGolden = flag.Bool("update", false, "regenerate testvectors.json")

// loadGoldenFile reads the committed testvectors.json.
func loadGoldenFile(t *testing.T) GoldenFile {
	t.Helper()
	raw, err := os.ReadFile("testvectors.json")
	if err != nil {
		t.Fatalf("read testvectors.json: %v", err)
	}
	var gf GoldenFile
	if err := json.Unmarshal(raw, &gf); err != nil {
		t.Fatalf("unmarshal testvectors.json: %v", err)
	}
	return gf
}

func envFromGolden(g goldenEnv) Envelope {
	return Envelope{
		V: g.V, Type: MsgType(g.Type), ID: g.ID, Corr: g.Corr, Stream: g.Stream,
		Src: g.Src, Dst: g.Dst, Tenant: g.Tenant, TTL: g.TTL, Hops: g.Hops, Hdr: g.Hdr,
	}
}

// TestGoldenVectorsEncodeExact asserts every committed vector re-encodes to the
// exact frozen frame hex. This is the cross-language wire anchor (DESIGN §7).
func TestGoldenVectorsEncodeExact(t *testing.T) {
	gf := loadGoldenFile(t)
	if gf.Slots != envelopeSlots {
		t.Fatalf("golden slots = %d, want %d", gf.Slots, envelopeSlots)
	}
	if len(gf.Vectors) == 0 {
		t.Fatal("no vectors in testvectors.json")
	}
	for _, vec := range gf.Vectors {
		t.Run(vec.Name, func(t *testing.T) {
			env := envFromGolden(vec.Env)
			payload, err := hex.DecodeString(vec.PayloadHex)
			if err != nil {
				t.Fatalf("bad payloadHex: %v", err)
			}
			frame, err := EncodeFrame(env, payload)
			if err != nil {
				t.Fatalf("EncodeFrame: %v", err)
			}
			gotHex := hex.EncodeToString(frame)
			if gotHex != vec.FrameHex {
				t.Fatalf("frame hex mismatch\n got=%s\nwant=%s", gotHex, vec.FrameHex)
			}

			// Round-trip: decode the frozen frame and re-encode; must match.
			decoded, gotPayload, err := DecodeFrame(frame)
			if err != nil {
				t.Fatalf("DecodeFrame: %v", err)
			}
			if !bytes.Equal(gotPayload, payload) {
				t.Fatalf("payload round-trip mismatch")
			}
			reframe, err := EncodeFrame(decoded, gotPayload)
			if err != nil {
				t.Fatalf("re-EncodeFrame: %v", err)
			}
			if !bytes.Equal(reframe, frame) {
				t.Fatal("decode->encode not byte-identical")
			}
		})
	}
}

// TestGoldenVectorsCoverAllTypes asserts all 21 §4.3 type values appear in the
// vector set and the three stream shapes with all STREAM_END statuses exist.
func TestGoldenVectorsCoverAllTypes(t *testing.T) {
	gf := loadGoldenFile(t)
	seen := map[uint8]bool{}
	for _, vec := range gf.Vectors {
		seen[vec.Type] = true
	}
	allTypes := []MsgType{
		HELLO, WELCOME, PING, PONG, SEND, REQUEST, RESPONSE, CANCEL, ACK, NACK,
		STREAM_OPEN, STREAM_DATA, STREAM_END, SUBSCRIBE, SUBACK, UNSUB, PUBLISH,
		TICKET_REQ, TICKET, P2P_HELLO, ERROR,
	}
	if len(allTypes) != 21 {
		t.Fatalf("expected 21 types, listed %d", len(allTypes))
	}
	for _, tp := range allTypes {
		if !seen[uint8(tp)] {
			t.Errorf("no vector covers type %s (0x%02X)", TypeName(tp), uint8(tp))
		}
	}

	// STREAM_END status coverage.
	statuses := map[string]bool{}
	var sawStreamData, sawStreamOpen bool
	for _, vec := range gf.Vectors {
		switch MsgType(vec.Type) {
		case STREAM_OPEN:
			sawStreamOpen = true
		case STREAM_DATA:
			sawStreamData = true
			if vec.Env.Hdr["seq"] == "" {
				t.Error("STREAM_DATA vector must carry seq in hdr")
			}
		case STREAM_END:
			statuses[vec.Env.Hdr["status"]] = true
		}
	}
	if !sawStreamOpen || !sawStreamData {
		t.Error("missing STREAM_OPEN or STREAM_DATA vector")
	}
	for _, s := range []string{"ok", "error", "aborted"} {
		if !statuses[s] {
			t.Errorf("missing STREAM_END status=%s vector", s)
		}
	}
}

// TestGoldenVectorsHdrSorted asserts every hdr with >=2 keys is encoded with
// ascending keys (B3), by checking the envelope bytes decode with sorted keys.
func TestGoldenVectorsHdrSorted(t *testing.T) {
	gf := loadGoldenFile(t)
	sawMulti := false
	for _, vec := range gf.Vectors {
		if len(vec.Env.Hdr) < 2 {
			continue
		}
		sawMulti = true
		env := envFromGolden(vec.Env)
		envBytes, err := encodeEnvelope(env)
		if err != nil {
			t.Fatalf("encodeEnvelope: %v", err)
		}
		wantEnv, err := hex.DecodeString(vec.EnvHex)
		if err != nil {
			t.Fatalf("bad envHex: %v", err)
		}
		if !bytes.Equal(envBytes, wantEnv) {
			t.Fatalf("%s: env bytes not stable (hdr sort?)", vec.Name)
		}
	}
	if !sawMulti {
		t.Fatal("no vector with >=2 hdr keys to exercise B3 sorting")
	}
}

// TestGoldenFileUpToDate fails if testvectors.json drifts from GenerateVectors.
// Regenerate with: go test -run TestGoldenFileUpToDate -update (see below).
func TestGoldenFileUpToDate(t *testing.T) {
	want, err := MarshalGolden()
	if err != nil {
		t.Fatalf("MarshalGolden: %v", err)
	}
	if *updateGolden {
		if err := os.WriteFile("testvectors.json", want, 0o644); err != nil {
			t.Fatalf("write testvectors.json: %v", err)
		}
		t.Log("regenerated testvectors.json")
		return
	}
	got, err := os.ReadFile("testvectors.json")
	if err != nil {
		t.Fatalf("read testvectors.json: %v", err)
	}
	if !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(want)) {
		t.Fatal("testvectors.json is stale; regenerate via GenerateVectors/MarshalGolden")
	}
}
