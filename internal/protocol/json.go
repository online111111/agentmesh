package protocol

import (
	"encoding/base64"
	"encoding/json"
)

// jsonFrame is the debug (?enc=json) representation of a split frame (DESIGN
// §4.1). It carries the same envelope fields plus the opaque payload as a
// base64 string so arbitrary binary payloads survive a JSON text frame. This
// mode is for packet capture / debugging only; the binary split frame is the
// canonical wire format.
type jsonFrame struct {
	V       uint8             `json:"v"`
	Type    uint8             `json:"type"`
	ID      string            `json:"id"`
	Corr    string            `json:"corr"`
	Stream  string            `json:"stream"`
	Src     string            `json:"src"`
	Dst     string            `json:"dst"`
	Tenant  string            `json:"tenant"`
	TTL     int32             `json:"ttl"`
	Hops    uint8             `json:"hops"`
	Hdr     map[string]string `json:"hdr"`
	Payload string            `json:"payload"` // base64(std)
}

// EncodeJSON encodes an envelope + opaque payload as a JSON text frame for the
// debug transport mode. The payload is base64-encoded (std alphabet).
func EncodeJSON(env Envelope, payload []byte) ([]byte, error) {
	jf := jsonFrame{
		V:       env.V,
		Type:    uint8(env.Type),
		ID:      env.ID,
		Corr:    env.Corr,
		Stream:  env.Stream,
		Src:     env.Src,
		Dst:     env.Dst,
		Tenant:  env.Tenant,
		TTL:     env.TTL,
		Hops:    env.Hops,
		Hdr:     env.Hdr,
		Payload: base64.StdEncoding.EncodeToString(payload),
	}
	return json.Marshal(jf)
}

// DecodeJSON parses a JSON text frame produced by EncodeJSON, returning the
// envelope and the decoded (base64 -> raw) payload.
func DecodeJSON(text []byte) (Envelope, []byte, error) {
	var jf jsonFrame
	if err := json.Unmarshal(text, &jf); err != nil {
		return Envelope{}, nil, err
	}
	payload, err := base64.StdEncoding.DecodeString(jf.Payload)
	if err != nil {
		return Envelope{}, nil, err
	}
	env := Envelope{
		V:      jf.V,
		Type:   MsgType(jf.Type),
		ID:     jf.ID,
		Corr:   jf.Corr,
		Stream: jf.Stream,
		Src:    jf.Src,
		Dst:    jf.Dst,
		Tenant: jf.Tenant,
		TTL:    jf.TTL,
		Hops:   jf.Hops,
		Hdr:    jf.Hdr,
	}
	return env, payload, nil
}
