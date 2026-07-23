package protocol

import "testing"

func TestEnvelopeZeroValue(t *testing.T) {
	var e Envelope
	if e.V != 0 || e.Type != 0 || e.ID != "" || e.Corr != "" || e.Stream != "" ||
		e.Src != "" || e.Dst != "" || e.Tenant != "" || e.TTL != 0 || e.Hops != 0 {
		t.Errorf("zero Envelope has non-zero fields: %+v", e)
	}
	if e.Hdr != nil {
		t.Errorf("zero Envelope.Hdr should be nil, got %v", e.Hdr)
	}
}

func TestEnvelopeConstruct(t *testing.T) {
	e := Envelope{
		V:      ProtocolVersion,
		Type:   REQUEST,
		ID:     "01ID",
		Corr:   "01CORR",
		Stream: "",
		Src:    "alice-laptop",
		Dst:    "bob-server",
		Tenant: "acme",
		TTL:    5000,
		Hops:   8,
		Hdr:    map[string]string{"ct": "application/x-mesh-task"},
	}
	if e.Type != REQUEST {
		t.Errorf("Type = %v", e.Type)
	}
	if e.Hdr["ct"] != "application/x-mesh-task" {
		t.Errorf("Hdr[ct] = %q", e.Hdr["ct"])
	}
	if ProtocolVersion != 1 {
		t.Errorf("ProtocolVersion = %d, want 1", ProtocolVersion)
	}
}

func TestErrorCodeConstants(t *testing.T) {
	cases := map[string]string{
		ErrAuthFailed:         "AUTH_FAILED",
		ErrNoRoute:            "NO_ROUTE",
		ErrTimeout:            "TIMEOUT",
		ErrRateLimited:        "RATE_LIMITED",
		ErrFrameTooBig:        "FRAME_TOO_BIG",
		ErrDuplicateAgentID:   "DUPLICATE_AGENT_ID",
		ErrQueueFull:          "QUEUE_FULL",
		ErrUnmappable:         "UNMAPPABLE",
		ErrTenantDenied:       "TENANT_DENIED",
		ErrUnsupportedVersion: "UNSUPPORTED_VERSION",
		ErrAgentIDForbidden:   "AGENTID_FORBIDDEN",
		ErrSessionTakeover:    "SESSION_TAKEOVER",
		ErrHopLimit:           "HOP_LIMIT",
		ErrCancelled:          "CANCELLED",
		ErrInsecureRefused:    "INSECURE_REFUSED",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("error code = %q, want %q", got, want)
		}
	}
}
