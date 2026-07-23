package protocol

// Stable error codes (DESIGN §4.5). These strings are part of the frozen
// contract carried in ERROR (0xFF) frames and control-plane responses.
const (
	ErrAuthFailed         = "AUTH_FAILED"         // token invalid/expired
	ErrNoRoute            = "NO_ROUTE"            // target agent offline/absent
	ErrTimeout            = "TIMEOUT"             // ttl elapsed without response
	ErrRateLimited        = "RATE_LIMITED"        // rate limit tripped
	ErrFrameTooBig        = "FRAME_TOO_BIG"       // exceeds MESH_MAX_FRAME_BYTES
	ErrDuplicateAgentID   = "DUPLICATE_AGENT_ID"  // agentId conflict in tenant
	ErrQueueFull          = "QUEUE_FULL"          // send queue full (backpressure)
	ErrUnmappable         = "UNMAPPABLE"          // protocol bridge cannot map
	ErrTenantDenied       = "TENANT_DENIED"       // cross-tenant access
	ErrUnsupportedVersion = "UNSUPPORTED_VERSION" // HELLO.v not supported
	ErrAgentIDForbidden   = "AGENTID_FORBIDDEN"   // agentId outside principal ns
	ErrSessionTakeover    = "SESSION_TAKEOVER"    // superseded by same-principal conn
	ErrHopLimit           = "HOP_LIMIT"           // exceeded hop limit (loop guard)
	ErrCancelled          = "CANCELLED"           // cancelled by initiator/disconnect
	ErrInsecureRefused    = "INSECURE_REFUSED"    // non-loopback bind w/o TLS or opt-in
)
