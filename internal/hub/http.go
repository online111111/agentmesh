package hub

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/protocol"
)

// HTTP is the control-plane mux (DESIGN §4.11): /health, /ready, /v1/agents,
// and (later) /v1/rpc. It shares the Gateway's authenticator and registry so
// listed agents match the live WebSocket connections.
type HTTP struct {
	gateway *Gateway
	auth    *auth.Authenticator
	mux     *http.ServeMux
}

// NewHTTP builds the control-plane handler. It also mounts the WebSocket
// gateway at /v1/ws so a single httptest.Server (or production http.Server) can
// serve both planes.
func NewHTTP(g *Gateway, a *auth.Authenticator) http.Handler {
	h := &HTTP{gateway: g, auth: a, mux: http.NewServeMux()}
	h.mux.HandleFunc("/health", h.handleHealth)
	h.mux.HandleFunc("/ready", h.handleReady)
	h.mux.HandleFunc("/v1/agents", h.handleAgents)
	h.mux.HandleFunc("/v1/rpc", h.handleRPC)
	h.mux.HandleFunc("/v1/ws", g.ServeWS)
	// Also accept the root path for WS (clients may dial hub URL directly).
	h.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		// Upgrade only when the client requests a WebSocket.
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			g.ServeWS(w, r)
			return
		}
		http.NotFound(w, r)
	})
	return h.mux
}

func (h *HTTP) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *HTTP) handleReady(w http.ResponseWriter, _ *http.Request) {
	// v1 readiness: authenticator is configured. Future deps (TLS, etc.) land here.
	if h.auth == nil {
		http.Error(w, `{"status":"not ready"}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

// agentEntry is one row in the /v1/agents response.
type agentEntry struct {
	AgentID string   `json:"agentId"`
	Tenant  string   `json:"tenant"`
	Caps    []string `json:"caps,omitempty"`
}

// agentsResponse is the JSON body of GET /v1/agents.
type agentsResponse struct {
	Agents []agentEntry `json:"agents"`
	Next   string       `json:"next,omitempty"`
}

func (h *HTTP) handleAgents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	identity, ok := h.bearerAuth(w, r)
	if !ok {
		return
	}

	ids := h.gateway.Registry().ListByTenant(identity.Tenant)

	// Optional ?caps= filter: keep agents whose registered caps include all of
	// the comma-separated values. Caps are not yet stored on Conn (Task 1.5
	// HELLO carries them but they are not retained); for now the filter is a
	// no-op pass-through so the query param is accepted without 400.
	_ = r.URL.Query().Get("caps")

	// Simple offset/limit pagination. Default limit 100.
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	if offset > len(ids) {
		offset = len(ids)
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	page := ids[offset:end]

	out := agentsResponse{Agents: make([]agentEntry, 0, len(page))}
	for _, id := range page {
		out.Agents = append(out.Agents, agentEntry{
			AgentID: id,
			Tenant:  identity.Tenant,
		})
	}
	if end < len(ids) {
		out.Next = strconv.Itoa(end)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// rpcRequest is the JSON body of POST /v1/rpc (DESIGN §4.11).
type rpcRequest struct {
	To      string `json:"to"`
	Payload string `json:"payload"` // base64-encoded opaque bytes
	TTLMs   int32  `json:"ttlMs"`
}

// rpcResponse is the JSON body of a successful /v1/rpc call.
type rpcResponse struct {
	From    string `json:"from"`
	Payload string `json:"payload"` // base64
	Corr    string `json:"corr,omitempty"`
}

// rpcError is the JSON body of a failed /v1/rpc call.
type rpcError struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// defaultRPCTimeoutMs is the MESH_RPC_TIMEOUT_MS default (DESIGN §9).
const defaultRPCTimeoutMs = 30000

// handleRPC implements POST /v1/rpc: authenticate, look up the target agent,
// inject a one-shot REQUEST, wait for RESPONSE (or TIMEOUT / NO_ROUTE).
// ttl is min(request.ttlMs, MESH_RPC_TIMEOUT_MS); 0 request ttl falls back to
// the configured default (DESIGN §4.6).
func (h *HTTP) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	identity, ok := h.bearerAuth(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeRPCError(w, http.StatusBadRequest, "BAD_REQUEST", "read body failed")
		return
	}
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeRPCError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
		return
	}
	if req.To == "" {
		writeRPCError(w, http.StatusBadRequest, "BAD_REQUEST", "to is required")
		return
	}
	payload, err := base64.StdEncoding.DecodeString(req.Payload)
	if err != nil {
		// Also accept raw UTF-8 when not valid base64, for CLI convenience.
		payload = []byte(req.Payload)
	}

	maxTTL := int32(defaultRPCTimeoutMs)
	if v := os.Getenv("MESH_RPC_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxTTL = int32(n)
		}
	}
	ttl := req.TTLMs
	if ttl <= 0 || ttl > maxTTL {
		ttl = maxTTL
	}

	// Offline short-circuit before installing a waiter.
	if _, found := h.gateway.Registry().Lookup(identity.Tenant, req.To); !found {
		writeRPCError(w, http.StatusNotFound, protocol.ErrNoRoute, "target offline or absent")
		return
	}

	corr := protocol.NewID()
	waiter := h.gateway.registerPending(corr)
	defer h.gateway.cancelPending(corr)

	env := protocol.Envelope{
		V:      protocol.ProtocolVersion,
		Type:   protocol.REQUEST,
		ID:     protocol.NewID(),
		Corr:   corr,
		Src:    "http-rpc", // synthetic source; identity overwrite applies only on WS path
		Dst:    req.To,
		Tenant: identity.Tenant,
		TTL:    ttl,
	}
	// Inject REQUEST directly into the target's send queue (Hub-originated).
	frame, err := protocol.EncodeFrame(env, payload)
	if err != nil {
		writeRPCError(w, http.StatusInternalServerError, "INTERNAL", "encode failed")
		return
	}
	dst, found := h.gateway.Registry().Lookup(identity.Tenant, req.To)
	if !found {
		writeRPCError(w, http.StatusNotFound, protocol.ErrNoRoute, "target offline or absent")
		return
	}
	if err := dst.Enqueue(frame); err != nil {
		code := protocol.ErrQueueFull
		if err != ErrQueueFull {
			code = protocol.ErrNoRoute
		}
		writeRPCError(w, http.StatusBadGateway, code, err.Error())
		return
	}

	timer := time.NewTimer(time.Duration(ttl) * time.Millisecond)
	defer timer.Stop()
	select {
	case res := <-waiter.ch:
		out := rpcResponse{
			From:    res.env.Src,
			Payload: base64.StdEncoding.EncodeToString(res.payload),
			Corr:    corr,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	case <-timer.C:
		writeRPCError(w, http.StatusGatewayTimeout, protocol.ErrTimeout, "rpc timed out")
	case <-r.Context().Done():
		writeRPCError(w, http.StatusGatewayTimeout, protocol.ErrCancelled, "client cancelled")
	}
}

func writeRPCError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(rpcError{Error: code, Message: msg})
}

// bearerAuth extracts "Authorization: Bearer <token>", authenticates it, and
// returns the identity. On failure it writes 401 and returns ok=false.
func (h *HTTP) bearerAuth(w http.ResponseWriter, r *http.Request) (*auth.Identity, bool) {
	hdr := r.Header.Get("Authorization")
	if hdr == "" {
		http.Error(w, `{"error":"AUTH_FAILED","message":"missing Authorization"}`, http.StatusUnauthorized)
		return nil, false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(hdr, prefix) {
		http.Error(w, `{"error":"AUTH_FAILED","message":"expected Bearer token"}`, http.StatusUnauthorized)
		return nil, false
	}
	token := strings.TrimSpace(hdr[len(prefix):])
	identity, err := h.auth.Authenticate(token)
	if err != nil {
		http.Error(w, `{"error":"AUTH_FAILED","message":"authentication failed"}`, http.StatusUnauthorized)
		return nil, false
	}
	return identity, true
}
