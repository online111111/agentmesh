package hub

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/online111111/agentmesh/internal/auth"
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
