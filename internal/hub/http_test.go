package hub

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/protocol"
)

// newTestHTTP builds a Gateway + full HTTP mux (control plane + WS) with the
// given API keys, and returns the gateway and httptest server.
func newTestHTTP(t *testing.T, keySpec string) (*Gateway, *httptest.Server) {
	t.Helper()
	keys, err := auth.ParseKeys(keySpec)
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	a := auth.NewAuthenticator(keys)
	g := NewGateway(a, NewRegistry(), 1<<20, 1<<20)
	h := NewHTTP(g, a)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return g, srv
}

func TestHTTPHealth(t *testing.T) {
	_, srv := newTestHTTP(t, "a:ka:alice:default")
	res, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "ok") {
		t.Fatalf("body: %s", body)
	}
}

func TestHTTPReady(t *testing.T) {
	_, srv := newTestHTTP(t, "a:ka:alice:default")
	res, err := http.Get(srv.URL + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestHTTPAgentsUnauthorized(t *testing.T) {
	_, srv := newTestHTTP(t, "a:ka:alice:default")
	res, err := http.Get(srv.URL + "/v1/agents")
	if err != nil {
		t.Fatalf("GET /v1/agents: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", res.StatusCode)
	}
}

func TestHTTPAgentsListsTenantOnly(t *testing.T) {
	// alice and bob share different tenants; each must only see own agents.
	g, srv := newTestHTTP(t, "a:ka:alice:t-a\nb:kb:bob:t-b")

	// Register alice-laptop via WS.
	cA, ctxA := dialWS(t, srv)
	sendHello(t, ctxA, cA, "ka", "alice-laptop")
	if env, _ := readFrame(t, ctxA, cA); env.Type != protocol.WELCOME {
		t.Fatalf("alice want WELCOME, got %s", protocol.TypeName(env.Type))
	}
	// Register bob-node via WS.
	cB, ctxB := dialWS(t, srv)
	sendHello(t, ctxB, cB, "kb", "bob-node")
	if env, _ := readFrame(t, ctxB, cB); env.Type != protocol.WELCOME {
		t.Fatalf("bob want WELCOME, got %s", protocol.TypeName(env.Type))
	}

	// alice lists → only alice-laptop.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/agents", nil)
	req.Header.Set("Authorization", "Bearer ka")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("alice list: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("alice list status %d", res.StatusCode)
	}
	var aliceBody struct {
		Agents []struct {
			AgentID string `json:"agentId"`
			Tenant  string `json:"tenant"`
		} `json:"agents"`
	}
	if err := json.NewDecoder(res.Body).Decode(&aliceBody); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(aliceBody.Agents) != 1 || aliceBody.Agents[0].AgentID != "alice-laptop" {
		t.Fatalf("alice saw wrong agents: %+v", aliceBody.Agents)
	}
	if aliceBody.Agents[0].Tenant != "t-a" {
		t.Fatalf("tenant: %s", aliceBody.Agents[0].Tenant)
	}

	// bob lists → only bob-node (cross-tenant invisible).
	reqB, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/agents", nil)
	reqB.Header.Set("Authorization", "Bearer kb")
	resB, err := http.DefaultClient.Do(reqB)
	if err != nil {
		t.Fatalf("bob list: %v", err)
	}
	defer resB.Body.Close()
	var bobBody struct {
		Agents []struct {
			AgentID string `json:"agentId"`
		} `json:"agents"`
	}
	if err := json.NewDecoder(resB.Body).Decode(&bobBody); err != nil {
		t.Fatalf("decode bob: %v", err)
	}
	if len(bobBody.Agents) != 1 || bobBody.Agents[0].AgentID != "bob-node" {
		t.Fatalf("bob saw wrong agents: %+v", bobBody.Agents)
	}

	// Sanity: registry has both under their tenants.
	if _, ok := g.Registry().Lookup("t-a", "alice-laptop"); !ok {
		t.Fatal("alice not registered")
	}
	if _, ok := g.Registry().Lookup("t-b", "bob-node"); !ok {
		t.Fatal("bob not registered")
	}
	_ = time.Second // keep import if unused in future
}

func TestHTTPAgentsBadToken(t *testing.T) {
	_, srv := newTestHTTP(t, "a:ka:alice:default")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/agents", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", res.StatusCode)
	}
}
