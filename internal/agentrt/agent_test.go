package agentrt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/hub"
)

func startTestHub(t *testing.T, keySpec string) string {
	t.Helper()
	keys, err := auth.ParseKeys(keySpec)
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	a := auth.NewAuthenticator(keys)
	g := hub.NewGateway(a, hub.NewRegistry(), 1<<20, 1<<20)
	h := hub.NewHTTP(g, a)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestAgentEchoViaRPC(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, Config{
			HubURL:  base,
			Token:   "ka",
			AgentID: "alice-echo",
			Caps:    []string{"echo"},
		})
	}()

	// Wait until agent is registered (poll /v1/agents).
	deadline := time.Now().Add(3 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("agent did not register in time")
		}
		req, _ := http.NewRequest(http.MethodGet, base+"/v1/agents", nil)
		req.Header.Set("Authorization", "Bearer ka")
		res, err := http.DefaultClient.Do(req)
		if err == nil {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode == 200 && strings.Contains(string(body), "alice-echo") {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// POST /v1/rpc should get the echo payload back.
	payloadB64 := base64.StdEncoding.EncodeToString([]byte("hello-agent"))
	body := `{"to":"alice-echo","payload":"` + payloadB64 + `","ttlMs":3000}`
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/rpc", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer ka")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("rpc: %v", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("rpc status %d body %s", res.StatusCode, raw)
	}
	var out struct {
		From    string `json:"from"`
		Payload string `json:"payload"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("json: %v body %s", err, raw)
	}
	if out.From != "alice-echo" {
		t.Fatalf("from: %q", out.From)
	}
	decoded, err := base64.StdEncoding.DecodeString(out.Payload)
	if err != nil {
		t.Fatalf("payload b64: %v", err)
	}
	if string(decoded) != "hello-agent" {
		t.Fatalf("echo payload: %q", decoded)
	}

	cancel()
	select {
	case err := <-errCh:
		// context.Canceled is expected; nil also fine if Run returns cleanly.
		if err != nil && err != context.Canceled && !strings.Contains(err.Error(), "context canceled") {
			// connection close after cancel is acceptable
			t.Logf("Run exit: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent Run did not exit after cancel")
	}
}

func TestAgentAuthFailed(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := Run(ctx, Config{
		HubURL:  base,
		Token:   "wrong",
		AgentID: "alice-echo",
	})
	if err == nil {
		t.Fatal("expected auth failure")
	}
}
