package cli

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

	"github.com/online111111/agentmesh/internal/agentrt"
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

func TestCallEcho(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = agentrt.Run(ctx, agentrt.Config{
			HubURL:  base,
			Token:   "ka",
			AgentID: "alice-echo",
			Caps:    []string{"echo"},
		})
	}()

	// Wait for registration.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("agent not registered")
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

	out, err := Call(ctx, CallOptions{
		HubURL:  base,
		Token:   "ka",
		To:      "alice-echo",
		Payload: []byte("hi-from-cli"),
		TTLMs:   3000,
	})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out.From != "alice-echo" {
		t.Fatalf("from: %q", out.From)
	}
	if string(out.Payload) != "hi-from-cli" {
		t.Fatalf("payload: %q", out.Payload)
	}
}

func TestCallNoRoute(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := Call(ctx, CallOptions{
		HubURL:  base,
		Token:   "ka",
		To:      "alice-missing",
		Payload: []byte("x"),
		TTLMs:   1000,
	})
	if err == nil {
		t.Fatal("expected NO_ROUTE")
	}
	if !strings.Contains(err.Error(), "NO_ROUTE") {
		t.Fatalf("want NO_ROUTE in error, got %v", err)
	}
}

// Ensure json/base64 stay imported if helper tests evolve.
var (
	_ = json.Marshal
	_ = base64.StdEncoding
)
