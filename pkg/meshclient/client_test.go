package meshclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/hub"
)

// startTestHub starts a full Hub (HTTP+WS) with the given key spec and returns
// its base HTTP URL (http://127.0.0.1:port).
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

func TestDialWelcome(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := Dial(ctx, Options{
		HubURL:  base,
		Token:   "ka",
		AgentID: "alice-laptop",
		Caps:    []string{"echo"},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if c.AgentID() != "alice-laptop" {
		t.Fatalf("AgentID: %q", c.AgentID())
	}
	if c.Session() == "" {
		t.Fatal("Session empty after WELCOME")
	}
}

func TestDialAuthFailed(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := Dial(ctx, Options{
		HubURL:  base,
		Token:   "wrong",
		AgentID: "alice-laptop",
	})
	if err == nil {
		t.Fatal("expected auth failure")
	}
}

func TestDialForbiddenAgentID(t *testing.T) {
	base := startTestHub(t, "a:ka:alice:default")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := Dial(ctx, Options{
		HubURL:  base,
		Token:   "ka",
		AgentID: "bob-x", // outside alice- namespace
	})
	if err == nil {
		t.Fatal("expected AGENTID_FORBIDDEN")
	}
}

// compile-time check that httptest is referenced if needed later
var _ = http.StatusOK
