package hub

import (
	"fmt"
	"sync"
	"testing"

	"github.com/online111111/agentmesh/internal/protocol"
)

// fakeSender is a minimal Sender used to test the registry without a real Conn
// (B1: registry depends only on the Sender interface).
type fakeSender struct {
	mu     sync.Mutex
	frames [][]byte
	closed bool
	id     string
}

func (f *fakeSender) Enqueue(frame []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return fmt.Errorf("closed")
	}
	f.frames = append(f.frames, frame)
	return nil
}

func (f *fakeSender) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func TestRegistryRegisterLookup(t *testing.T) {
	r := NewRegistry()
	s := &fakeSender{id: "a"}
	if err := r.Register("default", "alice-laptop", s); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, ok := r.Lookup("default", "alice-laptop")
	if !ok {
		t.Fatal("Lookup: not found after register")
	}
	if got != s {
		t.Fatalf("Lookup returned different sender: %v", got)
	}
}

func TestRegistryDuplicate(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("default", "alice-laptop", &fakeSender{}); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	err := r.Register("default", "alice-laptop", &fakeSender{})
	if err == nil {
		t.Fatal("second Register: expected DUPLICATE_AGENT_ID error")
	}
	if err.Error() != protocol.ErrDuplicateAgentID {
		t.Fatalf("want DUPLICATE_AGENT_ID, got %v", err)
	}
}

func TestRegistryTenantIsolation(t *testing.T) {
	r := NewRegistry()
	sa := &fakeSender{id: "a"}
	sb := &fakeSender{id: "b"}
	// Same agentId in different tenants must not collide.
	if err := r.Register("t1", "node", sa); err != nil {
		t.Fatalf("Register t1: %v", err)
	}
	if err := r.Register("t2", "node", sb); err != nil {
		t.Fatalf("Register t2 (same agentId, different tenant): %v", err)
	}
	g1, _ := r.Lookup("t1", "node")
	g2, _ := r.Lookup("t2", "node")
	if g1 != sa || g2 != sb {
		t.Fatal("tenant isolation broken: senders crossed")
	}
}

func TestRegistryRemove(t *testing.T) {
	r := NewRegistry()
	s := &fakeSender{}
	_ = r.Register("default", "n", s)
	r.Remove("default", "n", s)
	if _, ok := r.Lookup("default", "n"); ok {
		t.Fatal("Lookup: still present after Remove")
	}
	// Remove is idempotent.
	r.Remove("default", "n", s)
}

func TestRegistryRemoveOnlyOwnEntry(t *testing.T) {
	// Remove must not delete a newer entry registered by a different sender
	// (e.g. after takeover replaced the old sender for the same key).
	r := NewRegistry()
	old := &fakeSender{id: "old"}
	newer := &fakeSender{id: "new"}
	_ = r.Register("default", "n", old)
	r.Remove("default", "n", old)
	_ = r.Register("default", "n", newer)
	// Stale remove by the old sender must be a no-op.
	r.Remove("default", "n", old)
	got, ok := r.Lookup("default", "n")
	if !ok || got != newer {
		t.Fatalf("stale Remove clobbered newer entry: ok=%v got=%v", ok, got)
	}
}

func TestRegistryListByTenant(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("t1", "a", &fakeSender{})
	_ = r.Register("t1", "b", &fakeSender{})
	_ = r.Register("t2", "c", &fakeSender{})
	ids := r.ListByTenant("t1")
	if len(ids) != 2 {
		t.Fatalf("want 2 agents in t1, got %d: %v", len(ids), ids)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("ListByTenant missing entries: %v", ids)
	}
	if got := r.ListByTenant("t2"); len(got) != 1 || got[0] != "c" {
		t.Fatalf("t2 list wrong: %v", got)
	}
	if got := r.ListByTenant("none"); len(got) != 0 {
		t.Fatalf("empty tenant should be empty, got %v", got)
	}
}

func TestRegistryConcurrentRegister(t *testing.T) {
	r := NewRegistry()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			agentID := fmt.Sprintf("node-%d", i)
			if err := r.Register("default", agentID, &fakeSender{id: agentID}); err != nil {
				t.Errorf("Register %s: %v", agentID, err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		agentID := fmt.Sprintf("node-%d", i)
		if _, ok := r.Lookup("default", agentID); !ok {
			t.Errorf("missing %s after concurrent register", agentID)
		}
	}
}

func TestRegistryConcurrentSameKey(t *testing.T) {
	// Exactly one of many concurrent registrations of the same key wins.
	r := NewRegistry()
	const n = 50
	var wg sync.WaitGroup
	var okCount int
	var mu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := r.Register("default", "contended", &fakeSender{}); err == nil {
				mu.Lock()
				okCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if okCount != 1 {
		t.Fatalf("want exactly 1 successful register, got %d", okCount)
	}
}
