package protocol

import (
	"sync"
	"testing"
)

func TestNewIDFormat(t *testing.T) {
	id := NewID()
	if len(id) != 26 {
		t.Fatalf("ULID must be 26 chars, got %d: %q", len(id), id)
	}
	for _, c := range id {
		if !containsRune(crockford, c) {
			t.Fatalf("ULID char %q not in Crockford alphabet", c)
		}
	}
}

func TestNewIDUnique(t *testing.T) {
	const n = 10000
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		id := NewID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id at %d: %s", i, id)
		}
		seen[id] = struct{}{}
	}
}

func TestNewIDMonotonicInSameMs(t *testing.T) {
	// Two ids from the same millisecond must be strictly increasing (lexical),
	// because ULID text encoding is order-preserving.
	prev := NewID()
	for i := 0; i < 1000; i++ {
		cur := NewID()
		if cur <= prev {
			t.Fatalf("ids not monotonic: %s then %s", prev, cur)
		}
		prev = cur
	}
}

func TestNewIDConcurrent(t *testing.T) {
	const g, per = 20, 500
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[string]struct{}, g*per)
	wg.Add(g)
	for i := 0; i < g; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				id := NewID()
				mu.Lock()
				seen[id] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != g*per {
		t.Fatalf("expected %d unique ids, got %d", g*per, len(seen))
	}
}

func containsRune(s string, r rune) bool {
	for _, c := range s {
		if c == r {
			return true
		}
	}
	return false
}
