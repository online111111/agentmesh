// Package hub implements the AgentMesh relay: the sharded agent registry,
// per-connection send queues, the WebSocket gateway, relay routing, and the
// HTTP control plane. The Hub decodes only the small routing envelope and never
// copies or re-encodes the opaque payload tail (DESIGN §5 zero-copy relay).
package hub

import (
	"errors"
	"hash/fnv"
	"runtime"
	"sync"

	"github.com/online111111/agentmesh/internal/protocol"
)

// Sender is the minimal interface the registry depends on (B1: the registry
// must NOT reference the concrete *Conn defined in a later task, so it can build
// and be committed independently). A *Conn implements Sender.
type Sender interface {
	// Enqueue hands a fully-encoded frame to the connection's send queue.
	Enqueue(frame []byte) error
	// Close tears down the connection.
	Close()
}

// ErrDuplicateAgentID is returned by Register on an agentId conflict within a
// tenant. Its Error() is the stable protocol.ErrDuplicateAgentID code string.
var ErrDuplicateAgentID = errors.New(protocol.ErrDuplicateAgentID)

// regKey is the comparable, zero-allocation registry key (DESIGN §5).
type regKey struct {
	tenant  string
	agentID string
}

// shard is one lock-partitioned slice of the registry keyspace.
type shard struct {
	mu      sync.RWMutex
	senders map[regKey]Sender
}

// Registry is a sharded, concurrent agent registry providing O(1) routing with
// no global lock (DESIGN §5). Registration is an atomic compare-and-set within a
// shard: a conflicting agentId in the same tenant returns DUPLICATE_AGENT_ID.
type Registry struct {
	shards []*shard
	mask   uint32
}

// NewRegistry builds a Registry whose shard count is the next power of two >=
// GOMAXPROCS*8 (bounded), so the FNV hash mask distributes keys evenly.
func NewRegistry() *Registry {
	n := nextPow2(runtime.GOMAXPROCS(0) * 8)
	if n < 8 {
		n = 8
	}
	shards := make([]*shard, n)
	for i := range shards {
		shards[i] = &shard{senders: make(map[regKey]Sender)}
	}
	return &Registry{shards: shards, mask: uint32(n - 1)}
}

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func (r *Registry) shardFor(k regKey) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(k.tenant))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(k.agentID))
	return r.shards[h.Sum32()&r.mask]
}

// Register atomically binds (tenant, agentID) to s. It returns
// ErrDuplicateAgentID if the key is already occupied (compare-and-set under the
// shard lock, so concurrent registrations of the same key yield exactly one
// winner).
func (r *Registry) Register(tenant, agentID string, s Sender) error {
	k := regKey{tenant: tenant, agentID: agentID}
	sh := r.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, exists := sh.senders[k]; exists {
		return ErrDuplicateAgentID
	}
	sh.senders[k] = s
	return nil
}

// RegisterOrTakeover atomically replaces any existing sender for (tenant, agentID)
// with s, returning the previous Sender (if any) so the caller can tear it down.
// The new binding is installed under the shard lock before the old sender is
// returned, so a concurrent Lookup never sees a gap.
func (r *Registry) RegisterOrTakeover(tenant, agentID string, s Sender) (prev Sender) {
	k := regKey{tenant: tenant, agentID: agentID}
	sh := r.shardFor(k)
	sh.mu.Lock()
	prev = sh.senders[k]
	sh.senders[k] = s
	sh.mu.Unlock()
	return prev
}

// Lookup returns the Sender registered for (tenant, agentID), if any.
func (r *Registry) Lookup(tenant, agentID string) (Sender, bool) {
	k := regKey{tenant: tenant, agentID: agentID}
	sh := r.shardFor(k)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	s, ok := sh.senders[k]
	return s, ok
}

// Remove deletes the (tenant, agentID) entry only if it currently maps to want.
// This makes stale removals (e.g. an old connection cleaning up after a
// takeover replaced it) a no-op, so a newer entry is never clobbered. Passing a
// nil want removes unconditionally.
func (r *Registry) Remove(tenant, agentID string, want Sender) {
	k := regKey{tenant: tenant, agentID: agentID}
	sh := r.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if cur, ok := sh.senders[k]; ok {
		if want == nil || cur == want {
			delete(sh.senders, k)
		}
	}
}

// ListByTenant returns the agentIds currently registered under tenant. Order is
// unspecified.
func (r *Registry) ListByTenant(tenant string) []string {
	var ids []string
	for _, sh := range r.shards {
		sh.mu.RLock()
		for k := range sh.senders {
			if k.tenant == tenant {
				ids = append(ids, k.agentID)
			}
		}
		sh.mu.RUnlock()
	}
	return ids
}
