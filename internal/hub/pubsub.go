package hub

import (
	"strings"
	"sync"

	"github.com/online111111/agentmesh/internal/protocol"
)

// TopicPrefix is the required dst prefix for pub/sub destinations (DESIGN §4.8).
const TopicPrefix = "topic:"

// IsTopic reports whether dst is a topic address.
func IsTopic(dst string) bool {
	return strings.HasPrefix(dst, TopicPrefix)
}

// PubSub is a tenant-isolated, connection-level subscription table.
// Subscriptions are soft state: they die with the connection (DESIGN §4.8).
type PubSub struct {
	mu sync.RWMutex
	// tenant → topic → set of conn pointers (as map[*Conn]struct{})
	subs map[string]map[string]map[*Conn]struct{}
}

// NewPubSub constructs an empty subscription table.
func NewPubSub() *PubSub {
	return &PubSub{subs: make(map[string]map[string]map[*Conn]struct{})}
}

// Subscribe registers conn for topic within tenant. No-op if already subscribed.
func (p *PubSub) Subscribe(tenant, topic string, conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	byTopic, ok := p.subs[tenant]
	if !ok {
		byTopic = make(map[string]map[*Conn]struct{})
		p.subs[tenant] = byTopic
	}
	set, ok := byTopic[topic]
	if !ok {
		set = make(map[*Conn]struct{})
		byTopic[topic] = set
	}
	set[conn] = struct{}{}
}

// Unsubscribe removes conn from topic within tenant.
func (p *PubSub) Unsubscribe(tenant, topic string, conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	byTopic := p.subs[tenant]
	if byTopic == nil {
		return
	}
	set := byTopic[topic]
	if set == nil {
		return
	}
	delete(set, conn)
	if len(set) == 0 {
		delete(byTopic, topic)
	}
	if len(byTopic) == 0 {
		delete(p.subs, tenant)
	}
}

// UnsubscribeAll removes conn from every topic in tenant (connection teardown).
func (p *PubSub) UnsubscribeAll(tenant string, conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	byTopic := p.subs[tenant]
	if byTopic == nil {
		return
	}
	for topic, set := range byTopic {
		delete(set, conn)
		if len(set) == 0 {
			delete(byTopic, topic)
		}
	}
	if len(byTopic) == 0 {
		delete(p.subs, tenant)
	}
}

// Snapshot returns a slice of current subscribers for topic in tenant.
// The lock is held only while copying the set (DESIGN §4.8 snapshot fan-out).
func (p *PubSub) Snapshot(tenant, topic string) []*Conn {
	p.mu.RLock()
	defer p.mu.RUnlock()
	set := p.subs[tenant][topic]
	if len(set) == 0 {
		return nil
	}
	out := make([]*Conn, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	return out
}

// TopicFromEnv returns the topic string from a SUBSCRIBE/UNSUB/PUBLISH envelope.
// Prefers Dst when it has the topic: prefix; falls back to hdr["topic"].
func TopicFromEnv(env protocol.Envelope) string {
	if IsTopic(env.Dst) {
		return env.Dst
	}
	if env.Hdr != nil {
		if t := env.Hdr["topic"]; IsTopic(t) {
			return t
		}
	}
	return ""
}
