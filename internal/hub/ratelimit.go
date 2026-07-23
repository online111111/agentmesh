package hub

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TokenBucket is a simple thread-safe token bucket rate limiter.
type TokenBucket struct {
	mu          sync.Mutex
	rate        float64 // tokens per second
	burst       float64
	tokens      float64
	last        time.Time
	refuseUntil time.Time // optional cool-down after sustained failure
}

// NewTokenBucket creates a bucket that refills at rate tokens/sec with burst capacity.
func NewTokenBucket(rate float64, burst float64) *TokenBucket {
	if rate <= 0 {
		rate = 1
	}
	if burst < 1 {
		burst = 1
	}
	return &TokenBucket{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

// Allow reports whether one event is permitted and consumes a token if so.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if now.Before(b.refuseUntil) {
		return false
	}
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// RateLimiter tracks per-key token buckets (agentId or IP).
type RateLimiter struct {
	mu    sync.Mutex
	rate  float64
	burst float64
	bucks map[string]*TokenBucket
}

// NewRateLimiter builds a limiter with the given rate/burst for every key.
func NewRateLimiter(rate, burst float64) *RateLimiter {
	return &RateLimiter{rate: rate, burst: burst, bucks: make(map[string]*TokenBucket)}
}

// Allow returns true if key may proceed.
func (r *RateLimiter) Allow(key string) bool {
	r.mu.Lock()
	b, ok := r.bucks[key]
	if !ok {
		b = NewTokenBucket(r.rate, r.burst)
		r.bucks[key] = b
	}
	r.mu.Unlock()
	return b.Allow()
}

func envFloat(k string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return def
	}
	return f
}

// clientIP extracts a coarse client key from the request (RemoteAddr host).
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
