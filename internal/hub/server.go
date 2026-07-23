package hub

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/online111111/agentmesh/internal/auth"
	"github.com/online111111/agentmesh/internal/protocol"
)

// Config holds meshd runtime configuration loaded from environment variables
// (DESIGN §9). CheckSecurity must pass before ListenAndServe.
type Config struct {
	Host           string
	Port           int
	APIKeys        string
	MaxFrameBytes  int
	SendQueueBytes int
	TLSCert        string
	TLSKey         string
	Insecure       bool // MESH_INSECURE — allow non-loopback without TLS
	PublicURL      string
}

// Addr returns host:port for the listen address.
func (c Config) Addr() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// isLoopback reports whether host is a loopback bind target (DESIGN §6: loopback
// is always allowed without TLS). Recognizes 127.0.0.0/8, ::1, and "localhost".
func isLoopback(host string) bool {
	h := strings.TrimSpace(strings.ToLower(host))
	if h == "" || h == "localhost" {
		return true
	}
	// Strip zone id if present (e.g. fe80::1%lo0) — not needed for loopback.
	if i := strings.IndexByte(h, '%'); i >= 0 {
		h = h[:i]
	}
	ip := net.ParseIP(h)
	if ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// CheckSecurity enforces the mandatory-TLS guard (DESIGN §6/§9): non-loopback
// binds without TLS cert+key and without MESH_INSECURE must be refused with
// INSECURE_REFUSED. Loopback is always allowed. TLS cert+key or INSECURE=true
// allows any bind address.
func (c Config) CheckSecurity() error {
	if isLoopback(c.Host) {
		return nil
	}
	hasTLS := c.TLSCert != "" && c.TLSKey != ""
	if hasTLS || c.Insecure {
		return nil
	}
	return fmt.Errorf("%s: non-loopback bind %q requires TLS (MESH_TLS_CERT/KEY) or MESH_INSECURE=true",
		protocol.ErrInsecureRefused, c.Host)
}

// ConfigFromEnv loads Config from the MESH_* environment variables (DESIGN §9).
// MESH_API_KEYS is required (production must configure at least one key).
func ConfigFromEnv() (Config, error) {
	cfg := Config{
		Host:           envOr("MESH_HOST", "0.0.0.0"),
		Port:           envIntOr("MESH_PORT", 8080),
		APIKeys:        os.Getenv("MESH_API_KEYS"),
		MaxFrameBytes:  envIntOr("MESH_MAX_FRAME_BYTES", 1<<20),
		SendQueueBytes: envIntOr("MESH_SEND_QUEUE_BYTES", 1<<22),
		TLSCert:        os.Getenv("MESH_TLS_CERT"),
		TLSKey:         os.Getenv("MESH_TLS_KEY"),
		Insecure:       envBool("MESH_INSECURE"),
		PublicURL:      os.Getenv("MESH_PUBLIC_URL"),
	}
	if strings.TrimSpace(cfg.APIKeys) == "" {
		return Config{}, errors.New("MESH_API_KEYS is required")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return Config{}, fmt.Errorf("invalid MESH_PORT: %d", cfg.Port)
	}
	if cfg.MaxFrameBytes <= 0 {
		return Config{}, fmt.Errorf("invalid MESH_MAX_FRAME_BYTES: %d", cfg.MaxFrameBytes)
	}
	if cfg.SendQueueBytes <= 0 {
		return Config{}, fmt.Errorf("invalid MESH_SEND_QUEUE_BYTES: %d", cfg.SendQueueBytes)
	}
	return cfg, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envIntOr(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envBool(k string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

// Server is the production HTTP+WS Hub listener. It composes the Gateway and
// HTTP control plane under a single net/http.Server.
type Server struct {
	cfg  Config
	http *http.Server
	auth *auth.Authenticator
	gw   *Gateway
}

// NewServer builds a Server from cfg. It runs CheckSecurity and fails with
// INSECURE_REFUSED when the bind would violate the TLS guard. The returned
// Server is not yet listening; call ListenAndServe.
func NewServer(cfg Config) (*Server, error) {
	if err := cfg.CheckSecurity(); err != nil {
		return nil, err
	}
	keys, err := auth.ParseKeys(cfg.APIKeys)
	if err != nil {
		return nil, fmt.Errorf("parse MESH_API_KEYS: %w", err)
	}
	a := auth.NewAuthenticator(keys)
	// Align package-level frame cap with the configured limit so DecodeFrame
	// rejects oversize envelopes consistently with the WS read limit.
	protocol.MaxFrameBytes = cfg.MaxFrameBytes

	reg := NewRegistry()
	gw := NewGateway(a, reg, cfg.MaxFrameBytes, cfg.SendQueueBytes)
	handler := NewHTTP(gw, a)

	s := &Server{
		cfg:  cfg,
		auth: a,
		gw:   gw,
		http: &http.Server{
			Addr:              cfg.Addr(),
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
		},
	}
	return s, nil
}

// Gateway exposes the underlying gateway (tests / diagnostics).
func (s *Server) Gateway() *Gateway { return s.gw }

// ListenAndServe starts the HTTP(+TLS) listener and blocks until the server is
// closed or a fatal accept error occurs.
func (s *Server) ListenAndServe() error {
	if s.cfg.TLSCert != "" && s.cfg.TLSKey != "" {
		return s.http.ListenAndServeTLS(s.cfg.TLSCert, s.cfg.TLSKey)
	}
	return s.http.ListenAndServe()
}

// Close shuts the HTTP server down immediately.
func (s *Server) Close() error {
	return s.http.Close()
}

// Shutdown gracefully drains connections with the given context.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
