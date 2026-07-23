package hub

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/online111111/agentmesh/internal/protocol"
)

func TestConfigFromEnvDefaults(t *testing.T) {
	for _, k := range []string{
		"MESH_HOST", "MESH_PORT", "MESH_API_KEYS", "MESH_MAX_FRAME_BYTES",
		"MESH_SEND_QUEUE_BYTES", "MESH_TLS_CERT", "MESH_TLS_KEY", "MESH_INSECURE",
	} {
		t.Setenv(k, "")
	}
	t.Setenv("MESH_API_KEYS", "a:ka:alice:default")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.Host != "0.0.0.0" {
		t.Errorf("Host: %q", cfg.Host)
	}
	if cfg.Port != 8080 {
		t.Errorf("Port: %d", cfg.Port)
	}
	if cfg.MaxFrameBytes != 1<<20 {
		t.Errorf("MaxFrameBytes: %d", cfg.MaxFrameBytes)
	}
	if cfg.SendQueueBytes != 1<<22 {
		t.Errorf("SendQueueBytes: %d", cfg.SendQueueBytes)
	}
	if cfg.Insecure {
		t.Error("Insecure should default false")
	}
}

func TestConfigFromEnvMissingKeys(t *testing.T) {
	t.Setenv("MESH_API_KEYS", "")
	_, err := ConfigFromEnv()
	if err == nil {
		t.Fatal("expected error when MESH_API_KEYS empty")
	}
}

func TestSecurityCheckLoopbackAllowed(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "localhost", "::1"} {
		cfg := Config{Host: host, Port: 8080}
		if err := cfg.CheckSecurity(); err != nil {
			t.Fatalf("%s must be allowed without TLS: %v", host, err)
		}
	}
}

func TestSecurityCheckNonLoopbackRefused(t *testing.T) {
	cfg := Config{Host: "0.0.0.0", Port: 8080}
	err := cfg.CheckSecurity()
	if err == nil {
		t.Fatal("0.0.0.0 without TLS/INSECURE must refuse")
	}
	if !strings.Contains(err.Error(), protocol.ErrInsecureRefused) {
		t.Fatalf("want INSECURE_REFUSED, got %v", err)
	}
	cfg.Host = "192.168.1.10"
	if err := cfg.CheckSecurity(); err == nil {
		t.Fatal("public bind without TLS/INSECURE must refuse")
	}
}

func TestSecurityCheckInsecureOptIn(t *testing.T) {
	cfg := Config{Host: "0.0.0.0", Port: 8080, Insecure: true}
	if err := cfg.CheckSecurity(); err != nil {
		t.Fatalf("MESH_INSECURE=true must allow: %v", err)
	}
}

func TestSecurityCheckTLSAllows(t *testing.T) {
	cfg := Config{Host: "0.0.0.0", Port: 8080, TLSCert: "/tmp/cert.pem", TLSKey: "/tmp/key.pem"}
	if err := cfg.CheckSecurity(); err != nil {
		t.Fatalf("TLS cert+key must allow non-loopback: %v", err)
	}
}

func freeLoopbackPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func TestServerListenHealth(t *testing.T) {
	// End-to-end: Start on a free loopback port, GET /health returns ok.
	port := freeLoopbackPort(t)
	t.Setenv("MESH_API_KEYS", "a:ka:alice:default")
	t.Setenv("MESH_HOST", "127.0.0.1")
	t.Setenv("MESH_PORT", strconv.Itoa(port))
	t.Setenv("MESH_INSECURE", "")
	t.Setenv("MESH_TLS_CERT", "")
	t.Setenv("MESH_TLS_KEY", "")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go func() { _ = s.ListenAndServe() }()
	t.Cleanup(func() { _ = s.Close() })

	url := "http://" + cfg.Addr() + "/health"
	var res *http.Response
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		res, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
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

func TestServerRefuseNonLoopback(t *testing.T) {
	t.Setenv("MESH_API_KEYS", "a:ka:alice:default")
	t.Setenv("MESH_HOST", "0.0.0.0")
	t.Setenv("MESH_PORT", "18080")
	t.Setenv("MESH_INSECURE", "")
	t.Setenv("MESH_TLS_CERT", "")
	t.Setenv("MESH_TLS_KEY", "")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	_, err = NewServer(cfg)
	if err == nil {
		t.Fatal("NewServer must refuse non-loopback without TLS/INSECURE")
	}
	if !strings.Contains(err.Error(), protocol.ErrInsecureRefused) {
		t.Fatalf("want INSECURE_REFUSED, got %v", err)
	}
}

func TestServerAllowInsecureEnv(t *testing.T) {
	port := freeLoopbackPort(t)
	t.Setenv("MESH_API_KEYS", "a:ka:alice:default")
	t.Setenv("MESH_HOST", "0.0.0.0")
	t.Setenv("MESH_PORT", strconv.Itoa(port))
	t.Setenv("MESH_INSECURE", "true")
	t.Setenv("MESH_TLS_CERT", "")
	t.Setenv("MESH_TLS_KEY", "")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if !cfg.Insecure {
		t.Fatal("Insecure not set from env")
	}
	// Security check must pass for 0.0.0.0 + INSECURE.
	if err := cfg.CheckSecurity(); err != nil {
		t.Fatalf("CheckSecurity with INSECURE: %v", err)
	}
	// Listen on loopback so the test doesn't bind all interfaces; security
	// already validated against the original Host.
	cfg.Host = "127.0.0.1"
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	_ = s.Close()
}

func TestServerTLSListen(t *testing.T) {
	dir := t.TempDir()
	cert := dir + "/cert.pem"
	key := dir + "/key.pem"
	if err := GenerateSelfSigned(cert, key, []string{"127.0.0.1", "localhost"}); err != nil {
		t.Fatalf("GenerateSelfSigned: %v", err)
	}
	port := freeLoopbackPort(t)
	cfg := Config{
		Host:           "127.0.0.1",
		Port:           port,
		APIKeys:        "a:ka:alice:default",
		MaxFrameBytes:  1 << 20,
		SendQueueBytes: 1 << 20,
		TLSCert:        cert,
		TLSKey:         key,
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go func() { _ = s.ListenAndServe() }()
	t.Cleanup(func() { _ = s.Close() })

	// InsecureSkipVerify for self-signed
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	client := &http.Client{Transport: tr, Timeout: 2 * time.Second}
	url := fmt.Sprintf("https://127.0.0.1:%d/health", port)
	var res *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		res, err = client.Get(url)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET https /health: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestGenerateSelfSignedFiles(t *testing.T) {
	dir := t.TempDir()
	cert := dir + "/c.pem"
	key := dir + "/k.pem"
	if err := GenerateSelfSigned(cert, key, []string{"127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadTLSConfig(cert, key); err != nil {
		t.Fatal(err)
	}
}
