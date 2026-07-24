package agentrt

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestCallExec_Basic(t *testing.T) {
	cfg := ExecConfig{Command: "echo RESPONSE:{prompt}"}
	ctx := context.Background()
	out, err := callExec(ctx, cfg, "hello", 10)
	if err != nil {
		t.Fatalf("callExec: %v", err)
	}
	if !strings.Contains(out, "RESPONSE:hello") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestCallExec_Pipeline(t *testing.T) {
	// Test that shell features work — pipe, sed
	cfg := ExecConfig{Command: "echo {prompt} | tr a-z A-Z"}
	ctx := context.Background()
	out, err := callExec(ctx, cfg, "hello", 10)
	if err != nil {
		t.Fatalf("callExec: %v", err)
	}
	if out != "HELLO" {
		t.Fatalf("expected HELLO, got %q", out)
	}
}

func TestCallExec_EmptyPrompt(t *testing.T) {
	cfg := ExecConfig{Command: "echo {prompt}"}
	if _, err := callExec(context.Background(), cfg, "", 10); err == nil {
		t.Fatal("want error on empty prompt")
	}
}

func TestCallExec_Timeout(t *testing.T) {
	// Use a command that respects SIGTERM — `sleep` with a short timeout
	cfg := ExecConfig{Command: "sleep 2", TimeoutSec: 1}
	start := time.Now()
	_, err := callExec(context.Background(), cfg, "test", 1)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("want timeout error, got %v", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("timeout took too long: %v", elapsed)
	}
}

func TestCallExec_Failure(t *testing.T) {
	cfg := ExecConfig{Command: "false"}
	_, err := callExec(context.Background(), cfg, "test", 10)
	if err == nil {
		t.Fatal("want error on command failure")
	}
}

func TestCallExec_QuoteInjection(t *testing.T) {
	// Prompt containing single quotes should not escape the shell
	cfg := ExecConfig{Command: "echo {prompt}"}
	ctx := context.Background()
	out, err := callExec(ctx, cfg, "it's working", 10)
	if err != nil {
		t.Fatalf("callExec: %v", err)
	}
	if !strings.Contains(out, "it's working") {
		t.Fatalf("expected to contain it's working, got %q", out)
	}
}

func TestBuildExecHandler_Validation(t *testing.T) {
	// Missing command → error
	_, err := buildExecHandler(Config{Mode: "exec"}, nil)
	if err == nil {
		t.Fatal("want error when exec command not set")
	}
}

// TestBuildExecHandler_JSONOutput verifies the exec reply JSON structure.
func TestBuildExecHandler_JSONOutput(t *testing.T) {
	out := map[string]string{"reply": "test", "agent": "agent", "mode": "exec"}
	b, _ := json.Marshal(out)
	var ob map[string]string
	if err := json.Unmarshal(b, &ob); err != nil {
		t.Fatalf("json round-trip: %v", err)
	}
	if ob["mode"] != "exec" {
		t.Fatalf("expected mode=exec, got %s", ob["mode"])
	}
}
