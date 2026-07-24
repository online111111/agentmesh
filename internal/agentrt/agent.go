// Package agentrt implements the edge agent runtime: connect to a Hub, register
// with HELLO/WELCOME, and handle inbound REQUEST frames. Three modes:
//
//   - echo:  reply with the same payload (default, for connectivity tests)
//   - llm:   forward the payload as a user message to an OpenAI-compatible
//            chat-completions API and return the generated reply
//   - exec:  run an external command with the payload as input and return
//            its stdout as the reply. {prompt} in the command template is
//            replaced by the extracted prompt text. This enables integration
//            with any agent framework (Hermes, custom scripts, etc.)
package agentrt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/online111111/agentmesh/internal/protocol"
	"github.com/online111111/agentmesh/pkg/meshclient"
)

// Config configures a mesh agent runtime instance.
type Config struct {
	HubURL  string
	Token   string
	AgentID string
	Caps    []string

	// Mode is "echo" (default), "llm", or "exec".
	Mode string

	// LLM configures the upstream LLM API when Mode == "llm".
	LLM LLMConfig

	// Exec configures the external command when Mode == "exec".
	Exec ExecConfig
}

// ExecConfig configures an external command to handle inbound REQUEST frames.
// The command template in Command contains {prompt} as a placeholder that
// gets replaced by the extracted prompt text.
type ExecConfig struct {
	// Command is the full command template, e.g.
	//   "hermes chat -q {prompt} --max-turns 3"
	// or
	//   "python my_agent.py {prompt}"
	// The {prompt} placeholder is replaced with the message content.
	Command string
	// TimeoutSec bounds one command execution. Zero → 120.
	TimeoutSec int
}

// LLMConfig configures an OpenAI-compatible chat-completions endpoint used to
// answer inbound REQUEST frames. All fields required for llm mode.
type LLMConfig struct {
	// BaseURL is the API base, e.g. "https://your-llm-gateway.example.com/v1" or
	// "http://127.0.0.1:8317/v1". Must already include "/v1".
	BaseURL string
	// APIKey is the bearer token for the LLM API.
	APIKey string
	// Model is the model id, e.g. "grok-4.5" or "gpt-5.6-sol".
	Model string
	// SystemPrompt is an optional system prompt prepended to each request.
	SystemPrompt string
	// TimeoutMs bounds one LLM call. Zero → 120000.
	TimeoutMs int
}

// Agent is a running edge agent bound to a Hub connection.
type Agent struct {
	cfg    Config
	client *meshclient.Client
}

// Run dials the Hub, registers the agent, and blocks until ctx is cancelled or
// the connection dies. Inbound REQUEST frames are answered with a RESPONSE that
// echoes the payload (echo capability). SEND frames are ignored in v1 echo mode.
func Run(ctx context.Context, cfg Config) error {
	if cfg.HubURL == "" || cfg.Token == "" || cfg.AgentID == "" {
		return errors.New("agentrt: HubURL, Token, and AgentID are required")
	}
	caps := cfg.Caps
	if len(caps) == 0 {
		caps = []string{"echo"}
	}

	c, err := meshclient.Dial(ctx, meshclient.Options{
		HubURL:  cfg.HubURL,
		Token:   cfg.Token,
		AgentID: cfg.AgentID,
		Caps:    caps,
	})
	if err != nil {
		return err
	}
	defer c.Close()

	// Build the message handler depending on the mode.
	handler := buildEchoHandler(cfg.AgentID, c)
	if cfg.Mode == "llm" {
		h, err := buildLLMHandler(cfg, c)
		if err != nil {
			return err
		}
		handler = h
	}
	if cfg.Mode == "exec" {
		h, err := buildExecHandler(cfg, c)
		if err != nil {
			return err
		}
		handler = h
	}

	var writeMu sync.Mutex
	c.OnMessage(func(env protocol.Envelope, payload []byte) {
		if env.Type != protocol.REQUEST {
			return
		}
		handler(env, payload, &writeMu, ctx)
	})

	// Block until context cancelled or client closed.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.Done():
		return errors.New("agentrt: connection closed")
	}
}

// String returns a short description for logging.
func (a *Agent) String() string {
	return fmt.Sprintf("agent %s", a.cfg.AgentID)
}

// msgHandler answers one inbound REQUEST frame, serializing writes on writeMu.
type msgHandler func(env protocol.Envelope, payload []byte, writeMu *sync.Mutex, ctx context.Context)

// respond writes a RESPONSE frame that correlates with the inbound REQUEST.
func respond(ctx context.Context, c *meshclient.Client, agentID string, req protocol.Envelope, body []byte, writeMu *sync.Mutex) {
	resp := protocol.Envelope{
		V:      protocol.ProtocolVersion,
		Type:   protocol.RESPONSE,
		ID:     protocol.NewID(),
		Corr:   req.Corr,
		Src:    agentID,
		Dst:    req.Src,
		Tenant: req.Tenant,
	}
	writeMu.Lock()
	_ = c.WriteFrame(ctx, resp, body)
	writeMu.Unlock()
}

// buildEchoHandler returns the v1 echo handler: reply with the same payload.
func buildEchoHandler(agentID string, c *meshclient.Client) msgHandler {
	return func(env protocol.Envelope, payload []byte, writeMu *sync.Mutex, ctx context.Context) {
		respond(ctx, c, agentID, env, payload, writeMu)
	}
}

// buildExecHandler returns a handler that runs an external command with the
// prompt as input and returns the command's stdout as the reply.
// The command template's {prompt} placeholder is replaced by the extracted
// prompt text. The command runs via sh -c (or cmd /c on Windows).
func buildExecHandler(cfg Config, c *meshclient.Client) (msgHandler, error) {
	if cfg.Exec.Command == "" {
		return nil, errors.New("agentrt: exec mode requires Exec.Command")
	}
	timeout := cfg.Exec.TimeoutSec
	if timeout <= 0 {
		timeout = 120
	}

	return func(env protocol.Envelope, payload []byte, writeMu *sync.Mutex, ctx context.Context) {
		prompt := extractPrompt(payload)
		reply, err := callExec(ctx, cfg.Exec, prompt, timeout)
		if err != nil {
			eb, _ := json.Marshal(map[string]string{
				"error": err.Error(),
				"agent": cfg.AgentID,
			})
			respond(ctx, c, cfg.AgentID, env, eb, writeMu)
			return
		}
		ob, _ := json.Marshal(map[string]string{
			"reply": reply,
			"agent": cfg.AgentID,
			"mode":  "exec",
		})
		respond(ctx, c, cfg.AgentID, env, ob, writeMu)
	}, nil
}

// callExec runs the command template, replacing {prompt} with the prompt text.
// Returns the trimmed stdout as the reply string.
func callExec(ctx context.Context, cfg ExecConfig, prompt string, timeoutSec int) (string, error) {
	if prompt == "" {
		return "", errors.New("empty prompt")
	}
	// Replace {prompt} in the command template.
	// Use single-quotes to shell-escape the prompt safely.
	escaped := strings.ReplaceAll(prompt, "'", "'\"'\"'")
	cmdStr := strings.ReplaceAll(cfg.Command, "{prompt}", "'"+escaped+"'")

	// Apply timeout via context.
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	var cmd *exec.Cmd
	cmd = exec.CommandContext(execCtx, "sh", "-c", cmdStr)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if execCtx.Err() == context.DeadlineExceeded {
		return "", fmt.Errorf("exec timed out after %ds", timeoutSec)
	}
	if err != nil {
		stderrStr := stderr.String()
		if len(stderrStr) > 300 {
			stderrStr = stderrStr[:300] + "…"
		}
		return "", fmt.Errorf("exec failed: %w%s", err, func() string {
			if stderrStr != "" {
				return ", stderr: " + stderrStr
			}
			return ""
		}())
	}
	out := strings.TrimSpace(stdout.String())
	if out == "" {
		return "", errors.New("exec produced no output")
	}
	return out, nil
}

// buildLLMHandler returns a handler that forwards the request payload to an
// OpenAI-compatible chat-completions endpoint and replies with the generated
// text. The inbound payload may be raw text or a JSON object; see extractPrompt.
func buildLLMHandler(cfg Config, c *meshclient.Client) (msgHandler, error) {
	if cfg.LLM.BaseURL == "" || cfg.LLM.APIKey == "" {
		return nil, errors.New("agentrt: llm mode requires LLM.BaseURL and LLM.APIKey")
	}
	if cfg.LLM.Model == "" {
		cfg.LLM.Model = "gpt-4o"
	}
	timeout := cfg.LLM.TimeoutMs
	if timeout <= 0 {
		timeout = 120000
	}
	httpClient := &http.Client{Timeout: time.Duration(timeout) * time.Millisecond}

	return func(env protocol.Envelope, payload []byte, writeMu *sync.Mutex, ctx context.Context) {
		prompt := extractPrompt(payload)
		reply, err := callLLM(ctx, httpClient, cfg.LLM, prompt)
		if err != nil {
			// Return a structured error payload so the caller sees what failed.
			eb, _ := json.Marshal(map[string]string{
				"error": err.Error(),
				"agent": cfg.AgentID,
			})
			respond(ctx, c, cfg.AgentID, env, eb, writeMu)
			return
		}
		ob, _ := json.Marshal(map[string]string{
			"reply": reply,
			"model": cfg.LLM.Model,
			"agent": cfg.AgentID,
		})
		respond(ctx, c, cfg.AgentID, env, ob, writeMu)
	}, nil
}

// extractPrompt turns an inbound payload into a user prompt string. It accepts:
//   - a JSON object with a "prompt", "text", "message", or "msg" string field
//   - otherwise the raw payload bytes as text
func extractPrompt(payload []byte) string {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &obj); err == nil {
			for _, k := range []string{"prompt", "text", "message", "msg", "q", "question"} {
				if raw, ok := obj[k]; ok {
					var s string
					if json.Unmarshal(raw, &s) == nil && s != "" {
						return s
					}
				}
			}
		}
	}
	return string(trimmed)
}

// chatMessage is one OpenAI chat message.
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatRequest is the OpenAI chat-completions request body.
type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

// chatResponse is the subset of the chat-completions response we need.
type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// callLLM performs one chat-completions call and returns the assistant text.
func callLLM(ctx context.Context, hc *http.Client, cfg LLMConfig, prompt string) (string, error) {
	if prompt == "" {
		return "", errors.New("empty prompt")
	}
	msgs := make([]chatMessage, 0, 2)
	if cfg.SystemPrompt != "" {
		msgs = append(msgs, chatMessage{Role: "system", Content: cfg.SystemPrompt})
	}
	msgs = append(msgs, chatMessage{Role: "user", Content: prompt})

	body, err := json.Marshal(chatRequest{Model: cfg.Model, Messages: msgs, Stream: false})
	if err != nil {
		return "", err
	}
	url := cfg.BaseURL + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)

	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("llm http %d: %s", resp.StatusCode, truncate(string(raw), 300))
	}
	var cr chatResponse
	if err := json.Unmarshal(raw, &cr); err != nil {
		return "", fmt.Errorf("decode llm response: %w", err)
	}
	if cr.Error != nil && cr.Error.Message != "" {
		return "", fmt.Errorf("llm error: %s", cr.Error.Message)
	}
	if len(cr.Choices) == 0 {
		return "", errors.New("llm returned no choices")
	}
	return cr.Choices[0].Message.Content, nil
}

// truncate shortens s to at most n bytes for error messages.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
