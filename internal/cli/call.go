// Package cli implements mesh command-line helpers that talk to the Hub
// control plane (HTTP/JSON), primarily POST /v1/rpc for one-shot calls.
package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// CallOptions configures a one-shot RPC via the Hub control plane.
type CallOptions struct {
	HubURL  string
	Token   string
	To      string
	Payload []byte
	// TTLMs is the request timeout in milliseconds (0 → Hub default).
	TTLMs int
	// HTTPClient is optional; defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// CallResult is a successful /v1/rpc response.
type CallResult struct {
	From    string
	Payload []byte
	Corr    string
}

// Call POSTs to /v1/rpc and returns the decoded response payload.
// On Hub error codes (NO_ROUTE, TIMEOUT, AUTH_FAILED, ...) it returns an error
// whose message contains the stable code string.
func Call(ctx context.Context, opt CallOptions) (*CallResult, error) {
	if opt.HubURL == "" || opt.Token == "" || opt.To == "" {
		return nil, fmt.Errorf("cli: HubURL, Token, and To are required")
	}
	client := opt.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	body := map[string]any{
		"to":      opt.To,
		"payload": base64.StdEncoding.EncodeToString(opt.Payload),
	}
	if opt.TTLMs > 0 {
		body["ttlMs"] = opt.TTLMs
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	url := strings.TrimRight(opt.HubURL, "/") + "/v1/rpc"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+opt.Token)
	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cli: rpc: %w", err)
	}
	defer res.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("cli: read body: %w", err)
	}

	if res.StatusCode != http.StatusOK {
		var er struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(respBody, &er)
		if er.Error == "" {
			er.Error = fmt.Sprintf("HTTP_%d", res.StatusCode)
		}
		if er.Message == "" {
			er.Message = string(respBody)
		}
		return nil, fmt.Errorf("cli: %s: %s", er.Error, er.Message)
	}

	var ok struct {
		From    string `json:"from"`
		Payload string `json:"payload"`
		Corr    string `json:"corr"`
	}
	if err := json.Unmarshal(respBody, &ok); err != nil {
		return nil, fmt.Errorf("cli: decode response: %w", err)
	}
	payload, err := base64.StdEncoding.DecodeString(ok.Payload)
	if err != nil {
		// Hub may return raw UTF-8 in some paths; accept as-is.
		payload = []byte(ok.Payload)
	}
	return &CallResult{From: ok.From, Payload: payload, Corr: ok.Corr}, nil
}

// FormatResult returns a human-readable / machine-friendly line for CLI output.
// Prefer JSON when the payload looks like JSON; otherwise print as text with a
// small envelope so smoke scripts can grep.
func FormatResult(r *CallResult) string {
	// Try pretty JSON object for structured payloads.
	var js any
	if json.Unmarshal(r.Payload, &js) == nil {
		out, _ := json.Marshal(map[string]any{
			"from":    r.From,
			"payload": js,
			"corr":    r.Corr,
		})
		return string(out)
	}
	out, _ := json.Marshal(map[string]any{
		"from":    r.From,
		"payload": string(r.Payload),
		"corr":    r.Corr,
	})
	return string(out)
}

// DefaultHTTPTimeout is used when the caller does not set a context deadline.
const DefaultHTTPTimeout = 35 * time.Second
