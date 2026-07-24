package agentrt

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// mockLLM returns a test server that speaks minimal OpenAI chat-completions.
// It echoes the user prompt back inside the assistant content so the test can
// assert the prompt round-tripped correctly.
func mockLLM(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "bad path "+r.URL.Path, http.StatusNotFound)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			http.Error(w, "bad auth "+got, http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req chatRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		// Grab last user message.
		var user string
		for _, m := range req.Messages {
			if m.Role == "user" {
				user = m.Content
			}
		}
		resp := chatResponse{}
		resp.Choices = append(resp.Choices, struct {
			Message chatMessage `json:"message"`
		}{Message: chatMessage{Role: "assistant", Content: "ANSWER:" + user + "|model=" + req.Model}})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

func TestCallLLM_RawPrompt(t *testing.T) {
	srv := mockLLM(t)
	defer srv.Close()

	cfg := LLMConfig{
		BaseURL: srv.URL + "/v1",
		APIKey:  "test-key",
		Model:   "grok-4.5",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := callLLM(ctx, srv.Client(), cfg, "hello world")
	if err != nil {
		t.Fatalf("callLLM: %v", err)
	}
	if !strings.Contains(out, "ANSWER:hello world") {
		t.Fatalf("unexpected reply: %q", out)
	}
	if !strings.Contains(out, "model=grok-4.5") {
		t.Fatalf("model not forwarded: %q", out)
	}
}

func TestCallLLM_SystemPrompt(t *testing.T) {
	srv := mockLLM(t)
	defer srv.Close()

	cfg := LLMConfig{
		BaseURL:      srv.URL + "/v1",
		APIKey:       "test-key",
		Model:        "gpt-5.6-sol",
		SystemPrompt: "you are terse",
	}
	ctx := context.Background()
	out, err := callLLM(ctx, srv.Client(), cfg, "ping")
	if err != nil {
		t.Fatalf("callLLM: %v", err)
	}
	if !strings.Contains(out, "ANSWER:ping") {
		t.Fatalf("unexpected reply: %q", out)
	}
}

func TestCallLLM_EmptyPrompt(t *testing.T) {
	srv := mockLLM(t)
	defer srv.Close()
	cfg := LLMConfig{BaseURL: srv.URL + "/v1", APIKey: "test-key", Model: "m"}
	if _, err := callLLM(context.Background(), srv.Client(), cfg, ""); err == nil {
		t.Fatal("want error on empty prompt")
	}
}

func TestCallLLM_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"boom"}}`, http.StatusInternalServerError)
	}))
	defer srv.Close()
	cfg := LLMConfig{BaseURL: srv.URL + "/v1", APIKey: "k", Model: "m"}
	_, err := callLLM(context.Background(), srv.Client(), cfg, "hi")
	if err == nil {
		t.Fatal("want error on 500")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("want 500 in error, got %v", err)
	}
}

func TestExtractPrompt(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"raw", "just text", "just text"},
		{"json_prompt", `{"prompt":"from prompt"}`, "from prompt"},
		{"json_text", `{"text":"from text"}`, "from text"},
		{"json_message", `{"message":"from message"}`, "from message"},
		{"json_question", `{"question":"from q"}`, "from q"},
		{"json_no_known_key", `{"foo":"bar"}`, `{"foo":"bar"}`},
		{"json_priority", `{"prompt":"p","text":"t"}`, "p"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractPrompt([]byte(tc.in))
			if got != tc.want {
				t.Fatalf("extractPrompt(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestBuildLLMHandler_Validation(t *testing.T) {
	// Missing required fields → error.
	_, err := buildLLMHandler(Config{Mode: "llm"}, nil)
	if err == nil {
		t.Fatal("want error when LLM config incomplete")
	}
}
