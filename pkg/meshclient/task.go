package meshclient

import (
	"encoding/json"

	"github.com/online111111/agentmesh/internal/protocol"
)

// ContentTypeTask is the hdr["ct"] value for the LLM task-delegation envelope
// (DESIGN §4.10).
const ContentTypeTask = "application/x-mesh-task"

// DelegateTask is the SDK-layer task-delegation envelope carried in REQUEST
// payload when hdr["ct"] == ContentTypeTask. Hub does not parse this.
type DelegateTask struct {
	Task   string         `json:"task"`
	Caps   []string       `json:"caps,omitempty"`
	Input  map[string]any `json:"input,omitempty"`
	Stream bool           `json:"stream,omitempty"`
	Budget *TaskBudget    `json:"budget,omitempty"`
}

// TaskBudget is an optional soft limit the target agent may honor.
type TaskBudget struct {
	MaxTokens int   `json:"max_tokens,omitempty"`
	TimeoutMs int32 `json:"timeout_ms,omitempty"`
	// CostCents is an advisory upper bound in cents of account currency.
	CostCents int `json:"cost_cents,omitempty"`
}

// MarshalTask encodes a DelegateTask as JSON payload bytes.
func MarshalTask(t DelegateTask) ([]byte, error) {
	return json.Marshal(t)
}

// UnmarshalTask decodes a DelegateTask from payload bytes.
func UnmarshalTask(b []byte) (DelegateTask, error) {
	var t DelegateTask
	err := json.Unmarshal(b, &t)
	return t, err
}

// TaskEnvelope builds a REQUEST envelope with hdr ct set for task delegation.
// Corr/ID/Src/Dst/TTL/Hops are left for the caller (or Request/RequestStream).
func TaskEnvelope(task DelegateTask) (protocol.Envelope, []byte, error) {
	payload, err := MarshalTask(task)
	if err != nil {
		return protocol.Envelope{}, nil, err
	}
	env := protocol.Envelope{
		V:    protocol.ProtocolVersion,
		Type: protocol.REQUEST,
		Hdr:  map[string]string{"ct": ContentTypeTask},
	}
	return env, payload, nil
}

// IsTask reports whether env carries a task-delegation content type.
func IsTask(env protocol.Envelope) bool {
	return env.Hdr != nil && env.Hdr["ct"] == ContentTypeTask
}
