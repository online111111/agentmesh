package meshclient

import (
	"testing"

	"github.com/online111111/agentmesh/internal/protocol"
)

func TestMarshalUnmarshalTask(t *testing.T) {
	in := DelegateTask{
		Task:   "review this PR",
		Caps:   []string{"code", "go"},
		Input:  map[string]any{"pr": 42},
		Stream: true,
		Budget: &TaskBudget{MaxTokens: 1000, TimeoutMs: 60000},
	}
	b, err := MarshalTask(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalTask(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Task != in.Task || !out.Stream || out.Budget == nil || out.Budget.MaxTokens != 1000 {
		t.Fatalf("roundtrip: %+v", out)
	}
	if len(out.Caps) != 2 || out.Caps[0] != "code" {
		t.Fatalf("caps: %v", out.Caps)
	}
}

func TestTaskEnvelope(t *testing.T) {
	env, payload, err := TaskEnvelope(DelegateTask{Task: "hi", Stream: false})
	if err != nil {
		t.Fatal(err)
	}
	if env.Type != protocol.REQUEST {
		t.Fatalf("type: %s", protocol.TypeName(env.Type))
	}
	if !IsTask(env) {
		t.Fatal("IsTask false")
	}
	if env.Hdr["ct"] != ContentTypeTask {
		t.Fatalf("ct: %q", env.Hdr["ct"])
	}
	got, err := UnmarshalTask(payload)
	if err != nil || got.Task != "hi" {
		t.Fatalf("payload: %v %v", got, err)
	}
}
