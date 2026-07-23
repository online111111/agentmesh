// Package agentrt implements the edge agent runtime: connect to a Hub, register
// with HELLO/WELCOME, and handle inbound REQUEST frames with a built-in echo
// capability (reply RESPONSE with the same corr and payload).
package agentrt

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/online111111/agentmesh/internal/protocol"
	"github.com/online111111/agentmesh/pkg/meshclient"
)

// Config configures a mesh agent runtime instance.
type Config struct {
	HubURL  string
	Token   string
	AgentID string
	Caps    []string
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

	a := &Agent{cfg: cfg, client: c}

	var writeMu sync.Mutex
	c.OnMessage(func(env protocol.Envelope, payload []byte) {
		if env.Type != protocol.REQUEST {
			return
		}
		// Echo RESPONSE: same corr, swap src/dst, same payload.
		resp := protocol.Envelope{
			V:      protocol.ProtocolVersion,
			Type:   protocol.RESPONSE,
			ID:     protocol.NewID(),
			Corr:   env.Corr,
			Src:    cfg.AgentID,
			Dst:    env.Src,
			Tenant: env.Tenant,
		}
		writeMu.Lock()
		err := a.client.WriteFrame(ctx, resp, payload)
		writeMu.Unlock()
		if err != nil {
			// Connection likely closed; readLoop will exit.
			return
		}
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
