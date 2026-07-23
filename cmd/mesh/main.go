// Command mesh is the AgentMesh CLI and agent runtime.
//
//	mesh agent --hub URL --token KEY --agent-id ID [--caps CAP,...]
//	mesh call  --hub URL --token KEY --to AGENT --payload DATA   (Task 2.4)
//
// Configuration may also come from MESH_HUB / MESH_TOKEN environment variables.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/online111111/agentmesh/internal/agentrt"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "mesh: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mesh <agent|call|help>")
	}
	switch args[0] {
	case "agent":
		return runAgent(args[1:])
	case "call":
		return fmt.Errorf("call: not yet implemented (Task 2.4)")
	case "help", "-h", "--help":
		fmt.Fprintln(os.Stdout, `usage:
  mesh agent --hub URL --token KEY --agent-id ID [--caps CAP,...]
  mesh call  --hub URL --token KEY --to AGENT --payload DATA`)
		return nil
	default:
		return fmt.Errorf("unknown command %q (want: agent|call)", args[0])
	}
}

func runAgent(args []string) error {
	cfg, err := parseAgentFlags(args)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "mesh: agent %s connecting to %s\n", cfg.AgentID, cfg.HubURL)
	return agentrt.Run(ctx, cfg)
}

func parseAgentFlags(args []string) (agentrt.Config, error) {
	cfg := agentrt.Config{
		HubURL:  os.Getenv("MESH_HUB"),
		Token:   os.Getenv("MESH_TOKEN"),
		AgentID: os.Getenv("MESH_AGENT_ID"),
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		next := func() (string, error) {
			if i+1 >= len(args) {
				return "", fmt.Errorf("flag %s requires a value", a)
			}
			i++
			return args[i], nil
		}
		switch a {
		case "--hub":
			v, err := next()
			if err != nil {
				return cfg, err
			}
			cfg.HubURL = v
		case "--token":
			v, err := next()
			if err != nil {
				return cfg, err
			}
			cfg.Token = v
		case "--agent-id":
			v, err := next()
			if err != nil {
				return cfg, err
			}
			cfg.AgentID = v
		case "--caps":
			v, err := next()
			if err != nil {
				return cfg, err
			}
			if v != "" {
				cfg.Caps = strings.Split(v, ",")
			}
		case "-h", "--help":
			return cfg, fmt.Errorf("usage: mesh agent --hub URL --token KEY --agent-id ID [--caps CAP,...]")
		default:
			return cfg, fmt.Errorf("unknown flag %q", a)
		}
	}
	if cfg.HubURL == "" || cfg.Token == "" || cfg.AgentID == "" {
		return cfg, fmt.Errorf("agent requires --hub, --token, and --agent-id (or MESH_HUB/MESH_TOKEN/MESH_AGENT_ID)")
	}
	return cfg, nil
}
