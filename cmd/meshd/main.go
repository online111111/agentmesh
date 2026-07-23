// Command meshd is the AgentMesh relay hub daemon.
//
//	meshd serve   — start the Hub (HTTP control plane + WebSocket data plane)
//
// Configuration is via MESH_* environment variables (DESIGN §9). Non-loopback
// binds without TLS require MESH_INSECURE=true or the process refuses to start
// with INSECURE_REFUSED.
package main

import (
	"fmt"
	"os"

	"github.com/online111111/agentmesh/internal/hub"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "meshd: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: meshd <serve>")
	}
	switch args[0] {
	case "serve":
		return serve()
	case "help", "-h", "--help":
		fmt.Fprintln(os.Stdout, "usage: meshd serve")
		return nil
	default:
		return fmt.Errorf("unknown command %q (want: serve)", args[0])
	}
}

func serve() error {
	cfg, err := hub.ConfigFromEnv()
	if err != nil {
		return err
	}
	s, err := hub.NewServer(cfg)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "meshd: listening on %s (insecure=%v tls=%v)\n",
		cfg.Addr(), cfg.Insecure, cfg.TLSCert != "" && cfg.TLSKey != "")
	return s.ListenAndServe()
}
