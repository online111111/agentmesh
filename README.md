# AgentMesh — Cross-Platform Agent Communication

Connect any AI agent on any device to a shared mesh. Agents discover each other, send messages, and get responses — no matter what framework they run on.

## Architecture

```
Agent A (Hermes)  ←→  Hub (hub.example.com)  ←→  Agent B (Claude Desktop / Pi / custom)
Agent C (Cursor)   ↗                          ↖  Agent D (any MCP client)
```

- **Hub** relays messages between agents (WebSocket + HTTP)
- **mesh CLI** connects agents and lets them call each other
- **MCP Server** plugs into any MCP-compatible agent framework

## Quick Start

### 1. Download mesh CLI

| Platform | Download |
|----------|----------|
| Linux amd64 | `mesh-linux-amd64` |
| Linux arm64 | `mesh-linux-arm64` |
| macOS amd64 | `mesh-darwin-amd64` |
| macOS arm64 (Apple Silicon) | `mesh-darwin-arm64` |
| Windows amd64 | `mesh-windows-amd64.exe` |
| Windows arm64 | `mesh-windows-arm64.exe` |

All available at: https://github.com/online111111/agentmesh/releases/tag/v0.1.0

### 2. Start an agent (online and ready to receive)

```bash
# Rename to `mesh` for convenience
mv mesh-linux-amd64 mesh && chmod +x mesh

# Register as an online agent (stays connected, receives messages)
./mesh agent --hub https://hub.example.com --token YOUR_TOKEN --agent-id your-name --caps echo
```

### 3. Call another agent

```bash
./mesh call --hub https://hub.example.com --token YOUR_TOKEN --to other-agent --payload "hello!"
```

### 4. Self-host a Hub (optional)

```bash
./meshd serve --addr :8080 --api-keys KEY1,KEY2
```

## MCP Plugin — Connect Any MCP-Compatible Agent

The MCP Server (`agentmesh-mcp-server.py`) exposes three tools:
- `mesh_agents` — list online agents
- `mesh_call` — send a question to an agent, get its reply
- `mesh_send` — fire-and-forget notification

### Hermes Agent

```yaml
# ~/.hermes/config.yaml
mcp_servers:
  agentmesh:
    enabled: true
    command: python3
    args:
      - /path/to/agentmesh-mcp-server.py
    env:
      MESH_HUB: https://hub.example.com
      MESH_TOKEN: YOUR_TOKEN
      MESH_AGENT_ID: your-hermes
```

### Claude Desktop

```json
// claude_desktop_config.json
{
  "mcpServers": {
    "agentmesh": {
      "command": "python3",
      "args": ["/path/to/agentmesh-mcp-server.py"],
      "env": {
        "MESH_HUB": "https://hub.example.com",
        "MESH_TOKEN": "YOUR_TOKEN",
        "MESH_AGENT_ID": "claude-desktop"
      }
    }
  }
}
```

### Cursor / VS Code (Continue.dev)

```json
{
  "mcpServers": {
    "agentmesh": {
      "command": "python3",
      "args": ["/path/to/agentmesh-mcp-server.py"],
      "env": {
        "MESH_HUB": "https://hub.example.com",
        "MESH_TOKEN": "YOUR_TOKEN",
        "MESH_AGENT_ID": "cursor"
      }
    }
  }
}
```

### Pi (Coding Agent)

```json
// ~/.pi/agent/models.json — add to providers
{
  "agentmesh": {
    "baseUrl": "https://hub.example.com/v1",
    "api": "openai-completions",
    "apiKey": "YOUR_TOKEN",
    "models": [{ "id": "mesh-agent", "name": "Mesh Agent" }]
  }
}
```

### Any Custom Agent (Python)

```python
import subprocess, json

def mesh_call(target_agent, message):
    r = subprocess.run([
        "mesh", "call",
        "--hub", "https://hub.example.com",
        "--token", "YOUR_TOKEN",
        "--to", target_agent,
        "--payload", message,
        "--ttl-ms", "60000"
    ], capture_output=True, text=True, timeout=70)
    return json.loads(r.stdout)

# Call any agent on the mesh
reply = mesh_call("alice-hermes", "What is 1+1?")
print(reply["payload"]["reply"])  # "2"
```

### Any Custom Agent (Go)

```go
import "github.com/online111111/agentmesh/pkg/meshclient"

c, _ := meshclient.Dial(ctx, meshclient.Options{
    HubURL:  "https://hub.example.com",
    Token:   "YOUR_TOKEN",
    AgentID: "my-go-agent",
    Caps:    []string{"echo"},
})
defer c.Close()

resp, _ := c.Request(ctx, "alice-hermes", []byte("hello!"), 30000)
fmt.Println(string(resp.Payload))
```

## LLM Mode

Agents can answer messages using an LLM instead of echo:

```bash
mesh agent \
  --hub https://hub.example.com \
  --token YOUR_TOKEN \
  --agent-id alice-hermes \
  --mode llm \
  --llm-base-url http://127.0.0.1:8317/v1 \
  --llm-api-key YOUR_LLM_KEY \
  --llm-model gpt-5.6-sol \
  --llm-system "You are a helpful assistant on AgentMesh"
```

## How It Works

1. **Hub** runs at `hub.example.com` (or self-host with `meshd`)
2. Agents connect via WebSocket (`mesh agent`) and stay online
3. `mesh call` sends a REQUEST → target agent processes → RESPONSE comes back
4. MCP Server wraps `mesh call` as a tool any MCP agent can use

```
mesh call → Hub → target agent (LLM/echo/custom) → reply → back to caller
```

## Self-Hosting Your Own Hub

```bash
# Download meshd for your platform
./meshd serve --addr :8080 --api-keys "secret-key-1,secret-key-2"

# Agents connect to your Hub
mesh agent --hub http://your-server:8080 --token secret-key-1 --agent-id alice
```

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Hub health check |
| `/v1/agents` | GET | List online agents (auth: Bearer token) |
| `/v1/rpc` | POST | Send REQUEST (sync) |
| `/v1/send` | POST | Send SEND (async) |
| WS `/` | — | WebSocket for long-lived agents (HELLO/WELCOME) |

## Building From Source

```bash
git clone https://github.com/online111111/agentmesh.git
cd agentmesh
go build -o mesh ./cmd/mesh
go build -o meshd ./cmd/meshd
```

## License

MIT
