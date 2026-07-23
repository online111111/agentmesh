# AgentMesh

Cross-platform multi-agent communication mesh: a high-performance Go Hub
(`meshd`) plus CLI/runtime (`mesh`) and Go / Python / TypeScript SDKs.

Agents register over WebSocket, exchange SEND/REQUEST messages, stream token
results, cancel in-flight work, and publish to tenant-isolated topics. The Hub
relays with zero-copy payload handling and connection identity as the only
trust root.

## Install & three-command demo

```bash
go install github.com/online111111/agentmesh/cmd/meshd@latest   # or build from source
go install github.com/online111111/agentmesh/cmd/mesh@latest

# 1) Hub (loopback)
export MESH_HOST=127.0.0.1 MESH_PORT=8080
export MESH_API_KEYS='a:ka:alice:default'
meshd serve

# 2) Echo agent
mesh agent --hub http://127.0.0.1:8080 --token ka --agent-id alice-echo --caps echo

# 3) Call
mesh call --hub http://127.0.0.1:8080 --token ka --to alice-echo --payload '{"hello":"mesh"}'
```

Automated cross-process smoke:

```bash
./scripts/smoke.sh
```

## Build from source

```bash
git clone https://github.com/online111111/agentmesh
cd agentmesh
go build -o bin/meshd ./cmd/meshd
go build -o bin/mesh ./cmd/mesh
./scripts/build-all.sh   # cross-compile matrix → dist/
```

## SDKs

| Language | Path | Notes |
|----------|------|-------|
| Go | `pkg/meshclient` | Dial, Send, Request, RequestStream, Cancel, DelegateTask |
| Python | `sdk/python` | async websockets client + golden vector tests |
| TypeScript | `sdk/typescript` | ws client + vitest golden vectors |

Wire parity is enforced by `internal/protocol/testvectors.json`.

## Docs

- Design (frozen wire): [`docs/design/mesh-design.md`](docs/design/mesh-design.md)
- Protocol summary: [`docs/guides/protocol.md`](docs/guides/protocol.md)
- LAN quickstart: [`docs/guides/quickstart-lan.md`](docs/guides/quickstart-lan.md)
- Public deploy: [`docs/guides/quickstart-public.md`](docs/guides/quickstart-public.md)
- Bench baseline: [`bench/RESULTS.md`](bench/RESULTS.md)

## Security highlights

- Hub **overwrites** `src`/`tenant` from authenticated connection identity.
- agentId bound to principal namespace; takeover only same principal.
- Non-loopback bind without TLS requires `MESH_INSECURE=true` or is refused
  (`INSECURE_REFUSED`).
- Per-agent message rate limit + per-IP connect anti-flood.

## License

See repository for license terms.
