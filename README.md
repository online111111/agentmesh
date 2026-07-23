# AgentMesh

跨平台 · 跨设备 · 高性能的多 Agent 通信中继。让 Hermes、Codex CLI 等 LLM/编码 Agent 在不同设备、不同操作系统之间互相发现、委派任务、流式回传结果。

- **单静态二进制** Hub(Go),交叉编译到 Linux / macOS / Windows / Android(Termux) / ARM。
- **开放 wire 协议**:WebSocket + MessagePack;官方 SDK 覆盖 Go / Python / TypeScript,协议字节级一致。
- **热路径零拷贝**:Hub 只解析路由头,payload 原字节转发;目标万级并发长连、低延迟。
- **LLM 原生**:任务委派信封 + 流式 token 回传 + 任务取消,针对编码 Agent 委派场景设计。

> **当前状态:设计阶段。** 完整方案见 [`docs/design/mesh-design.md`](docs/design/mesh-design.md)(v0.4,已吸收三维独立评审的全部 BLOCKING)。实现按 P0–P7 阶段、严格 TDD 推进。

## 快速开始(实现后)

```bash
# 起 Hub
MESH_API_KEYS='a:ka:alice:default' meshd serve

# 设备 A 起 agent
mesh agent --hub http://HUB:8080 --token ka --agent-id laptop-a --caps echo

# 任意机器发起调用
mesh call --hub http://HUB:8080 --token ka --to laptop-a --payload '{"hello":"world"}'
```

## 文档

- 设计方案:`docs/design/mesh-design.md`
- 快速上手(局域网 / 公网):`docs/guides/`(实现后补)
- Wire 协议规范:`docs/guides/protocol.md`(实现后补)

## License

TBD
