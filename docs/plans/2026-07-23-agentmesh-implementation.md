# AgentMesh 实现计划(P0–P7,严格 TDD)

> **For Pi (claude-opus-4-8, thinking=max):** 按本计划任务顺序实施。每个任务严格 RED→GREEN→REFACTOR→Full gate→单 commit。不要跳测试。测试先失败、看到预期失败原因,再写最小实现。
> **For Hermes:** 用 subagent-driven-development 监督;每任务后做规格符合性 + 代码质量双审,门禁校验,不代写业务代码。
> **权威设计文档:** `docs/design/mesh-design.md`(v0.4,已冻结 wire 协议)。任务与设计冲突时以设计文档 §4 协议条款为准。

**Goal:** 交付「装上就能用」的跨平台多 Agent 通信产品:Go 中继 Hub(`meshd`)+ CLI/agent 运行时(`mesh`)+ Go/Python/TS SDK,让不同设备上的 LLM/编码 Agent 互相委派任务、流式回结果。

**Architecture:** 单 Go module。控制面 HTTP/JSON + 数据面 WebSocket(分离帧:type + env_len + envelope(msgpack 整型键) + 不透明 payload 尾部)。中继优先、热路径零拷贝、连接身份为唯一可信来源。

**Tech Stack:** Go 1.23+、`coder/websocket`(或 gorilla + NextReader)、`tinylib/msgp`(热路径)/`vmihailenco/msgpack`(控制面)、`stretchr/testify`、Python SDK(`websockets`+`msgpack`)、TS SDK(`ws`+`@msgpack/msgpack`)。

**验收硬门槛(DoD):** 任一阶段结束若不能用三条命令完成「起 Hub → 起 2 个 agent → CLI/HTTP 互调拿到结果」,该阶段未完成。契约测试绿 ≠ 完成;进程跨设备互通才算完成。

**Full gate(每任务结束):**
```bash
unset ALL_PROXY HTTP_PROXY HTTPS_PROXY all_proxy http_proxy https_proxy
cd /root/agentmesh
go build ./... && go test ./... && go vet ./... && gofmt -l . | (! grep .)
```
(golangci-lint 在装好后加入;资源紧张禁并行,OOM(137) 降 `-p 1 -parallel 1` 重试一次。)

**环境:** 每个联网命令前 `unset ALL_PROXY HTTP_PROXY HTTPS_PROXY`(WARP/代理会破坏 loopback 与 go proxy)。`.pi/` 已在 `.gitignore`。

---

## 阶段与出口标准

| 阶段 | 名称 | 出口标准 |
|------|------|----------|
| **P0** | 协议内核(冻结 wire) | 分离帧编解码 + testvectors 全绿;每消息子类型独立 type;跨语言字节样本产出 |
| **P1** | Hub 内核 + 鉴权 + 控制面 | `meshd serve` 真 listen;API Key + 身份覆写;`/health`/`/ready`/`/v1/agents`/`/v1/rpc` |
| **P2** | Go SDK + agent 运行时 + CLI | `mesh agent`/`mesh call` 真连;`scripts/smoke.sh` 跨进程互调绿 |
| **P3** | request/response + 流式 + pub/sub | corr/ttl;STREAM_* 流式回结果 + CANCEL;topic 广播;错误码矩阵 |
| **P4** | 性能硬化 | 零拷贝转发、按字节背压、分片注册表、goroutine 生命周期;bench + allocs/op 门禁 |
| **P5** | 公网 + 内网双部署 | LAN 直连 + 公网(Tunnel/反代 TLS)两条路径跑通;强制 TLS 护栏;两套冒烟 |
| **P6** | 跨平台 + Python/TS SDK + 发布 | 全平台交叉编译;testvectors 三语言一致;两份 Quickstart;release 证据 |
| **P7**(后置) | P2P 直连 | ticket 签发 + SSRF 黑名单 + 防重放(默认关) |

---

# P0 — 协议内核(先冻结 wire,再写任何上层)

> P0 是最高风险区:wire 一旦被 SDK/testvectors 固化,改动代价极高。三审共识:P0 前必须把分离帧、子类型独立 type、流生命周期状态机全部落地并测。

### Task 0.1:初始化 Go module 与目录骨架

**Objective:** 可编译的空 module + 目录结构。

**Files:**
- Create: `go.mod`(module `github.com/online111111/agentmesh`,go 1.23)
- Create: `internal/protocol/doc.go`(package 注释占位 `package protocol`)
- Create: `cmd/meshd/main.go`、`cmd/mesh/main.go`(各 `func main(){}` 占位)

**Step 1:** `go mod init github.com/online111111/agentmesh`
**Step 2:** 写占位文件。
**Step 3 (verify):** `go build ./...` 应成功。
**Step 4 (commit):** `chore: bootstrap Go module and directory skeleton`

---

### Task 0.2:定义消息类型常量(每子类型独立 type)

**Objective:** §4.3 的 type 值落成 Go 常量,消除子类型歧义。

**Files:**
- Create: `internal/protocol/types.go`
- Test: `internal/protocol/types_test.go`

**Step 1 (RED):** 写测试断言常量值:`HELLO=0x01, WELCOME=0x02, PING=0x03, PONG=0x04, SEND=0x10, REQUEST=0x11, RESPONSE=0x12, CANCEL=0x13, ACK=0x1E, NACK=0x1F, STREAM_OPEN=0x20, STREAM_DATA=0x21, STREAM_END=0x22, SUBSCRIBE=0x30, SUBACK=0x31, UNSUB=0x32, PUBLISH=0x33, TICKET_REQ=0x40, TICKET=0x41, P2P_HELLO=0x42, ERROR=0xFF`。断言 `TypeName(t)` 返回可读名。
**Step 2:** `go test ./internal/protocol/ -run TestTypes -v` → FAIL(未定义)。
**Step 3 (GREEN):** 定义 `type MsgType uint8` + 常量 + `TypeName`。
**Step 4:** 测试通过。
**Step 5 (commit):** `feat(protocol): define message type constants with distinct subtype values`

---

### Task 0.3:定义 Envelope 结构与错误码

**Objective:** §4.2 envelope 字段 + §4.5 错误码常量。

**Files:**
- Create: `internal/protocol/envelope.go`(struct `Envelope{V uint8; Type MsgType; ID, Corr, Stream, Src, Dst, Tenant string; TTL int32; Hops uint8; Hdr map[string]string}`)
- Create: `internal/protocol/errors.go`(错误码字符串常量:`AUTH_FAILED, NO_ROUTE, TIMEOUT, RATE_LIMITED, FRAME_TOO_BIG, DUPLICATE_AGENT_ID, QUEUE_FULL, UNMAPPABLE, TENANT_DENIED, UNSUPPORTED_VERSION, AGENTID_FORBIDDEN, SESSION_TAKEOVER, HOP_LIMIT, CANCELLED, INSECURE_REFUSED`)
- Test: `internal/protocol/envelope_test.go`

**Step 1 (RED):** 测试构造 Envelope、断言零值、断言错误码常量字符串。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(protocol): add envelope struct and stable error codes`

---

### Task 0.4:分离帧编码(EncodeFrame)

**Objective:** §4.1 分离帧:`type(1) + env_len(varint) + envelope(msgpack) + payload`。envelope 用整型键 msgpack。

**Files:**
- Create: `internal/protocol/frame.go`(`func EncodeFrame(env Envelope, payload []byte) ([]byte, error)`)
- Test: `internal/protocol/frame_test.go`
- Modify: `go.mod`(加 `github.com/vmihailenco/msgpack/v5`;先用它跑通,P4 再评估 msgp 代码生成)

**Step 1 (RED):** 测试 `EncodeFrame` 产出:第 1 字节 = env.Type;随后 varint = envelope 字节长;末尾 payload 原样附加。用固定输入断言前缀字节与 payload 尾部完全相等(证明 payload 未被改写)。
**Step 2:** RED fail。
**Step 3 (GREEN):** msgpack 序列化 envelope(整型键:用 `msgpack:"1"` 等 tag 或 array 编码),varint 写长度,拼 payload。
**Step 4:** pass。
**Step 5 (commit):** `feat(protocol): implement split-frame encoder (routing header + opaque payload)`

---

### Task 0.5:分离帧解码(DecodeFrame)零拷贝 payload

**Objective:** 解出 envelope,payload 作为**指向原缓冲的 slice**(不拷贝),证明零拷贝可行。

**Files:**
- Modify: `internal/protocol/frame.go`(`func DecodeFrame(buf []byte) (Envelope, []byte, error)`)
- Test: `internal/protocol/frame_test.go`

**Step 1 (RED):** round-trip 测试:Encode 后 Decode 得回等价 envelope;断言返回的 payload slice 与原 buf **共享底层数组**(`&payload[0] == &buf[offset]`,用 unsafe 或 cap 检查);`env_len` 超 `maxFrameBytes` 在分配前返回 `FRAME_TOO_BIG`。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(protocol): implement zero-copy split-frame decoder`

---

### Task 0.6:非法帧与边界测试

**Objective:** 防御性解码:截断帧、env_len 溢出、payload 缺失、未知 type。

**Files:**
- Test: `internal/protocol/frame_test.go`(表驱动)

**Step 1 (RED):** 表驱动:空 buf、只有 type、varint 截断、env_len 大于剩余、msgpack 损坏 → 各自返回明确 error 不 panic。
**Step 2–4:** RED→GREEN(补 DecodeFrame 边界检查)→pass。
**Step 5 (commit):** `test(protocol): cover malformed frame boundaries`

---

### Task 0.7:JSON 调试编码(?enc=json)

**Objective:** §4.1 调试模式:同 envelope+payload 用 JSON text frame 表达,便于抓包。

**Files:**
- Create: `internal/protocol/json.go`(`EncodeJSON`/`DecodeJSON`,payload base64)
- Test: `internal/protocol/json_test.go`

**Step 1–4:** RED→GREEN→pass(round-trip)。
**Step 5 (commit):** `feat(protocol): add JSON debug codec`

---

### Task 0.8:产出跨语言黄金样本 testvectors.json

**Objective:** §7 SDK 一致性根:固定输入的 canonical 帧字节(hex)+ JSON 表达,供三语言 SDK 断言。

**Files:**
- Create: `internal/protocol/testvectors.json`(覆盖每个 type、含 stream 帧、含空 payload、含 hdr、含多字节 UTF-8)
- Create: `internal/protocol/testvectors_test.go`(读 json,对每条 vector 断言 Encode 输出 hex 完全一致 + Decode 往返一致)
- Create: `internal/protocol/testvectors.go`(`GenerateVectors()` 可重生成)

**Step 1 (RED):** 测试加载 testvectors.json,逐条 Encode → hex 比对。首次先写死几条期望 hex(手算/固定),失败驱动实现稳定编码顺序。
**Step 2–4:** RED→GREEN→pass。**关键:** envelope 字段编码顺序必须确定(整型键升序或固定 array 顺序),否则跨语言不一致。
**Step 5 (commit):** `feat(protocol): freeze wire format with cross-language golden test vectors`

> **P0 出口:** `go test ./internal/protocol/...` 全绿;testvectors.json 冻结;wire 协议锁定。此后改帧格式需显式版本升级。

---

# P1 — Hub 内核 + 鉴权 + 控制面

### Task 1.1:API Key 解析与鉴权器

**Objective:** §6 每连接强制鉴权;Key 格式 `id:key:principal:tenant`;principal→agentId 命名空间授权。

**Files:**
- Create: `internal/auth/apikey.go`(`ParseKeys(spec string) ([]Key, error)`;`type Key{ID, Secret, Principal, Tenant string; AgentIDPrefix string}`;`Authenticate(token string) (*Identity, error)` 常量时间比较)
- Test: `internal/auth/apikey_test.go`

**Step 1 (RED):** 测试解析多行/JSON;有效 token 返回 Identity{Principal,Tenant};无效返回 `AUTH_FAILED`;`subtle.ConstantTimeCompare` 使用(断言不同长度也不 panic)。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(auth): API key parsing and constant-time authentication`

---

### Task 1.2:agentId 命名空间授权

**Objective:** §4.7/§6 agentId 必须落在 principal 授权命名空间,否则 `AGENTID_FORBIDDEN`。

**Files:**
- Modify: `internal/auth/apikey.go`(`(*Identity) AllowsAgentID(id string) bool`)
- Test: `internal/auth/apikey_test.go`

**Step 1 (RED):** principal 授权前缀 `alice-*` → `alice-laptop` 允许、`bob-x` 拒。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(auth): bind agentId namespace to principal`

---

### Task 1.3:分片注册表(register/lookup/remove)

**Objective:** §5 分片注册表,key=`struct{tenant,agentId}`,原子 CAS 注册。

**Files:**
- Create: `internal/hub/registry.go`(`Register(tenant,agentID string, conn *Conn) error`(冲突返回 `DUPLICATE_AGENT_ID`)、`Lookup`、`Remove`、`ListByTenant`)
- Test: `internal/hub/registry_test.go`

**Step 1 (RED):** 注册后可查;重复注册返回 DUPLICATE_AGENT_ID;不同 tenant 同 agentId 互不干扰;并发 100 goroutine 注册无 race(`go test -race`)。
**Step 2–4:** RED→GREEN→pass。用分片 map + RWMutex。
**Step 5 (commit):** `feat(hub): sharded agent registry with atomic registration`

---

### Task 1.4:连接抽象 + 有界发送队列(按字节)

**Objective:** §5 每连接有界发送队列(字节限界防 OOM)+ 背压。

**Files:**
- Create: `internal/hub/conn.go`(`type Conn`,`Enqueue(frame []byte) error`(超 `MESH_SEND_QUEUE_BYTES` 返回 `QUEUE_FULL`),write goroutine 消费队列)
- Test: `internal/hub/conn_test.go`

**Step 1 (RED):** 入队字节累计到上限后 `Enqueue` 返回 QUEUE_FULL;消费后可再入;关闭时 goroutine 退出(用 done channel + `go test -race`)。用内存 fake writer。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(hub): per-connection byte-bounded send queue with backpressure`

---

### Task 1.5:WebSocket 握手 + HELLO/WELCOME 鉴权

**Objective:** §4.3 数据面:WS 升级 → 首帧必须 HELLO → 鉴权 → 覆写身份 → 注册 → 回 WELCOME。

**Files:**
- Create: `internal/hub/gateway.go`(WS handler:升级、读首帧、`auth.Authenticate`、`AllowsAgentID`、注册、回 WELCOME;失败回 ERROR 并关连接)
- Modify: `go.mod`(加 `github.com/coder/websocket`)
- Test: `internal/hub/gateway_test.go`(用 `httptest.Server` + 真 WS 客户端)

**Step 1 (RED):** 起测试服务器;客户端连 + 发 HELLO(有效 token)→ 收 WELCOME;无效 token → 收 ERROR{AUTH_FAILED} 且连接关闭;首帧非 HELLO → ERROR。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(hub): websocket gateway with HELLO/WELCOME authentication`

---

### Task 1.6:身份覆写(src/tenant 不可伪造)

**Objective:** §6 信任根:Hub 对每帧用连接身份覆写 src/tenant。

**Files:**
- Modify: `internal/hub/gateway.go`(转发前 `env.Src = conn.AgentID; env.Tenant = conn.Tenant`)
- Test: `internal/hub/gateway_test.go`

**Step 1 (RED):** 客户端发 SEND 时把 src 填成别的 agentId;Hub 转发到目标的帧里 src 必须是**连接真实 agentId**,tenant 同理。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(hub): overwrite src/tenant from connection identity (anti-spoofing)`

---

### Task 1.7:SEND 中继(点对点,零拷贝转发)

**Objective:** §4.6 SEND at-most-once;目标在线转发、离线回 `NO_ROUTE`。

**Files:**
- Modify: `internal/hub/gateway.go`(读 SEND → lookup dst → Enqueue 原帧;不在线回 ERROR{NO_ROUTE})
- Test: `internal/hub/gateway_test.go`(两个客户端 A、B:A SEND 给 B,B 收到;A SEND 给不存在者收 NO_ROUTE)

**Step 1 (RED):** 双客户端跨「连接」真收发。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(hub): relay SEND point-to-point with NO_ROUTE on offline target`

---

### Task 1.8:HTTP 控制面 /health /ready /v1/agents

**Objective:** §4.11 控制面基础端点。

**Files:**
- Create: `internal/hub/http.go`(`/health`、`/ready`、`/v1/agents`(Bearer+tenant,支持 `?caps=` 过滤 + 分页))
- Test: `internal/hub/http_test.go`

**Step 1 (RED):** `/health`=200 ok;`/v1/agents` 无 token=401;有效 token 列出本 tenant 在线 agent;跨 tenant 不可见。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(hub): HTTP control plane — health, ready, agents listing`

---

### Task 1.9:POST /v1/rpc(HTTP 发起一次中继调用)

**Objective:** §4.11 让不连 WS 的调用方发起 request,同步拿 response;ttl 与 `MESH_RPC_TIMEOUT_MS` 取小。

**Files:**
- Modify: `internal/hub/http.go`(`/v1/rpc`:body `{to,payload,ttlMs}` → 内部建一次性 REQUEST 到目标 WS 连接 → 等 RESPONSE → 返回;超时回 TIMEOUT;目标离线 NO_ROUTE)
- Test: `internal/hub/http_test.go`(起一个 WS agent 回声,HTTP POST /v1/rpc 拿到回显)

**Step 1 (RED):** WS agent 注册并对 REQUEST 回 RESPONSE;HTTP `/v1/rpc` 调用拿到 payload;目标离线回 NO_ROUTE;超时回 TIMEOUT。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(hub): POST /v1/rpc synchronous relay endpoint`

---

### Task 1.10:meshd serve 启动 + 配置 + 强制 TLS 护栏

**Objective:** §9 真正 listen;§6 非 loopback 无 TLS 且无 MESH_INSECURE → 拒启(`INSECURE_REFUSED`)。

**Files:**
- Create: `internal/hub/server.go`(读环境变量、组合 HTTP+WS、`http.Server.ListenAndServe(TLS)`;启动期安全检查)
- Modify: `cmd/meshd/main.go`(解析 `serve` 子命令,调 server)
- Test: `internal/hub/server_test.go`(非 loopback bind 无 TLS 无 INSECURE → 启动返回错误;设 MESH_INSECURE=true → 允许;loopback → 允许)

**Step 1 (RED):** 安全护栏三分支测试 + `/health` 端到端可达(用随机端口)。
**Step 2–4:** RED→GREEN→pass。
**Step 5 (commit):** `feat(meshd): serve command with config and mandatory-TLS guard`

> **P1 出口:** `meshd serve` 真 listen;WS 鉴权 + SEND 中继 + `/v1/rpc` 端到端;身份不可伪造。

---

# P2 — Go SDK + agent 运行时 + CLI(第一次「装上能用」)

### Task 2.1:Go SDK 连接 + HELLO

**Objective:** `pkg/meshclient` 可连 Hub、注册。

**Files:**
- Create: `pkg/meshclient/client.go`(`Dial(ctx, hubURL, token, agentID, caps) (*Client, error)`)
- Test: `pkg/meshclient/client_test.go`(对真 Hub `httptest` 连接、收 WELCOME)

**Step 1–5:** RED→GREEN→pass→commit `feat(sdk-go): client dial and registration`

---

### Task 2.2:Go SDK Send / OnMessage

**Objective:** SDK 发 SEND、注册入站 handler。

**Files:**
- Modify: `pkg/meshclient/client.go`(`Send(dst string, payload []byte)`、`OnMessage(func(Envelope, []byte))`)
- Test: `pkg/meshclient/client_test.go`(两个 client 经 Hub 互发)

**Step 1–5:** RED→GREEN→pass→commit `feat(sdk-go): Send and inbound message handler`

---

### Task 2.3:mesh agent 子命令(边侧运行时)

**Objective:** `mesh agent --hub --token --agent-id --caps`,注册并对入站 REQUEST 用内置 echo handler 回 RESPONSE。

**Files:**
- Create: `internal/agentrt/agent.go`(运行时循环,echo capability)
- Modify: `cmd/mesh/main.go`(子命令分发:`agent`)
- Test: `internal/agentrt/agent_test.go`

**Step 1–5:** RED→GREEN→pass→commit `feat(mesh): agent runtime subcommand with echo capability`

---

### Task 2.4:mesh call 子命令(CLI 发起调用)

**Objective:** `mesh call --hub --token --to --payload` 经 `/v1/rpc` 拿结果打印。

**Files:**
- Modify: `cmd/mesh/main.go`(`call` 子命令,HTTP POST /v1/rpc)
- Create: `internal/cli/call.go`
- Test: `internal/cli/call_test.go`

**Step 1–5:** RED→GREEN→pass→commit `feat(mesh): call subcommand via /v1/rpc`

---

### Task 2.5:冒烟脚本(跨进程真互通)

**Objective:** DoD 三条命令脚本化,断言回显 JSON。

**Files:**
- Create: `scripts/smoke.sh`(起 meshd(loopback)→ 起 `mesh agent` 后台 → `mesh call` → grep 回显 → 清理)
- Test: 脚本自身即验收;`scripts/smoke.sh` 退出码 0

**Step 1 (RED):** 先写脚本断言,跑失败(功能未串起来)。
**Step 2–4:** 调通 → 脚本绿。
**Step 5 (commit):** `test(e2e): cross-process smoke test — hub + agent + call`

> **P2 出口:** `scripts/smoke.sh` 绿 = 第一次真正「装上能用」。

---

# P3 — request/response + 流式 + pub/sub + CANCEL

### Task 3.1:REQUEST/RESPONSE with corr + ttl

**Files:** `internal/hub/gateway.go`、`pkg/meshclient/client.go`(`Request(ctx,dst,payload) (resp,err)`,corr 生成 ULID,ttl 到期 TIMEOUT),tests。
**Commit:** `feat: request/response with correlation id and relative ttl`

### Task 3.2:corr 唯一性 + 迟到/重复 RESPONSE 丢弃

**Files:** gateway.go(已完成 corr 丢弃逻辑)、tests(迟到 RESPONSE 被丢、目标不明 RESPONSE 计数丢弃)。
**Commit:** `feat: drop late/duplicate/unroutable responses`

### Task 3.3:STREAM_OPEN/DATA/END + SDK 异步迭代器

**Files:** `internal/hub/gateway.go`(流帧路由,STREAM_DATA 只带 stream+seq)、`pkg/meshclient`(`RequestStream` 返回 channel)、tests(REQUEST(stream)→OPEN→DATA*→END;seq 有序)。
**Commit:** `feat: streaming responses (STREAM_OPEN/DATA/END) with async iterator`

### Task 3.4:流终止语义 + 断连合成 END

**Files:** gateway.go(目标断线 → 为未结束流向发起方合成 STREAM_END{aborted};STREAM_END 唯一终态)、tests。
**Commit:** `feat: synthesize STREAM_END on target disconnect`

### Task 3.5:流内禁静默丢帧(背压整流中止)

**Files:** conn.go/gateway.go(流内 Enqueue 满 → 整流 STREAM_END{aborted}/QUEUE_FULL,不留空洞)、tests。
**Commit:** `feat: abort whole stream on backpressure, never drop mid-stream`

### Task 3.6:CANCEL 帧 + 连接断开自动取消

**Files:** gateway.go(CANCEL 路由给目标;发起方连接断开 → 为其所有在途 corr 下发 CANCEL;ttl 到期下发 CANCEL)、SDK(context 取消回调)、tests。
**Commit:** `feat: CANCEL primitive with disconnect/timeout-triggered cancellation`

### Task 3.7:多跳 hops 防环

**Files:** gateway.go(每转发递减 hops,0 回 HOP_LIMIT;CANCEL 跨跳传播;ttl 沿链递减)、tests。
**Commit:** `feat: multi-hop delegation with hop-limit loop guard`

### Task 3.8:SUBSCRIBE/SUBACK/UNSUB/PUBLISH

**Files:** `internal/hub/pubsub.go`(topic 订阅表,tenant 隔离,SUBACK 后生效,快照 fanout,self-delivery 默认关,agentId 禁 `topic:` 前缀)、`/v1/publish`、tests。
**Commit:** `feat: pub/sub with SUBACK, tenant isolation, snapshot fan-out`

### Task 3.9:错误码矩阵端到端

**Files:** tests(逐个错误码触发路径:NO_ROUTE/TIMEOUT/RATE_LIMITED/FRAME_TOO_BIG/DUPLICATE_AGENT_ID/QUEUE_FULL/TENANT_DENIED/UNSUPPORTED_VERSION/AGENTID_FORBIDDEN/SESSION_TAKEOVER/HOP_LIMIT/CANCELLED)。
**Commit:** `test: end-to-end error code matrix`

### Task 3.10:LLM 任务委派信封 helper(SDK 层)

**Files:** `pkg/meshclient/task.go`(`DelegateTask{Task,Caps,Input,Stream,Budget}` 编解码,`hdr["ct"]="application/x-mesh-task"`)、tests。
**Commit:** `feat(sdk-go): task delegation envelope helper`

> **P3 出口:** LLM 核心场景可用:委派任务 + 流式回 token + 可取消 + 广播。

---

# P4 — 性能硬化

### Task 4.1:连接冲突 takeover(仅同 principal,完整拆旧连)
**Commit:** `feat(hub): same-principal takeover with full old-conn teardown`

### Task 4.2:goroutine 生命周期(read/write 双 goroutine 共享 done,泄漏测试)
**Commit:** `feat(hub): connection goroutine lifecycle, no leaks under churn`

### Task 4.3:限流(每 agent msg/s)+ 反洪泛(每 IP 建连/失败率)
**Commit:** `feat(hub): per-agent rate limit and per-IP anti-flood`

### Task 4.4:流式写合并(coalescing,限界窗口保 p99)
**Commit:** `feat(hub): stream write coalescing with bounded window`

### Task 4.5:bench 负载工具 + allocs/op 门禁
**Files:** `bench/relay_bench_test.go`(`go test -bench -benchmem`,断言热路径 allocs/op ≤ 阈值)、`bench/loadgen/`(开环负载 + HdrHistogram)。
**Commit:** `perf(bench): relay load tool with allocs/op regression gate`

### Task 4.6:压测跑一轮 + 结果表
**Files:** `bench/RESULTS.md`(4C8G 实测:msg/s、并发连、p50/p99;GOGC/GOMEMLIMIT/GOMAXPROCS 记录)。
**Commit:** `docs(bench): baseline performance results on 4C8G`

> **P4 出口:** 零拷贝、按字节背压、无泄漏;实测基线达标或标注差距。

---

# P5 — 公网 + 内网双部署

### Task 5.1:进程内 TLS(LAN 直连)
**Commit:** `feat(meshd): in-process TLS for LAN direct connections`

### Task 5.2:Docker + compose(业务 Hub)
**Files:** `Dockerfile`(多阶段,静态二进制)、`deploy/compose.yaml`、tests(compose config 校验)。
**Commit:** `feat(deploy): Dockerfile and compose for hub`

### Task 5.3:公网 Quickstart(Cloudflare Tunnel/nginx 反代 TLS)
**Files:** `docs/guides/quickstart-public.md`、`deploy/tunnel-example.yml`。
**Commit:** `docs: public deployment quickstart (Tunnel/nginx TLS)`

### Task 5.4:LAN Quickstart + 两套冒烟
**Files:** `docs/guides/quickstart-lan.md`、`scripts/smoke-public.sh`(可选,需环境)。
**Commit:** `docs: LAN quickstart and dual smoke scripts`

> **P5 出口:** LAN 与公网两条路径都文档化 + 可跑;强制 TLS 护栏生效。

---

# P6 — 跨平台 + Python/TS SDK + 发布

### Task 6.1:交叉编译脚本(全平台矩阵)
**Files:** `scripts/build-all.sh`(linux/amd64,arm64;darwin amd64,arm64;windows amd64;android/arm64)、tests(至少构建成功)。
**Commit:** `build: cross-compilation for all target platforms`

### Task 6.2:Python SDK(连接 + Send + Request + 流式 async 迭代器)
**Files:** `sdk/python/agentmesh/`(`websockets`+`msgpack`)、`sdk/python/tests/`(pytest,连真 meshd)。
**Commit:** `feat(sdk-python): client with send/request/stream`

### Task 6.3:Python SDK testvectors 一致性
**Files:** `sdk/python/tests/test_vectors.py`(读 `internal/protocol/testvectors.json`,断言编解码字节一致)。
**Commit:** `test(sdk-python): assert wire byte-parity via golden vectors`

### Task 6.4:TypeScript SDK(连接 + Send + Request + 流式 async iterable)
**Files:** `sdk/typescript/src/`(`ws`+`@msgpack/msgpack`)、`sdk/typescript/test/`(vitest)。
**Commit:** `feat(sdk-ts): client with send/request/stream`

### Task 6.5:TS SDK testvectors 一致性
**Commit:** `test(sdk-ts): assert wire byte-parity via golden vectors`

### Task 6.6:protocol.md 规范文档
**Files:** `docs/guides/protocol.md`(帧格式、type、envelope、状态机,权威 wire 规范)。
**Commit:** `docs: authoritative wire protocol specification`

### Task 6.7:README 产品化 + release 证据 + golangci-lint 接入 CI
**Files:** `README.md`(装即用入口)、`.github/workflows/ci.yml`(build+test+vet+lint+vectors)、`.golangci.yml`。
**Commit:** `ci: release gate workflow and product README`

> **P6 出口:** 全平台二进制 + 三语言 SDK 字节一致 + CI 门禁 + 两份 Quickstart。

---

# P7 —(后置)P2P 直连

### Task 7.1:TICKET_REQ/TICKET 签发(单次 nonce + TTL + scoped)
### Task 7.2:P2P_HELLO 直连握手 + 目标侧本地校验
### Task 7.3:SSRF 黑名单(169.254/169.254、私网段、元数据端点)+ 防重放
### Task 7.4:feature flag MESH_ENABLE_P2P 默认关 + 文档

> P7 仅在用户明确需要大吞吐直连时启动;默认全程中继。

---

## 执行说明

- Pi 每完成一个任务 commit 一次;`.pi/` 不入库。
- 进度追加到 `/tmp/pi-agentmesh-progress.md`:`Task X.Y: DONE - <sha>`。
- 卡住 3 次跳过并记 `/tmp/agentmesh-skipped.md`,继续下一个。
- 需要外部基础设施(Docker/公网)不可用时用内存 fake/跳过并在 commit 注明。
- Full gate 每任务跑;golangci-lint 在 Task 6.7 前用 `go vet + gofmt` 兜底。
- Hermes 监督:关键任务(P0 全部、1.5–1.7、1.10、3.3–3.6、4.1–4.2)做规格+质量双审再放行。
