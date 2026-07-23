# AgentMesh — 跨平台 · 跨设备 · 高性能多 Agent 通信 · 设计方案

> **状态:** 设计评审稿(v0.4,已吸收 3 维独立审查的全部 BLOCKING)。全新独立仓库,不继承任何旧代码。
> **作者:** Hermes(评审文档)。实现由自主 Agent 按 TDD 推进,Hermes 只做校验/审查/派发。
> **交付定义:** 第一版必须是「装上就能用的产品」——不同操作系统、不同设备上启动,agent 之间真正互相收发消息并拿到结果。契约测试绿 ≠ 完成;**进程跨设备互通**才算完成。

## 决策锁定(2026-07-23 用户确认)

| # | 决策 | 影响 |
|---|---|---|
| Q1 | **server 用 Go**,SDK 给 Python/TS | 单二进制跨平台,性能优先 |
| Q2 | **目标离线即回 `NO_ROUTE`,不排队、不持久化** | v1 无离线消息队列 |
| Q5 | **agent = LLM/编码工具**(Hermes、Codex CLI 等) | 「任务委派 + 流式结果回传」是核心;**流式提为核心能力(P3)**;新增 §4.10 LLM-Agent 语义约定层 |
| 文件 | **大文件走其他工具传输**,不由 mesh 承担 | **P2P/大文件传输降级**(默认关,非 v1 重点) |
| Q6 | **公网 + 内网 v1 都要** | 两份 Quickstart + 两套冒烟都进 v1 |

## v0.4 评审修订(3 维独立审查 BLOCKING 全部吸收)

| 维度 | BLOCKING | 修订落点 |
|---|---|---|
| 协议/性能(共识) | 帧格式与零拷贝自相矛盾 | §4.1 改**物理分离帧**(type+env_len+envelope+不透明 payload 尾部) |
| 协议 | `type` 单字节无法区分子类型 | §4.3 **每子类型独立 type 值** |
| 协议 | streamId/corr 矛盾、流终止/断连清理未定义 | §4.2 加 `stream` 字段;§4.10 **STREAM_END 唯一终态 + 断连合成 END** |
| 协议 | REQUEST 如何被 STREAM 应答未定义 | §4.6/§4.10 **请求↔流绑定状态机** |
| 协议 | corr 复用/迟到响应串话、deadline 时钟偏移 | §4.6 corr ttl 窗口唯一 + **相对 ttl 取代绝对 deadline** |
| 安全 | `src`/`tenant` 可伪造 | §4.2/§6 **Hub 强制以连接身份覆写** |
| 安全 | agentId 未绑 principal → 冒名/劫持 | §4.7/§6 **agentId 绑 principal,takeover 仅同 principal** |
| 安全 | 公网裸跑明文泄露 key | §6/§9 **无 TLS 非 loopback 拒启(MESH_INSECURE)** |
| LLM | 缺任务取消 → 远端失控烧钱 | §4.3 新增 `CANCEL` 帧;§4.10 断连/超时下发取消 |
| LLM | 背压丢帧毁 token 流 | §4.10 **流内禁静默丢帧,只整流中止** |
| LLM | 多跳委派防环 | §4.2 `hops` 字段 + §4.12 |
| 性能 | 队列按条数×万连=OOM | §5/§9 **改按字节限界** + goroutine 生命周期 + 库选型 + CI 用 allocs/op |

> 三份审查结论均为「**有条件进入实现**」,条件是上述 BLOCKING 在 P0 冻结 wire 协议前清零——本版已全部落实为协议条款。IMPORTANT/NICE-TO-HAVE 项存于审查原文(`/root/.hermes/cache/delegation/subagent-summary-{0,1,2}-*.txt`),在对应阶段前处理。

---

## 0. 一句话定位

一个**单静态二进制**的 Agent 通信中继(Hub)+ 多语言 SDK,让任意语言、任意设备、任意平台上的 Agent 通过统一的开放协议互相发现、点对点调用、广播与流式通信,热路径零拷贝、高吞吐低延迟。

---

## 1. 目标与非目标

### 1.1 目标(v1 必达)
1. **跨平台**:Linux / macOS / Windows / Android(Termux) / ARM,同一套二进制协议。
2. **跨设备**:公网(经反代/Tunnel 终止 TLS)与局域网(直连)同一二进制、同一协议,仅改 URL。
3. **多 Agent 互通**:Go/Python/TypeScript 官方 SDK,协议开放,任意语言可实现。
4. **高性能**:中继热路径零拷贝、二进制编码、单机万级并发长连、p99 低延迟;附实测压测基线。
5. **装上即用**:三条命令验收 + 冒烟脚本 + Quickstart 文档。

### 1.2 非目标(v1 明确不做,YAGNI)
- 不做 MCP/A2A 全协议桥接产品化(协议层预留,后续版本)。
- 不做 Web 控制台 UI。
- 不做多 Hub 集群/强制 Redis(单进程内存注册表起步;水平扩展见 §14)。
- 不做完整 OIDC 登录(API Key 起步)。
- 不做 workflow DAG 编排(agent 自己编排,mesh 只做通信底座)。

---

## 2. 技术选型

| 维度 | 选择 | 理由 |
|---|---|---|
| **语言/运行时** | **Go 1.23+** | 单静态二进制、交叉编译全平台、goroutine 高并发、GC 低延迟、零依赖部署。 |
| **agent 数据面** | **WebSocket over HTTP/HTTPS** | 穿代理/防火墙,TLS 走标准反代/Tunnel,所有语言有成熟库。 |
| **控制面** | **HTTP/JSON REST** | 列 agent、发起调用、健康检查;易调试、易接第三方。 |
| **编码** | **MessagePack**(默认)+ JSON(`?enc=json` 调试) | 紧凑、快、跨语言。 |
| **低延迟可选传输** | **QUIC**(v1.1 插件) | LAN/高吞吐绕 TCP 队头阻塞;v1 先 WS。 |
| **注册表** | 分片内存 map | O(1) 路由,无外部依赖;v2 可换 Redis/etcd。 |
| **鉴权** | 每 agent API Key + tenant | 最小可用安全,每连接强制校验。 |

**唯一重大待确认默认:** server 用 Go(SDK 给 Python/TS)。若必须纯 Python server,会牺牲性能与部署简洁——见 §15 Q1。

---

## 3. 架构总览

```text
                        公网 / 局域网
  ┌──────────────┐   WSS + HTTPS      ┌───────────────────────────┐
  │  mesh-agent  │ ────────────────►  │  反代 / Cloudflare Tunnel  │  (公网时 TLS 终止)
  │  设备 A       │                    └─────────────┬─────────────┘
  └──────────────┘                                  │
  ┌──────────────┐    WS 长连接        ┌─────────────▼─────────────┐
  │  mesh-agent  │ ─────────────────► │        meshd (Hub)         │
  │  设备 B       │                    │  :8080 HTTP(控制)+WS(数据)  │
  └──────────────┘                    │  Registry · Relay · PubSub │
  ┌──────────────┐   HTTP 控制/发起     │  Auth · RateLimit · Audit  │
  │  mesh CLI    │ ─────────────────► │  (分片注册表 · 零拷贝转发)   │
  └──────────────┘                    └───────────────────────────┘
        ┌───────────────────────────────────────────────┐
        │  可选:P2P 直连(Hub 签发短期 ticket 后旁路热路径) │
        └───────────────────────────────────────────────┘
```

**三个产物,同一 Go module:** `meshd`(Hub)、`mesh`(CLI+agent 运行时)、`meshclient`(Go SDK;另发 Python/TS)。

**设计原则:**
1. **中继优先**:agent 主动外连 Hub(NAT 友好),默认全流量经 Hub。
2. **零拷贝热路径**:Hub 只解析路由头(dst/tenant/corr),不反序列化用户 payload,原字节转发。
3. **P2P 可选加速**:大吞吐/低延迟场景 Hub 签票,双方直连,Hub 退出热路径。
4. **协议开放**:wire 格式文档化,任意语言可实现 agent。

---

## 4. Wire 协议(v1 冻结)

### 4.1 帧格式(v0.4 修订:路由头与 payload 物理分离,兑现零拷贝)

> **三份审查一致命中的最高优先级修正。** 原设计把 envelope 与 payload 塞进同一个 MessagePack body,与「零拷贝中继」自相矛盾:要读路由头就得解码整个 body,payload 至少被扫一遍;且 Hub 覆写 `src`/`tenant` 时必须重编码整个 body,零拷贝彻底失效。改为物理分离:

```
┌────────┬───────────────┬─────────────────────┬────────────────────────┐
│ type:1 │ env_len:varint│  envelope(msgpack)  │  payload(不透明尾部字节) │
└────────┴───────────────┴─────────────────────┴────────────────────────┘
```

- **type(1B)**:消息类型/子类型(见 §4.3,每个子类型独立值,无歧义)。
- **env_len(varint)**:envelope 字节数,Hub 据此定位 payload 起点。
- **envelope(msgpack)**:小而定长的路由头,**用整型键或定长 positional 数组**(非字符串键 map),解码快、帧小、跨语言顺序确定。
- **payload(尾部)**:不透明字节切片,Hub **全程不解码、不拷贝**,`[]byte` slice 直接转发。
- Hub 只解码 envelope(廉价),可安全覆写 `src`/`tenant`(见 §6 信任根)后仅重编码这一小段,payload 边界不受影响。
- WS **binary frame**;`type` 单字节快速分流。调试模式 JSON text frame。
- `env_len` 上限校验在**分配缓冲之前**执行,防内存放大;`env_len + payload` 总长受 `MESH_MAX_FRAME_BYTES` 约束。
- **流式续帧优化**:`STREAM_DATA` 的 envelope 只带 `{type, stream, seq}`,**不重复** src/dst/tenant/id(路由在 STREAM_OPEN 时已绑定),避免每 token 重复元数据造成写放大。

### 4.2 Envelope 字段
| 字段 | 类型 | 说明 |
|---|---|---|
| `v` | u8 | 协议版本 |
| `type` | u8 | 见 4.3 |
| `id` | str | 消息唯一 ID(ULID) |
| `corr` | str | 请求/响应关联 ID(见 §4.6 唯一性) |
| `stream` | str | 流标识(仅 STREAM_* 帧;一个 corr 下唯一,见 §4.10) |
| `src` | str | 源 agentId — **Hub 强制以连接鉴权身份覆写,客户端上报值一律忽略**(见 §6) |
| `dst` | str | 目标 agentId 或 `topic:<name>` |
| `tenant` | str | 租户 — **Hub 强制以连接绑定租户覆写,客户端上报值一律忽略**(见 §6) |
| `ttl` | i32 | **相对**超时(ms),接收侧落地为本地单调时钟绝对时间(取代绝对 deadline,规避跨设备时钟偏移) |
| `hops` | u8 | 剩余跳数(多跳委派防环,见 §4.12);到 0 回 `HOP_LIMIT` |
| `hdr` | map | 可选头(content-type、trace 等) |

> payload **不在 envelope 内**,是帧的独立尾部字节(§4.1),Hub 全程不解析。
> `src`/`tenant` 是**连接身份派生**,不是客户端可信输入——这是 §6 信任根的基石。

### 4.3 消息类型

> **v0.4 修订:每个子类型独立 type 值**(原 0x20/0x30 复合导致收到帧无法区分 OPEN/DATA/END,直接卡编解码)。testvectors 覆盖每个值。

| type | 名称 | 语义 |
|---|---|---|
| 0x01 | `HELLO` | 注册:token + agentId + capabilities + protocols |
| 0x02 | `WELCOME` | 注册成功,回 session + 心跳参数 + 已启用特性集 |
| 0x03 | `PING` | 心跳请求 |
| 0x04 | `PONG` | 心跳响应 + RTT |
| 0x10 | `SEND` | 点对点 fire-and-forget |
| 0x11 | `REQUEST` | 带 corr + ttl 的请求(响应可为单个 RESPONSE 或一段 STREAM 序列) |
| 0x12 | `RESPONSE` | 对 REQUEST 的**非流式**终态回应 |
| 0x13 | `CANCEL` | **发起方→目标**:取消 corr 对应的在途请求/流(见 §4.10 生命周期) |
| 0x1E | `ACK` | SEND 弱确认:已入队目标连接(按原帧 `id` 关联) |
| 0x1F | `NACK` | SEND 弱确认失败(入队失败;不再另发 ERROR) |
| 0x20 | `STREAM_OPEN` | 开流,绑定到某 corr;此后 DATA 只带 {stream,seq} |
| 0x21 | `STREAM_DATA` | 流数据块(seq 有序,禁止静默丢帧) |
| 0x22 | `STREAM_END` | **唯一终态**:status(ok/error/aborted)+ usage?(stream=true 时不再发 RESPONSE) |
| 0x30 | `SUBSCRIBE` | 订阅 topic(Hub 回 0x31 确认) |
| 0x31 | `SUBACK` | 订阅生效确认 |
| 0x32 | `UNSUB` | 取消订阅 |
| 0x33 | `PUBLISH` | 向 topic 广播(dst 必须是 `topic:*`) |
| 0x40 | `TICKET_REQ` | (P7)P2P 票据请求 |
| 0x41 | `TICKET` | (P7)P2P 票据签发 |
| 0x42 | `P2P_HELLO` | (P7)P2P 直连握手 |
| 0xFF | `ERROR` | 稳定错误码(见 §4.5) |

### 4.4 四个核心原语
`register` · `send` · `request/response` · `subscribe/publish`。流式(0x20)与 P2P(0x40)为增强,不属最小集。

### 4.5 错误码矩阵(冻结)
| code | 触发 | 客户端应对 |
|---|---|---|
| `AUTH_FAILED` | token 无效/过期 | 停止重连,报错退出 |
| `NO_ROUTE` | 目标 agent 不在线/不存在 | 上层决定重试或失败 |
| `TIMEOUT` | 超过 ttl 未响应 | 重试(幂等时)或失败 |
| `RATE_LIMITED` | 触发限流 | 指数退避重试 |
| `FRAME_TOO_BIG` | 超 `MESH_MAX_FRAME_BYTES` | 分片(STREAM)或拒绝 |
| `DUPLICATE_AGENT_ID` | 同 tenant 内 agentId 冲突 | 见 §4.7 抢占策略 |
| `QUEUE_FULL` | 目标发送队列满(背压) | 退避重试;流内触发则整流 `STREAM_END{aborted}` |
| `UNMAPPABLE` | 未来协议桥不可映射 | 报错,不静默降级 |
| `TENANT_DENIED` | 跨租户访问 | 报错退出 |
| `UNSUPPORTED_VERSION` | HELLO.v 不被支持 | 读 `supported[]` 降级或退出 |
| `AGENTID_FORBIDDEN` | agentId 不在 principal 允许命名空间(见 §6) | 改用授权的 agentId |
| `SESSION_TAKEOVER` | 本连接被同 principal 新连接顶替 | 旧连接退出;in-flight 视为失败 |
| `HOP_LIMIT` | 多跳委派超 `hops` 上限(防环) | 报错,不再转发 |
| `CANCELLED` | 请求/流被发起方 CANCEL 或其连接断开 | 目标停工回收资源 |
| `INSECURE_REFUSED` | 非 loopback 绑定且无 TLS 且未设 MESH_INSECURE | 配置 TLS 或显式放行 |

### 4.6 投递语义(明确,不含糊)
- `SEND`:**at-most-once**,fire-and-forget,不保证送达,不重试。
- `REQUEST`:**at-most-once 请求 + 显式响应**;响应形态二选一:单个 `RESPONSE`(0x12,非流式)**或** 一段 `STREAM_OPEN→DATA*→END`(0x20-22,流式,见 §4.10 绑定规则)。**幂等由业务负责**,Hub 不自动重放非幂等请求。
- **超时权威(取代绝对 deadline):** 用相对 `ttl`(ms),接收侧落地为本地单调时钟。以**首个到期方为准**统一回 `TIMEOUT`;`/v1/rpc` 的 `MESH_RPC_TIMEOUT_MS` 与请求 `ttl` **取小**。缺省 `ttl` 用 `MESH_RPC_TIMEOUT_MS` 兜底,`ttl=0` 表示不超时(仅限长任务 + 心跳保活,见 §4.10)。
- **corr 唯一性:** corr 在其 ttl 窗口内**全局唯一**;窗口结束后不得复用同一 corr,发起方须生成新 corr。对已完成 corr 的**迟到/重复 RESPONSE 一律丢弃**;Hub 对目标不明的 RESPONSE 丢弃并计数(不透传)。
- 目标离线:立即回 `NO_ROUTE`,**不排队等待**(v1 不做离线消息存储;见 §14 演进)。
- 可选 `ACK`(0x1E)/`NACK`(0x1F):`SEND` 可请求 Hub 侧「已入队目标连接」的弱确认(按原帧 `id` 关联),**不等于目标已处理**;入队失败回 NACK,不再另发 ERROR。
- **连接内 FIFO:** 同一连接内 Hub 保证投递顺序(依赖 TCP,写死为契约)。

### 4.7 身份与连接冲突
- 同 `tenant` 内 `agentId` **唯一**;agentId 必须落在该连接 principal 被授权的命名空间(见 §6),否则回 `AGENTID_FORBIDDEN`。
- 冲突默认策略:`MESH_AGENT_CONFLICT=reject`(拒绝新连接,回 `DUPLICATE_AGENT_ID`)或 `takeover`(新连接顶掉旧连接)。**takeover 仅允许同 principal**。默认 `reject`。
- **注册原子性:** 注册走注册表单点原子 compare-and-swap;takeover 时旧连接**先收 `SESSION_TAKEOVER` 终止帧、再从路由表摘除**(避免摘除窗口内消息丢失),旧连接完整拆除(见 §5 goroutine 生命周期)。
- **断线重连 = 全新会话(v1 语义,不含糊):** 重连不恢复任何状态——in-flight 请求/流一律视为失败(发起方按 ttl 收 `TIMEOUT`,或 Hub 为其未结束流合成 `STREAM_END{aborted}`),订阅需重建(§4.8)。`session` 令牌仅作**审计关联 ID**,不承诺状态恢复。

### 4.8 pub/sub 语义(明确)
- 订阅是**连接级软状态**:断线即失去订阅,重连需重新 `SUBSCRIBE`(v1 不做持久订阅)。
- `SUBSCRIBE` 有确认:Hub 回 `SUBACK`(0x31),**订阅自 SUBACK 起生效**(此前的 PUBLISH 不投递给该订阅者)。
- `PUBLISH` 投递:**best-effort fan-out**;Hub 在读锁内**快照当前在线订阅者切片**后释放锁,再锁外非阻塞入队(避免持锁遍历放大竞争)。满队列订阅者按背压丢弃**该 PUBLISH**(pub/sub 允许丢,与流内禁丢不同,见 §4.10)。
- **命名空间隔离:** `dst = "topic:<name>"`;**禁止 agentId 以 `topic:` 开头**(注册时校验)。向 topic 只能用 `PUBLISH`;`SEND`/`REQUEST` 的 dst 必须是 agentId。topic 受 tenant 隔离。
- **self-delivery:** 发布者自身若订阅同 topic,**默认不收自己的 PUBLISH**(可用 hdr 开关)。
- 无订阅者时 `PUBLISH` 静默成功(计数入 metrics)。

### 4.9 版本协商
- `HELLO.v` 报协议版本;Hub 不兼容时回 `ERROR{code:"UNSUPPORTED_VERSION", supported:[...]}`。
- 帧头 `v` 字段允许未来演进;**v1 冻结 v=1**。

### 4.10 LLM-Agent 语义约定层(SDK 层,Hub 仍不解析内容)

因为 agent 是 Hermes / Codex CLI 这类 LLM/编码工具,SDK 在通用 `payload` 之上提供一层**标准 helper**,让「委派任务 + 流式回结果」开箱即用。Hub 依旧只看路由头、不理解内容,语义完全由 SDK 双端约定实现。

**任务委派信封(payload 内的约定 JSON/MessagePack,`hdr["ct"]="application/x-mesh-task"`):**
| 字段 | 说明 |
|---|---|
| `task` | 任务指令文本(自然语言 prompt 或结构化) |
| `caps` | 期望目标能力(如 `code`, `research`) |
| `input` | 结构化输入(可选) |
| `stream` | bool,是否要求流式回结果 |
| `budget` | 可选:max_tokens / 超时 / 成本上限(由目标 agent 自愿遵守) |

**请求↔流绑定(核心场景状态机,v0.4 补全):**
- 发起方发 `REQUEST{corr=X, ttl}`(委派任务,payload 为任务信封)。目标决定响应形态:
  - **非流式**:回单个 `RESPONSE{corr=X}`。
  - **流式**:回 `STREAM_OPEN{corr=X, stream=S}` → `STREAM_DATA{stream=S, seq}`(token 增量)→ `STREAM_END{stream=S, status, usage?}`。
- **`STREAM_END` 是唯一终态;stream=true 时不再发 RESPONSE**(消除双终止信号歧义)。
- 一个 `corr` 下 `stream` 唯一;并发多流用不同 stream 隔离(N2 修订:流键为 `stream`,不再让 corr 语义过载)。
- 发起方 SDK 暴露异步迭代器(Python `async for`,Go channel,TS async iterable),把流接到发起请求上。

**流的可靠性(禁止静默损坏 token 流):**
- 同一 `stream` 内 `STREAM_DATA` **必须有序且不得静默丢帧**(`seq` 从 0 递增、不跳号,接收端可校验完整性)。
- 背压触发时**不允许在流中间留空洞**:只能**整流中止**,下发 `STREAM_END{status=aborted}` 或 `QUEUE_FULL`,由发起方决定重试。
- WS/TCP 无应用层「降速」——v1 明确只做「有序 or 整流中止」,不做逐帧丢弃(pub/sub 才允许丢)。
- 目标**中途断线**:Hub 为其未结束流合成 `STREAM_END{status=aborted}` 下发发起方(避免 async 迭代器悬挂)。

**任务取消 / 生命周期(LLM 场景关键,防远端失控烧钱):**
- 发起方主动停(用户 Ctrl-C)→ 发 `CANCEL{corr}`,Hub 路由给目标,目标 SDK 落成 context 取消、停止产 token。
- 发起方**连接断开** → Hub 主动为其所有在途 `corr` 向对应目标下发 `CANCEL`(不是仅本地报错)。
- `ttl` 到期 → Hub/发起方同样下发 `CANCEL` 给目标,而非只让发起方本地超时。
- **长任务**:用 `ttl=0` + 定期 `PING`/心跳保活或流式增量证明存活,避免 30s 硬超时误杀几分钟的编码任务。

**工具调用转发(约定,非 Hub 内建):**
- LLM agent 若需让远端 agent 执行工具,用 `REQUEST` 承载 `{tool, args}` 约定信封,远端执行后 `RESPONSE`/流回结果。属 payload 约定,Hub 不感知。

**全双工用法(Hermes 常态:既派活又接活):**
- 同一连接上 SDK 同时:注册入站任务 handler(收别人的 REQUEST)+ 发起出站调用(REQUEST 别人)。
- 入站/出站 `corr` 按方向分命名空间消歧(SDK 负责),避免自发自收串号。

**大文件不走 mesh:** 需要传文件时,payload 内放**引用**(URL / 对象存储 key / 现有传输工具句柄),由业务侧其他工具完成实际传输。mesh 只传消息与流式增量文本,不做文件通道。

### 4.11 HTTP 控制面
| Method | Path | 鉴权 | 语义 |
|---|---|---|---|
| GET | `/health` | 无 | 存活 |
| GET | `/ready` | 无 | 安全依赖就绪 |
| GET | `/v1/agents` | Bearer+tenant | 列在线 agent + 能力(分页) |
| POST | `/v1/rpc` | 同上 | `{to,payload,ttlMs}`→中继一次 request,同步返回**单个** response |
| POST | `/v1/rpc/stream` | 同上 | 同上但以 **SSE/chunked** 回流式结果(承载 STREAM_*) |
| POST | `/v1/publish` | 同上 | 向 topic 广播(dst 必须 `topic:*`) |
| GET | `/v1/agents?caps=code:go` | 同上 | 列在线 agent,支持按 caps 过滤 + 分页 |
| GET | `/metrics` | 可选 | Prometheus 指标 |

> `/v1/rpc` 让不想连 WS 的调用方(CLI、脚本、外部服务)也能发起调用;Hub 内部代其建一次性请求(临时条目及时清理,不与主热路径争锁)。
> **流式经 HTTP:** 同步 `/v1/rpc` 遇 `stream=true` 请求回错误提示改用 `/v1/rpc/stream`(SSE);WS 客户端天然支持流式,无此限制。

### 4.12 多跳委派与防环(A 委派 B,B 再委派 C)
- 真实网格必然出现多跳。envelope 带 `hops`(剩余跳数),每经一次 Hub 转发递减;到 0 回 `HOP_LIMIT`。默认上限 `MESH_MAX_HOPS=8`。
- `CANCEL` 必须能**跨跳传播**(A cancel → B,B 若已委派 C 则续发 cancel → C),避免链路末端失控。
- `ttl` 沿链路递减(减去已耗时),防止总时长失控。


---

## 5. 高性能设计(可量化)

| 技法 | 做法 | 收益 |
|---|---|---|
| 零拷贝中继 | 分离帧(§4.1):只 decode 小 envelope,payload 尾部 `[]byte` slice 直转,不拷不重编码 | 去掉最贵的反序列化 |
| envelope 定长整型键 | positional 数组 / 整型键 msgpack,热路径手写扫描器,零反射 | 解码更快、帧更小 |
| 分片注册表 | key 为可比较 `struct{tenant,agentId}`(零分配),分片数随 GOMAXPROCS(如 256),读多写少用分片 RWMutex / atomic 不可变 map | 高并发 O(1) 无全局锁,不用 sync.Map 装箱 |
| 每连接有界发送队列(**按字节限界**) | **每连接字节水位 + 进程级总字节预算**(非按条数);超限丢弃/断连 | 防 `1024条×1万连=10GB` OOM |
| 流式写合并 | STREAM_DATA 攒够 N 帧或 T 微秒批量 flush(类 Nagle,**限界窗口保 p99**) | 降 syscall/写放大 |
| 连接多路复用 | 单 WS 多逻辑流(`stream` 字段,§4.2/§4.10) | 省连接/握手 |
| goroutine 生命周期 | 每连接 read/write 双 goroutine 共享 done;任一侧失败取消另一侧 + 队列拆除 + 注册表清理;takeover 完整拆旧连 | 防万级连下 goroutine/内存泄漏 |
| 可选 P2P(P7) | Hub 签票后双方直连 | 热路径去 Hub |
| QUIC(v1.1) | 插件式传输 | 绕 TCP 队头阻塞 |

**库选型(v0.4 定,配合零拷贝):**
- WebSocket:`coder/websocket`(context 优先、`Reader` 流式读)或 `gorilla/websocket`(用 `NextReader` 自控缓冲,避免 `ReadMessage` 隐式拷贝)。v1 goroutine-per-conn 到万级可行;探到 10 万级再切 `gobwas/ws + netpoll` 事件循环。
- MessagePack 热路径:`tinylib/msgp`(代码生成、零反射、可跳字段);`vmihailenco/msgpack` 仅用于控制面/SDK 便利场景。

**压测基线(交付需实测,非估算),在**固定专用硬件**上(非 CI):**
- 中继吞吐:≥ 50k msg/s(1KB payload,单 Hub,4C8G)。
- 并发长连:≥ 10k 稳定连接。
- 延迟:relay p50 < 2ms,p99 < 20ms(同机回环);跨设备叠加网络 RTT。
- **压测方法学**:开环负载 + HdrHistogram(防协调遗漏 coordinated omission),负载生成端独立机器。
- **CI 回归门禁用确定性指标**:`go test -benchmem` 的 `allocs/op` 与 `ns/op`(benchstat 对比),**不在共享 CI 上硬卡绝对 p99**(必 flaky);绝对吞吐/延迟作固定硬件定期基准。
- 交付固化 `GOGC`/`GOMEMLIMIT`(防 OOM)/`GOMAXPROCS`。

---

## 6. 安全

> **信任根(v0.4 核心修订):连接鉴权身份是唯一可信来源。** 三份审查都指出原稿 `src`/`tenant`/`agentId` 可被客户端伪造,多租户 + takeover 下冒充与消息劫持敞开。以下条款闭合信任边界:

- **每连接强制鉴权**:`HELLO.token` 经 API Key 校验,绑定 principal + tenant;失败回 `AUTH_FAILED` 断连。
- **身份覆写(不可伪造)**:Hub 对**每一帧**用连接鉴权身份**强制覆写 `src` 和 `tenant`**,客户端上报值一律忽略;`src` 纳入必解析路由头。agent A 无法冒充 B 发消息,无法伪造 tenant。
- **agentId 绑定 principal**:可注册的 agentId 集合(或前缀命名空间)由 principal 授权决定;越权回 `AGENTID_FORBIDDEN`。**takeover 仅同 principal**——杜绝「持本租户任一 key 即可顶掉他人连接、劫持其消息」。
- **租户隔离**:`/v1/agents`、路由、pub/sub 全按连接绑定 tenant 隔离,跨租户回 `TENANT_DENIED`。
- **强制 TLS 护栏**:绑定**非 loopback** 且未设 `MESH_TLS_CERT` 且未显式 `MESH_INSECURE=true` 时**拒绝启动**(回 `INSECURE_REFUSED`),防公网裸跑明文泄露 API Key。公网经反代/Tunnel 终止;LAN 可进程内 TLS。Quickstart-public 以 TLS 为默认姿势。
- **Key 轮转即吊销**:`SIGHUP` reload 后,配置中**已删除的 key 对应的在途连接立即断开**(不只是拒绝新连);key 常量时间比较、哈希存储。
- **反洪泛**:鉴权前(还无 agentId)按 **IP/未授权连接**限制建连速率与失败率,防公网暴力试 token / DoS。
- **限流 + 帧上限**:`MESH_MAX_FRAME_BYTES`、每 agent 速率限制;`env_len` 校验在分配前。
- **P2P ticket(P7)**:单次性 nonce 防重放 + 签发密钥/TTL + scoped(限对端/能力/时限)+ 目标侧本地校验;**SSRF 黑名单枚举**:`169.254.169.254`、`10/8`、`172.16/12`、`192.168/16`、`127/8`、`::1`、`fc00::/7` 及云元数据端点。设计条款现在就定,不留 P7 裸实现。
- **审计**:注册/调用/错误结构化日志;**payload 不落盘**;`hdr` **白名单式**记录(防 SDK 塞敏感信息泄露);agentId 命名指引规避 PII。
- **caps 自报**:能力标签由 agent 自声明、未验证,由发起方自担风险(v1 信任模型)。

---

## 7. 跨平台 / 跨设备交付

**交叉编译矩阵(单命令产全平台):**
```
linux/amd64  linux/arm64  darwin/amd64  darwin/arm64  windows/amd64  android/arm64(Termux)
```
- 同一二进制:`meshd serve` / `mesh agent` / `mesh call`。
- 官方 SDK:**Go**(原生零依赖)、**Python**(`websockets`+`msgpack`,便于内嵌 Hermes/LLM agent)、**TypeScript**(`ws`+`@msgpack/msgpack`,Node)。
- 三 SDK 协议字节级一致 → 跨语言 agent 自由互通。
- **SDK 一致性保障**:共享一份 `protocol/testvectors.json`(编解码黄金样本),三语言 SDK 各跑一遍断言字节一致。

---

## 8. 可观测性

- `/metrics`(Prometheus):连接数、注册数、msg/s、字节/s、队列深度、丢弃数、p50/p99 延迟、错误码计数(按 tenant 低基数)。
- 结构化日志(JSON):level、event、agentId、tenant、corr、code;payload 不记录。
- 可选 trace 头透传(`hdr.traceparent`),Hub 不强制 APM。

---

## 9. 配置契约

**服务端 `meshd`:**
| 变量 | 默认 | 含义 |
|---|---|---|
| `MESH_HOST` | `0.0.0.0` | 监听地址 |
| `MESH_PORT` | `8080` | HTTP+WS 端口 |
| `MESH_PUBLIC_URL` | 空 | 对外广告 URL |
| `MESH_API_KEYS` | 生产必填 | `id:key:principal:tenant` 多行或 JSON |
| `MESH_MAX_FRAME_BYTES` | `1048576` | 单帧上限(env+payload) |
| `MESH_RPC_TIMEOUT_MS` | `30000` | `/v1/rpc` 兜底超时(与请求 `ttl` 取小) |
| `MESH_SEND_QUEUE_BYTES` | `4194304` | **每连接发送队列字节上限(按字节非条数,防 OOM)** |
| `MESH_TOTAL_QUEUE_BYTES` | `0`(=自动按内存) | 进程级发送缓冲总字节预算 |
| `MESH_MAX_HOPS` | `8` | 多跳委派跳数上限(防环) |
| `MESH_AGENT_CONFLICT` | `reject` | `reject`\|`takeover`(仅同 principal) |
| `MESH_TLS_CERT`/`KEY` | 空 | 可选进程内 TLS |
| `MESH_INSECURE` | `false` | 显式放行非 loopback 无 TLS 启动(否则拒启) |
| `MESH_ENABLE_P2P` | `false` | 放行 P2P ticket(P7) |
| `MESH_RATE_LIMIT` | `1000` | 每 agent msg/s 上限 |
| `MESH_CONN_RATE_LIMIT` | `50` | 每 IP 建连/鉴权失败速率(反洪泛) |

**客户端:** `MESH_HUB_URL` / `MESH_TOKEN` / `MESH_AGENT_ID` / `MESH_TENANT_ID`(默认 default)/ `MESH_CAPS`(逗号分隔能力标签)。

---

## 10. 验收标准(Definition of Done)

**三条命令必须真跑通:**
```bash
# 1) 起 Hub
MESH_API_KEYS='a:ka:alice:default
b:kb:bob:default' meshd serve

# 2) 进程 A 起 agent(能力 echo)
mesh agent --hub http://HUB:8080 --token ka --agent-id laptop-a --caps echo

# 3) 任意机器发起调用,拿到 A 回显
mesh call --hub http://HUB:8080 --token kb --to laptop-a --payload '{"hello":"from-b"}'
# → {"agentId":"laptop-a","echo":{"hello":"from-b"}}
```
- LAN 仅换 `HUB` 为 `192.168.x.x`,同二进制同协议。
- `scripts/smoke.sh`:本机拉 Hub + 2 agent + 1 call,断言结果。
- 跨平台二进制 + Quickstart(公网 + LAN)。
- 压测结果表。

---

## 11. 阶段与门禁(TDD:每任务 RED→GREEN→回归→Full gate→单 commit)

| 阶段 | 名称 | 出口标准 |
|---|---|---|
| **P0** | 协议 + 中继内核 | 帧编解码单测(+testvectors);WS register+send 跨进程真通 |
| **P1** | 鉴权 + 控制面 | API Key;`/v1/agents`;`/v1/rpc` 拿结果 |
| **P2** | 客户端三件套 | Go SDK + agent 运行时 + CLI 真连;冒烟绿 |
| **P3** | request/response + **流式** + pub/sub | corr/ttl;**STREAM_* 流式回结果(LLM 核心)**;任务委派信封;topic 广播;错误码矩阵 |
| **P4** | 性能硬化 | 零拷贝、背压、分片注册表;压测达标 + CI 阈值 |
| **P5** | 公网 + 内网双部署 | 公网(Tunnel/反代 TLS)+ LAN(直连/进程内 TLS)两条路径都跑通,两套冒烟 |
| **P6** | 跨平台 + 发布 | 全平台交叉编译;两份 Quickstart;Python/TS SDK;release 证据 |
| **P7**(可选/后置) | P2P 直连 | STREAM 大吞吐场景;P2P ticket 直连(flag,默认关) |

> **流式提为 P3 核心**(agent 是 LLM/编码工具,流式回结果是主用法);**P2P 降为可选后置 P7**(文件走其他工具,mesh 不承担大文件)。

**Full gate(每任务):** `go test ./... && go vet ./... && golangci-lint run && go build ./...`。资源紧张禁并行,OOM 降并发重试。
**热重载(P1):** `SIGHUP` reload `MESH_API_KEYS`,支持 key 轮转不断连。

---

## 12. 仓库布局

```text
agentmesh/                 # 全新独立 Go module
  cmd/
    meshd/                 # 服务端入口
    mesh/                  # CLI(agent / call / agents 子命令)
  internal/
    protocol/              # 帧编解码、envelope、消息类型、testvectors
    hub/                   # 注册表、中继、pub/sub、背压
    auth/                  # API Key、tenant
    transport/             # ws(v1)、quic(v1.1)
  pkg/
    meshclient/            # Go SDK(第三方可 import)
  sdk/
    python/  typescript/   # 多语言 SDK
  bench/                   # 负载工具 + 结果
  scripts/smoke.sh
  deploy/                  # compose + Tunnel/nginx 示例
  docs/
    design/mesh-design.md
    guides/quickstart-lan.md  quickstart-public.md  protocol.md
```

---

## 13. 风险与对策

| 风险 | 对策 |
|---|---|
| Go 与 Python/Hermes 栈割裂 | Python SDK 一等公民,Hermes 侧内嵌 agent 走 Python SDK |
| 公网 TLS/穿透复杂 | 默认 Cloudflare Tunnel/反代;文档两条现成路径 |
| 慢消费者拖垮 Hub | 有界队列 + 背压 + 慢连断开 |
| P2P 引入 SSRF | 默认关闭;ticket scoped + 私网禁用 + 目标侧校验 |
| 性能吹牛 | 交付必附实测压测表 + CI 阈值回归 |
| SDK 三语言漂移 | 共享 testvectors 黄金样本,三语言各自断言 |

---

## 14. 演进路线(v1 之后,非当前范围)

- **v1.1:** QUIC 传输;流式增强;P2P 正式化。
- **v2:** 水平扩展(多 Hub + Redis/etcd 共享注册表 + 一致性哈希路由);离线消息队列(可选持久化);持久订阅。
- **v3:** MCP/A2A 协议桥接产品化;能力语义发现;Web 控制台。

---

## 15. 决策状态

| # | 问题 | 结论 |
|---|---|---|
| Q1 | server 语言 | ✅ **Go**(SDK 给 Python/TS) |
| Q2 | 离线投递 | ✅ **立即 `NO_ROUTE`,不排队、不持久化** |
| Q3 | 通信形态 | ✅ RPC + **流式(核心)** + pub/sub 都做 |
| Q4 | P2P 直连 | ✅ **降为可选后置 P7,默认关**(文件走其他工具) |
| Q5 | agent 语义 | ✅ **LLM/编码工具**;SDK 提供任务委派+流式 helper,Hub 不解析内容(§4.10) |
| Q6 | 部署场景 | ✅ **公网 + 内网 v1 都要**(P5 两条路径 + 两套冒烟) |
| Q7 | 规模 | 按万级设计,功能优先,P4 压满(未特别指定,走默认) |
| Q8 | GitHub | 待确认仓库名/可见性(默认 `online111111/agentmesh` 公开) |

> 决策已锁定。下一步:派独立子 Agent 多维审查本方案 → 据审查意见定稿 → 建 GitHub 仓库推初始 commit → 出 P0–P7 实现计划 → 交自主 Agent 按 TDD 推进。Hermes 全程只做双审 + 门禁 + 派发 + 归档,不代写业务代码。
