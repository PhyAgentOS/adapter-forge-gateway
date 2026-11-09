# Gateway API 文档

本项目同时提供本地 HTTP/WebSocket 后端并作为 Dora node 运行。HTTP 侧负责 runtime 控制、录制回放控制和 Tool discovery/invoke；Dora 侧负责接收状态输入、Tool provider/caller 输入并输出 `PolicyCommand` 与 Tool 消息。

## Web 数据采集面板

`GET /` 返回 Gateway 内置的数据采集面板，静态资源位于 `/static/*`。页面直接使用下述同源 HTTP/WebSocket 接口，可查看实时相机、readiness、关节与录制状态，并执行 start / stop / discard 和场景复位；无需单独部署前端。

Gateway API 当前不包含鉴权。默认 `host: 127.0.0.1` 仅允许本机访问；监听 `0.0.0.0` 时应通过可信局域网、防火墙或认证反向代理限制控制权限。

## Tool API

Gateway 是 caller-visible 的 Tool discovery/routing authority。现有 Query 同步行为保持不变；配置绑定的 Action 使用独立 invocation resource。

### `GET /tools`

配置了 `tools.specs` 时列出 ToolSpec 及其当前 descriptor binding 可用性；未配置时保持兼容行为，列出当前 lease 有效的 endpoint descriptor。响应不暴露 pinned instance、Dora route 或 monotonic lease 时间。

```json
{
  "ok": true,
  "data": {
    "revision": 3,
    "tools": [
      {
        "endpoint_id": "vision.yolo",
        "descriptor": {
          "protocol_version": "forge.tool.endpoint/v1alpha1",
          "endpoint_id": "vision.yolo",
          "operations": [
            {
              "name": "detect",
              "semantics": "query",
              "cancellable": false,
              "stoppable": false,
              "status_supported": false,
              "max_concurrency": 1
            }
          ]
        }
      }
    ]
  }
}
```

### `POST /tools/{endpoint_id}/{operation}:invoke`

通过与 Dora caller 相同的 `ToolGatewayService` 调用一个 Query。HTTP handler 只向有界 mailbox 提交请求并异步等待 correlated completion，不直接调用 Dora node。Query deadline 从 Gateway lifecycle admission 的 monotonic `processed_at` 开始；provider response 必须在 deadline 前由 lifecycle 处理完毕。HTTP 与 Dora pending invocation 的总数受 service `outbound_capacity` 限制，provider request 已被 lifecycle claim 后仍计数；达到上限的新调用返回 `FORGE_TOOL_GATEWAY_BUSY`，且不创建 pending state。

```json
{
  "arguments": {
    "image_id": "front"
  },
  "caller_id": "collector-ui",
  "timeout_ms": 5000
}
```

`arguments` 缺省为 `{}`；`caller_id` 和 `timeout_ms` 可省略。`tools.invoke_timeout_ms` 是配置上限而不只是默认值：省略 `timeout_ms` 时使用该值，显式值必须是 `1..tools.invoke_timeout_ms`，超出上限返回 `400`。成功返回 terminal Forge invoke payload。错误状态包括：

- `400`：body、字段类型或 timeout 非法；
- `404`：endpoint 未配置，或 active descriptor 不包含该 Query operation；
- `409`：相同 correlation identity 仍有 pending invocation；
- `422`：provider 明确拒绝 Query；
- `503`：Gateway disabled/closing/busy、endpoint 当前无 active instance、provider transport 失败或 outbound mailbox 已满；
- `504`：pending invocation 超过 deadline。

HTTP 输出不包含 provider-pinned `endpoint_instance_id`。HTTP handler 使用独立 monotonic hard deadline wait；即使 Dora lifecycle sweep 停止，也会通过 service cancellation path 结束 pending Query。provider response 在 deadline 前被 reader 收到但仍排队时不会延长 HTTP deadline。provider response、transport output failure、caller cancellation、Gateway close 等 Query terminal completion 在同一 service lock 下按 monotonic observation 统一仲裁。Gateway 不执行 retry、dedup 或 Session 调度。

如果 endpoint operation 是 Action，则必须存在 semantics 一致的 ToolSpec binding，接口立即返回 `202` invocation resource，不等待执行完成。

### ToolSpec 与 Action invocation

- `GET /tools/{tool_id}`：返回配置 ToolSpec。
- `GET /tools/{tool_id}/context`：返回 ToolSpec binding、Gateway readiness、当前 `endpoint.status` 与 `RobotFrameProfile`。
- `POST /tools/{tool_id}:invoke`：创建 Action invocation，Gateway 生成 invocation/attempt identity 并 pin 当前 endpoint instance，返回 `202`。
- `GET /invocations/{invocation_id}`：返回 phase；非 terminal 时触发一次有界 provider status refresh。
- `GET /invocations/{invocation_id}/result`：若 store 已保留 result（包括 `unknown`），HTTP 返回 `200` 且 `data.status=available`，不再触发 provider lookup；这是 HTTP 读取语义。若尚无 retained result，返回 `202` 并尝试一次有界 provider result refresh；refresh 抛 `RuntimeError`（包括 outbound 满）会被抑制，HTTP 仍返回 `202`，不会改写为 503。
- Dora/Tool 协议 caller 路径是另一条路径。仅 `tool.result.request` 在 result 已保留且 phase 为非 unknown terminal 时直接返回 instance-less `tool.result.response`；`tool.status.request` 不使用该缓存直出，仍会发起或挂接 provider status lookup。`unknown` 仍不直出，result request 会发起或挂接 provider result lookup，只有迟到的 terminal provider result 才能把它调回 terminal。对 result/status 两类在途 exchange，同一 purpose 的自动 refresh 已在途且尚无 waiter 时，第一个 caller 会先预留一个响应槽位再挂接该 exchange；已有 waiter 时返回可重试的 `FORGE_TOOL_GATEWAY_BUSY`。若 outbound 邮箱绝对无容量，该 caller 路径按有界邮箱语义抛出 `ToolGatewayMailboxFull`，不越界写队列，也不伪造 BUSY。
- `POST /invocations/{invocation_id}/cancel`：发送 cancel control。`202 requested/accepted` 仅表示 control 被接纳，不表示 invocation 已 cancelled。
- `GET /invocations/{invocation_id}/events`：SSE，支持 `Last-Event-ID`；游标早于保留窗口返回 `410`，每个 invocation 只保留配置上限内的最新事件。

Action invocation store 受 `tools.invocation_capacity`、`event_capacity` 和 `retention_sec` 限制，其中 invocation capacity 是 active 与 terminal retention 的总量硬上限。内部以 `(invocation_id, attempt_id)` 关联；HTTP 自生成 invocation ID 保持单 ID resource。terminal result 在 phase/event 之前建立，provider invoke accepted 使用独立 barrier，早到事件在 accepted 前不对 caller 暴露。lease expiry、provider instance replacement/restart 或 transport 失败导致已 dispatch Action 结果不可恢复时记录 terminal `unknown`，不盲目 retry。deadline 到达不会向 provider 发送 stop/cancel；只要原 provider 仍可能执行，`unknown` 继续占用 operation concurrency，直到 instance 被隔离。cancel accepted 也不会自行改写 terminal phase。Session/PAOS/Skill Runtime 不在此链路中。

## Runtime API

### `GET /runtime/status`

返回 readiness 和轻量状态快照。

### `POST /runtime/start`

当 readiness 满足时发送启动命令，默认 command 为 `start`。

请求示例：

```json
{
  "command": "start",
  "inputs": {}
}
```

### `POST /runtime/reset_scene`

发送 `reset_scene` runtime 命令，用于仿真或场景复位。

### `POST /runtime/stop`

请求停止本地 gateway 或关联 launcher。

## Record/Playback API

### `POST /record/control`

控制录制。

```json
{
  "action": "START",
  "output_path": "optional.mcap",
  "metadata": {
    "workflow": {"episode_id": "optional-episode-id"},
    "robot": {"robot_type": "optional-robot-type"}
  }
}
```

`action` 支持 `START`、`STOP`、`DISCARD`。`metadata` 仅用于 `START`，必须是 object；`DISCARD` 会结束当前 session 并删除输出文件。

### `POST /record/set_root`

设置录制根目录。

```json
{
  "root": "/tmp/records"
}
```

### `GET /record/status`

返回最新 `record_status`。

### `POST /playback/control`

控制回放。

```json
{
  "action": "START",
  "mcap_path": "optional.mcap"
}
```

`action` 支持 `START`、`PAUSE`、`RESUME`、`RESET`。

### `GET /playback/status`

返回最新 `playback_status`。

## Policy Command API

### `POST /policy/command`

直接发送 `PolicyCommand`，适合调试或非 Agent 调用方。

```json
{
  "command": "start",
  "inputs": {}
}
```

## WebSocket

### `/ws/state`

按 `state_broadcast_hz` 推送轻量状态快照，包括关节状态、动作、runtime 状态和 readiness。

### `/ws/images`

按 `image_broadcast_hz` 推送图像增量。压缩图像会直接 base64 转发，原始 `Image` 会编码为 JPEG。

## Dora 端口

输入：

- `tick`
- `proprio_state`
- `action`
- `runtime_status`
- `record_status`
- `playback_status`
- 配置中的 `image_input_ids`
- `tools.request_input_id`：public caller 的 logical `tool.invoke.request`
- `tools.providers[*].input_id`：provider→Gateway 共享输入，接受 management、invoke/status/result/control response、`tool.event`、`tool.error`

输出：

- `policy_command`
- `tools.response_output_id`：返回 public Dora caller 的 correlated Tool response/error
- `tools.providers[*].output_id`：Gateway→provider 共享输出，发送 registry response 或 pinned invoke/status/result/control request

每条 provider config 仅包含 `endpoint_id`、`input_id`、`output_id`。Directory 不使用 source ID、source generation、tombstone 或 heartbeat。`endpoint.register` 是幂等 announce/renew；同 route 新 instance 原子替换 current。`endpoint_instance_id` 是 Gateway 私有路由状态：public caller 必须省略且 discovery/response 不暴露，Gateway resolve active registration 后只在 provider-facing invoke 上 pin instance。

Dora reader 为所有 provider/caller Tool 输入捕获 monotonic `received_at`，并把它们放入同一个有界有序 FIFO；不会像图像/状态输入一样按 input ID 合并。`endpoint.register`/`endpoint.unregister` 的 Directory lease effect 继续使用这个 reader observation。Query admission、配置 timeout 起点和 provider response deadline decision 使用 Gateway lifecycle 的 monotonic `processed_at`：response 必须在 deadline 前完成 service processing，deadline 前到达 reader 但仍排队不会获胜或延长 HTTP API deadline。public Dora `ToolContext.deadline_ms` 只能缩短 `tools.invoke_timeout_ms` 配置上限，不能延长它。

provider-facing `tool.invoke.request` 的 dispatch linearization point 是 lifecycle 在 `ToolGatewayService` lock 下从 outbound mailbox claim 有效消息并标记 `dispatch_claimed`。timeout/cancel/close 在 claim 前完成会使消息失效，drain 会跳过且不调用 Dora `send_output`；claim 后 send 仍在 lock 外执行，所以 provider 可能收到并完成 Query，但 invocation 在 terminal completion 前始终占用 pending capacity。所有 pending terminal path 共享 deadline arbiter：在 deadline 或之后线性化的 provider response、output failure、caller cancellation、close 等都完成为 timeout；deadline 前线性化的原结果保持不变。pending 已结束后的合法 provider response/error 静默丢弃，现有 pending 的 wrong-route response 仍拒绝。此语义只允许用于 Query：Gateway 不承诺 provider cancellation，也不会把 claim 后发送扩展到 Action 或 Session。所有 Dora `send_output` 只在 lifecycle thread 执行；每次 drain 有固定上限，shutdown 在拒绝新输入后执行一次 bounded final caller-response drain。Dora pending admission 会在有界 mailbox 预留 terminal response slot；management mutation 前预留 ACK slot。HTTP 与 Dora pending 总数受 service `outbound_capacity` 限制，HTTP thread 通过同一 service 提交请求，独立 hard deadline 与 lifecycle sweep 共享原子 cancellation/completion path。
