# Forge Gateway

[English](README.md) | [简体中文](README_zh.md)

## BREAKING CHANGE：Forge 2.x / Dora 1.x 迁移

本次从 Forge 1.x / Dora 0.4.x 升级需要协同部署。节点对接前必须对齐 Forge 包、Dora CLI/coordinator/daemon、本节点及上下游节点的版本；不支持在 Forge 1.x 部署中只替换单个节点。可执行产物也需要使用匹配环境重新构建和部署。仍使用旧技术栈的部署请保留在 `stable/1.x`。

本轮验证的 Python 基线为 `forge-msgs==2.1.0`、`Pillow==13.0.0`、`forge-common==2.1.0`、按需使用的 `forge-robot` / `forge-policy` / `forge-kinematics` / `forge-tool` 为 `2.0.0`，以及 `dora-rs==1.0.1`。同时遵守本仓库的 Python 和 SDK 平台要求。Forge 各包族独立发布，无需强行使用同一个版本号。Rust 节点继续使用已发布的 `forge_msgs` crate `2.0.0` 和 Dora `1.0.1`；Python Msgs `2.1.0` 不要求升级到尚未发布的 Rust `2.1.0`。消息命名空间 `forge_msgs.v1` 保持不变。

这里的 breaking change 指旧技术栈的部署迁移，不代表真机、真实模型或长时间运行已完成验收。实际测试和未完成项见[本轮验证报告](migration/msgs-2.1.0-report.json)。

`gateway` 是统一的运行时入口，提供本地 HTTP 控制 API、runtime readiness/start gate，以及状态/图像 WebSocket。

本项目是 Forge Gateway 的独立开源仓库，由 Forge Runtime 中的 Gateway
节点演进而来。完整接口说明见本项目的 [Gateway API 文档](docs/api.md)，
安全部署边界见 [SECURITY.md](SECURITY.md)，发布检查记录见
[OPEN_SOURCE_AUDIT.md](OPEN_SOURCE_AUDIT.md)。

> [!WARNING]
> Gateway 的控制接口不内置身份认证或 TLS。默认仅监听 `127.0.0.1`；
> 如需远程访问，必须放在可信网络和经过认证的加密反向代理之后。连接真机前，
> 还必须独立验证急停、工作区隔离和设备安全限制。

## Node、Endpoint 与 Tool API

| 项目 | 当前实现 |
| --- | --- |
| 生产 Node | 1：`gateway`；源码入口 `main.py`，打包入口 `dist/gateway`；无独立 console script |
| Tool 支持 | Gateway caller-visible discovery、ToolSpec、provider lease/路由及 Query/Action/Session 调用 |
| 自有业务 Endpoint 数 | 0；Gateway 不实现 motion/gripper/benchmark 等业务 provider |
| 路由 Endpoint / operation 数 | 由 `tools.providers` 配置、provider descriptor 和当前 lease 决定，没有仓库固定值 |
| Gateway Tool 数 | 由 `tools.specs` 定义；可用性还取决于 provider 注册与 readiness，不能按 Node 或 Endpoint 数推算 |
| 直接 HTTP Tool API | 10 个 Tool/Invocation 路由模板，见下表；不含其他 runtime/record/playback/WebSocket 接口 |

| 方法 | 路由模板 | 用途 |
| --- | --- | --- |
| GET | `/tools` | Tool 目录/发现 |
| GET | `/tools/{tool_id}` | ToolSpec |
| GET | `/tools/{tool_id}/context` | readiness 和 frame context |
| POST | `/tools/{tool_id}:invoke` | Action 或 Session 创建调用 |
| POST | `/tools/{endpoint_id}/{operation}:invoke` | Query 调用 |
| GET | `/invocations/{invocation_id}` | 状态 |
| GET | `/invocations/{invocation_id}/result` | 结果 |
| POST | `/invocations/{invocation_id}/cancel` | Action 取消 |
| POST | `/invocations/{invocation_id}/stop` | Session 停止 |
| GET | `/invocations/{invocation_id}/events` | SSE 事件 |

例如 robot-control-sim 的路由链为：
`sim.set_gripper_opening -> gripper.controller / set_opening -> gripper_action_policy`。
ToolSpec 选择业务操作，provider route 决定 Dora 通道；Node ID、Endpoint ID 与 Tool ID 是不同身份。

当前源码支持 Query、Action、Session；Session start/stop/status/result 与 Action cancel 分别处理。
这不证明某个历史 Release 或 Skill 锁定的二进制具备同等能力。核对部署版本后再使用对应接口。
实现：[HTTP routes](src/forge_gateway/controllers/tool_controller.py)、
[Tool service](src/forge_gateway/services/tool_gateway_service.py)、
[Session lifecycle tests](tests/test_tool_session.py)。

兼容性：当前依赖声明为 `dora-rs>=1.0.1,<2`、`forge-msgs>=2.0.0,<3`、`forge-common>=2.1.0,<3`；以 [pyproject.toml](pyproject.toml) 与 [uv.lock](uv.lock) 为准。 Tool 协议实现来自正式 `forge-tool[dora]` 分发包。 这些声明不构成跨版本互操作保证；使用制品时还需核对 build revision 和 Skill lock。

后续修改 Node 入口、Endpoint 或 Tool API 时，请同步更新本节及对应配置/契约，遵循 [Node README 规范](https://github.com/PhyAgentOS/forge/blob/master/CONTRIBUTING.md#node-readme-contract)。

## 独立开发

项目固定使用 Python `3.12`，依赖与开发/构建工具均由 `uv` 管理。

同步全部依赖组：

```bash
uv sync --frozen --all-groups
```

运行测试：

```bash
uv run pytest -q
```

从源码启动并检查帮助信息：

```bash
uv run python main.py --config config.example.yaml
uv run python main.py --help
uv run python main.py --version
```

这是一个直接从 checkout 运行的节点项目，不构建或发布 wheel/sdist，也不安装 console script。`pyproject.toml` 和 `uv.lock` 仅用于管理 Python 与依赖；根目录的 `main.py` 是节点入口，`config.py` 保留历史源码导入兼容。依赖通过公开索引锁定；尚未独立发布的 Apache-2.0 `forge-tool` 协议实现随源码仓库提供，以保证公开 checkout 可复现运行。普通 `pip install .` 不属于受支持的部署路径。

构建独立可执行文件：

```bash
./scripts/build_pyinstaller.sh
```

构建脚本使用临时 `.venv_build` 环境，将产物写入 `dist/gateway`，并在完成后执行 `dist/gateway --help` 与 `dist/gateway --version` 验证入口和版本。

## 应用生命周期

`GatewayApplication` 统一管理 Runtime、Dora reader、launcher watcher 和 Uvicorn：

- Dora Node/runner 在 HTTP 端口开放前完成初始化。
- Uvicorn 必须完成端口绑定 handshake；端口占用或后台线程提前退出会触发启动回滚。
- `SIGINT`、`SIGTERM`、launcher 消失和 `/runtime/stop` 都进入同一 shutdown 路径。
- shutdown 首先关闭 command/image admission，再停止 HTTP/Dora ingress，最后执行 Runtime close barrier。
- cleanup 会尝试所有已获取的组件；超时或异常通过结构化结果返回，未完成的 close 可以重试。

线程 join 使用统一的 application budget；Runtime 自身仍使用独立的 dispatch/image cleanup budget。如果底层 Dora iterator 永久阻塞，Gateway 会报告 shutdown incomplete 并以非零状态退出，而不会把它误报为正常关闭。

## 配置

```yaml
host: 127.0.0.1
port: 9001
joint_order: [joint1, joint2, joint3, joint4, joint5, joint6, gripper]
image_input_ids:
  - image/wrist
  - image/top
state_broadcast_hz: 50
image_broadcast_hz: 24
ws_send_timeout_sec: 1.0
jpeg_quality: 85
policy_id: default
command_queue_capacity: 256
readiness:
  require_proprio_state: true
  require_images: true
  require_state_client: false
  require_image_client: false
  proprio_stale_after_sec: 2.0  # 设为 null 可显式使用旧的 presence-only 行为
  image_stale_after_sec: 2.0

tools:
  enabled: false
  lease_ttl_ms: 15000
  invoke_timeout_ms: 5000  # 所有 caller timeout 的最大值
  request_input_id: tool_request
  response_output_id: tool_response
  providers: []
  # providers:
  #   - endpoint_id: vision.yolo
  #     input_id: yolo/to_gateway
  #     output_id: gateway/to_yolo
```

Gateway 对配置执行严格校验：未知或重复的 YAML key、字符串形式的布尔值、非有限数值、空/重复 input ID 都会直接报错。旧的 `broadcast_hz` alias 仍可单独使用，但不能与 `state_broadcast_hz` 同时配置。

当 `readiness.require_images: true` 时，`image_input_ids` 不能为空，否则 readiness 会报告缺少 `image_input_ids`。本体状态必须携带 position、velocity 或 effort 数值，并至少匹配一个 `joint_order` 中的 joint；partial joint state 仍受支持。

`tools.enabled: true` 时，Gateway 可以作为 Tool-only Dora 节点启动，不要求 `joint_order`。Gateway 是唯一 caller-visible discovery/routing authority：provider 共享输入接受 management、invoke/status/result/control response、`tool.event` 和 `tool.error`；共享输出发送 registry response 及 pinned Tool request。

Directory 对每个 `endpoint_id` 维护一个带 monotonic lease 的 current instance，并允许 Query/Action/Session descriptor。Query 同步行为保持不变。配置 `tools.specs` 后，Action 使用独立有界 invocation store：Gateway 生成 invocation/attempt、pin provider instance，并路由 invoke/status/result/control/event。terminal result 与事件按配置保留；lease expiry/provider replacement 的不确定结果记为 `unknown`。deadline 不等于 stop，cancel accepted 也不等于 cancelled。Session 使用 start/stop/status/result，运行期不受 Action deadline 限制，但握手有界且 provider lease 仍有效。Gateway 不实现自动 retry、通用 dedup、PAOS 或 Skill Runtime。

启动：

```bash
uv run python main.py --config gateway.yaml
```

## 数据采集面板

启动 dataflow 后打开 <http://127.0.0.1:9001>（端口以配置为准）。Gateway 内置的零构建依赖 Web 面板提供：

- 多路实时相机与 readiness / 连接状态；
- 关节实测值、动作指令和 recorder 统计；
- 带唯一输出路径与采集元数据的开始录制；
- 停止并保留、丢弃当前 session、场景复位。

页面与 API 同源，不需要单独启动前端服务。默认仅监听 `127.0.0.1`；如果改为 `0.0.0.0`，同一网络中的客户端也能调用控制 API，部署时应自行增加可信网络隔离或认证代理。

## HTTP API

- `POST /record/control`，body: `{"action":"START|STOP|DISCARD","output_path":"optional.mcap","metadata":"optional object"}`
- `POST /record/set_root`，body: `{"root":"..."}`
- `GET /record/status`
- `POST /playback/control`，body: `{"action":"START|PAUSE|RESUME|RESET","mcap_path":"optional.mcap"}`
- `GET /playback/status`
- `POST /runtime/reset_scene`：发送 `reset_scene` runtime 命令，用于仿真场景复位；不同于 playback 的 `RESET`
- `POST /runtime/stop`
- `GET /tools`：列出当前 active Tool descriptor；不暴露 provider instance、route 或 monotonic lease 时间。
- `GET /tools/{tool_id}`、`GET /tools/{tool_id}/context`：读取 ToolSpec 与 readiness/frame context。
- `POST /tools/{tool_id}:invoke`：创建 Action 或 Session invocation，返回 `202`。
- `POST /tools/{endpoint_id}/{operation}:invoke`：调用 Query Tool，body: `{"arguments":{},"caller_id":"optional","timeout_ms":5000}`；`timeout_ms` 可省略，否则必须在 `1..tools.invoke_timeout_ms`；使用与 Dora caller 相同的 Tool Gateway service。
- `GET /invocations/{id}`、`GET /invocations/{id}/result`、`POST /invocations/{id}/cancel`、`POST /invocations/{id}/stop`、`GET /invocations/{id}/events`：调用状态、结果、Action 取消、Session 停止和 SSE。

- `POST /policy/command`：直接发送 `PolicyCommand`，body: `{"command":"...","inputs":{}}`
- `GET /runtime/status`：返回状态快照与 readiness
- `POST /runtime/start`：仅在 readiness 满足时发送默认 `start` 命令，可用 body 覆盖 `command` 和 `inputs`

## WebSocket

- `/ws/state`：按 `state_broadcast_hz` 推送轻量 JSON 状态，包括 `proprio_state`、`action`、运行状态与 readiness。
- `/ws/images`：按 `image_broadcast_hz` 推送每路图像的增量消息。压缩图像会直接 base64 转发；原始 `Image` 会编码为 JPEG。
- `ws_send_timeout_sec`：单次 WebSocket 发送超时时间。连接写入停滞超过该时间会被 gateway 主动断开，避免阻塞后续广播。

图像消息包含：

```json
{
  "type": "image",
  "id": "image/wrist",
  "seq": 1,
  "timestamp": 1710000000.0,
  "format": "jpeg",
  "content_type": "image/jpeg",
  "data": "...base64..."
}
```

## Dora 输入输出

输入：

- `tick`
- `proprio_state`
- `action`
- `runtime_status`
- `record_status`
- `playback_status`
- 配置中的 `image_input_ids`
- `tools.request_input_id`：public caller 的 logical `tool.invoke.request`
- `tools.providers[*].input_id`：provider 的 register/unregister/invoke.response/tool.error 共享输入

输出：

- `policy_command`
- `tools.response_output_id`：public caller 的 correlated invoke.response/tool.error
- `tools.providers[*].output_id`：发给 provider 的 registry.response/invoke.request 共享输出

所有 provider/caller Tool 输入使用同一个有界有序 FIFO，不参与图像/状态的 latest-value coalescing；FIFO 满时 Gateway 显式失败，不静默丢失 registration、invoke response 或 error。Tool outbound mailbox 同样有界：management 在 Directory mutation 前保留 ACK capacity，Dora invocation 在 admission 时保留 terminal response capacity；只剩一个 slot 时返回 immediate busy error，没有 slot 时不改变状态。HTTP 与 Dora pending invocation 总数也由同一个 service `outbound_capacity` 限制；达到上限时新 invocation 返回 `FORGE_TOOL_GATEWAY_BUSY` 且不创建 pending state，已被 lifecycle claim 的 provider request 仍计入该上限。register/unregister 的 Directory effect 保留 reader `received_at` 语义；Query deadline 从 lifecycle `processed_at` 开始，并要求 provider response 在 deadline 前完成 lifecycle processing，deadline 前进入 reader FIFO 但尚未处理的 response 不会延长 deadline。

provider invoke 的 dispatch linearization point 是 lifecycle 在 `ToolGatewayService` lock 下从 mailbox claim request，并同步标记 `dispatch_claimed`。timeout/cancel/close 在 claim 前发生时，该 request 会失效且不会调用 Dora `send_output`；claim 后 Dora send 仍在 lock 外执行，因此可能继续发给 provider。所有异步 terminal completion 在 service lock 下统一仲裁：其 monotonic observation 达到或超过 pending deadline 时必须返回 `FORGE_TOOL_INVOKE_TIMEOUT`（HTTP `504`），而不是更晚发生的 provider、transport、caller-cancelled 或 closing 结果；deadline 前已完成线性化的结果保持不变。pending 已结束后的合法 provider response/error 视为 late/duplicate 并静默丢弃。该有限的 claim-after-effect 只适用于无 Gateway retry/cancel 协议的 Query，不适用于 Action 或 Session。每次 lifecycle drain 有固定上限，shutdown 会在 lifecycle thread 做一次最终 bounded caller-response drain。HTTP handler 的独立 monotonic wait 是 hard upper bound；它与 lifecycle sweep 共享同一个原子 cancellation/completion path。

## 开源与许可证

本仓库自有代码、配置、静态资源和文档采用
[Apache License 2.0](LICENSE)。运行时依赖不随源码仓库分发，并保留各自的
许可证；详见 [NOTICE](NOTICE) 和
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。发布独立可执行文件前，
需要对 PyInstaller 产物中的完整依赖集合另行执行许可证审查。

## Forge 2.x 迁移

本分支准备节点版本 `2.0.0`，适配 Dora 1.x，迁移验证锁定 Dora `1.0.1`。使用正式 Forge 2.x 分发包；Common 至少为 `2.1.0`。此次更新包括依赖锁文件与各独立可执行入口。

每轮报告必须列出仓库、改动和测试证据，包括各节点的 `dora run` 记录，以及包含 `adapter-forge-gateway` 的串联测试。缺少硬件或模型的验收项保持待验证。源码 PR 不等于正式发布；整体验收通过后再同步 GitLab。

## 运行验证

安装匹配的 `dora-rs-cli==1.0.1` 后执行：

```bash
uv run --frozen python tests/integration/run_dora_smoke.py --mode single
uv run --frozen python tests/integration/run_dora_smoke.py --mode chain
```

`build/forge2-acceptance/` 保存生成的 YAML、实际 `dora run` 命令、依赖版本、运行日志、HTTP 健康检查、WebSocket 状态／图像断言及退出码。串联使用合成 JPEG 彩色、原始 float32 深度和关节状态；Gateway 实际接收消息并通过 HTTP/WebSocket 提供被检查的数据。本测试不替代真机或持续运行验收。
