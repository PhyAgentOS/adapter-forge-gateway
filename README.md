# Forge Gateway

[English](README.md) | [简体中文](README_zh.md)

`gateway` is the unified runtime entrypoint for local HTTP control, runtime readiness/start gates, and state/image WebSockets.

This standalone open-source repository evolved from the Forge Runtime Gateway Node.
See the [Gateway API guide](docs/api.md) for complete interfaces,
[SECURITY.md](SECURITY.md) for deployment boundaries, and
[OPEN_SOURCE_AUDIT.md](OPEN_SOURCE_AUDIT.md) for the release review.

> [!WARNING]
> Gateway control APIs have no built-in authentication or TLS and listen on `127.0.0.1` by default.
> Remote access requires a trusted network and an authenticated, encrypted reverse proxy.
> Independently verify emergency stops, workspace isolation and device safety limits before connecting hardware.

## Node, Endpoint and Tool API

| Item | Current implementation |
| --- | --- |
| Production Nodes | 1: `gateway`; source entrypoint `main.py`, packaged entrypoint `dist/gateway`; no standalone console script |
| Tool support | Caller-visible discovery, ToolSpec, provider leases/routing, and Query/Action/Session invocation |
| Own business Endpoints | 0; Gateway does not implement motion/gripper/benchmark business providers |
| Routed Endpoints / operations | Defined by `tools.providers`, provider descriptors and current leases; no repository-fixed count |
| Gateway Tools | Defined by `tools.specs`; availability also depends on provider registration/readiness and cannot be inferred from Node or Endpoint count |
| Direct HTTP Tool API | 10 Tool/Invocation route templates below; excludes other runtime/record/playback/WebSocket interfaces |

| Method | Route template | Purpose |
| --- | --- | --- |
| GET | `/tools` | Tool catalog/discovery |
| GET | `/tools/{tool_id}` | ToolSpec |
| GET | `/tools/{tool_id}/context` | Readiness and frame context |
| POST | `/tools/{tool_id}:invoke` | Create Action or Session invocation |
| POST | `/tools/{endpoint_id}/{operation}:invoke` | Invoke Query |
| GET | `/invocations/{invocation_id}` | Status |
| GET | `/invocations/{invocation_id}/result` | Result |
| POST | `/invocations/{invocation_id}/cancel` | Cancel Action |
| POST | `/invocations/{invocation_id}/stop` | Stop Session |
| GET | `/invocations/{invocation_id}/events` | SSE events |

For example, robot-control-sim routes:
`sim.set_gripper_opening -> gripper.controller / set_opening -> gripper_action_policy`.
ToolSpec selects the business operation; the provider route selects Dora channels.
Node IDs, Endpoint IDs and Tool IDs are separate identities.

Current source supports Query, Action and Session, with Session start/stop/status/result handled separately from Action cancel.
This does not establish equivalent capability in a historical Release or Skill-locked binary. Check deployment revisions first.
HTTP `GET /invocations/{id}/result` returns `200` with any retained result, including an `unknown` result, and does not start a provider lookup; when no result is retained it returns `202` and attempts a bounded refresh, suppressing `RuntimeError` from that refresh. The Dora/Tool caller result path (`tool.result.request`) is separate: a retained non-unknown terminal result (`completed`/`failed`/`cancelled`/`stopped`) is served directly as an instance-less `tool.result.response`; `tool.status.request` does not use this direct-out path and still queues or attaches a provider status lookup. For both result and status requests, a waiter-less automatic refresh in flight is joined by the first caller, which reserves one response slot and reuses the single provider lookup; a further concurrent waiter receives retryable `FORGE_TOOL_GATEWAY_BUSY`; `unknown` still requires provider reconciliation; and an absolutely full outbound mailbox raises `ToolGatewayMailboxFull` instead of fabricating BUSY. That mailbox error is not an HTTP status code.
Implementation: [HTTP routes](src/forge_gateway/controllers/tool_controller.py),
[Tool service](src/forge_gateway/services/tool_gateway_service.py),
[Session lifecycle tests](tests/test_tool_session.py).

Compatibility: current dependency declarations are `dora-rs==0.4.1`, `forge-msgs==1.3.0`, `forge-common>=1.0.0,<2`. See [pyproject.toml](pyproject.toml) and [uv.lock](uv.lock). The Tool protocol implementation is vendored in this repository. These declarations do not guarantee cross-version interoperability; also check the build revision and Skill lock when using artifacts.

Changes to Node entrypoints, Endpoints or Tool APIs must update this section and the associated configuration/contracts. Follow the [Node README contract](https://github.com/PhyAgentOS/forge/blob/master/CONTRIBUTING.md#node-readme-contract).

## Standalone development

The project uses Python `3.12`; `uv` manages dependencies and development/build tools.

Synchronize all dependency groups:

```bash
uv sync --frozen --all-groups
```

Run tests:

```bash
uv run pytest -q
```

Start from source and inspect the CLI:

```bash
uv run python main.py --config config.example.yaml
uv run python main.py --help
uv run python main.py --version
```

This Node runs directly from a checkout. It neither builds/publishes a wheel or sdist nor installs a console script.
`pyproject.toml` and `uv.lock` manage Python and dependencies; root `main.py` is the Node entrypoint,
and `config.py` preserves historical source-import compatibility. Dependencies are locked against public indexes.
The Apache-2.0 `forge-tool` protocol implementation is vendored to make the public checkout reproducible before
its independent publication. Plain `pip install .` is not a supported deployment path.

Build a standalone executable:

```bash
./scripts/build_pyinstaller.sh
```

The build script uses a temporary `.venv_build`, writes `dist/gateway`, and verifies its entrypoint/version
with `dist/gateway --help` and `dist/gateway --version`.

## Application lifecycle

`GatewayApplication` manages Runtime, the Dora reader, launcher watcher and Uvicorn:

- Initialize the Dora Node/runner before opening HTTP ingress.
- Uvicorn must complete its port-binding handshake; port conflicts or early background-thread exit roll startup back.
- `SIGINT`, `SIGTERM`, launcher disappearance and `/runtime/stop` share one shutdown path.
- Shutdown closes command/image admission, stops HTTP/Dora ingress, then executes the Runtime close barrier.
- Cleanup attempts every acquired component; timeouts/errors produce structured results, and incomplete close can be retried.

Thread joins share an application budget; Runtime retains separate dispatch/image cleanup budgets.
If a Dora iterator blocks indefinitely, Gateway reports incomplete shutdown and exits nonzero instead of reporting a clean close.

## Configuration

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
  proprio_stale_after_sec: 2.0  # null explicitly restores legacy presence-only behavior
  image_stale_after_sec: 2.0

tools:
  enabled: false
  lease_ttl_ms: 15000
  invoke_timeout_ms: 5000  # Maximum timeout for every caller
  request_input_id: tool_request
  response_output_id: tool_response
  providers: []
  # providers:
  #   - endpoint_id: vision.yolo
  #     input_id: yolo/to_gateway
  #     output_id: gateway/to_yolo
```

Configuration validation rejects unknown/duplicate YAML keys, string-valued booleans,
non-finite numbers and empty/duplicate input IDs. The legacy `broadcast_hz` alias remains
supported alone, but cannot coexist with `state_broadcast_hz`.

When `readiness.require_images: true`, `image_input_ids` must be nonempty or readiness reports
it missing. Proprioceptive state must carry position, velocity or effort values and match at
least one joint in `joint_order`; partial joint states remain supported.

With `tools.enabled: true`, Gateway can start as a Tool-only Dora Node without `joint_order`.
Gateway is the sole caller-visible discovery/routing authority. Shared provider inputs accept
management, invoke/status/result/control responses, `tool.event` and `tool.error`; shared outputs
send registry responses and pinned Tool requests.

The Directory maintains one current instance with a monotonic lease per `endpoint_id` and accepts
Query/Action/Session descriptors. Query retains synchronous behavior. With `tools.specs` configured,
Actions use a separate bounded invocation store: Gateway generates invocation/attempt identities,
pins the provider instance and routes invoke/status/result/control/event messages. Terminal results
and events follow configured retention. Lease expiry/provider replacement makes uncertain results
`unknown`. A deadline is not a stop, and accepted cancellation is not cancellation completion.
Session uses start/stop/status/result; its running lifetime has no Action deadline, but the handshake
is bounded and provider leases still apply. Gateway implements no automatic retry, general deduplication,
PAOS or Skill Runtime.

Start:

```bash
uv run python main.py --config gateway.yaml
```

## Data collection panel

After starting the dataflow, open <http://127.0.0.1:9001> (use the configured port).
Gateway includes a web panel requiring no frontend build:

- Multiple live cameras and readiness/connection status.
- Measured joints, action commands and recorder statistics.
- Start recording with a unique output path and collection metadata.
- Stop and keep, discard the current recording session, or reset the scene.

The page and API share an origin; no separate frontend service is required. The default bind address
is `127.0.0.1`. Binding `0.0.0.0` also lets other clients on that network invoke control APIs;
provide trusted-network isolation or an authenticating proxy for such deployments.

## HTTP API

- `POST /record/control`, body: `{"action":"START|STOP|DISCARD","output_path":"optional.mcap","metadata":"optional object"}`
- `POST /record/set_root`, body: `{"root":"..."}`
- `GET /record/status`
- `POST /playback/control`, body: `{"action":"START|PAUSE|RESUME|RESET","mcap_path":"optional.mcap"}`
- `GET /playback/status`
- `POST /runtime/reset_scene`: sends the runtime command `reset_scene` to reset a simulation scene; distinct from playback `RESET`.
- `POST /runtime/stop`
- `GET /tools`: active Tool descriptors, without provider instances, routes or monotonic lease times.
- `GET /tools/{tool_id}`, `GET /tools/{tool_id}/context`: ToolSpec and readiness/frame context.
- `POST /tools/{tool_id}:invoke`: creates an Action or Session invocation; returns `202`.
- `POST /tools/{endpoint_id}/{operation}:invoke`: invokes a Query; body `{"arguments":{},"caller_id":"optional","timeout_ms":5000}`. `timeout_ms` is optional; when supplied it must be in `1..tools.invoke_timeout_ms`. Uses the same Tool Gateway service as Dora callers.
- `GET /invocations/{id}`, `GET /invocations/{id}/result`, `POST /invocations/{id}/cancel`, `POST /invocations/{id}/stop`, `GET /invocations/{id}/events`: invocation status, results, Action cancellation, Session stop and SSE.

- `POST /policy/command`: sends `PolicyCommand` directly; body `{"command":"...","inputs":{}}`.
- `GET /runtime/status`: status snapshot and readiness.
- `POST /runtime/start`: sends the default `start` only when ready; the body may override `command` and `inputs`.

## WebSocket

- `/ws/state`: lightweight JSON state at `state_broadcast_hz`, including `proprio_state`, `action`, runtime status and readiness.
- `/ws/images`: per-camera incremental messages at `image_broadcast_hz`; compressed images are forwarded as base64, while raw `Image` payloads are JPEG-encoded.
- `ws_send_timeout_sec`: per-send WebSocket timeout; a stalled connection is disconnected to avoid blocking later broadcasts.

Image messages contain:

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

## Dora inputs and outputs

Inputs:

- `tick`
- `proprio_state`
- `action`
- `runtime_status`
- `record_status`
- `playback_status`
- Configured `image_input_ids`.
- `tools.request_input_id`: public caller's logical `tool.invoke.request`.
- `tools.providers[*].input_id`: shared provider register/unregister/invoke.response/tool.error input.

Outputs:

- `policy_command`
- `tools.response_output_id`: correlated invoke.response/tool.error for public callers.
- `tools.providers[*].output_id`: shared registry.response/invoke.request output to providers.

All provider/caller Tool inputs use one bounded ordered FIFO, separate from image/state latest-value
coalescing. Overflow fails explicitly rather than silently dropping registrations, invoke responses or
errors. The outbound Tool mailbox is also bounded: management reserves ACK capacity before Directory
mutation; Dora invocation reserves terminal-response capacity at admission. One remaining slot yields
an immediate busy error; no slot means no state mutation. HTTP and Dora pending invocations share the
service `outbound_capacity` limit. New invocations beyond that limit return `FORGE_TOOL_GATEWAY_BUSY`
without creating pending state; lifecycle-claimed provider requests still count toward it.
Register/unregister Directory effects preserve reader `received_at`. Query deadlines begin at lifecycle
`processed_at` and require provider responses to complete lifecycle processing before the deadline;
entering the reader FIFO before the deadline does not extend it.

Provider invoke dispatch linearizes when the lifecycle claims the mailbox request under the
`ToolGatewayService` lock and marks `dispatch_claimed`. Timeout/cancel/close before that point
invalidates the request without calling Dora `send_output`; sending after a claim still occurs
outside the lock and may reach the provider. All asynchronous terminal completions arbitrate under
the service lock: observations at or beyond the pending monotonic deadline return
`FORGE_TOOL_INVOKE_TIMEOUT` (HTTP `504`), rather than later provider, transport, caller-cancelled or
closing outcomes. Results linearized before the deadline remain unchanged. Valid responses/errors
after completion are silently discarded as late/duplicate. This bounded claim-after-effect behavior
applies only to Query without Gateway retry/cancel protocol, not Action or Session. Lifecycle drains
are bounded; shutdown performs a final bounded caller-response drain on the lifecycle thread.
The HTTP handler's independent monotonic wait is a hard upper bound and shares the lifecycle sweep's
atomic cancellation/completion path.

## Open source and licensing

Repository-owned code, configuration, static resources and documentation use
[Apache License 2.0](LICENSE). Runtime dependencies are not distributed with this source repository
and retain their own licenses; see [NOTICE](NOTICE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
Review the complete dependency set in a PyInstaller artifact before publishing standalone executables.
