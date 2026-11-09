from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from forge_tool import (
    TOOL_ENDPOINT_PROTOCOL,
    EndpointStatus,
    ToolAccepted,
    ToolContext,
    ToolControlResponse,
    ToolEndpointDescriptor,
    ToolEnvelope,
    ToolEvent,
    ToolExecutionKey,
    ToolExecutionStatus,
    ToolOperationDescriptor,
    ToolRequest,
    ToolError,
    ToolResult,
    ToolResultResponse,
    error_from_payload,
    invoke_request_from_envelope,
    make_control_response_envelope,
    make_endpoint_status_envelope,
    make_event_envelope,
    make_invoke_response_envelope,
    make_registration_envelope,
    make_result_response_envelope,
    make_status_response_envelope,
    result_response_from_payload,
    validate_message_envelope,
)

from forge_gateway.config import GatewayConfig
from forge_gateway.controllers.tool_controller import register_tool_routes
from forge_gateway.domain.action_invocations import (
    ActionInvocation,
    ActionInvocationCapacityError,
    ActionInvocationStore,
)
from forge_gateway.services.runtime_service import GatewayRuntime
from forge_gateway.services.tool_gateway_service import (
    ToolGatewayMailboxFull,
    ToolGatewayService,
    make_logical_invoke_request_envelope,
)


def _config(*, lease_ttl_ms: int = 15_000) -> GatewayConfig:
    return GatewayConfig.from_dict(
        {
            "joint_order": [],
            "tools": {
                "enabled": True,
                "lease_ttl_ms": lease_ttl_ms,
                "invoke_timeout_ms": 5_000,
                "invocation_capacity": 2,
                "event_capacity": 2,
                "retention_sec": 60,
                "providers": [
                    {
                        "endpoint_id": "robot.arm",
                        "input_id": "arm/to_gateway",
                        "output_id": "gateway/to_arm",
                    }
                ],
                "specs": [
                    {
                        "tool_id": "robot.pick",
                        "implementation_id": "arm.primary",
                        "endpoint_id": "robot.arm",
                        "operation": "pick",
                        "semantics": "action",
                        "readiness": ["proprio_state"],
                        "robot_frame_profile": {
                            "robot_id": "arm-1",
                            "base_frame": "base",
                            "tool_frame": "gripper",
                            "directions": {
                                "forward": {
                                    "frame": "base",
                                    "axis": "x",
                                    "sign": 1,
                                }
                            },
                        },
                    }
                ],
            },
        }
    )


def _register(
    runtime: GatewayRuntime,
    *,
    now: float | None = None,
    max_concurrency: int = 2,
) -> None:
    descriptor = ToolEndpointDescriptor(
        protocol_version=TOOL_ENDPOINT_PROTOCOL,
        endpoint_id="robot.arm",
        operations=(
            ToolOperationDescriptor(
                name="pick",
                semantics="action",
                cancellable=True,
                status_supported=True,
                max_concurrency=max_concurrency,
            ),
        ),
    )
    runtime.tool_gateway.handle_input(
        "arm/to_gateway",
        make_registration_envelope(
            descriptor,
            endpoint_instance_id="arm-instance-1",
            request_id="register-1",
        ),
        received_at=time.monotonic() if now is None else now,
    )
    assert runtime.tool_gateway.take_outbound() is not None


def test_action_http_lifecycle_result_cancel_and_events() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        _register(runtime)
        created = client.post(
            "/tools/robot.pick:invoke",
            json={"arguments": {"object": "cup"}, "caller_id": "test"},
        )
        assert created.status_code == 202
        created_data = created.json()["data"]
        invocation_id = created_data["invocation_id"]
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        assert provider_invoke.envelope.endpoint_instance_id == "arm-instance-1"

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        assert client.get(f"/invocations/{invocation_id}").json()["data"]["phase"] == (
            "accepted"
        )
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_status_response_envelope(
                ToolExecutionStatus(phase="running"), status_request.envelope
            ),
            received_at=time.monotonic(),
        )

        context = provider_invoke.envelope
        _, tool_context = invoke_request_from_envelope(context)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_event_envelope(
                ToolEvent(type="progress", data={"percent": 25}),
                tool_context,
                endpoint_instance_id="arm-instance-1",
                sequence=0,
            ),
            received_at=time.monotonic(),
        )
        events = runtime.tool_gateway.action_invocations.events_after(
            (invocation_id, created_data["attempt_id"]), -1, now=time.monotonic()
        )
        assert events is not None
        assert events[0].data == {"percent": 25}

        cancel = client.post(f"/invocations/{invocation_id}/cancel")
        assert cancel.status_code == 202
        control_request = runtime.tool_gateway.take_outbound()
        assert control_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_control_response_envelope(
                ToolControlResponse(command="cancel", status="accepted"),
                control_request.envelope,
            ),
            received_at=time.monotonic(),
        )
        snapshot = runtime.tool_gateway.action_invocation_snapshot(invocation_id)
        assert snapshot is not None
        assert snapshot["cancel_status"] == "accepted"
        assert snapshot["phase"] == "running"

        pending = client.get(f"/invocations/{invocation_id}/result")
        assert pending.status_code == 202
        result_request = runtime.tool_gateway.take_outbound()
        assert result_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(status="cancelled", outputs={}),
                ),
                result_request.envelope,
            ),
            received_at=time.monotonic(),
        )
        result = client.get(f"/invocations/{invocation_id}/result")
        assert result.status_code == 200
        assert result.json()["data"]["result"]["status"] == "cancelled"
    finally:
        runtime.close()


def test_endpoint_status_context_and_lease_loss_become_unknown() -> None:
    runtime = GatewayRuntime(_config(lease_ttl_ms=100))
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        now = time.monotonic()
        _register(runtime, now=now)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_endpoint_status_envelope(
                EndpointStatus(endpoint_id="robot.arm", state="ready"),
                endpoint_instance_id="arm-instance-1",
            ),
            received_at=now + 0.01,
        )
        context = client.get("/tools/robot.pick/context")
        assert context.status_code == 200
        assert context.json()["data"]["endpoint_status"]["state"] == "ready"
        expected_directions = {"forward": {"frame": "base", "axis": "x", "sign": 1}}
        assert context.json()["data"]["robot_frame_profile"]["directions"] == (
            expected_directions
        )
        tool = client.get("/tools/robot.pick")
        assert tool.status_code == 200
        assert tool.json()["data"]["robot_frame_profile"]["directions"] == (
            expected_directions
        )

        created = runtime.tool_gateway.submit_http_action(
            "robot.pick", {}, now=now + 0.02
        )
        runtime.tool_gateway.sweep(now=now + 0.1)
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"], now=now + 0.1
        )
        assert snapshot is not None
        assert snapshot["phase"] == "failed"
        assert snapshot["result"]["status"] == "failed"
        assert runtime.tool_gateway.take_outbound() is None
    finally:
        runtime.close()


def test_dora_action_invoke_returns_accepted_without_provider_instance() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        context = ToolContext(
            execution_key=ToolExecutionKey("caller-invocation", "caller-attempt"),
            tool_id="robot.pick",
            implementation_id="arm.primary",
            endpoint_id="robot.arm",
            operation="pick",
            caller_id="dora:test",
        )
        caller_request = make_logical_invoke_request_envelope(
            ToolRequest({"object": "cup"}),
            context,
            request_id="caller-request",
        )
        runtime.tool_gateway.handle_input(
            "tool_request",
            caller_request,
            received_at=time.monotonic(),
        )
        provider_request = runtime.tool_gateway.take_outbound()
        assert provider_request is not None
        assert provider_request.envelope.endpoint_instance_id == "arm-instance-1"

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(
                ToolAccepted(details={"queued": True}), provider_request.envelope
            ),
            received_at=time.monotonic(),
        )
        caller_response = runtime.tool_gateway.take_outbound()
        assert caller_response is not None
        assert caller_response.output_id == "tool_response"
        assert caller_response.envelope.endpoint_instance_id is None
        assert caller_response.envelope.payload["outcome"] == "accepted"

        _, provider_context = invoke_request_from_envelope(provider_request.envelope)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_event_envelope(
                ToolEvent(type="progress", data={"percent": 50}),
                provider_context,
                endpoint_instance_id="arm-instance-1",
                sequence=0,
            ),
            received_at=time.monotonic(),
        )
        caller_event = runtime.tool_gateway.take_outbound()
        assert caller_event is not None
        assert caller_event.envelope.message_type == "tool.event"
        assert caller_event.envelope.endpoint_instance_id is None
        assert caller_event.envelope.sequence == 0
    finally:
        runtime.close()


def test_action_phase_never_regresses_and_accepted_is_independent_barrier() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        key = (created["invocation_id"], created["attempt_id"])
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None

        assert runtime.tool_gateway.refresh_action(created["invocation_id"], "status")
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_status_response_envelope(
                ToolExecutionStatus(phase="running"), status_request.envelope
            ),
            received_at=time.monotonic(),
        )
        _, context = invoke_request_from_envelope(provider_invoke.envelope)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_event_envelope(
                ToolEvent(type="progress", data={"early": True}),
                context,
                endpoint_instance_id="arm-instance-1",
                sequence=0,
            ),
            received_at=time.monotonic(),
        )
        assert runtime.tool_gateway.action_invocations.events_after(
            key, -1, now=time.monotonic()
        ) == ()

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        snapshot = runtime.tool_gateway.action_invocations.snapshot(
            key, now=time.monotonic()
        )
        assert snapshot is not None
        assert snapshot["phase"] == "running"
        assert snapshot["accepted"] is True
        assert len(
            runtime.tool_gateway.action_invocations.events_after(
                key, -1, now=time.monotonic()
            )
            or ()
        ) == 1
    finally:
        runtime.close()


def test_terminal_status_queues_result_barrier_lookup() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        assert runtime.tool_gateway.refresh_action(created["invocation_id"], "status")
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_status_response_envelope(
                ToolExecutionStatus(phase="completed"), status_request.envelope
            ),
            received_at=time.monotonic(),
        )

        result_request = runtime.tool_gateway.take_outbound()
        assert result_request is not None
        assert result_request.envelope.message_type == "tool.result.request"
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["terminal_status_hint"] == "completed"
        assert snapshot["phase"] == "accepted"

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(status="succeeded", outputs={"ok": True}),
                ),
                result_request.envelope,
            ),
            received_at=time.monotonic(),
        )
        assert runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )["phase"] == "completed"
    finally:
        runtime.close()


def test_repeated_cancel_uses_one_exchange_and_does_not_regress_status() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )

        assert (
            runtime.tool_gateway.cancel_action(created["invocation_id"])
            == "requested"
        )
        assert (
            runtime.tool_gateway.cancel_action(created["invocation_id"])
            == "requested"
        )
        control = runtime.tool_gateway.take_outbound()
        assert control is not None
        assert control.envelope.message_type == "tool.control.request"
        assert runtime.tool_gateway.take_outbound() is None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_control_response_envelope(
                ToolControlResponse(command="cancel", status="accepted"),
                control.envelope,
            ),
            received_at=time.monotonic(),
        )
        assert (
            runtime.tool_gateway.cancel_action(created["invocation_id"])
            == "accepted"
        )
        assert runtime.tool_gateway.take_outbound() is None
    finally:
        runtime.close()


def test_observation_exchange_failure_does_not_make_execution_unknown() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        runtime.tool_gateway.refresh_action(created["invocation_id"], "status")
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None

        assert runtime.tool_gateway.output_failed(
            status_request, RuntimeError("status send failed")
        )
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["phase"] == "accepted"
        assert snapshot["observation_error"]["code"] == (
            "FORGE_TOOL_TRANSPORT_UNAVAILABLE"
        )
    finally:
        runtime.close()


def test_deadline_unknown_keeps_concurrency_until_instance_isolated() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime, max_concurrency=1)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        item = runtime.tool_gateway.action_invocations.get(
            (created["invocation_id"], created["attempt_id"]),
            now=time.monotonic(),
        )
        assert item is not None and item.deadline_ms is not None
        runtime.tool_gateway.action_invocations.mark_deadlines_ambiguous(
            now=time.monotonic(), epoch_ms=item.deadline_ms
        )
        with pytest.raises(ActionInvocationCapacityError):
            runtime.tool_gateway.submit_http_action("robot.pick", {})

        runtime.tool_gateway.action_invocations.mark_instance_ambiguous(
            "robot.arm",
            "arm-instance-1",
            now=time.monotonic(),
            reason="instance isolated",
        )
        replacement = runtime.tool_gateway.submit_http_action("robot.pick", {})
        assert replacement["phase"] == "dispatching"
    finally:
        runtime.close()


def test_action_exchange_timeout_is_swept_without_poisoning_observation() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        runtime.tool_gateway.refresh_action(created["invocation_id"], "status")
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None
        runtime.tool_gateway.sweep(now=time.monotonic() + 6)

        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["phase"] == "accepted"
        assert snapshot["observation_error"]["code"] == "FORGE_TOOL_EXCHANGE_TIMEOUT"
        assert not runtime.tool_gateway._action_exchanges
    finally:
        runtime.close()


def test_dora_execution_key_attempts_are_isolated_and_http_id_is_ambiguous() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        for attempt_id in ("attempt-1", "attempt-2"):
            context = ToolContext(
                execution_key=ToolExecutionKey("shared-invocation", attempt_id),
                tool_id="robot.pick",
                implementation_id="arm.primary",
                endpoint_id="robot.arm",
                operation="pick",
            )
            runtime.tool_gateway.handle_input(
                "tool_request",
                make_logical_invoke_request_envelope(
                    ToolRequest({"attempt": attempt_id}),
                    context,
                    request_id=f"request-{attempt_id}",
                ),
                received_at=time.monotonic(),
            )
        provider_requests = [
            runtime.tool_gateway.take_outbound(),
            runtime.tool_gateway.take_outbound(),
        ]
        assert all(request is not None for request in provider_requests)
        assert {
            request.envelope.attempt_id
            for request in provider_requests
            if request is not None
        } == {"attempt-1", "attempt-2"}
        for provider_request in provider_requests:
            assert provider_request is not None
            runtime.tool_gateway.handle_input(
                "arm/to_gateway",
                make_invoke_response_envelope(
                    ToolAccepted(), provider_request.envelope
                ),
                received_at=time.monotonic(),
            )
        assert runtime.tool_gateway.take_outbound() is not None
        assert runtime.tool_gateway.take_outbound() is not None

        for message_type, attempt_id in (
            ("tool.status.request", "attempt-2"),
            ("tool.result.request", "attempt-2"),
            ("tool.control.request", "attempt-1"),
        ):
            payload = (
                {"command": "cancel"}
                if message_type == "tool.control.request"
                else {}
            )
            logical = ToolEnvelope(
                protocol=TOOL_ENDPOINT_PROTOCOL,
                message_type=message_type,  # type: ignore[arg-type]
                request_id=f"{message_type}-{attempt_id}",
                invocation_id="shared-invocation",
                attempt_id=attempt_id,
                endpoint_id="robot.arm",
                endpoint_instance_id=None,
                operation="pick",
                payload=payload,
            )
            runtime.tool_gateway.handle_input(
                "tool_request", logical, received_at=time.monotonic()
            )
            provider_request = runtime.tool_gateway.take_outbound()
            assert provider_request is not None
            assert provider_request.envelope.attempt_id == attempt_id
            assert provider_request.envelope.message_type == message_type
        assert runtime.tool_gateway.action_invocation_snapshot(
            "shared-invocation"
        ) is None
    finally:
        runtime.close()


def test_dora_action_admission_rejection_is_correlated_and_instance_less() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        context = ToolContext(
            execution_key=ToolExecutionKey("bad-invocation", "bad-attempt"),
            tool_id="robot.pick",
            implementation_id="wrong.implementation",
            endpoint_id="robot.arm",
            operation="pick",
        )
        request = make_logical_invoke_request_envelope(
            ToolRequest({}), context, request_id="bad-request"
        )
        runtime.tool_gateway.handle_input(
            "tool_request", request, received_at=time.monotonic()
        )
        response = runtime.tool_gateway.take_outbound()
        assert response is not None
        assert response.envelope.endpoint_instance_id is None
        assert response.envelope.request_id == request.request_id
        assert error_from_payload(response.envelope.payload).code == (
            "FORGE_TOOL_ACTION_ADMISSION_REJECTED"
        )
        assert runtime.tool_gateway.take_outbound() is None
    finally:
        runtime.close()


def test_duplicate_dora_execution_key_rejection_preserves_original() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        context = ToolContext(
            execution_key=ToolExecutionKey("duplicate-invocation", "attempt"),
            tool_id="robot.pick",
            implementation_id="arm.primary",
            endpoint_id="robot.arm",
            operation="pick",
        )
        first = make_logical_invoke_request_envelope(
            ToolRequest({"generation": 1}), context, request_id="first"
        )
        duplicate = make_logical_invoke_request_envelope(
            ToolRequest({"generation": 2}), context, request_id="duplicate"
        )
        runtime.tool_gateway.handle_input(
            "tool_request", first, received_at=time.monotonic()
        )
        provider_request = runtime.tool_gateway.take_outbound()
        assert provider_request is not None
        runtime.tool_gateway.handle_input(
            "tool_request", duplicate, received_at=time.monotonic()
        )
        rejection = runtime.tool_gateway.take_outbound()
        assert rejection is not None
        assert rejection.envelope.request_id == "duplicate"
        assert runtime.tool_gateway.action_invocations.snapshot(
            ("duplicate-invocation", "attempt"), now=time.monotonic()
        ) is not None
        assert any(
            exchange.provider_request == provider_request.envelope
            for exchange in runtime.tool_gateway._action_exchanges.values()
        )
    finally:
        runtime.close()


def test_store_total_capacity_is_hard_bounded_by_terminal_eviction() -> None:
    store = ActionInvocationStore(capacity=2, event_capacity=1, retention_seconds=60)

    def invocation(index: int) -> ActionInvocation:
        return ActionInvocation(
            invocation_id=f"inv-{index}",
            attempt_id=f"attempt-{index}",
            tool_id="robot.pick",
            implementation_id="arm.primary",
            endpoint_id="robot.arm",
            endpoint_instance_id="instance",
            operation="pick",
            caller_id=None,
            deadline_ms=None,
            created_at=float(index),
        )

    for index in range(3):
        value = invocation(index)
        store.create(value, now=float(index))
        store.set_phase(
            value.key,
            "completed",
            now=float(index),
            result=ToolResult(status="succeeded", outputs={}),
        )
    assert len(store._items) == 2
    assert store.snapshot(("inv-0", "attempt-0"), now=3.0) is None


def test_toolspec_binding_and_readiness_configuration_are_strict() -> None:
    base = {
        "joint_order": [],
        "tools": {
            "enabled": True,
            "providers": [
                {
                    "endpoint_id": "robot.arm",
                    "input_id": "arm/to_gateway",
                    "output_id": "gateway/to_arm",
                }
            ],
            "specs": [
                {
                    "tool_id": "robot.pick",
                    "implementation_id": "arm.primary",
                    "endpoint_id": "robot.arm",
                    "operation": "pick",
                    "semantics": "action",
                }
            ],
        },
    }
    duplicate = {
        **base,
        "tools": {
            **base["tools"],
            "specs": [
                *base["tools"]["specs"],
                {
                    **base["tools"]["specs"][0],
                    "tool_id": "robot.pick.alias",
                },
            ],
        },
    }
    with pytest.raises(ValueError, match="bindings must be unique"):
        GatewayConfig.from_dict(duplicate)

    invalid_readiness = {
        **base,
        "tools": {
            **base["tools"],
            "specs": [
                {
                    **base["tools"]["specs"][0],
                    "readiness": ["proprio_typo"],
                }
            ],
        },
    }
    with pytest.raises(ValueError, match="unknown requirement"):
        GatewayConfig.from_dict(invalid_readiness)


@pytest.mark.parametrize(
    ("directions", "match"),
    [
        pytest.param([], "directions must be a YAML mapping", id="directions-list"),
        pytest.param({"": {}}, "direction name", id="blank-direction-name"),
        pytest.param(
            {"forward": []}, "forward must be a YAML mapping", id="entry-list"
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "x"}},
            "missing .*sign",
            id="missing-sign",
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "x", "sign": 1, "unit": "m"}},
            "unknown .*unit",
            id="unknown-entry-key",
        ),
        pytest.param(
            {"forward": {"frame": "", "axis": "x", "sign": 1}},
            "forward.frame",
            id="blank-frame",
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "forward", "sign": 1}},
            "forward.axis",
            id="invalid-axis",
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "x", "sign": 0}},
            "forward.sign",
            id="invalid-sign",
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "x", "sign": True}},
            "forward.sign",
            id="boolean-sign",
        ),
    ],
)
def test_robot_frame_directions_configuration_is_strict(
    directions: object, match: str
) -> None:
    spec = {
        "tool_id": "robot.pick",
        "implementation_id": "arm.primary",
        "endpoint_id": "robot.arm",
        "operation": "pick",
        "semantics": "action",
        "robot_frame_profile": {
            "robot_id": "arm-1",
            "base_frame": "base",
            "directions": directions,
        },
    }
    with pytest.raises(ValueError, match=match):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "tools": {"specs": [spec]},
            }
        )


def test_sse_reports_retention_gap_and_sets_streaming_headers() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        key = (created["invocation_id"], created["attempt_id"])
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        _, context = invoke_request_from_envelope(provider_invoke.envelope)
        for sequence in range(3):
            runtime.tool_gateway.handle_input(
                "arm/to_gateway",
                make_event_envelope(
                    ToolEvent(type="progress", data={"sequence": sequence}),
                    context,
                    endpoint_instance_id="arm-instance-1",
                    sequence=sequence,
                ),
                received_at=time.monotonic(),
            )
        runtime.tool_gateway.action_invocations.set_phase(
            key,
            "completed",
            now=time.monotonic(),
            result=ToolResult(status="succeeded", outputs={}),
        )

        gap = client.get(
            f"/invocations/{created['invocation_id']}/events",
            headers={"Last-Event-ID": "-1"},
        )
        assert gap.status_code == 410
        stream = client.get(
            f"/invocations/{created['invocation_id']}/events",
            headers={"Last-Event-ID": "0"},
        )
        assert stream.status_code == 200
        assert stream.headers["cache-control"] == "no-cache"
        assert stream.headers["x-accel-buffering"] == "no"
        assert "id: 1" in stream.text
        assert "id: 2" in stream.text
    finally:
        runtime.close()


def test_query_unknown_operation_returns_404_instead_of_500() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    try:
        _register(runtime)
        response = TestClient(app).post(
            "/tools/robot.arm/missing:invoke", json={"arguments": {}}
        )
        assert response.status_code == 404
    finally:
        runtime.close()


def test_deadline_unknown_releases_concurrency_on_late_terminal_event() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime, max_concurrency=1)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        _, provider_context = invoke_request_from_envelope(provider_invoke.envelope)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        item = runtime.tool_gateway.action_invocations.get(
            (created["invocation_id"], created["attempt_id"]),
            now=time.monotonic(),
        )
        assert item is not None and item.deadline_ms is not None
        runtime.tool_gateway.action_invocations.mark_deadlines_ambiguous(
            now=time.monotonic(), epoch_ms=item.deadline_ms
        )
        with pytest.raises(ActionInvocationCapacityError):
            runtime.tool_gateway.submit_http_action("robot.pick", {})

        # The provider finishes after the deadline closed the attempt: the
        # late terminal event must release the concurrency fence...
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_event_envelope(
                ToolEvent(type="executor_completed", data={"final": True}),
                provider_context,
                endpoint_instance_id="arm-instance-1",
                sequence=0,
            ),
            received_at=time.monotonic(),
        )
        replacement = runtime.tool_gateway.submit_http_action("robot.pick", {})
        assert replacement["phase"] == "dispatching"

        # ...and the terminal event must trigger the retained-result lookup,
        # whose reply reconciles the closed-unknown attempt to its outcome.
        result_request = runtime.tool_gateway.take_outbound()
        while result_request is not None and (
            result_request.envelope.message_type != "tool.result.request"
        ):
            result_request = runtime.tool_gateway.take_outbound()
        assert result_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(
                        status="succeeded", outputs={"final_positions": [0.5]}
                    ),
                ),
                result_request.envelope,
            ),
            received_at=time.monotonic(),
        )
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["phase"] == "completed"
        assert snapshot["result"]["outputs"] == {"final_positions": [0.5]}
    finally:
        runtime.close()


def test_cancel_action_on_deadline_unknown_forwards_to_provider() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime, max_concurrency=1)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        _, provider_context = invoke_request_from_envelope(provider_invoke.envelope)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        item = runtime.tool_gateway.action_invocations.get(
            (created["invocation_id"], created["attempt_id"]),
            now=time.monotonic(),
        )
        assert item is not None and item.deadline_ms is not None
        runtime.tool_gateway.action_invocations.mark_deadlines_ambiguous(
            now=time.monotonic(), epoch_ms=item.deadline_ms
        )

        # A deadline-unknown attempt still fences concurrency because the
        # provider keeps executing: cancelling it must forward to the provider
        # instead of short-circuiting as "terminal".
        assert (
            runtime.tool_gateway.cancel_action(created["invocation_id"])
            == "requested"
        )
        control = runtime.tool_gateway.take_outbound()
        assert control is not None
        assert control.envelope.message_type == "tool.control.request"
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_control_response_envelope(
                ToolControlResponse(command="cancel", status="accepted"),
                control.envelope,
            ),
            received_at=time.monotonic(),
        )
        with pytest.raises(ActionInvocationCapacityError):
            runtime.tool_gateway.submit_http_action("robot.pick", {})

        # Provider confirms termination with the cancelled terminal event,
        # which releases the fence.
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_event_envelope(
                ToolEvent(type="cancelled", data={}),
                provider_context,
                endpoint_instance_id="arm-instance-1",
                sequence=0,
            ),
            received_at=time.monotonic(),
        )
        replacement = runtime.tool_gateway.submit_http_action("robot.pick", {})
        assert replacement["phase"] == "dispatching"
    finally:
        runtime.close()


def _accepted_action(
    gateway: ToolGatewayService,
) -> tuple[dict[str, Any], ToolContext]:
    created = gateway.submit_http_action("robot.pick", {})
    provider_invoke = gateway.take_outbound()
    assert provider_invoke is not None
    _, provider_context = invoke_request_from_envelope(provider_invoke.envelope)
    gateway.handle_input(
        "arm/to_gateway",
        make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
        received_at=time.monotonic(),
    )
    return created, provider_context


def _caller_result_request(
    created: dict[str, Any], request_id: str
) -> ToolEnvelope:
    return ToolEnvelope(
        protocol=TOOL_ENDPOINT_PROTOCOL,
        message_type="tool.result.request",
        request_id=request_id,
        invocation_id=str(created["invocation_id"]),
        attempt_id=str(created["attempt_id"]),
        endpoint_id=str(created["endpoint_id"]),
        endpoint_instance_id=None,
        operation=str(created["operation"]),
        payload={},
    )


def _terminal_event(
    provider_context: ToolContext, *, sequence: int = 0
) -> ToolEnvelope:
    return make_event_envelope(
        ToolEvent(type="executor_completed", data={"final": True}),
        provider_context,
        endpoint_instance_id="arm-instance-1",
        sequence=sequence,
    )


def _drain_outbound(gateway: ToolGatewayService) -> list[Any]:
    messages: list[Any] = []
    while True:
        message = gateway.take_outbound()
        if message is None:
            return messages
        messages.append(message)


def _set_cached_result(
    gateway: ToolGatewayService,
    created: dict[str, Any],
    phase: str,
    result: ToolResult,
) -> None:
    changed = gateway.action_invocations.set_phase(
        (str(created["invocation_id"]), str(created["attempt_id"])),
        phase,  # type: ignore[arg-type]
        now=time.monotonic(),
        result=result,
        error=result.error,
    )
    assert changed


def test_auto_result_lookup_coalesces_caller_result() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created, provider_context = _accepted_action(runtime.tool_gateway)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            _terminal_event(provider_context),
            received_at=time.monotonic(),
        )
        auto_lookups = [
            message
            for message in _drain_outbound(runtime.tool_gateway)
            if message.envelope.message_type == "tool.result.request"
        ]
        assert len(auto_lookups) == 1

        caller = _caller_result_request(created, "caller-result-coalesce")
        runtime.tool_gateway.handle_input(
            "tool_request", caller, received_at=time.monotonic()
        )
        attached = _drain_outbound(runtime.tool_gateway)
        busy_codes = [
            error_from_payload(message.envelope.payload).code
            for message in attached
            if message.envelope.message_type == "tool.error"
        ]
        assert "FORGE_TOOL_GATEWAY_BUSY" not in busy_codes, (
            "caller result request must attach to the in-flight automatic "
            f"lookup instead of returning BUSY: {busy_codes}"
        )
        assert not [
            message
            for message in attached
            if message.envelope.message_type == "tool.result.request"
        ], "attaching the caller must not create a second provider lookup"
        assert runtime.tool_gateway.reserved_outbound_count == 1

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(status="succeeded", outputs={"ok": True}),
                ),
                auto_lookups[0].envelope,
            ),
            received_at=time.monotonic(),
        )
        responses = [
            message
            for message in _drain_outbound(runtime.tool_gateway)
            if message.envelope.message_type == "tool.result.response"
        ]
        assert len(responses) == 1
        response = result_response_from_payload(responses[0].envelope.payload)
        assert response.status == "available"
        assert response.result is not None
        assert response.result.status == "succeeded"
        assert responses[0].envelope.request_id == caller.request_id
        assert responses[0].envelope.endpoint_instance_id is None
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["phase"] == "completed"
        assert runtime.tool_gateway.reserved_outbound_count == 0
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("phase", "expected_result"),
    [
        (
            "completed",
            ToolResult(status="succeeded", outputs={"cached": "completed"}),
        ),
        (
            "failed",
            ToolResult(
                status="failed",
                outputs={"cached": "failed"},
                error=ToolError(code="E_CACHED", message="cached failure"),
            ),
        ),
        (
            "cancelled",
            ToolResult(status="cancelled", outputs={"cached": "cancelled"}),
        ),
        (
            "stopped",
            ToolResult(status="stopped", outputs={"cached": "stopped"}),
        ),
    ],
    ids=["succeeded", "failed", "cancelled", "stopped"],
)
def test_cached_result_available_direct_out(
    phase: str, expected_result: ToolResult
) -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created, _ = _accepted_action(runtime.tool_gateway)
        _set_cached_result(runtime.tool_gateway, created, phase, expected_result)
        assert _drain_outbound(runtime.tool_gateway) == []

        caller = _caller_result_request(created, f"cached-{phase}-1")
        runtime.tool_gateway.handle_input(
            "tool_request", caller, received_at=time.monotonic()
        )
        first = _drain_outbound(runtime.tool_gateway)
        provider_lookups = [
            message
            for message in first
            if message.envelope.message_type == "tool.result.request"
        ]
        assert not provider_lookups, (
            f"a cached non-unknown {phase} result must be served directly, "
            "not looked up again at the provider"
        )
        responses = [
            message
            for message in first
            if message.envelope.message_type == "tool.result.response"
        ]
        assert len(responses) == 1
        validate_message_envelope(responses[0].envelope)
        response = result_response_from_payload(responses[0].envelope.payload)
        assert response.status == "available"
        assert response.result is not None
        assert response.result.status == expected_result.status
        assert response.result.outputs == expected_result.outputs
        assert response.result.error == expected_result.error
        assert responses[0].envelope.endpoint_instance_id is None
        assert responses[0].envelope.request_id == caller.request_id

        second_caller = _caller_result_request(
            created, f"cached-{phase}-2"
        )
        runtime.tool_gateway.handle_input(
            "tool_request", second_caller, received_at=time.monotonic()
        )
        second = _drain_outbound(runtime.tool_gateway)
        assert not [
            message
            for message in second
            if message.envelope.message_type == "tool.result.request"
        ], "the direct-out path must not create a persistent exchange"
        second_responses = [
            message
            for message in second
            if message.envelope.message_type == "tool.result.response"
        ]
        assert len(second_responses) == 1
        assert (
            result_response_from_payload(
                second_responses[0].envelope.payload
            ).status
            == "available"
        )
        assert runtime.tool_gateway.reserved_outbound_count == 0
    finally:
        runtime.close()


def test_cached_result_unknown_skips_direct_out() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created, _ = _accepted_action(runtime.tool_gateway)
        unknown_error = ToolError(
            code="FORGE_TOOL_EXECUTION_OUTCOME_UNKNOWN",
            message="provider outcome is unknown",
        )
        unknown_result = ToolResult(
            status="unknown", outputs={}, error=unknown_error
        )
        _set_cached_result(
            runtime.tool_gateway, created, "unknown", unknown_result
        )
        assert _drain_outbound(runtime.tool_gateway) == []

        caller = _caller_result_request(created, "unknown-cache-1")
        runtime.tool_gateway.handle_input(
            "tool_request", caller, received_at=time.monotonic()
        )
        queued = _drain_outbound(runtime.tool_gateway)
        provider_lookups = [
            message
            for message in queued
            if message.envelope.message_type == "tool.result.request"
        ]
        assert len(provider_lookups) == 1
        assert not [
            message
            for message in queued
            if message.envelope.message_type == "tool.result.response"
        ], "unknown must never be served as a cached available result"
        assert runtime.tool_gateway.reserved_outbound_count == 1

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(
                        status="succeeded", outputs={"late": True}
                    ),
                ),
                provider_lookups[0].envelope,
            ),
            received_at=time.monotonic(),
        )
        responses = [
            message
            for message in _drain_outbound(runtime.tool_gateway)
            if message.envelope.message_type == "tool.result.response"
        ]
        assert len(responses) == 1
        response = result_response_from_payload(responses[0].envelope.payload)
        assert response.status == "available"
        assert response.result is not None
        assert response.result.status == "succeeded"
        assert responses[0].envelope.request_id == caller.request_id
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["phase"] == "completed"
        assert runtime.tool_gateway.reserved_outbound_count == 0
    finally:
        runtime.close()


@pytest.mark.parametrize("outcome", ["not_found", "timeout"])
def test_result_lookup_not_found_or_timeout_stays_unknown(
    outcome: str,
) -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created, _ = _accepted_action(runtime.tool_gateway)
        if outcome == "timeout":
            _set_cached_result(
                runtime.tool_gateway,
                created,
                "unknown",
                ToolResult(
                    status="unknown",
                    outputs={},
                    error=ToolError(
                        code="FORGE_TOOL_EXECUTION_OUTCOME_UNKNOWN",
                        message="provider outcome is unknown before lookup",
                    ),
                ),
            )

        caller = _caller_result_request(created, f"lookup-{outcome}")
        runtime.tool_gateway.handle_input(
            "tool_request", caller, received_at=time.monotonic()
        )
        lookup_messages = _drain_outbound(runtime.tool_gateway)
        provider_lookups = [
            message
            for message in lookup_messages
            if message.envelope.message_type == "tool.result.request"
        ]
        assert len(provider_lookups) == 1

        if outcome == "not_found":
            runtime.tool_gateway.handle_input(
                "arm/to_gateway",
                make_result_response_envelope(
                    ToolResultResponse(status="not_found"),
                    provider_lookups[0].envelope,
                ),
                received_at=time.monotonic(),
            )
            responses = [
                message
                for message in _drain_outbound(runtime.tool_gateway)
                if message.envelope.message_type == "tool.result.response"
            ]
            assert len(responses) == 1
            assert (
                result_response_from_payload(responses[0].envelope.payload).status
                == "not_found"
            )
        else:
            runtime.tool_gateway.sweep(now=time.monotonic() + 10.0)
            errors = [
                message
                for message in _drain_outbound(runtime.tool_gateway)
                if message.envelope.message_type == "tool.error"
            ]
            assert len(errors) == 1
            error = error_from_payload(errors[0].envelope.payload)
            assert error.code == "FORGE_TOOL_EXCHANGE_TIMEOUT"
            assert error.retryable is True
            assert errors[0].envelope.request_id == caller.request_id

        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["phase"] == "unknown"
        assert snapshot["result"]["status"] == "unknown"
        assert runtime.tool_gateway.reserved_outbound_count == 0
    finally:
        runtime.close()


def test_unknown_reconcile_after_queued_result_lookup() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created, provider_context = _accepted_action(runtime.tool_gateway)
        item = runtime.tool_gateway.action_invocations.get(
            (str(created["invocation_id"]), str(created["attempt_id"])),
            now=time.monotonic(),
        )
        assert item is not None and item.deadline_ms is not None
        runtime.tool_gateway.action_invocations.mark_deadlines_ambiguous(
            now=time.monotonic(), epoch_ms=item.deadline_ms
        )
        unknown_snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert unknown_snapshot is not None
        assert unknown_snapshot["phase"] == "unknown"
        assert unknown_snapshot["result"]["status"] == "unknown"

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            _terminal_event(provider_context),
            received_at=time.monotonic(),
        )
        auto_lookups = [
            message
            for message in _drain_outbound(runtime.tool_gateway)
            if message.envelope.message_type == "tool.result.request"
        ]
        assert len(auto_lookups) == 1

        caller = _caller_result_request(created, "unknown-reconcile-caller")
        runtime.tool_gateway.handle_input(
            "tool_request", caller, received_at=time.monotonic()
        )
        attached = _drain_outbound(runtime.tool_gateway)
        busy_codes = [
            error_from_payload(message.envelope.payload).code
            for message in attached
            if message.envelope.message_type == "tool.error"
        ]
        assert "FORGE_TOOL_GATEWAY_BUSY" not in busy_codes, (
            "unknown reconciliation must attach to the automatic lookup "
            f"instead of returning BUSY: {busy_codes}"
        )
        assert not [
            message
            for message in attached
            if message.envelope.message_type == "tool.result.request"
        ], "the caller must reuse the in-flight provider lookup"
        assert runtime.tool_gateway.reserved_outbound_count == 1

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(
                        status="succeeded", outputs={"reconciled": True}
                    ),
                ),
                auto_lookups[0].envelope,
            ),
            received_at=time.monotonic(),
        )
        responses = [
            message
            for message in _drain_outbound(runtime.tool_gateway)
            if message.envelope.message_type == "tool.result.response"
        ]
        assert len(responses) == 1
        response = result_response_from_payload(responses[0].envelope.payload)
        assert response.status == "available"
        assert response.result is not None
        assert response.result.status == "succeeded"
        assert responses[0].envelope.request_id == caller.request_id
        reconciled = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert reconciled is not None
        assert reconciled["phase"] == "completed"
        assert runtime.tool_gateway.reserved_outbound_count == 0
    finally:
        runtime.close()


def test_cached_result_busy_only_when_waiter_or_mailbox_full() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created, provider_context = _accepted_action(runtime.tool_gateway)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            _terminal_event(provider_context),
            received_at=time.monotonic(),
        )
        auto_lookups = [
            message
            for message in _drain_outbound(runtime.tool_gateway)
            if message.envelope.message_type == "tool.result.request"
        ]
        assert len(auto_lookups) == 1

        first_caller = _caller_result_request(created, "waiter-first")
        runtime.tool_gateway.handle_input(
            "tool_request", first_caller, received_at=time.monotonic()
        )
        first_attached = _drain_outbound(runtime.tool_gateway)
        first_busy = [
            error_from_payload(message.envelope.payload).code
            for message in first_attached
            if message.envelope.message_type == "tool.error"
        ]
        assert "FORGE_TOOL_GATEWAY_BUSY" not in first_busy, (
            "the first caller must attach to the waiter-less automatic lookup: "
            f"{first_busy}"
        )
        assert runtime.tool_gateway.reserved_outbound_count == 1

        second_caller = _caller_result_request(created, "waiter-second")
        runtime.tool_gateway.handle_input(
            "tool_request", second_caller, received_at=time.monotonic()
        )
        second_outbound = _drain_outbound(runtime.tool_gateway)
        second_errors = [
            error_from_payload(message.envelope.payload)
            for message in second_outbound
            if message.envelope.message_type == "tool.error"
        ]
        assert len(second_errors) == 1
        assert second_errors[0].code == "FORGE_TOOL_GATEWAY_BUSY"
        assert second_errors[0].retryable is True
        assert second_outbound[0].envelope.request_id == second_caller.request_id

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(status="succeeded", outputs={"waiter": 1}),
                ),
                auto_lookups[0].envelope,
            ),
            received_at=time.monotonic(),
        )
        responses = [
            message
            for message in _drain_outbound(runtime.tool_gateway)
            if message.envelope.message_type == "tool.result.response"
        ]
        assert len(responses) == 1
        assert responses[0].envelope.request_id == first_caller.request_id
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["phase"] == "completed"
        assert runtime.tool_gateway.reserved_outbound_count == 0
    finally:
        runtime.close()

    service = ToolGatewayService(_config().tools, outbound_capacity=1)
    fake_runtime = SimpleNamespace(tool_gateway=service)
    try:
        _register(fake_runtime)  # type: ignore[arg-type]
        created, provider_context = _accepted_action(service)
        service.handle_input(
            "arm/to_gateway",
            _terminal_event(provider_context),
            received_at=time.monotonic(),
        )
        assert service.outbound_count == 1

        blocked_caller = _caller_result_request(created, "mailbox-blocked")
        with pytest.raises(ToolGatewayMailboxFull):
            service.handle_input(
                "tool_request", blocked_caller, received_at=time.monotonic()
            )
        assert service.reserved_outbound_count == 0

        drained = _drain_outbound(service)
        assert len(drained) == 1
        lookups = [
            message
            for message in drained
            if message.envelope.message_type == "tool.result.request"
        ]
        assert len(lookups) == 1

        retry_caller = _caller_result_request(created, "mailbox-retry")
        service.handle_input(
            "tool_request", retry_caller, received_at=time.monotonic()
        )
        retry_outbound = _drain_outbound(service)
        retry_busy = [
            error_from_payload(message.envelope.payload).code
            for message in retry_outbound
            if message.envelope.message_type == "tool.error"
        ]
        assert "FORGE_TOOL_GATEWAY_BUSY" not in retry_busy, (
            "a mailbox-full failure must not partially reserve or mark the "
            f"exchange as having a waiter: {retry_busy}"
        )
        assert service.reserved_outbound_count == 1

        service.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(status="succeeded", outputs={"retry": 1}),
                ),
                lookups[0].envelope,
            ),
            received_at=time.monotonic(),
        )
        responses = [
            message
            for message in _drain_outbound(service)
            if message.envelope.message_type == "tool.result.response"
        ]
        assert len(responses) == 1
        assert responses[0].envelope.request_id == retry_caller.request_id
        assert service.reserved_outbound_count == 0
    finally:
        service.begin_close()


def test_http_retained_unknown_result_lookup_is_not_triggered() -> None:
    """HTTP serves any retained result, including unknown, without a lookup."""
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        _register(runtime)
        created, _ = _accepted_action(runtime.tool_gateway)
        unknown = ToolResult(
            status="unknown",
            outputs={},
            error=ToolError(
                code="FORGE_TOOL_EXECUTION_OUTCOME_UNKNOWN",
                message="provider outcome is unknown",
            ),
        )
        _set_cached_result(runtime.tool_gateway, created, "unknown", unknown)
        assert _drain_outbound(runtime.tool_gateway) == []

        response = client.get(
            f"/invocations/{created['invocation_id']}/result"
        )

        assert response.status_code == 200
        body = response.json()["data"]
        assert body["status"] == "available"
        assert body["result"]["status"] == "unknown"
        assert _drain_outbound(runtime.tool_gateway) == [], (
            "HTTP must not queue a provider lookup for a retained unknown result"
        )
    finally:
        runtime.close()


def test_http_pending_result_lookup_full_outbound_returns_202() -> None:
    """A full outbound mailbox suppresses refresh failure on the HTTP route."""
    service = ToolGatewayService(_config().tools, outbound_capacity=1)
    fake_runtime = SimpleNamespace(tool_gateway=service)
    app = FastAPI()
    register_tool_routes(app, fake_runtime)
    client = TestClient(app)
    try:
        _register(fake_runtime)  # type: ignore[arg-type]
        created, _ = _accepted_action(service)
        service.submit_http_action("robot.pick", {})
        assert service.outbound_count == 1

        response = client.get(
            f"/invocations/{created['invocation_id']}/result"
        )

        assert response.status_code == 202
        assert response.json()["data"]["status"] == "pending"
        assert service.outbound_count == 1
        assert service.action_result_snapshot(created["invocation_id"]) == {
            "status": "pending"
        }
    finally:
        service.begin_close()
