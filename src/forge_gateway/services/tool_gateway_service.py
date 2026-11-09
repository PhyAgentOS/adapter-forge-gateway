"""Gateway-owned Tool discovery and Query invocation application service."""

from __future__ import annotations

import math
import threading
import time
import uuid
from collections import deque
from collections.abc import Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Literal

from forge_tool import (
    TOOL_ENDPOINT_PROTOCOL,
    ToolAccepted,
    ToolContext,
    ToolEnvelope,
    ToolError,
    ToolExecutionKey,
    ToolRequest,
    ToolResult,
    ToolResultResponse,
    control_response_from_payload,
    endpoint_descriptor_to_payload,
    endpoint_status_from_envelope,
    error_from_payload,
    error_to_payload,
    event_from_payload,
    invoke_request_from_envelope,
    invoke_request_to_payload,
    invoke_response_from_payload,
    make_endpoint_registry_response_envelope,
    make_invoke_request_envelope,
    make_result_response_envelope,
    result_response_from_payload,
    status_response_from_payload,
    validate_message_envelope,
    validate_response_correlation,
)

from forge_gateway.config import ToolGatewayConfig, ToolProviderRouteConfig
from forge_gateway.domain.action_invocations import (
    TERMINAL_PHASES,
    ActionInvocation,
    ActionInvocationCapacityError,
    ActionInvocationStore,
    ActionKey,
)
from forge_gateway.domain.tool_catalog import ToolSpec, ToolSpecCatalog
from forge_gateway.domain.tool_directory import (
    EndpointDirectory,
    RegisteredEndpoint,
    ToolOperationNotFoundError,
    ToolProviderRouteIdentity,
)

_MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991
DEFAULT_TOOL_OUTBOUND_CAPACITY = 256

LogicalToolMessageType = Literal[
    "tool.invoke.request",
    "tool.invoke.response",
    "tool.status.response",
    "tool.result.response",
    "tool.control.response",
    "tool.error",
]
ToolOutboundKind = Literal[
    "provider.registry_response",
    "provider.invoke_request",
    "caller.invoke_response",
    "provider.action_request",
    "caller.action_response",
]
CorrelationKey = tuple[str, str, str, str, str | None, str]


class ToolGatewayUnavailable(RuntimeError):
    """The Tool Gateway cannot admit a new invocation."""


class ToolGatewayMailboxFull(ToolGatewayUnavailable):
    """The bounded Tool outbound mailbox has no remaining capacity."""


@dataclass(frozen=True)
class ToolOutboundMessage:
    """One envelope awaiting lifecycle-thread Dora dispatch."""

    output_id: str
    envelope: ToolEnvelope
    kind: ToolOutboundKind
    pending_key: CorrelationKey | None = None
    pending_token: str | None = None


@dataclass(frozen=True)
class ToolInvocationTicket:
    """Thread-safe completion handle returned to an HTTP caller."""

    request: ToolEnvelope
    future: Future[ToolEnvelope]
    pending_key: CorrelationKey | None
    deadline: float


@dataclass(frozen=True)
class ToolGatewaySweep:
    """State removed by one monotonic service sweep."""

    expired_registrations: tuple[RegisteredEndpoint, ...]
    timed_out_invocations: int


@dataclass
class _PendingInvocation:
    logical_request: ToolEnvelope
    provider_request: ToolEnvelope
    route: ToolProviderRouteIdentity
    logical_key: CorrelationKey
    provider_key: CorrelationKey
    pending_token: str
    deadline: float
    future: Future[ToolEnvelope] | None
    terminal_slot_reserved: bool
    dispatch_claimed: bool = False


@dataclass
class _ActionExchange:
    execution_key: ActionKey
    provider_request: ToolEnvelope
    route: ToolProviderRouteIdentity
    provider_key: CorrelationKey
    purpose: str
    deadline: float
    logical_request: ToolEnvelope | None = None
    caller_response_reserved: bool = False
    dispatch_claimed: bool = False


def _logical_envelope(
    *,
    message_type: LogicalToolMessageType,
    payload: Mapping[str, Any],
    request_id: str,
    invocation_id: str,
    attempt_id: str,
    endpoint_id: str,
    operation: str,
) -> ToolEnvelope:
    """Construct a validated instance-less public Gateway envelope."""
    return ToolEnvelope(
        protocol=TOOL_ENDPOINT_PROTOCOL,
        message_type=message_type,  # type: ignore[arg-type]
        request_id=request_id,
        invocation_id=invocation_id,
        attempt_id=attempt_id,
        endpoint_id=endpoint_id,
        endpoint_instance_id=None,
        operation=operation,
        sequence=None,
        payload=dict(payload),
    )


def make_logical_invoke_request_envelope(
    request: ToolRequest,
    context: ToolContext,
    *,
    request_id: str,
) -> ToolEnvelope:
    """Build the public Gateway invoke shape, which never requires an instance."""
    return _logical_envelope(
        message_type="tool.invoke.request",
        request_id=request_id,
        invocation_id=context.invocation_id,
        attempt_id=context.attempt_id,
        endpoint_id=context.endpoint_id,
        operation=context.operation,
        payload=invoke_request_to_payload(request, context),
    )


def _logical_error_response(error: ToolError, request: ToolEnvelope) -> ToolEnvelope:
    assert request.request_id is not None
    assert request.invocation_id is not None
    assert request.attempt_id is not None
    assert request.endpoint_id is not None
    assert request.operation is not None
    if request.endpoint_instance_id is not None:
        raise ValueError("logical caller request must not contain endpoint_instance_id")
    return _logical_envelope(
        message_type="tool.error",
        request_id=request.request_id,
        invocation_id=request.invocation_id,
        attempt_id=request.attempt_id,
        endpoint_id=request.endpoint_id,
        operation=request.operation,
        payload=error_to_payload(error),
    )


def _logical_provider_response(
    response: ToolEnvelope,
    request: ToolEnvelope,
) -> ToolEnvelope:
    assert request.request_id is not None
    assert request.invocation_id is not None
    assert request.attempt_id is not None
    assert request.endpoint_id is not None
    assert request.operation is not None
    if request.endpoint_instance_id is not None:
        raise ValueError("logical caller request must not contain endpoint_instance_id")
    return _logical_envelope(
        message_type=response.message_type,
        request_id=request.request_id,
        invocation_id=request.invocation_id,
        attempt_id=request.attempt_id,
        endpoint_id=request.endpoint_id,
        operation=request.operation,
        payload=response.payload,
    )


class ToolGatewayService:
    """Single caller-visible authority for Tool discovery and Query routing."""

    def __init__(
        self,
        config: ToolGatewayConfig,
        *,
        outbound_capacity: int = DEFAULT_TOOL_OUTBOUND_CAPACITY,
    ) -> None:
        if not isinstance(config, ToolGatewayConfig):
            raise TypeError("config must be a ToolGatewayConfig")
        if (
            isinstance(outbound_capacity, bool)
            or not isinstance(outbound_capacity, int)
            or outbound_capacity < 1
        ):
            raise ValueError("outbound_capacity must be a positive integer")
        self.config = config
        self._directory = EndpointDirectory(lease_ttl_ms=config.lease_ttl_ms)
        routes = (
            tuple(self._route_identity(provider) for provider in config.providers)
            if config.enabled
            else ()
        )
        self._routes_by_endpoint_id = {route.endpoint_id: route for route in routes}
        self._routes_by_input_id = {route.input_id: route for route in routes}
        self._lock = threading.RLock()
        self._accepting = config.enabled
        self._outbound_capacity = outbound_capacity
        self._outbound: deque[ToolOutboundMessage] = deque()
        self._reserved_outbound_slots = 0
        self._pending_by_provider_key: dict[CorrelationKey, _PendingInvocation] = {}
        self._pending_logical_keys: set[CorrelationKey] = set()
        self.catalog = ToolSpecCatalog(config.specs)
        self.action_invocations = ActionInvocationStore(
            capacity=config.invocation_capacity,
            event_capacity=config.event_capacity,
            retention_seconds=config.retention_sec,
        )
        self._action_exchanges: dict[CorrelationKey, _ActionExchange] = {}
        self._endpoint_statuses: dict[tuple[str, str], dict[str, Any]] = {}
        self._dora_action_event_cursors: dict[ActionKey, int] = {}

    @staticmethod
    def _route_identity(provider: ToolProviderRouteConfig) -> ToolProviderRouteIdentity:
        return ToolProviderRouteIdentity(
            endpoint_id=provider.endpoint_id,
            input_id=provider.input_id,
            output_id=provider.output_id,
        )

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    @property
    def provider_input_ids(self) -> frozenset[str]:
        if not self.config.enabled:
            return frozenset()
        return frozenset(self._routes_by_input_id)

    @property
    def caller_input_ids(self) -> frozenset[str]:
        if not self.config.enabled:
            return frozenset()
        return frozenset((self.config.request_input_id,))

    @property
    def input_ids(self) -> frozenset[str]:
        if not self.config.enabled:
            return frozenset()
        return self.provider_input_ids | self.caller_input_ids

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending_by_provider_key)

    @property
    def outbound_capacity(self) -> int:
        return self._outbound_capacity

    @property
    def lease_ttl_ms(self) -> int:
        return self._directory.lease_ttl_ms

    @property
    def registry_revision(self) -> int:
        return self._directory.revision

    def endpoint_registrations(
        self,
        *,
        now: float | None = None,
    ) -> tuple[RegisteredEndpoint, ...]:
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            expired = self._directory.expire(observation)
            _ = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=observation,
            )
            return self._directory.registrations(now=observation)

    def resolve_registered_endpoint(
        self,
        endpoint_id: str,
        operation: str,
        *,
        now: float | None = None,
    ) -> RegisteredEndpoint | None:
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            expired = self._directory.expire(observation)
            _ = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=observation,
            )
            return self._directory.resolve(endpoint_id, operation, now=observation)

    @property
    def outbound_count(self) -> int:
        with self._lock:
            return len(self._outbound)

    @property
    def reserved_outbound_count(self) -> int:
        with self._lock:
            return self._reserved_outbound_slots

    @staticmethod
    def _correlation_key(envelope: ToolEnvelope) -> CorrelationKey:
        request_id = envelope.request_id
        invocation_id = envelope.invocation_id
        attempt_id = envelope.attempt_id
        endpoint_id = envelope.endpoint_id
        operation = envelope.operation
        if None in (request_id, invocation_id, attempt_id, endpoint_id, operation):
            raise ValueError("invoke envelope is missing correlation identity")
        assert request_id is not None
        assert invocation_id is not None
        assert attempt_id is not None
        assert endpoint_id is not None
        assert operation is not None
        return (
            request_id,
            invocation_id,
            attempt_id,
            endpoint_id,
            envelope.endpoint_instance_id,
            operation,
        )

    @staticmethod
    def _validate_now(now: float) -> float:
        if isinstance(now, bool) or not isinstance(now, (int, float)):
            raise ValueError("now must be a finite monotonic time")  # noqa: TRY004
        value = float(now)
        if not math.isfinite(value) or value < 0:
            raise ValueError("now must be a finite non-negative monotonic time")
        return value

    @staticmethod
    def _validate_timeout_ms(timeout_ms: int) -> int:
        if (
            isinstance(timeout_ms, bool)
            or not isinstance(timeout_ms, int)
            or not 1 <= timeout_ms <= _MAX_SAFE_JSON_INTEGER
        ):
            raise ValueError(f"timeout_ms must be in [1, {_MAX_SAFE_JSON_INTEGER}]")
        return timeout_ms

    def discovery_snapshot(self, *, now: float | None = None) -> dict[str, object]:
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            expired = self._directory.expire(observation)
            _ = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=observation,
            )
            snapshot = self._directory.snapshot(now=observation)
            return {
                "revision": snapshot.revision,
                "tools": [
                    {
                        "endpoint_id": registration.endpoint_id,
                        "descriptor": endpoint_descriptor_to_payload(
                            registration.descriptor
                        )["descriptor"],
                    }
                    for registration in snapshot.registrations
                ],
            }

    def tool_spec_snapshot(self, *, now: float | None = None) -> dict[str, object]:
        """Return configured ToolSpecs with current descriptor binding state."""
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            expired = self._directory.expire(observation)
            self._finish_unclaimed_for_removed_locked(
                expired, observed_at=observation
            )
            directory = self._directory.snapshot(now=observation)
            registrations = {
                registration.endpoint_id: registration
                for registration in directory.registrations
            }
            tools: list[dict[str, Any]] = []
            for spec in self.catalog.list():
                value = spec.to_dict()
                registration = registrations.get(spec.endpoint_id)
                if registration is None:
                    value["available"] = False
                else:
                    try:
                        self.catalog.validate_binding(spec, registration)
                        value["available"] = True
                    except ValueError as error:
                        value["available"] = False
                        value["binding_error"] = str(error)
                tools.append(value)
            return {"revision": directory.revision, "tools": tools}

    def get_tool_spec(self, tool_id: str) -> ToolSpec | None:
        return self.catalog.get(tool_id)

    def resolve_tool_spec(
        self, tool_id: str, *, now: float | None = None
    ) -> tuple[ToolSpec, RegisteredEndpoint]:
        spec = self.catalog.get(tool_id)
        if spec is None:
            raise KeyError(tool_id)
        registration = self.resolve_registered_endpoint(
            spec.endpoint_id, spec.operation, now=now
        )
        if registration is None:
            raise ToolGatewayUnavailable(
                f"endpoint {spec.endpoint_id!r} has no active provider instance"
            )
        self.catalog.validate_binding(spec, registration)
        return spec, registration

    def submit_http_action(
        self,
        tool_id: str,
        arguments: dict[str, object],
        *,
        caller_id: str | None = None,
        timeout_ms: int | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Create, pin and dispatch one Action invocation without waiting."""
        observation = time.monotonic() if now is None else self._validate_now(now)
        effective_timeout = self._validate_timeout_ms(
            self.config.invoke_timeout_ms if timeout_ms is None else timeout_ms
        )
        if effective_timeout > self.config.invoke_timeout_ms:
            raise ValueError(
                "timeout_ms must not exceed configured tools.invoke_timeout_ms "
                f"maximum {self.config.invoke_timeout_ms}"
            )
        spec, registration = self.resolve_tool_spec(tool_id, now=observation)
        if spec.semantics not in ("action", "session"):
            raise ValueError(f"ToolSpec {tool_id!r} is not an Action or Session")
        operation_descriptor = next(
            item
            for item in registration.descriptor.operations
            if item.name == spec.operation
        )
        identity = uuid.uuid4().hex
        invocation_id = f"gateway-{identity}"
        attempt_id = f"gateway-{identity}"
        # Session invocations are unbounded by design: deadline machinery
        # (mark_deadlines_ambiguous) must never pin them to `unknown`.
        # The bounded handshake exchange below keeps its own timeout, and
        # provider death mid-session is caught by lease-expiry ambiguity.
        deadline_ms = (
            None
            if spec.semantics == "session"
            else min(
                _MAX_SAFE_JSON_INTEGER,
                int(time.time() * 1_000) + effective_timeout,
            )
        )
        context = ToolContext(
            execution_key=ToolExecutionKey(invocation_id, attempt_id),
            tool_id=spec.tool_id,
            implementation_id=spec.implementation_id,
            endpoint_id=spec.endpoint_id,
            operation=spec.operation,
            caller_id=caller_id,
            deadline_ms=deadline_ms,
        )
        provider_request = make_invoke_request_envelope(
            ToolRequest(arguments),
            context,
            request_id=f"gateway-provider-{uuid.uuid4().hex}",
            endpoint_instance_id=registration.endpoint_instance_id,
        )
        invocation = ActionInvocation(
            invocation_id=invocation_id,
            attempt_id=attempt_id,
            tool_id=spec.tool_id,
            implementation_id=spec.implementation_id,
            endpoint_id=spec.endpoint_id,
            endpoint_instance_id=registration.endpoint_instance_id,
            operation=spec.operation,
            caller_id=caller_id,
            deadline_ms=deadline_ms,
            created_at=time.time(),
        )
        with self._lock:
            self._ensure_accepting_locked()
            current = self._directory.resolve(
                spec.endpoint_id, spec.operation, now=observation
            )
            if (
                current is None
                or current.endpoint_instance_id != registration.endpoint_instance_id
            ):
                raise ToolGatewayUnavailable(
                    "provider instance changed during Action admission"
                )
            self._reserve_outbound_slots_locked()
            try:
                self.action_invocations.create(
                    invocation,
                    now=observation,
                    operation_capacity=operation_descriptor.max_concurrency,
                )
                self._queue_action_exchange_locked(
                    invocation.key,
                    provider_request,
                    registration.route,
                    purpose="invoke",
                    deadline=observation + effective_timeout / 1_000,
                )
            except BaseException:
                self._release_outbound_slots_locked()
                self.action_invocations.discard(invocation.key)
                raise
        snapshot = self.action_invocations.snapshot(invocation.key, now=observation)
        assert snapshot is not None
        return snapshot

    def _http_action_key(self, invocation_id: str, *, now: float) -> ActionKey | None:
        return self.action_invocations.resolve_http_key(invocation_id, now=now)

    def action_invocation_snapshot(
        self, invocation_id: str, *, now: float | None = None
    ) -> dict[str, Any] | None:
        observation = time.monotonic() if now is None else self._validate_now(now)
        key = self._http_action_key(invocation_id, now=observation)
        return (
            None
            if key is None
            else self.action_invocations.snapshot(key, now=observation)
        )

    def action_result_snapshot(
        self, invocation_id: str, *, now: float | None = None
    ) -> dict[str, Any] | None:
        observation = time.monotonic() if now is None else self._validate_now(now)
        key = self._http_action_key(invocation_id, now=observation)
        return (
            None
            if key is None
            else self.action_invocations.result_snapshot(key, now=observation)
        )

    def cancel_action(self, invocation_id: str, *, now: float | None = None) -> str:
        """Request provider cancellation; acceptance never implies termination."""
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            key = self._http_action_key(invocation_id, now=observation)
            if key is None:
                raise KeyError(invocation_id)
            item = self.action_invocations.get(key, now=observation)
            if item is None:
                raise KeyError(invocation_id)
            if item.phase in TERMINAL_PHASES and not (
                item.phase == "unknown"
                and item.occupies_concurrency
                and not item.terminal_event_seen
            ):
                return "terminal"
            if item.cancel_status in (
                "requested",
                "accepted",
                "terminal",
                "unsupported",
            ):
                return item.cancel_status
            if any(
                exchange.execution_key == key and exchange.purpose == "cancel"
                for exchange in self._action_exchanges.values()
            ):
                return item.cancel_status or "requested"
            expired = self._directory.expire(observation)
            self._finish_unclaimed_for_removed_locked(
                expired, observed_at=observation
            )
            registration = self._directory.resolve(
                item.endpoint_id, item.operation, now=observation
            )
            if (
                registration is None
                or registration.endpoint_instance_id != item.endpoint_instance_id
            ):
                self.action_invocations.mark_instance_ambiguous(
                    item.endpoint_id,
                    item.endpoint_instance_id,
                    now=observation,
                    reason="pinned provider instance is no longer current",
                )
                return "unknown"
            operation = next(
                entry
                for entry in registration.descriptor.operations
                if entry.name == item.operation
            )
            if not operation.cancellable:
                return "unsupported"
            request = ToolEnvelope(
                protocol=TOOL_ENDPOINT_PROTOCOL,
                message_type="tool.control.request",
                request_id=f"gateway-provider-{uuid.uuid4().hex}",
                invocation_id=item.invocation_id,
                attempt_id=item.attempt_id,
                endpoint_id=item.endpoint_id,
                endpoint_instance_id=item.endpoint_instance_id,
                operation=item.operation,
                payload={"command": "cancel"},
            )
            self._reserve_outbound_slots_locked()
            try:
                self.action_invocations.set_cancel_status(key, "requested")
                self._queue_action_exchange_locked(
                    key,
                    request,
                    registration.route,
                    purpose="cancel",
                    deadline=observation + self.config.invoke_timeout_ms / 1_000,
                )
            except BaseException:
                self._release_outbound_slots_locked()
                self.action_invocations.set_cancel_status(key, "transport_error")
                raise
        return "requested"

    def stop_action(self, invocation_id: str, *, now: float | None = None) -> str:
        """Request provider stop; acceptance never implies termination."""
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            key = self._http_action_key(invocation_id, now=observation)
            if key is None:
                raise KeyError(invocation_id)
            item = self.action_invocations.get(key, now=observation)
            if item is None:
                raise KeyError(invocation_id)
            if item.phase in TERMINAL_PHASES:
                return "terminal"
            if item.stop_status in (
                "requested",
                "accepted",
                "terminal",
                "unsupported",
            ):
                return item.stop_status
            if any(
                exchange.execution_key == key and exchange.purpose == "stop"
                for exchange in self._action_exchanges.values()
            ):
                return item.stop_status or "requested"
            expired = self._directory.expire(observation)
            self._finish_unclaimed_for_removed_locked(
                expired, observed_at=observation
            )
            registration = self._directory.resolve(
                item.endpoint_id, item.operation, now=observation
            )
            if (
                registration is None
                or registration.endpoint_instance_id != item.endpoint_instance_id
            ):
                self.action_invocations.mark_instance_ambiguous(
                    item.endpoint_id,
                    item.endpoint_instance_id,
                    now=observation,
                    reason="pinned provider instance is no longer current",
                )
                return "unknown"
            operation = next(
                entry
                for entry in registration.descriptor.operations
                if entry.name == item.operation
            )
            if not operation.stoppable:
                return "unsupported"
            request = ToolEnvelope(
                protocol=TOOL_ENDPOINT_PROTOCOL,
                message_type="tool.control.request",
                request_id=f"gateway-provider-{uuid.uuid4().hex}",
                invocation_id=item.invocation_id,
                attempt_id=item.attempt_id,
                endpoint_id=item.endpoint_id,
                endpoint_instance_id=item.endpoint_instance_id,
                operation=item.operation,
                payload={"command": "stop"},
            )
            self._reserve_outbound_slots_locked()
            try:
                self.action_invocations.set_stop_status(key, "requested")
                self._queue_action_exchange_locked(
                    key,
                    request,
                    registration.route,
                    purpose="stop",
                    deadline=observation + self.config.invoke_timeout_ms / 1_000,
                )
            except BaseException:
                self._release_outbound_slots_locked()
                self.action_invocations.set_stop_status(key, "transport_error")
                raise
        return "requested"

    def refresh_action(
        self,
        invocation_id: str,
        purpose: Literal["status", "result"],
        *,
        now: float | None = None,
    ) -> bool:
        """Queue one bounded provider lookup for a retained pinned Action."""
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            key = self._http_action_key(invocation_id, now=observation)
            if key is None:
                raise KeyError(invocation_id)
            return self._refresh_action_key_locked(key, purpose, now=observation)

    def _refresh_action_key_locked(
        self,
        key: ActionKey,
        purpose: Literal["status", "result"],
        *,
        now: float,
        logical_request: ToolEnvelope | None = None,
        caller_response_reserved: bool = False,
    ) -> bool:
        item = self.action_invocations.get(key, now=now)
        if item is None:
            raise KeyError(key)
        if any(
            exchange.execution_key == key and exchange.purpose == purpose
            for exchange in self._action_exchanges.values()
        ):
            return False
        expired = self._directory.expire(now)
        self._finish_unclaimed_for_removed_locked(expired, observed_at=now)
        registration = self._directory.resolve(
            item.endpoint_id, item.operation, now=now
        )
        if (
            registration is None
            or registration.endpoint_instance_id != item.endpoint_instance_id
        ):
            self.action_invocations.mark_instance_ambiguous(
                item.endpoint_id,
                item.endpoint_instance_id,
                now=now,
                reason="pinned provider instance is no longer current",
            )
            return False
        request = ToolEnvelope(
            protocol=TOOL_ENDPOINT_PROTOCOL,
            message_type=f"tool.{purpose}.request",  # type: ignore[arg-type]
            request_id=f"gateway-provider-{uuid.uuid4().hex}",
            invocation_id=item.invocation_id,
            attempt_id=item.attempt_id,
            endpoint_id=item.endpoint_id,
            endpoint_instance_id=item.endpoint_instance_id,
            operation=item.operation,
            payload={},
        )
        reserve_count = 2 if caller_response_reserved else 1
        self._reserve_outbound_slots_locked(reserve_count)
        try:
            self._queue_action_exchange_locked(
                key,
                request,
                registration.route,
                purpose=purpose,
                deadline=now + self.config.invoke_timeout_ms / 1_000,
                logical_request=logical_request,
                caller_response_reserved=caller_response_reserved,
            )
        except BaseException:
            self._release_outbound_slots_locked(reserve_count)
            raise
        return True

    def _queue_action_exchange_locked(
        self,
        execution_key: ActionKey,
        request: ToolEnvelope,
        route: ToolProviderRouteIdentity,
        *,
        purpose: str,
        deadline: float,
        logical_request: ToolEnvelope | None = None,
        caller_response_reserved: bool = False,
    ) -> None:
        key = self._correlation_key(request)
        if len(self._action_exchanges) >= self._outbound_capacity:
            raise ToolGatewayMailboxFull(
                "Action exchange capacity is exhausted"
            )
        if key in self._action_exchanges:
            raise ValueError("Action provider correlation identity is already pending")
        self._action_exchanges[key] = _ActionExchange(
            execution_key=execution_key,
            provider_request=request,
            route=route,
            provider_key=key,
            purpose=purpose,
            deadline=deadline,
            logical_request=logical_request,
            caller_response_reserved=caller_response_reserved,
        )
        self._append_reserved_outbound_locked(
            ToolOutboundMessage(
                output_id=route.output_id,
                envelope=request,
                kind="provider.action_request",
                pending_key=key,
            )
        )

    def handle_input(
        self,
        input_id: str,
        envelope: ToolEnvelope,
        *,
        received_at: float,
        processed_at: float | None = None,
    ) -> None:
        """Consume one decoded provider or public caller envelope.

        Valid late or duplicate provider invoke responses are quietly ignored.
        """
        if not isinstance(envelope, ToolEnvelope):
            raise TypeError("envelope must be a ToolEnvelope")
        receive_observation = self._validate_now(received_at)
        process_observation = (
            receive_observation
            if processed_at is None
            else self._validate_now(processed_at)
        )
        with self._lock:
            route = self._routes_by_input_id.get(input_id)
            if route is not None and envelope.message_type == "tool.event":
                self._handle_action_event_locked(route, envelope)
                return
            if route is not None and envelope.message_type == "endpoint.status":
                self._handle_endpoint_status_locked(
                    route, envelope, observed_at=receive_observation
                )
                return
            if (
                route is not None
                and envelope.message_type
                in (
                    "tool.invoke.response",
                    "tool.status.response",
                    "tool.result.response",
                    "tool.control.response",
                    "tool.error",
                )
                and self._correlation_key(envelope) in self._action_exchanges
            ):
                self._handle_action_response_locked(
                    route, envelope, processed_at=process_observation
                )
                return
            if route is not None and envelope.message_type in (
                "tool.invoke.response",
                "tool.error",
            ):
                _ = self._handle_provider_response_locked(
                    route,
                    envelope,
                    processed_at=process_observation,
                )
                return
            self._ensure_accepting_locked()
            if route is not None:
                self._handle_provider_input_locked(
                    route,
                    envelope,
                    received_at=receive_observation,
                    processed_at=process_observation,
                )
                return
            if input_id == self.config.request_input_id:
                if envelope.message_type == "tool.invoke.request":
                    self._handle_caller_invoke_locked(
                        envelope,
                        processed_at=process_observation,
                    )
                elif envelope.message_type in (
                    "tool.status.request",
                    "tool.result.request",
                    "tool.control.request",
                ):
                    self._handle_caller_action_request_locked(envelope)
                else:
                    raise ValueError(
                        "public Tool input accepts "
                        "invoke/status/result/control requests"
                    )
                return
            raise ValueError(f"unconfigured Tool input: {input_id!r}")

    def _handle_provider_input_locked(
        self,
        route: ToolProviderRouteIdentity,
        envelope: ToolEnvelope,
        *,
        received_at: float,
        processed_at: float,
    ) -> None:
        if envelope.message_type in ("endpoint.register", "endpoint.unregister"):
            self._reserve_outbound_slots_locked()
            reserved = True
            try:
                if envelope.message_type == "endpoint.register":
                    mutation = self._directory.register_with_change(
                        envelope,
                        route,
                        now=received_at,
                    )
                else:
                    mutation = self._directory.unregister_with_change(
                        envelope,
                        route,
                        now=received_at,
                    )
                self._append_reserved_outbound_locked(
                    ToolOutboundMessage(
                        output_id=route.output_id,
                        envelope=make_endpoint_registry_response_envelope(
                            mutation.response,
                            envelope,
                        ),
                        kind="provider.registry_response",
                    )
                )
                reserved = False
                _ = self._finish_unclaimed_for_removed_locked(
                    mutation.removed,
                    observed_at=processed_at,
                )
            finally:
                if reserved:
                    self._release_outbound_slots_locked()
            return
        if envelope.message_type in ("tool.invoke.response", "tool.error"):
            _ = self._handle_provider_response_locked(
                route,
                envelope,
                processed_at=processed_at,
            )
            return
        if envelope.message_type in (
            "tool.status.response",
            "tool.result.response",
            "tool.control.response",
        ):
            self._handle_action_response_locked(
                route, envelope, processed_at=processed_at
            )
            return
        if envelope.message_type == "tool.event":
            self._handle_action_event_locked(route, envelope)
            return
        if envelope.message_type == "endpoint.status":
            self._handle_endpoint_status_locked(
                route, envelope, observed_at=received_at
            )
            return
        raise ValueError(
            "provider input accepts only endpoint.register, endpoint.unregister, "
            "endpoint.status, Tool responses/events, or tool.error"
        )

    def _handle_caller_invoke_locked(
        self,
        request: ToolEnvelope,
        *,
        processed_at: float,
    ) -> None:
        if request.message_type != "tool.invoke.request":
            raise ValueError("public Tool input accepts only tool.invoke.request")
        if request.endpoint_instance_id is not None:
            raise ValueError(
                "public Tool invoke must omit endpoint_instance_id; "
                "Gateway owns provider instance routing"
            )
        validate_message_envelope(request)
        _, context = invoke_request_from_envelope(request)
        deadline = processed_at + self.config.invoke_timeout_ms / 1_000
        if context.deadline_ms is not None:
            remaining_ms = context.deadline_ms - int(time.time() * 1_000)
            absolute_deadline = processed_at + remaining_ms / 1_000
            deadline = min(deadline, absolute_deadline)
        expired = self._directory.expire(processed_at)
        self._finish_unclaimed_for_removed_locked(
            expired, observed_at=processed_at
        )
        try:
            registration = self._directory.resolve(
                context.endpoint_id, context.operation, now=processed_at
            )
        except ToolOperationNotFoundError:
            registration = None
        operation_descriptor = (
            None
            if registration is None
            else next(
                item
                for item in registration.descriptor.operations
                if item.name == context.operation
            )
        )
        if (
            operation_descriptor is not None
            and operation_descriptor.semantics in ("action", "session")
        ):
            self._admit_dora_action_locked(
                request, context, registration, processed_at=processed_at
            )
            return
        _ = self._admit_invoke_locked(
            request,
            processed_at=processed_at,
            deadline=deadline,
            future=None,
        )

    def _admit_dora_action_locked(
        self,
        request: ToolEnvelope,
        context: ToolContext,
        registration: RegisteredEndpoint,
        *,
        processed_at: float,
    ) -> None:
        # Reserve the caller terminal slot first, so every expected rejection can
        # be returned even when no provider-request slot remains.
        self._reserve_outbound_slots_locked()
        provider_reserved = False
        invocation_created = False
        try:
            spec = self.catalog.get(context.tool_id)
            if spec is None:
                raise KeyError(f"ToolSpec {context.tool_id!r} is not configured")
            if (
                spec.implementation_id != context.implementation_id
                or spec.endpoint_id != context.endpoint_id
                or spec.operation != context.operation
                or spec.semantics not in ("action", "session")
            ):
                raise ValueError(
                    "Dora Action or Session context does not exactly match its "
                    "configured ToolSpec"
                )
            self.catalog.validate_binding(spec, registration)
            provider_request = make_invoke_request_envelope(
                ToolRequest(invoke_request_from_envelope(request)[0].arguments),
                context,
                request_id=f"gateway-provider-{uuid.uuid4().hex}",
                endpoint_instance_id=registration.endpoint_instance_id,
            )
            invocation = ActionInvocation(
                invocation_id=context.invocation_id,
                attempt_id=context.attempt_id,
                tool_id=context.tool_id,
                implementation_id=context.implementation_id,
                endpoint_id=context.endpoint_id,
                endpoint_instance_id=registration.endpoint_instance_id,
                operation=context.operation,
                caller_id=context.caller_id,
                deadline_ms=context.deadline_ms,
                created_at=time.time(),
            )
            operation_descriptor = next(
                item
                for item in registration.descriptor.operations
                if item.name == context.operation
            )
            self.action_invocations.create(
                invocation,
                now=processed_at,
                operation_capacity=operation_descriptor.max_concurrency,
            )
            invocation_created = True
            self._dora_action_event_cursors[invocation.key] = -1
            self._reserve_outbound_slots_locked()
            provider_reserved = True
            self._queue_action_exchange_locked(
                invocation.key,
                provider_request,
                registration.route,
                purpose="invoke",
                deadline=processed_at + self.config.invoke_timeout_ms / 1_000,
                logical_request=request,
                caller_response_reserved=True,
            )
            provider_reserved = False
        except (
            KeyError,
            ValueError,
            ActionInvocationCapacityError,
            ToolGatewayUnavailable,
        ) as error:
            if provider_reserved:
                self._release_outbound_slots_locked()
            key = (context.invocation_id, context.attempt_id)
            if invocation_created:
                self.action_invocations.discard(key)
                self._dora_action_event_cursors.pop(key, None)
            tool_error = self._tool_error(
                (
                    "FORGE_TOOL_GATEWAY_BUSY"
                    if isinstance(
                        error,
                        (ActionInvocationCapacityError, ToolGatewayMailboxFull),
                    )
                    else "FORGE_TOOL_ACTION_ADMISSION_REJECTED"
                ),
                str(error),
                retryable=isinstance(
                    error, (ActionInvocationCapacityError, ToolGatewayUnavailable)
                ),
            )
            self._append_reserved_outbound_locked(
                ToolOutboundMessage(
                    output_id=self.config.response_output_id,
                    envelope=_logical_error_response(tool_error, request),
                    kind="caller.action_response",
                )
            )
        except BaseException:
            if provider_reserved:
                self._release_outbound_slots_locked()
            self._release_outbound_slots_locked()
            key = (context.invocation_id, context.attempt_id)
            if invocation_created:
                self.action_invocations.discard(key)
                self._dora_action_event_cursors.pop(key, None)
            raise

    def _handle_caller_action_request_locked(self, request: ToolEnvelope) -> None:
        if request.endpoint_instance_id is not None:
            raise ValueError("public Tool requests must omit endpoint_instance_id")
        validate_message_envelope(request)
        assert request.invocation_id is not None
        assert request.attempt_id is not None
        now = time.monotonic()
        key = (request.invocation_id, request.attempt_id)
        item = self.action_invocations.get(key, now=now)
        if item is None:
            self._respond_logical_action_error_locked(
                request,
                self._tool_error(
                    "FORGE_TOOL_ACTION_NOT_FOUND",
                    "Action ToolExecutionKey is not retained",
                ),
            )
            return
        if (
            request.endpoint_id != item.endpoint_id
            or request.operation != item.operation
        ):
            self._respond_logical_action_error_locked(
                request,
                self._tool_error(
                    "FORGE_TOOL_REQUEST_CONFLICT",
                    "public request does not match the retained Action",
                ),
            )
            return
        if request.message_type in ("tool.status.request", "tool.result.request"):
            purpose = (
                "status" if request.message_type == "tool.status.request" else "result"
            )
            # A retained terminal result is authoritative.  Serve it directly
            # without creating a provider lookup or an exchange.  ``unknown`` is
            # deliberately excluded: only a late terminal provider result may
            # reconcile it.
            if (
                purpose == "result"
                and item.result is not None
                and item.result.status != "unknown"
                and item.phase in ("completed", "failed", "cancelled", "stopped")
            ):
                direct_response = make_result_response_envelope(
                    ToolResultResponse(status="available", result=item.result),
                    request,
                )
                self._reserve_outbound_slots_locked()
                self._append_reserved_outbound_locked(
                    ToolOutboundMessage(
                        output_id=self.config.response_output_id,
                        envelope=direct_response,
                        kind="caller.action_response",
                    )
                )
                return

            existing = next(
                (
                    exchange
                    for exchange in self._action_exchanges.values()
                    if exchange.execution_key == key and exchange.purpose == purpose
                ),
                None,
            )
            if existing is not None:
                if (
                    existing.logical_request is not None
                    or existing.caller_response_reserved
                ):
                    self._respond_logical_action_error_locked(
                        request,
                        self._tool_error(
                            "FORGE_TOOL_GATEWAY_BUSY",
                            f"an Action {purpose} lookup is already pending",
                            retryable=True,
                        ),
                    )
                    return
                # A terminal event may have started an automatic, waiter-less
                # result lookup.  Reserve one caller response slot, then attach
                # to that same lookup; never enqueue a second provider request.
                self._reserve_outbound_slots_locked()
                existing.logical_request = request
                existing.caller_response_reserved = True
                return

            try:
                queued = self._refresh_action_key_locked(
                    key,
                    purpose,
                    now=now,
                    logical_request=request,
                    caller_response_reserved=True,
                )
            except ToolGatewayMailboxFull as error:
                # Exchange-table capacity can be full while the outbound
                # mailbox still has room for a retryable BUSY response.  If the
                # outbound mailbox itself is full, writing BUSY also fails with
                # ToolGatewayMailboxFull, preserving the bounded-mailbox
                # backpressure contract.
                self._respond_logical_action_error_locked(
                    request,
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_BUSY",
                        str(error),
                        retryable=True,
                    ),
                )
                return
            except (RuntimeError, ValueError) as error:
                self._respond_logical_action_error_locked(
                    request,
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_UNAVAILABLE",
                        str(error),
                        retryable=True,
                    ),
                )
                return
            if not queued:
                self._respond_logical_action_error_locked(
                    request,
                    self._tool_error(
                        "FORGE_TOOL_EXECUTION_OUTCOME_UNKNOWN",
                        "pinned provider instance is no longer current",
                    ),
                )
            return

        command = request.payload.get("command")
        expired = self._directory.expire(now)
        self._finish_unclaimed_for_removed_locked(expired, observed_at=now)
        registration = self._directory.resolve(
            item.endpoint_id, item.operation, now=now
        )
        if (
            registration is None
            or registration.endpoint_instance_id != item.endpoint_instance_id
        ):
            self.action_invocations.mark_instance_ambiguous(
                item.endpoint_id,
                item.endpoint_instance_id,
                now=now,
                reason="pinned provider instance is no longer current",
            )
            self._respond_logical_action_error_locked(
                request,
                self._tool_error(
                    "FORGE_TOOL_EXECUTION_OUTCOME_UNKNOWN",
                    "pinned provider instance is no longer current",
                ),
            )
            return
        operation = next(
            entry
            for entry in registration.descriptor.operations
            if entry.name == item.operation
        )
        if (command == "cancel" and not operation.cancellable) or (
            command == "stop" and not operation.stoppable
        ):
            self._respond_logical_action_error_locked(
                request,
                self._tool_error(
                    "FORGE_TOOL_CONTROL_UNSUPPORTED",
                    f"operation does not support {command}",
                ),
            )
            return
        purpose = f"control:{command}"
        if any(
            exchange.execution_key == key and exchange.purpose == purpose
            for exchange in self._action_exchanges.values()
        ):
            self._respond_logical_action_error_locked(
                request,
                self._tool_error(
                    "FORGE_TOOL_GATEWAY_BUSY",
                    f"an Action or Session {command} request is already pending",
                    retryable=True,
                ),
            )
            return
        provider_request = ToolEnvelope(
            protocol=request.protocol,
            message_type=request.message_type,
            request_id=f"gateway-provider-{uuid.uuid4().hex}",
            invocation_id=item.invocation_id,
            attempt_id=item.attempt_id,
            endpoint_id=item.endpoint_id,
            endpoint_instance_id=item.endpoint_instance_id,
            operation=item.operation,
            payload=dict(request.payload),
        )
        self._reserve_outbound_slots_locked()
        provider_reserved = False
        try:
            self._reserve_outbound_slots_locked()
            provider_reserved = True
            if command == "cancel":
                self.action_invocations.set_cancel_status(key, "requested")
            elif command == "stop":
                self.action_invocations.set_stop_status(key, "requested")
            self._queue_action_exchange_locked(
                key,
                provider_request,
                registration.route,
                purpose=purpose,
                deadline=now + self.config.invoke_timeout_ms / 1_000,
                logical_request=request,
                caller_response_reserved=True,
            )
            provider_reserved = False
        except (RuntimeError, ValueError) as error:
            if provider_reserved:
                self._release_outbound_slots_locked()
            if command == "cancel":
                self.action_invocations.set_cancel_status(key, "transport_error")
            elif command == "stop":
                self.action_invocations.set_stop_status(key, "transport_error")
            self._append_reserved_outbound_locked(
                ToolOutboundMessage(
                    output_id=self.config.response_output_id,
                    envelope=_logical_error_response(
                        self._tool_error(
                            "FORGE_TOOL_GATEWAY_BUSY",
                            str(error),
                            retryable=True,
                        ),
                        request,
                    ),
                    kind="caller.action_response",
                )
            )
        except BaseException:
            if provider_reserved:
                self._release_outbound_slots_locked()
            self._release_outbound_slots_locked()
            raise

    def _respond_logical_action_error_locked(
        self, request: ToolEnvelope, error: ToolError
    ) -> None:
        self._reserve_outbound_slots_locked()
        self._append_reserved_outbound_locked(
            ToolOutboundMessage(
                output_id=self.config.response_output_id,
                envelope=_logical_error_response(error, request),
                kind="caller.action_response",
            )
        )

    def submit_http_invoke(
        self,
        endpoint_id: str,
        operation: str,
        arguments: dict[str, object],
        *,
        caller_id: str | None = None,
        timeout_ms: int | None = None,
        now: float | None = None,
    ) -> ToolInvocationTicket:
        """Admit an HTTP Query without performing Dora I/O on the HTTP thread."""
        if not isinstance(endpoint_id, str) or not endpoint_id.strip():
            raise ValueError("endpoint_id must be a non-empty string")
        if not isinstance(operation, str) or not operation.strip():
            raise ValueError("operation must be a non-empty string")
        if not isinstance(arguments, dict):
            raise ValueError("arguments must be a JSON object")  # noqa: TRY004
        if caller_id is not None and (
            not isinstance(caller_id, str) or not caller_id.strip()
        ):
            raise ValueError("caller_id must be a non-empty string when provided")
        effective_timeout = self._validate_timeout_ms(
            self.config.invoke_timeout_ms if timeout_ms is None else timeout_ms
        )
        if effective_timeout > self.config.invoke_timeout_ms:
            raise ValueError(
                "timeout_ms must not exceed configured tools.invoke_timeout_ms "
                f"maximum {self.config.invoke_timeout_ms}"
            )
        observation = time.monotonic() if now is None else self._validate_now(now)
        identity = uuid.uuid4().hex
        context = ToolContext(
            execution_key=ToolExecutionKey(
                invocation_id=f"gateway-{identity}",
                attempt_id=f"gateway-{identity}",
            ),
            tool_id=endpoint_id,
            implementation_id=endpoint_id,
            endpoint_id=endpoint_id,
            operation=operation,
            caller_id=caller_id,
            deadline_ms=min(
                _MAX_SAFE_JSON_INTEGER,
                int(time.time() * 1_000) + effective_timeout,
            ),
        )
        request = make_logical_invoke_request_envelope(
            ToolRequest(arguments),
            context,
            request_id=f"gateway-{identity}",
        )
        future: Future[ToolEnvelope] = Future()
        deadline = observation + effective_timeout / 1_000
        with self._lock:
            try:
                pending_key = self._admit_invoke_locked(
                    request,
                    processed_at=observation,
                    deadline=deadline,
                    future=future,
                )
            except ToolGatewayUnavailable as error:
                pending_key = None
                self._complete_without_pending_locked(
                    request,
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_UNAVAILABLE",
                        str(error),
                        retryable=True,
                    ),
                    future,
                    terminal_slot_reserved=False,
                )
        return ToolInvocationTicket(
            request=request,
            future=future,
            pending_key=pending_key,
            deadline=deadline,
        )

    def _admit_invoke_locked(
        self,
        logical_request: ToolEnvelope,
        *,
        processed_at: float,
        deadline: float,
        future: Future[ToolEnvelope] | None,
    ) -> CorrelationKey | None:
        self._ensure_accepting_locked()
        terminal_reserved = False
        provider_reserved = False

        if future is None:
            self._reserve_outbound_slots_locked()
            terminal_reserved = True

        def complete(error: ToolError) -> None:
            nonlocal provider_reserved, terminal_reserved
            if provider_reserved:
                self._release_outbound_slots_locked()
                provider_reserved = False
            self._complete_without_pending_locked(
                logical_request,
                error,
                future,
                terminal_slot_reserved=terminal_reserved,
            )
            terminal_reserved = False

        try:
            if len(self._pending_by_provider_key) >= self._outbound_capacity:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_BUSY",
                        "Tool Gateway pending invocation capacity is exhausted",
                        retryable=True,
                    )
                )
                return None

            try:
                self._reserve_outbound_slots_locked()
                provider_reserved = True
            except ToolGatewayMailboxFull:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_BUSY",
                        "Tool Gateway outbound mailbox is busy",
                        retryable=True,
                    )
                )
                return None

            if deadline <= processed_at:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_INVOKE_TIMEOUT",
                        "Tool Query invocation deadline already elapsed",
                        retryable=True,
                    )
                )
                return None

            endpoint_id = logical_request.endpoint_id
            operation = logical_request.operation
            assert endpoint_id is not None
            assert operation is not None
            route = self._routes_by_endpoint_id.get(endpoint_id)
            if route is None:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_ENDPOINT_UNKNOWN",
                        f"endpoint {endpoint_id!r} is not configured",
                    )
                )
                return None
            expired = self._directory.expire(processed_at)
            _ = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=processed_at,
            )
            try:
                registration = self._directory.resolve(
                    endpoint_id,
                    operation,
                    now=processed_at,
                )
            except ToolOperationNotFoundError as error:
                complete(self._tool_error("FORGE_TOOL_OPERATION_NOT_FOUND", str(error)))
                return None
            if registration is None:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_ENDPOINT_UNAVAILABLE",
                        f"endpoint {endpoint_id!r} has no active provider instance",
                        retryable=True,
                    )
                )
                return None

            tool_request, context = invoke_request_from_envelope(logical_request)
            provider_request = make_invoke_request_envelope(
                tool_request,
                context,
                request_id=f"gateway-provider-{uuid.uuid4().hex}",
                endpoint_instance_id=registration.endpoint_instance_id,
            )
            logical_key = self._correlation_key(logical_request)
            if logical_key in self._pending_logical_keys:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_REQUEST_CONFLICT",
                        "an invocation with the same correlation identity is pending",
                    )
                )
                return None
            provider_key = self._correlation_key(provider_request)
            if provider_key in self._pending_by_provider_key:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_REQUEST_CONFLICT",
                        "provider correlation identity is already pending",
                    )
                )
                return None

            pending_token = uuid.uuid4().hex
            pending = _PendingInvocation(
                logical_request=logical_request,
                provider_request=provider_request,
                route=route,
                logical_key=logical_key,
                provider_key=provider_key,
                pending_token=pending_token,
                deadline=deadline,
                future=future,
                terminal_slot_reserved=terminal_reserved,
            )
            self._append_reserved_outbound_locked(
                ToolOutboundMessage(
                    output_id=route.output_id,
                    envelope=provider_request,
                    kind="provider.invoke_request",
                    pending_key=provider_key,
                    pending_token=pending_token,
                )
            )
            provider_reserved = False
            self._pending_by_provider_key[provider_key] = pending
            self._pending_logical_keys.add(logical_key)
            terminal_reserved = False
            return provider_key
        finally:
            if provider_reserved:
                self._release_outbound_slots_locked()
            if terminal_reserved:
                self._release_outbound_slots_locked()

    def _handle_provider_response_locked(
        self,
        route: ToolProviderRouteIdentity,
        response: ToolEnvelope,
        *,
        processed_at: float,
    ) -> bool:
        validate_message_envelope(response)
        provider_key = self._correlation_key(response)
        pending = self._pending_by_provider_key.get(provider_key)
        if pending is None:
            return False
        if pending.route != route:
            raise ValueError("provider response arrived on the wrong configured route")
        validate_response_correlation(pending.provider_request, response)
        if response.message_type == "tool.invoke.response" and isinstance(
            invoke_response_from_payload(response.payload), ToolAccepted
        ):
            caller_response = _logical_error_response(
                self._tool_error(
                    "FORGE_TOOL_QUERY_NON_TERMINAL_RESPONSE",
                    "a Query provider must return a terminal invoke response",
                ),
                pending.logical_request,
            )
        else:
            caller_response = _logical_provider_response(
                response,
                pending.logical_request,
            )
        return self._finish_pending_locked(
            pending,
            caller_response,
            observed_at=processed_at,
        )

    @staticmethod
    def _phase_for_result(result: Any) -> str:
        return {
            "succeeded": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
            "stopped": "stopped",
            "unknown": "unknown",
        }[result.status]

    def _handle_action_response_locked(
        self,
        route: ToolProviderRouteIdentity,
        response: ToolEnvelope,
        *,
        processed_at: float,
    ) -> bool:
        validate_message_envelope(response)
        key = self._correlation_key(response)
        exchange = self._action_exchanges.get(key)
        if exchange is None:
            return False
        if exchange.route != route:
            raise ValueError("provider response arrived on the wrong configured route")
        validate_response_correlation(exchange.provider_request, response)
        if processed_at >= exchange.deadline:
            return self._finish_action_exchange_error_locked(
                exchange,
                self._tool_error(
                    "FORGE_TOOL_EXCHANGE_TIMEOUT",
                    f"Action {exchange.purpose} exchange timed out",
                    retryable=exchange.purpose != "invoke",
                ),
                observed_at=processed_at,
                execution_ambiguous=(
                    exchange.purpose == "invoke" and exchange.dispatch_claimed
                ),
            )
        del self._action_exchanges[key]
        execution_key = exchange.execution_key
        if response.message_type == "tool.error":
            error = error_from_payload(response.payload)
            if exchange.purpose == "invoke":
                result = ToolResult(status="unknown", outputs={}, error=error)
                self.action_invocations.set_phase(
                    execution_key,
                    "unknown",
                    now=processed_at,
                    result=result,
                    error=error,
                    release_concurrency=False,
                )
            elif exchange.purpose.startswith("control:"):
                command = exchange.purpose.partition(":")[2]
                if command == "cancel":
                    self.action_invocations.set_cancel_status(
                        execution_key, "transport_error"
                    )
                self.action_invocations.set_observation_error(execution_key, error)
            else:
                self.action_invocations.set_observation_error(execution_key, error)
        elif response.message_type == "tool.invoke.response":
            outcome = invoke_response_from_payload(response.payload)
            if isinstance(outcome, ToolAccepted):
                self.action_invocations.establish_accepted(
                    execution_key, now=processed_at
                )
            elif isinstance(outcome, ToolError):
                result = ToolResult(status="failed", outputs={}, error=outcome)
                self.action_invocations.set_phase(
                    execution_key,
                    "failed",
                    now=processed_at,
                    result=result,
                    error=outcome,
                )
            else:
                self.action_invocations.set_phase(
                    execution_key,
                    self._phase_for_result(outcome),  # type: ignore[arg-type]
                    now=processed_at,
                    result=outcome,
                    error=outcome.error,
                )
        elif response.message_type == "tool.status.response":
            status = status_response_from_payload(response.payload)
            if status.phase in TERMINAL_PHASES:
                self.action_invocations.set_terminal_status_hint(
                    execution_key,
                    status.phase,  # type: ignore[arg-type]
                    error=status.error,
                )
                item = self.action_invocations.get(
                    execution_key, now=processed_at
                )
                if item is not None and item.result is None:
                    try:
                        self._refresh_action_key_locked(
                            execution_key, "result", now=processed_at
                        )
                    except RuntimeError as error:
                        self.action_invocations.set_observation_error(
                            execution_key,
                            self._tool_error(
                                "FORGE_TOOL_RESULT_LOOKUP_UNAVAILABLE",
                                str(error),
                                retryable=True,
                            ),
                        )
            else:
                self.action_invocations.set_phase(
                    execution_key,
                    status.phase,  # type: ignore[arg-type]
                    now=processed_at,
                    error=status.error,
                )
        elif response.message_type == "tool.result.response":
            lookup = result_response_from_payload(response.payload)
            if lookup.status == "available":
                assert lookup.result is not None
                self.action_invocations.set_phase(
                    execution_key,
                    self._phase_for_result(lookup.result),  # type: ignore[arg-type]
                    now=processed_at,
                    result=lookup.result,
                    error=lookup.result.error,
                )
            elif lookup.status == "not_found":
                error = self._tool_error(
                    "FORGE_TOOL_EXECUTION_OUTCOME_UNKNOWN",
                    "provider no longer retains the Action result",
                )
                item = self.action_invocations.get(execution_key, now=processed_at)
                release = bool(
                    item is not None and item.terminal_status_hint in TERMINAL_PHASES
                )
                self.action_invocations.set_phase(
                    execution_key,
                    "unknown",
                    now=processed_at,
                    result=ToolResult(status="unknown", outputs={}, error=error),
                    error=error,
                    release_concurrency=release,
                )
        elif response.message_type == "tool.control.response":
            control = control_response_from_payload(response.payload)
            if control.command == "cancel":
                self.action_invocations.set_cancel_status(
                    execution_key, control.status
                )
            elif control.command == "stop":
                self.action_invocations.set_stop_status(
                    execution_key, control.status
                )

        if exchange.logical_request is not None:
            if not exchange.caller_response_reserved:
                raise RuntimeError("Dora Action response has no reserved mailbox slot")
            self._append_reserved_outbound_locked(
                ToolOutboundMessage(
                    output_id=self.config.response_output_id,
                    envelope=_logical_provider_response(
                        response, exchange.logical_request
                    ),
                    kind="caller.action_response",
                )
            )
            exchange.caller_response_reserved = False
        self._queue_dora_events_locked(execution_key, now=processed_at)
        return True

    def _queue_dora_events_locked(
        self, execution_key: ActionKey, *, now: float
    ) -> None:
        cursor = self._dora_action_event_cursors.get(execution_key)
        if cursor is None:
            return
        events = self.action_invocations.events_after(execution_key, cursor, now=now)
        if events is None:
            self._dora_action_event_cursors.pop(execution_key, None)
            return
        item = self.action_invocations.get(execution_key, now=now)
        if item is None:
            return
        for event in events:
            self._reserve_outbound_slots_locked()
            envelope = ToolEnvelope(
                protocol=TOOL_ENDPOINT_PROTOCOL,
                message_type="tool.event",
                request_id=None,
                invocation_id=item.invocation_id,
                attempt_id=item.attempt_id,
                endpoint_id=item.endpoint_id,
                endpoint_instance_id=None,
                operation=item.operation,
                sequence=event.sequence,
                payload={"type": event.type, "data": dict(event.data)},
            )
            self._append_reserved_outbound_locked(
                ToolOutboundMessage(
                    output_id=self.config.response_output_id,
                    envelope=envelope,
                    kind="caller.action_response",
                )
            )
            self._dora_action_event_cursors[execution_key] = event.sequence

    def _handle_action_event_locked(
        self, route: ToolProviderRouteIdentity, envelope: ToolEnvelope
    ) -> None:
        validate_message_envelope(envelope)
        assert envelope.invocation_id is not None
        assert envelope.attempt_id is not None
        execution_key = (envelope.invocation_id, envelope.attempt_id)
        item = self.action_invocations.get(execution_key, now=time.monotonic())
        if item is None:
            return
        if (
            route.endpoint_id != item.endpoint_id
            or envelope.endpoint_instance_id != item.endpoint_instance_id
            or envelope.attempt_id != item.attempt_id
            or envelope.operation != item.operation
        ):
            raise ValueError("Tool event does not match the pinned Action attempt")
        assert envelope.sequence is not None
        event = event_from_payload(envelope.payload)
        appended = self.action_invocations.append_event(
            execution_key,
            event,
            provider_sequence=envelope.sequence,
        )
        if appended:
            self._queue_dora_events_locked(execution_key, now=time.monotonic())
            if event.type in (
                "executor_completed",
                "executor_failed",
                "cancelled",
                "stopped",
            ):
                try:
                    self._refresh_action_key_locked(
                        execution_key, "result", now=time.monotonic()
                    )
                except RuntimeError as error:
                    self.action_invocations.set_observation_error(
                        execution_key,
                        self._tool_error(
                            "FORGE_TOOL_RESULT_LOOKUP_UNAVAILABLE",
                            str(error),
                            retryable=True,
                        ),
                    )

    def _handle_endpoint_status_locked(
        self,
        route: ToolProviderRouteIdentity,
        envelope: ToolEnvelope,
        *,
        observed_at: float,
    ) -> None:
        validate_message_envelope(envelope)
        status = endpoint_status_from_envelope(envelope)
        expired = self._directory.expire(observed_at)
        self._finish_unclaimed_for_removed_locked(expired, observed_at=observed_at)
        current = next(
            (
                registration
                for registration in self._directory.registrations(now=observed_at)
                if registration.endpoint_id == route.endpoint_id
            ),
            None,
        )
        if current is None:
            return
        if (
            envelope.endpoint_id != route.endpoint_id
            or envelope.endpoint_instance_id != current.endpoint_instance_id
        ):
            raise ValueError(
                "endpoint.status is not from the current provider instance"
            )
        self._endpoint_statuses[(route.endpoint_id, current.endpoint_instance_id)] = {
            "state": status.state,
            "active_invocations": status.active_invocations,
            "details": dict(status.details),
        }

    def endpoint_readiness(
        self, endpoint_id: str, endpoint_instance_id: str
    ) -> dict[str, Any] | None:
        with self._lock:
            value = self._endpoint_statuses.get((endpoint_id, endpoint_instance_id))
            return None if value is None else dict(value)

    @staticmethod
    def _tool_error(
        code: str,
        message: str,
        *,
        retryable: bool = False,
    ) -> ToolError:
        return ToolError(code=code, message=message, retryable=retryable)

    def _complete_without_pending_locked(
        self,
        request: ToolEnvelope,
        error: ToolError,
        future: Future[ToolEnvelope] | None,
        *,
        terminal_slot_reserved: bool,
    ) -> None:
        response = _logical_error_response(error, request)
        if future is not None:
            if not future.done():
                future.set_result(response)
            return
        if not terminal_slot_reserved:
            raise RuntimeError("Dora caller completion has no reserved outbound slot")
        self._append_reserved_outbound_locked(
            ToolOutboundMessage(
                output_id=self.config.response_output_id,
                envelope=response,
                kind="caller.invoke_response",
            )
        )

    def _timeout_response_locked(
        self,
        pending: _PendingInvocation,
    ) -> ToolEnvelope:
        return _logical_error_response(
            self._tool_error(
                "FORGE_TOOL_INVOKE_TIMEOUT",
                "Tool Query invocation timed out",
                retryable=True,
            ),
            pending.logical_request,
        )

    def _finish_unclaimed_for_removed_locked(
        self,
        removed: tuple[RegisteredEndpoint, ...],
        *,
        observed_at: float,
    ) -> tuple[int, int]:
        if not removed:
            return 0, 0
        removed_routes = {
            (
                registration.endpoint_id,
                registration.endpoint_instance_id,
                registration.route,
            )
            for registration in removed
        }
        action_exchanges = tuple(
            exchange
            for exchange in self._action_exchanges.values()
            if (
                exchange.provider_request.endpoint_id,
                exchange.provider_request.endpoint_instance_id,
                exchange.route,
            )
            in removed_routes
        )
        for exchange in action_exchanges:
            error = self._tool_error(
                "FORGE_TOOL_ENDPOINT_UNAVAILABLE",
                "pinned provider instance became unavailable",
                retryable=not exchange.dispatch_claimed,
            )
            self._finish_action_exchange_error_locked(
                exchange,
                error,
                observed_at=observed_at,
                execution_ambiguous=exchange.dispatch_claimed,
            )
        for registration in removed:
            self._endpoint_statuses.pop(
                (registration.endpoint_id, registration.endpoint_instance_id), None
            )
            self.action_invocations.mark_instance_ambiguous(
                registration.endpoint_id,
                registration.endpoint_instance_id,
                now=observed_at,
                reason=(
                    "pinned provider lease expired or provider instance restarted; "
                    "the Action outcome is ambiguous"
                ),
            )
        affected = tuple(
            pending
            for pending in self._pending_by_provider_key.values()
            if not pending.dispatch_claimed
            and (
                pending.provider_request.endpoint_id,
                pending.provider_request.endpoint_instance_id,
                pending.route,
            )
            in removed_routes
        )
        completed = 0
        timed_out = 0
        for pending in affected:
            endpoint_id = pending.provider_request.endpoint_id
            instance_id = pending.provider_request.endpoint_instance_id
            assert endpoint_id is not None
            assert instance_id is not None
            response = _logical_error_response(
                self._tool_error(
                    "FORGE_TOOL_ENDPOINT_UNAVAILABLE",
                    f"endpoint {endpoint_id!r} instance {instance_id!r} "
                    "became unavailable before provider dispatch",
                    retryable=True,
                ),
                pending.logical_request,
            )
            deadline_elapsed = observed_at >= pending.deadline
            if self._finish_pending_locked(
                pending,
                response,
                observed_at=observed_at,
            ):
                completed += 1
                timed_out += int(deadline_elapsed)
        return completed, timed_out

    def _finish_action_exchange_error_locked(
        self,
        exchange: _ActionExchange,
        error: ToolError,
        *,
        observed_at: float,
        execution_ambiguous: bool,
    ) -> bool:
        current = self._action_exchanges.get(exchange.provider_key)
        if current is not exchange:
            return False
        del self._action_exchanges[exchange.provider_key]
        key = exchange.execution_key
        if exchange.purpose == "invoke":
            if execution_ambiguous:
                result_error = self._tool_error(
                    "FORGE_TOOL_EXECUTION_OUTCOME_UNKNOWN",
                    error.message,
                    retryable=False,
                )
                self.action_invocations.set_phase(
                    key,
                    "unknown",
                    now=observed_at,
                    result=ToolResult(
                        status="unknown", outputs={}, error=result_error
                    ),
                    error=result_error,
                    release_concurrency=False,
                )
            else:
                self.action_invocations.set_phase(
                    key,
                    "failed",
                    now=observed_at,
                    result=ToolResult(status="failed", outputs={}, error=error),
                    error=error,
                )
        elif exchange.purpose.startswith("control:"):
            if exchange.purpose == "control:cancel":
                self.action_invocations.set_cancel_status(key, "transport_error")
            self.action_invocations.set_observation_error(key, error)
        else:
            self.action_invocations.set_observation_error(key, error)
        if exchange.caller_response_reserved:
            assert exchange.logical_request is not None
            self._append_reserved_outbound_locked(
                ToolOutboundMessage(
                    output_id=self.config.response_output_id,
                    envelope=_logical_error_response(
                        error, exchange.logical_request
                    ),
                    kind="caller.action_response",
                )
            )
            exchange.caller_response_reserved = False
        return True

    def _remove_pending_locked(self, pending: _PendingInvocation) -> None:
        current = self._pending_by_provider_key.get(pending.provider_key)
        if current is not pending:
            return
        del self._pending_by_provider_key[pending.provider_key]
        self._pending_logical_keys.discard(pending.logical_key)

    def _finish_pending_locked(
        self,
        pending: _PendingInvocation,
        response: ToolEnvelope,
        *,
        observed_at: float,
        timed_out: bool = False,
    ) -> bool:
        if self._pending_by_provider_key.get(pending.provider_key) is not pending:
            return False
        if timed_out or observed_at >= pending.deadline:
            response = self._timeout_response_locked(pending)
        if pending.future is not None:
            if not pending.future.done():
                pending.future.set_result(response)
            self._remove_pending_locked(pending)
            return True
        if not pending.terminal_slot_reserved:
            raise RuntimeError("Dora pending invocation has no terminal reservation")
        self._append_reserved_outbound_locked(
            ToolOutboundMessage(
                output_id=self.config.response_output_id,
                envelope=response,
                kind="caller.invoke_response",
            )
        )
        pending.terminal_slot_reserved = False
        self._remove_pending_locked(pending)
        return True

    def _reserve_outbound_slots_locked(self, count: int = 1) -> None:
        occupied = len(self._outbound) + self._reserved_outbound_slots
        if occupied + count > self._outbound_capacity:
            raise ToolGatewayMailboxFull(
                f"Tool outbound mailbox capacity {self._outbound_capacity} exceeded"
            )
        self._reserved_outbound_slots += count

    def _release_outbound_slots_locked(self, count: int = 1) -> None:
        if count < 1 or self._reserved_outbound_slots < count:
            raise RuntimeError("Tool outbound mailbox reservation underflow")
        self._reserved_outbound_slots -= count

    def _append_reserved_outbound_locked(self, message: ToolOutboundMessage) -> None:
        if self._reserved_outbound_slots < 1:
            raise RuntimeError("Tool outbound message has no capacity reservation")
        self._reserved_outbound_slots -= 1
        self._outbound.append(message)

    def take_outbound(self) -> ToolOutboundMessage | None:
        """Claim and pop one valid output for lifecycle-thread dispatch."""
        with self._lock:
            while self._outbound:
                message = self._outbound.popleft()
                if message.kind == "provider.invoke_request":
                    pending_key = message.pending_key
                    if pending_key is None:
                        continue
                    pending = self._pending_by_provider_key.get(pending_key)
                    if (
                        pending is None
                        or pending.pending_token != message.pending_token
                        or pending.dispatch_claimed
                    ):
                        continue
                    pending.dispatch_claimed = True
                elif message.kind == "provider.action_request":
                    pending_key = message.pending_key
                    if pending_key is None:
                        continue
                    exchange = self._action_exchanges.get(pending_key)
                    if exchange is None or exchange.dispatch_claimed:
                        continue
                    exchange.dispatch_claimed = True
                return message
            return None

    def cancel_http_invoke(
        self,
        ticket: ToolInvocationTicket,
        *,
        timed_out: bool,
        now: float | None = None,
    ) -> bool:
        """Atomically stop an HTTP wait and invalidate any unclaimed dispatch."""
        if not isinstance(ticket, ToolInvocationTicket):
            raise TypeError("ticket must be a ToolInvocationTicket")
        pending_key = ticket.pending_key
        if pending_key is None:
            return False
        explicit_observation = None if now is None else self._validate_now(now)
        with self._lock:
            observation = (
                time.monotonic()
                if explicit_observation is None
                else explicit_observation
            )
            pending = self._pending_by_provider_key.get(pending_key)
            if pending is None or pending.future is not ticket.future:
                return False
            error = self._tool_error(
                "FORGE_TOOL_INVOKE_TIMEOUT"
                if timed_out
                else "FORGE_TOOL_CALLER_CANCELLED",
                "Tool Query invocation timed out"
                if timed_out
                else "HTTP caller stopped waiting for Tool Query invocation",
                retryable=timed_out,
            )
            response = _logical_error_response(error, pending.logical_request)
            return self._finish_pending_locked(
                pending,
                response,
                observed_at=observation,
                timed_out=timed_out,
            )

    def output_failed(
        self,
        message: ToolOutboundMessage,
        error: BaseException,
        *,
        now: float | None = None,
    ) -> bool:
        """Fail an invocation whose one-shot provider dispatch did not succeed."""
        if (
            message.kind == "provider.action_request"
            and message.pending_key is not None
        ):
            with self._lock:
                exchange = self._action_exchanges.get(message.pending_key)
                if exchange is None:
                    return False
                tool_error = self._tool_error(
                    "FORGE_TOOL_TRANSPORT_UNAVAILABLE",
                    f"provider request dispatch failed: {error}",
                    retryable=True,
                )
                return self._finish_action_exchange_error_locked(
                    exchange,
                    tool_error,
                    observed_at=(
                        time.monotonic()
                        if now is None
                        else self._validate_now(now)
                    ),
                    # send_output may have crossed the transport boundary.
                    execution_ambiguous=(
                        exchange.purpose == "invoke" and exchange.dispatch_claimed
                    ),
                )
        if message.kind != "provider.invoke_request" or message.pending_key is None:
            return False
        explicit_observation = None if now is None else self._validate_now(now)
        with self._lock:
            observation = (
                time.monotonic()
                if explicit_observation is None
                else explicit_observation
            )
            pending = self._pending_by_provider_key.get(message.pending_key)
            if pending is None or pending.pending_token != message.pending_token:
                return False
            response = _logical_error_response(
                self._tool_error(
                    "FORGE_TOOL_TRANSPORT_UNAVAILABLE",
                    f"provider request dispatch failed: {error}",
                    retryable=True,
                ),
                pending.logical_request,
            )
            return self._finish_pending_locked(
                pending,
                response,
                observed_at=observation,
            )

    def sweep(self, *, now: float | None = None) -> ToolGatewaySweep:
        explicit_observation = None if now is None else self._validate_now(now)
        with self._lock:
            observation = (
                time.monotonic()
                if explicit_observation is None
                else explicit_observation
            )
            expired = self._directory.expire(observation)
            _, transition_timeouts = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=observation,
            )
            timed_out = tuple(
                pending
                for pending in self._pending_by_provider_key.values()
                if pending.deadline <= observation
            )
            action_timeouts = tuple(
                exchange
                for exchange in self._action_exchanges.values()
                if exchange.deadline <= observation
            )
            completed = transition_timeouts
            for exchange in action_timeouts:
                error = self._tool_error(
                    "FORGE_TOOL_EXCHANGE_TIMEOUT",
                    f"Action {exchange.purpose} exchange timed out",
                    retryable=exchange.purpose != "invoke",
                )
                self._finish_action_exchange_error_locked(
                    exchange,
                    error,
                    observed_at=observation,
                    execution_ambiguous=(
                        exchange.purpose == "invoke"
                        and exchange.dispatch_claimed
                    ),
                )
            self.action_invocations.mark_deadlines_ambiguous(
                now=observation,
                epoch_ms=int(time.time() * 1_000),
            )
            for pending in timed_out:
                response = _logical_error_response(
                    self._tool_error(
                        "FORGE_TOOL_INVOKE_TIMEOUT",
                        "Tool Query invocation timed out",
                        retryable=True,
                    ),
                    pending.logical_request,
                )
                if self._finish_pending_locked(
                    pending,
                    response,
                    observed_at=observation,
                ):
                    completed += 1
            return ToolGatewaySweep(
                expired_registrations=expired,
                timed_out_invocations=completed,
            )

    def begin_close(self, *, now: float | None = None) -> None:
        explicit_observation = None if now is None else self._validate_now(now)
        with self._lock:
            observation = (
                time.monotonic()
                if explicit_observation is None
                else explicit_observation
            )
            self._accepting = False
            for exchange in tuple(self._action_exchanges.values()):
                self._finish_action_exchange_error_locked(
                    exchange,
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_UNAVAILABLE",
                        "Tool Gateway is closing",
                        retryable=True,
                    ),
                    observed_at=observation,
                    execution_ambiguous=(
                        exchange.purpose == "invoke"
                        and exchange.dispatch_claimed
                    ),
                )
            pending_invocations = tuple(self._pending_by_provider_key.values())
            for pending in pending_invocations:
                response = _logical_error_response(
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_UNAVAILABLE",
                        "Tool Gateway is closing",
                        retryable=True,
                    ),
                    pending.logical_request,
                )
                _ = self._finish_pending_locked(
                    pending,
                    response,
                    observed_at=observation,
                )

    def close(self, *, now: float | None = None) -> None:
        self.begin_close(now=now)

    def _ensure_accepting_locked(self) -> None:
        if not self.config.enabled:
            raise ToolGatewayUnavailable("Tool Gateway is disabled")
        if not self._accepting:
            raise ToolGatewayUnavailable("Tool Gateway is closing")


def tool_error_from_response(response: ToolEnvelope) -> ToolError | None:
    """Return a rejected/error outcome from a completed caller response."""
    if response.message_type == "tool.error":
        return error_from_payload(response.payload)
    if response.message_type != "tool.invoke.response":
        raise ValueError("response must be tool.invoke.response or tool.error")
    outcome = invoke_response_from_payload(response.payload)
    return outcome if isinstance(outcome, ToolError) else None


__all__ = [
    "DEFAULT_TOOL_OUTBOUND_CAPACITY",
    "ToolGatewayMailboxFull",
    "ToolGatewayService",
    "ToolGatewaySweep",
    "ToolGatewayUnavailable",
    "ToolInvocationTicket",
    "ToolOutboundMessage",
    "make_logical_invoke_request_envelope",
    "tool_error_from_response",
]
