"""Bounded Gateway-owned Action invocation state and retention."""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Literal

from forge_tool import (
    ToolError,
    ToolEvent,
    ToolResult,
    error_to_payload,
    tool_result_to_payload,
)

TERMINAL_PHASES = frozenset(("completed", "failed", "cancelled", "stopped", "unknown"))
_TERMINAL_EVENTS = frozenset(
    ("executor_completed", "executor_failed", "cancelled", "stopped")
)
_NONTERMINAL_ORDER = {"dispatching": 0, "accepted": 1, "running": 2, "stopping": 3}
ActionKey = tuple[str, str]
ActionPhase = Literal[
    "dispatching",
    "accepted",
    "running",
    "stopping",
    "completed",
    "failed",
    "cancelled",
    "stopped",
    "unknown",
]


class ActionInvocationCapacityError(RuntimeError):
    """The bounded invocation store cannot admit more retained work."""


@dataclass(frozen=True)
class ActionInvocationEvent:
    sequence: int
    type: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"sequence": self.sequence, "type": self.type, "data": dict(self.data)}


@dataclass
class ActionInvocation:
    invocation_id: str
    attempt_id: str
    tool_id: str
    implementation_id: str
    endpoint_id: str
    endpoint_instance_id: str
    operation: str
    caller_id: str | None
    deadline_ms: int | None
    created_at: float
    phase: ActionPhase = "dispatching"
    result: ToolResult | None = None
    error: ToolError | None = None
    observation_error: ToolError | None = None
    cancel_status: str | None = None
    stop_status: str | None = None
    terminal_at: float | None = None
    accepted_established: bool = False
    occupies_concurrency: bool = True
    terminal_status_hint: ActionPhase | None = None
    events: deque[ActionInvocationEvent] = field(default_factory=deque)
    next_event_sequence: int = 0
    last_provider_sequence: int = -1
    terminal_event_seen: bool = False

    @property
    def key(self) -> ActionKey:
        return (self.invocation_id, self.attempt_id)


class ActionInvocationStore:
    """Thread-safe, execution-keyed store with hard total and event bounds."""

    def __init__(
        self,
        *,
        capacity: int = 128,
        event_capacity: int = 128,
        retention_seconds: float = 300.0,
    ) -> None:
        if capacity < 1 or event_capacity < 1 or retention_seconds < 0:
            raise ValueError("invocation capacities must be positive")
        self.capacity = capacity
        self.event_capacity = event_capacity
        self.retention_seconds = float(retention_seconds)
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)
        self._items: dict[ActionKey, ActionInvocation] = {}

    def create(
        self,
        invocation: ActionInvocation,
        *,
        now: float,
        operation_capacity: int | None = None,
    ) -> None:
        with self._changed:
            self._purge_locked(now)
            key = invocation.key
            if key in self._items:
                raise ValueError("ToolExecutionKey already exists")
            self._make_total_capacity_locked()
            operation_active = sum(
                item.endpoint_id == invocation.endpoint_id
                and item.operation == invocation.operation
                and item.occupies_concurrency
                for item in self._items.values()
            )
            if (
                operation_capacity is not None
                and operation_active >= operation_capacity
            ):
                raise ActionInvocationCapacityError(
                    f"operation concurrency {operation_capacity} is exhausted"
                )
            invocation.events = deque(maxlen=self.event_capacity)
            self._items[key] = invocation
            self._changed.notify_all()

    def get(
        self, key: ActionKey, *, now: float
    ) -> ActionInvocation | None:
        with self._lock:
            self._purge_locked(now)
            return self._items.get(key)

    def resolve_http_key(self, invocation_id: str, *, now: float) -> ActionKey | None:
        """Resolve only an unambiguous invocation ID; Dora callers always use pairs."""
        with self._lock:
            self._purge_locked(now)
            matches = [key for key in self._items if key[0] == invocation_id]
            return matches[0] if len(matches) == 1 else None

    def active_count(self, *, endpoint_id: str, operation: str, now: float) -> int:
        with self._lock:
            self._purge_locked(now)
            return sum(
                item.endpoint_id == endpoint_id
                and item.operation == operation
                and item.occupies_concurrency
                for item in self._items.values()
            )

    def discard(self, key: ActionKey) -> None:
        with self._changed:
            self._items.pop(key, None)
            self._changed.notify_all()

    def snapshot(self, key: ActionKey, *, now: float) -> dict[str, Any] | None:
        with self._lock:
            self._purge_locked(now)
            item = self._items.get(key)
            return None if item is None else self._snapshot_locked(item)

    def result_snapshot(self, key: ActionKey, *, now: float) -> dict[str, Any] | None:
        with self._lock:
            self._purge_locked(now)
            item = self._items.get(key)
            if item is None:
                return None
            if item.result is None:
                return {"status": "pending"}
            return {
                "status": "available",
                "result": tool_result_to_payload(item.result),
            }

    def establish_accepted(self, key: ActionKey, *, now: float) -> bool:
        with self._changed:
            item = self._items.get(key)
            if item is None or item.phase in TERMINAL_PHASES:
                return False
            item.accepted_established = True
            if _NONTERMINAL_ORDER[item.phase] < _NONTERMINAL_ORDER["accepted"]:
                item.phase = "accepted"
            item.observation_error = None
            self._changed.notify_all()
            return True

    def set_phase(
        self,
        key: ActionKey,
        phase: ActionPhase,
        *,
        now: float,
        result: ToolResult | None = None,
        error: ToolError | None = None,
        release_concurrency: bool = True,
    ) -> bool:
        with self._changed:
            item = self._items.get(key)
            if item is None:
                return False
            if item.phase in TERMINAL_PHASES and item.phase != "unknown":
                return False
            if item.phase == "unknown" and phase not in TERMINAL_PHASES:
                # Only a late terminal outcome may reconcile a closed-unknown
                # item; it can never reopen into a nonterminal phase.
                return False
            if phase in TERMINAL_PHASES:
                if result is None:
                    raise ValueError("terminal phase requires a retained result")
                item.result = result
                item.error = error
                item.phase = phase
                item.terminal_at = now
                item.occupies_concurrency = not release_concurrency
            else:
                if result is not None:
                    raise ValueError(
                        "nonterminal phase cannot retain a terminal result"
                    )
                if _NONTERMINAL_ORDER[phase] <= _NONTERMINAL_ORDER[item.phase]:
                    return False
                item.phase = phase
                item.error = error
            item.observation_error = None
            self._changed.notify_all()
            return True

    def set_terminal_status_hint(
        self,
        key: ActionKey,
        phase: ActionPhase,
        *,
        error: ToolError | None,
    ) -> bool:
        if phase not in TERMINAL_PHASES:
            raise ValueError("terminal status hint must be terminal")
        with self._changed:
            item = self._items.get(key)
            if item is None or item.phase in TERMINAL_PHASES:
                return False
            item.terminal_status_hint = phase
            item.error = error
            item.observation_error = None
            self._changed.notify_all()
            return True

    def set_observation_error(self, key: ActionKey, error: ToolError) -> bool:
        with self._changed:
            item = self._items.get(key)
            if item is None:
                return False
            item.observation_error = error
            self._changed.notify_all()
            return True

    def set_cancel_status(self, key: ActionKey, status: str) -> bool:
        with self._changed:
            item = self._items.get(key)
            if item is None:
                return False
            item.cancel_status = status
            self._changed.notify_all()
            return True

    def set_stop_status(self, key: ActionKey, status: str) -> bool:
        with self._changed:
            item = self._items.get(key)
            if item is None:
                return False
            item.stop_status = status
            self._changed.notify_all()
            return True

    def append_event(
        self,
        key: ActionKey,
        event: ToolEvent,
        *,
        provider_sequence: int,
    ) -> bool:
        with self._changed:
            item = self._items.get(key)
            if item is None:
                return False
            if item.phase in TERMINAL_PHASES and item.phase != "unknown":
                return False
            if item.terminal_event_seen:
                raise ValueError("provider event arrived after a terminal event")
            if provider_sequence != item.last_provider_sequence + 1:
                raise ValueError("provider event sequence is not contiguous")
            item.last_provider_sequence = provider_sequence
            if event.type in _TERMINAL_EVENTS:
                item.terminal_event_seen = True
                if item.phase == "unknown":
                    # The provider finished an execution the deadline already
                    # closed as unknown: the terminal event proves the
                    # execution is over, so the concurrency fence can go even
                    # before the retained result arrives.
                    item.occupies_concurrency = False
            item.events.append(
                ActionInvocationEvent(
                    sequence=item.next_event_sequence,
                    type=event.type,
                    data=dict(event.data),
                )
            )
            item.next_event_sequence += 1
            self._changed.notify_all()
            return True

    def events_after(
        self, key: ActionKey, sequence: int, *, now: float
    ) -> tuple[ActionInvocationEvent, ...] | None:
        with self._lock:
            self._purge_locked(now)
            item = self._items.get(key)
            if item is None:
                return None
            if not item.accepted_established:
                return ()
            return tuple(
                event
                for event in item.events
                if event.sequence > sequence
                and (event.type not in _TERMINAL_EVENTS or item.result is not None)
            )

    def event_window(
        self, key: ActionKey, *, now: float
    ) -> tuple[int, int] | None:
        with self._lock:
            self._purge_locked(now)
            item = self._items.get(key)
            if item is None:
                return None
            oldest = (
                item.events[0].sequence
                if item.events
                else item.next_event_sequence
            )
            return oldest, item.next_event_sequence

    def mark_instance_ambiguous(
        self, endpoint_id: str, endpoint_instance_id: str, *, now: float, reason: str
    ) -> int:
        error = ToolError(
            code="FORGE_TOOL_EXECUTION_OUTCOME_UNKNOWN",
            message=reason,
            retryable=False,
        )
        result = ToolResult(status="unknown", outputs={}, error=error)
        changed = 0
        with self._changed:
            for item in self._items.values():
                if (
                    item.endpoint_id == endpoint_id
                    and item.endpoint_instance_id == endpoint_instance_id
                ):
                    if item.phase not in TERMINAL_PHASES:
                        item.phase = "unknown"
                        item.error = error
                        item.result = result
                        item.terminal_at = now
                        changed += 1
                    item.occupies_concurrency = False
            if changed:
                self._changed.notify_all()
        return changed

    def mark_deadlines_ambiguous(self, *, now: float, epoch_ms: int) -> int:
        # Session invocations are admitted with deadline_ms=None (see
        # tool_gateway_service.submit_http_action) and are therefore never
        # marked ambiguous here: a session is unbounded by design and `stop`
        # is its only termination path. Provider death mid-session is caught
        # by lease-expiry ambiguity instead.
        error = ToolError(
            code="FORGE_TOOL_DEADLINE_EXCEEDED_OUTCOME_UNKNOWN",
            message=(
                "Action deadline elapsed; deadline does not stop provider execution "
                "and the final outcome is unknown"
            ),
            retryable=False,
        )
        result = ToolResult(status="unknown", outputs={}, error=error)
        changed = 0
        with self._changed:
            for item in self._items.values():
                if (
                    item.deadline_ms is not None
                    and item.deadline_ms <= epoch_ms
                    and item.phase not in TERMINAL_PHASES
                ):
                    item.phase = "unknown"
                    item.error = error
                    item.result = result
                    item.terminal_at = now
                    # The provider may still be executing. Keep the concurrency fence.
                    item.occupies_concurrency = True
                    changed += 1
            if changed:
                self._changed.notify_all()
        return changed

    def reap_unresolved_fences(self, *, now: float, max_age_seconds: float) -> int:
        """Release fences held by ambiguous records whose provider never answered.

        ``mark_deadlines_ambiguous`` keeps the concurrency fence because the provider
        may still be executing, and a late provider terminal event (or a late
        ``tool.result``) releases it. But ``unknown`` is a terminal phase: when the
        provider never sends anything at all -- it is wedged, it dropped the goal
        event, or it died without the lease expiring -- no write path can ever clear
        the fence, so the ``(endpoint, operation)`` slot stays occupied until the
        Gateway process restarts. Callers observe that as a permanent
        ``operation concurrency N is exhausted`` rejection for one operation.

        This sweep bounds the window. Releasing the fence cannot make two
        executions run concurrently: every provider enforces its own
        per-operation concurrency and rejects a second goal with ``BUSY``, so the
        worst case is a fast rejection instead of an indefinite lockout.

        Returns the number of fences released.
        """
        released = 0
        with self._changed:
            for item in self._items.values():
                if (
                    item.phase == "unknown"
                    and item.occupies_concurrency
                    and item.terminal_at is not None
                    and now - item.terminal_at >= max_age_seconds
                ):
                    item.occupies_concurrency = False
                    released += 1
            if released:
                self._changed.notify_all()
        return released

    def _make_total_capacity_locked(self) -> None:
        if len(self._items) < self.capacity:
            return
        evictable = [
            (item.terminal_at or item.created_at, key)
            for key, item in self._items.items()
            if item.phase in TERMINAL_PHASES and not item.occupies_concurrency
        ]
        if not evictable:
            raise ActionInvocationCapacityError(
                f"Action invocation capacity {self.capacity} is exhausted"
            )
        _, oldest_key = min(evictable)
        del self._items[oldest_key]

    def _purge_locked(self, now: float) -> None:
        expired = [
            key
            for key, item in self._items.items()
            if item.terminal_at is not None
            and not item.occupies_concurrency
            and item.terminal_at + self.retention_seconds <= now
        ]
        for key in expired:
            del self._items[key]

    @staticmethod
    def _snapshot_locked(item: ActionInvocation) -> dict[str, Any]:
        value: dict[str, Any] = {
            "invocation_id": item.invocation_id,
            "attempt_id": item.attempt_id,
            "tool_id": item.tool_id,
            "implementation_id": item.implementation_id,
            "endpoint_id": item.endpoint_id,
            "operation": item.operation,
            "phase": item.phase,
            "accepted": item.accepted_established,
            "occupies_concurrency": item.occupies_concurrency,
            "cancel_status": item.cancel_status,
            "stop_status": item.stop_status,
            "created_at": item.created_at,
        }
        if item.deadline_ms is not None:
            value["deadline_ms"] = item.deadline_ms
        if item.result is not None:
            value["result"] = tool_result_to_payload(item.result)
        if item.error is not None:
            value["error"] = error_to_payload(item.error)["error"]
        if item.observation_error is not None:
            value["observation_error"] = error_to_payload(
                item.observation_error
            )["error"]
        if item.terminal_status_hint is not None:
            value["terminal_status_hint"] = item.terminal_status_hint
        return value


__all__ = [
    "TERMINAL_PHASES",
    "ActionInvocation",
    "ActionInvocationCapacityError",
    "ActionInvocationEvent",
    "ActionInvocationStore",
    "ActionKey",
]
