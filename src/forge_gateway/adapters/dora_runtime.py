"""Dora reader thread and lifecycle loop for Gateway."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

if TYPE_CHECKING:
    from forge_gateway.services.runtime_service import GatewayRuntime

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


from forge_gateway.adapters.dora_adapter import (
    DoraEventBuffer,
    drain_commands,
    handle_dora_input,
)

logger = get_logger("forge_gateway.cli")

_TOOL_RECEIVED_AT_KEY = "_forge_gateway_received_monotonic"

DoraRunReason = Literal[
    "stop",
    "error",
    "reader_error",
    "eof",
    "shutdown",
]


class DoraNodeLike(Protocol):
    def __iter__(self) -> Iterator[dict[str, Any]]: ...

    def send_output(self, output_id: str, data: Any, /) -> None: ...


def handle_tool_input(
    runtime: GatewayRuntime,
    input_id: str,
    value: object,
    *,
    received_at: float | None = None,
) -> object:
    from forge_gateway.adapters.tool_dora import handle_tool_input as handle

    return handle(runtime, input_id, value, received_at=received_at)


def sweep_tool_gateway(runtime: GatewayRuntime) -> object | None:
    if not runtime.config.tools.enabled:
        return None
    from forge_gateway.adapters.tool_dora import sweep_tool_gateway as sweep

    return sweep(runtime)


def drain_tool_outputs(
    runtime: GatewayRuntime,
    node: DoraNodeLike,
    *,
    max_messages: int | None = None,
) -> tuple[object, ...]:
    if not runtime.config.tools.enabled:
        return ()
    from forge_gateway.adapters.tool_dora import drain_tool_outputs as drain

    if max_messages is None:
        return drain(runtime, node)
    return drain(runtime, node, max_messages=max_messages)


class GatewayDoraRunner:
    """Read Dora events in the background and process them on one lifecycle thread."""

    def __init__(
        self,
        *,
        runtime: GatewayRuntime,
        node: DoraNodeLike,
        stop_event: threading.Event,
        poll_timeout: float = 0.2,
        tool_input_fifo_capacity: int = 256,
    ) -> None:
        self._runtime = runtime
        self._node = node
        self._stop_event = stop_event
        self._poll_timeout = poll_timeout
        self._events = DoraEventBuffer(
            fifo_input_ids=runtime.tool_input_ids,
            fifo_capacity=tool_input_fifo_capacity,
        )
        self._end_sentinel = object()
        self._reader_started = threading.Event()
        self._reader_error_lock = threading.Lock()
        self._reader_error: BaseException | None = None
        self._reader_thread = threading.Thread(
            target=self._read_node,
            name="gateway_dora_iter",
            daemon=True,
        )

    @property
    def reader_error(self) -> BaseException | None:
        with self._reader_error_lock:
            return self._reader_error

    def start(self) -> None:
        self._reader_thread.start()
        if not self._reader_started.wait(timeout=1.0):
            raise RuntimeError("Dora reader thread did not start")

    def run(self) -> DoraRunReason:
        while True:
            event = self._events.get_priority()
            if event is None:
                if self._apply_reader_error():
                    return self._finish_run("reader_error")
                if self._stop_event.is_set():
                    return self._finish_run("shutdown")
                event = self._events.get(timeout=self._poll_timeout)
            if event is self._end_sentinel:
                return self._finish_run("eof")
            reason = self.handle_poll(cast(dict[str, Any] | None, event))
            if reason is not None:
                return self._finish_run(reason)

    def handle_poll(self, event: dict[str, Any] | None) -> DoraRunReason | None:
        """Process one event-buffer poll result; ``None`` represents a timeout."""
        if event is None:
            self._maintain_outputs()
            return None

        event_type = event.get("type")
        if event_type == "STOP":
            return "stop"
        if event_type == "ERROR":
            with self._runtime.lock:
                self._runtime.last_error = (
                    f"dora error: {event.get('error', 'unknown')}"
                )
            return "error"
        if event_type == "READER_ERROR":
            self._apply_reader_error()
            return "reader_error"
        if event_type == "INPUT":
            input_id = str(event.get("id"))
            try:
                if input_id in self._runtime.tool_input_ids:
                    handle_tool_input(
                        self._runtime,
                        input_id,
                        event.get("value"),
                        received_at=event.get(_TOOL_RECEIVED_AT_KEY),
                    )
                else:
                    handle_dora_input(
                        self._runtime,
                        input_id,
                        event.get("value"),
                    )
            except Exception as error:
                logger.warning(
                    "gateway: handle input %s failed: %s",
                    input_id,
                    error,
                )
                with self._runtime.lock:
                    self._runtime.last_error = f"input {input_id} failed: {error}"

        self._maintain_outputs()
        return None

    def _maintain_outputs(self) -> None:
        sweep_tool_gateway(self._runtime)
        try:
            drain_tool_outputs(self._runtime, self._node)
        except Exception as error:
            logger.warning("gateway: Tool output dispatch failed: %s", error)
            with self._runtime.lock:
                self._runtime.last_error = f"Tool output dispatch failed: {error}"
        drain_commands(self._runtime, self._node)

    def _finish_run(self, reason: DoraRunReason) -> DoraRunReason:
        if not self._runtime.config.tools.enabled:
            return reason
        try:
            self._runtime.tool_gateway.begin_close()
            drain_tool_outputs(
                self._runtime,
                self._node,
                max_messages=self._runtime.tool_gateway.outbound_capacity,
            )
        except Exception as error:
            logger.warning("gateway: final Tool output drain failed: %s", error)
            with self._runtime.lock:
                self._runtime.last_error = f"final Tool output drain failed: {error}"
        return reason

    def join_reader(self, timeout: float) -> bool:
        if self._reader_thread.is_alive():
            self._reader_thread.join(timeout=timeout)
        return not self._reader_thread.is_alive()

    def _apply_reader_error(self) -> bool:
        error = self.reader_error
        if error is None:
            return False
        with self._runtime.lock:
            self._runtime.last_error = f"dora reader failed: {error}"
        return True

    def _node_events(self) -> Iterator[dict[str, Any]]:
        receive = getattr(self._node, "next", None)
        if not callable(receive):
            yield from self._node
            return
        # Bound native receives so the HTTP and image threads can make progress
        # and shutdown does not depend on another data message arriving.
        while not self._stop_event.is_set():
            event = receive(timeout=self._poll_timeout)
            if event is None:
                return
            if event.get("type") == "ERROR" and "timed out" in str(event.get("error", "")).lower():
                continue
            yield event
            if event.get("type") in {"STOP", "ERROR"}:
                return

    def _read_node(self) -> None:
        self._reader_started.set()
        try:
            for event in self._node_events():
                received_at = time.monotonic()
                if self._stop_event.is_set():
                    break
                if (
                    isinstance(event, dict)
                    and event.get("type") == "INPUT"
                    and str(event.get("id")) in self._runtime.tool_input_ids
                ):
                    event = dict(event)
                    event[_TOOL_RECEIVED_AT_KEY] = received_at
                self._events.put(event)
        except BaseException as error:
            with self._reader_error_lock:
                self._reader_error = error
            logger.exception("gateway: dora iteration failed: %s", error)
            self._events.put({"type": "READER_ERROR", "error": str(error)})
        finally:
            self._events.put(self._end_sentinel)
