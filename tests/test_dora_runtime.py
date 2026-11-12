"""Tests for the Gateway Dora reader and lifecycle runner."""

from __future__ import annotations

from collections.abc import Iterator
import threading
from typing import Any

from forge_msgs import PolicyCommand
import pytest

from forge_gateway import config
from forge_gateway.adapters import dora_runtime
from forge_gateway.adapters.dora_runtime import GatewayDoraRunner
from forge_gateway.services.runtime_service import GatewayRuntime


class _Node:
    def __init__(self, events: list[dict[str, Any]] | None = None) -> None:
        self.events = list(events or [])
        self.outputs: list[tuple[str, Any]] = []
        self.iterator_thread_id: int | None = None
        self.send_thread_ids: list[int] = []

    def __iter__(self) -> Iterator[dict[str, Any]]:
        self.iterator_thread_id = threading.get_ident()
        yield from self.events

    def send_output(self, output_id: str, data: Any, /) -> None:
        self.send_thread_ids.append(threading.get_ident())
        self.outputs.append((output_id, data))


class _FailingNode(_Node):
    def __iter__(self) -> Iterator[dict[str, Any]]:
        self.iterator_thread_id = threading.get_ident()
        raise RuntimeError("reader failed")
        yield  # pragma: no cover


class _BlockingNode(_Node):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def __iter__(self) -> Iterator[dict[str, Any]]:
        self.iterator_thread_id = threading.get_ident()
        self.entered.set()
        self.release.wait(timeout=2.0)
        return
        yield  # pragma: no cover


def _runtime() -> GatewayRuntime:
    return GatewayRuntime(config.GatewayConfig.from_dict({"joint_order": ["j1"]}))


def _runner(
    runtime: GatewayRuntime,
    node: _Node,
    *,
    stop_event: threading.Event | None = None,
    poll_timeout: float = 0.01,
) -> GatewayDoraRunner:
    return GatewayDoraRunner(
        runtime=runtime,
        node=node,
        stop_event=stop_event or threading.Event(),
        poll_timeout=poll_timeout,
    )


def test_poll_timeout_and_non_terminal_events_drain_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    node = _Node()
    calls: list[str] = []

    def handle_input(runtime_arg: object, input_id: str, value: object) -> None:
        assert runtime_arg is runtime
        calls.append(f"input:{input_id}:{value}")

    def drain(runtime_arg: object, node_arg: object) -> None:
        assert runtime_arg is runtime
        assert node_arg is node
        calls.append("drain")

    monkeypatch.setattr(dora_runtime, "handle_dora_input", handle_input)
    monkeypatch.setattr(dora_runtime, "drain_commands", drain)
    runner = _runner(runtime, node)
    try:
        assert runner.handle_poll(None) is None
        assert runner.handle_poll({"type": "UNKNOWN"}) is None
        assert runner.handle_poll({"type": "INPUT", "id": "tick", "value": 3}) is None

        assert calls == ["drain", "drain", "input:tick:3", "drain"]
    finally:
        runtime.close()


def test_input_failure_sets_error_and_still_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    node = _Node()
    drains: list[None] = []

    def fail_input(runtime_arg: object, input_id: str, value: object) -> None:
        del runtime_arg, input_id, value
        raise ValueError("bad payload")

    monkeypatch.setattr(dora_runtime, "handle_dora_input", fail_input)
    monkeypatch.setattr(
        dora_runtime,
        "drain_commands",
        lambda runtime_arg, node_arg: drains.append(None),
    )
    runner = _runner(runtime, node)
    try:
        assert runner.handle_poll(
            {"type": "INPUT", "id": "proprio_state", "value": object()}
        ) is None

        assert drains == [None]
        with runtime.lock:
            assert runtime.last_error == "input proprio_state failed: bad payload"
    finally:
        runtime.close()


def test_stop_and_error_do_not_drain_pending_commands() -> None:
    for event, expected_reason in (
        ({"type": "STOP"}, "stop"),
        ({"type": "ERROR", "error": "broken"}, "error"),
    ):
        runtime = _runtime()
        node = _Node()
        runtime.enqueue_policy_command("pending")
        try:
            reason = _runner(runtime, node).handle_poll(event)

            assert reason == expected_reason
            assert runtime.command_queue.qsize() == 1
            assert node.outputs == []
            if expected_reason == "error":
                with runtime.lock:
                    assert runtime.last_error == "dora error: broken"
        finally:
            runtime.close()


def test_timeout_drains_mailbox_on_lifecycle_thread_only() -> None:
    runtime = _runtime()
    node = _BlockingNode()
    stop_event = threading.Event()
    runner = _runner(runtime, node, stop_event=stop_event)
    runtime.enqueue_policy_command("from_http", {"value": 1})
    runner.start()
    try:
        assert node.entered.wait(timeout=1.0)

        assert runner.handle_poll(None) is None

        assert runtime.command_queue.empty()
        assert len(node.outputs) == 1
        message = PolicyCommand.from_arrow(node.outputs[0][1])
        assert message.command == "from_http"
        assert node.send_thread_ids == [threading.get_ident()]
        assert node.iterator_thread_id is not None
        assert node.iterator_thread_id != node.send_thread_ids[0]
    finally:
        stop_event.set()
        node.release.set()
        assert runner.join_reader(timeout=1.0)
        runtime.close()


def test_reader_natural_eof_returns_without_processing_pending_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    node = _Node([{"type": "INPUT", "id": "tick", "value": None}])
    handled: list[str] = []
    monkeypatch.setattr(
        dora_runtime,
        "handle_dora_input",
        lambda runtime_arg, input_id, value: handled.append(input_id),
    )
    runner = _runner(runtime, node)
    try:
        runner.start()
        assert runner.join_reader(timeout=1.0)

        assert runner.run() == "eof"
        assert handled == []
    finally:
        runtime.close()


def test_control_event_overtakes_pending_coalesced_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    node = _Node(
        [
            {"type": "INPUT", "id": "proprio_state", "value": "pending"},
            {"type": "STOP"},
        ]
    )
    handled: list[str] = []
    monkeypatch.setattr(
        dora_runtime,
        "handle_dora_input",
        lambda runtime_arg, input_id, value: handled.append(input_id),
    )
    runner = _runner(runtime, node)
    try:
        runner.start()
        assert runner.join_reader(timeout=1.0)

        assert runner.run() == "stop"
        assert handled == []
    finally:
        runtime.close()


def test_reader_exception_is_durable_across_shutdown_race() -> None:
    runtime = _runtime()
    stop_event = threading.Event()
    runner = _runner(runtime, _FailingNode(), stop_event=stop_event)
    try:
        runner.start()
        assert runner.join_reader(timeout=1.0)
        stop_event.set()

        assert runner.run() == "reader_error"
        with runtime.lock:
            assert runtime.last_error == "dora reader failed: reader failed"
    finally:
        runtime.close()


def test_preexisting_shutdown_skips_polling() -> None:
    runtime = _runtime()
    stop_event = threading.Event()
    stop_event.set()
    try:
        assert _runner(runtime, _Node(), stop_event=stop_event).run() == "shutdown"
    finally:
        runtime.close()


def test_join_reader_reports_blocked_iterator() -> None:
    runtime = _runtime()
    node = _BlockingNode()
    stop_event = threading.Event()
    runner = _runner(runtime, node, stop_event=stop_event)
    runner.start()
    try:
        assert node.entered.wait(timeout=1.0)
        assert runner.join_reader(timeout=0.0) is False
    finally:
        stop_event.set()
        node.release.set()
        assert runner.join_reader(timeout=1.0)
        runtime.close()


def test_unexpected_drain_exception_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()

    def interrupt(runtime_arg: object, node_arg: object) -> None:
        del runtime_arg, node_arg
        raise KeyboardInterrupt

    monkeypatch.setattr(dora_runtime, "drain_commands", interrupt)
    try:
        with pytest.raises(KeyboardInterrupt):
            _runner(runtime, _Node()).handle_poll(None)
    finally:
        runtime.close()


class _PollingNode(_Node):
    def next(self, timeout: float) -> dict[str, Any] | None:
        assert timeout > 0
        return self.events.pop(0) if self.events else None


def test_native_receive_timeout_does_not_stop_gateway() -> None:
    runtime = _runtime()
    node = _PollingNode([
        {"type": "ERROR", "error": "Timeout event stream error: Receiver timed out\n\nLocation: native.rs:1"},
        {"type": "INPUT", "id": "tick"},
        {"type": "STOP"},
    ])
    runner = _runner(runtime, node)
    try:
        runner.start()
        assert runner.run() == "stop"
        assert runtime.last_error is None
    finally:
        runner.join_reader(1.0)
        runtime.close()


def test_native_receive_eof_ends_gateway_loop() -> None:
    runtime = _runtime()
    runner = _runner(runtime, _PollingNode())
    try:
        runner.start()
        assert runner.run() == "eof"
    finally:
        runner.join_reader(1.0)
        runtime.close()


@pytest.mark.parametrize("error", ["transport failed", "upstream peer timed out"])
def test_native_receive_error_remains_a_failure(error: str) -> None:
    runtime = _runtime()
    runner = _runner(runtime, _PollingNode([{"type": "ERROR", "error": error}]))
    try:
        runner.start()
        assert runner.run() == "error"
        assert error in str(runtime.last_error)
    finally:
        runner.join_reader(1.0)
        runtime.close()
