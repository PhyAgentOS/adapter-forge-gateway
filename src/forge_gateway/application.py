"""Process-level lifecycle owner for the Gateway application."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, cast

import uvicorn

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

from forge_gateway.adapters.dora_runtime import (
    DoraNodeLike,
    DoraRunReason,
    GatewayDoraRunner,
)
from forge_gateway.adapters.http_app import create_app
from forge_gateway.config import GatewayConfig
from forge_gateway.services.runtime_service import GatewayRuntime

logger = get_logger(__name__)

ApplicationPhase = Literal[
    "new",
    "starting",
    "running",
    "stopping",
    "stopped",
    "incomplete",
]


class ServerLike(Protocol):
    started: bool
    should_exit: bool

    def run(self) -> None: ...


class RunnerLike(Protocol):
    @property
    def reader_error(self) -> BaseException | None: ...

    def start(self) -> None: ...

    def run(self) -> DoraRunReason: ...

    def join_reader(self, timeout: float) -> bool: ...


class RunnerFactory(Protocol):
    def __call__(
        self,
        *,
        runtime: GatewayRuntime,
        node: DoraNodeLike,
        stop_event: threading.Event,
    ) -> RunnerLike: ...


@dataclass(frozen=True)
class CloseStepResult:
    """Outcome of one application cleanup step."""

    attempted: bool
    completed: bool
    error: BaseException | None = None

    @property
    def ok(self) -> bool:
        return (not self.attempted) or (self.completed and self.error is None)

    @classmethod
    def skipped(cls) -> CloseStepResult:
        return cls(attempted=False, completed=True)


@dataclass(frozen=True)
class ApplicationCloseResult:
    """Aggregate shutdown outcome without hiding partial cleanup failures."""

    run_loop: CloseStepResult
    server: CloseStepResult
    dora_reader: CloseStepResult
    launcher_watcher: CloseStepResult
    runtime: CloseStepResult

    @property
    def ok(self) -> bool:
        return all(step.ok for _, step in self.steps())

    @property
    def quiescent(self) -> bool:
        return all(step.completed for _, step in self.steps())

    def steps(self) -> tuple[tuple[str, CloseStepResult], ...]:
        return (
            ("run_loop", self.run_loop),
            ("server", self.server),
            ("dora_reader", self.dora_reader),
            ("launcher_watcher", self.launcher_watcher),
            ("runtime", self.runtime),
        )

    def describe_failures(self) -> str:
        failures: list[str] = []
        for name, step in self.steps():
            if step.ok:
                continue
            if step.error is not None:
                failures.append(f"{name}: {step.error}")
            elif not step.completed:
                failures.append(f"{name}: did not stop before shutdown timeout")
            else:
                failures.append(f"{name}: cleanup failed")
        return "; ".join(failures)

    @classmethod
    def empty(cls) -> ApplicationCloseResult:
        skipped = CloseStepResult.skipped()
        return cls(
            run_loop=skipped,
            server=skipped,
            dora_reader=skipped,
            launcher_watcher=skipped,
            runtime=skipped,
        )


class GatewayStartupError(RuntimeError):
    """Raised when the application cannot commit a complete startup."""


@dataclass
class _ApplicationCloseAttempt:
    completed: threading.Event = field(default_factory=threading.Event)
    result: ApplicationCloseResult | None = None
    error: BaseException | None = None


def create_uvicorn_server(
    runtime: GatewayRuntime,
    stop_event: threading.Event,
) -> uvicorn.Server:
    app = create_app(runtime, stop_event=stop_event)
    config = uvicorn.Config(
        app,
        host=runtime.config.host,
        port=runtime.config.port,
        log_level="info",
        lifespan="off",
        ws_ping_interval=None,
        timeout_graceful_shutdown=1,
    )
    return uvicorn.Server(config)


def create_dora_node() -> DoraNodeLike:
    # Initialize Arrow conversion before the Dora reader starts on another thread.
    import pyarrow  # noqa: F401
    import forge_msgs  # noqa: F401
    from dora import Node

    return cast(DoraNodeLike, Node())


def watch_launcher(stop_event: threading.Event) -> None:
    """Request shutdown when the owning forge_launcher disappears."""
    pid_file = os.environ.get("FORGE_LAUNCHER_PID_FILE")
    if not pid_file:
        return
    path = Path(pid_file)
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline and not path.is_file():
        if stop_event.wait(timeout=0.2):
            return
    if not path.is_file():
        logger.error("FORGE_LAUNCHER_PID_FILE=%s did not appear; gateway exits", pid_file)
        stop_event.set()
        return
    while not stop_event.wait(timeout=1.0):
        if not path.is_file():
            logger.info("forge_launcher PID file disappeared; gateway exits")
            stop_event.set()
            return
        try:
            pid = int(path.read_text().strip())
            os.kill(pid, 0)
        except ProcessLookupError:
            logger.info("forge_launcher is no longer running; gateway exits")
            stop_event.set()
            return
        except (PermissionError, OSError, ValueError):
            continue


class GatewayApplication:
    """Own startup, execution, rollback, and shutdown for all Gateway components."""

    def __init__(
        self,
        config: GatewayConfig,
        *,
        runtime_factory: Callable[[GatewayConfig], GatewayRuntime] = GatewayRuntime,
        server_factory: Callable[
            [GatewayRuntime, threading.Event], ServerLike
        ] = create_uvicorn_server,
        node_factory: Callable[[], DoraNodeLike] = create_dora_node,
        runner_factory: RunnerFactory = GatewayDoraRunner,
        watcher_target: Callable[[threading.Event], None] = watch_launcher,
        startup_timeout_sec: float = 5.0,
        thread_join_timeout_sec: float = 2.0,
    ) -> None:
        if startup_timeout_sec <= 0:
            raise ValueError("startup_timeout_sec must be positive")
        if thread_join_timeout_sec < 0:
            raise ValueError("thread_join_timeout_sec must be non-negative")
        self.config = config
        self.stop_event = threading.Event()
        self._runtime_factory = runtime_factory
        self._server_factory = server_factory
        self._node_factory = node_factory
        self._runner_factory = runner_factory
        self._watcher_target = watcher_target
        self._startup_timeout_sec = startup_timeout_sec
        self._thread_join_timeout_sec = thread_join_timeout_sec

        self._lifecycle_lock = threading.RLock()
        self._run_condition = threading.Condition(self._lifecycle_lock)
        self._background_lock = threading.Lock()
        self._phase: ApplicationPhase = "new"
        self._run_active = False
        self._run_started = False
        self._close_result: ApplicationCloseResult | None = None
        self._close_in_progress = False
        self._close_attempt: _ApplicationCloseAttempt | None = None

        self._runtime: GatewayRuntime | None = None
        self._server: ServerLike | None = None
        self._server_thread: threading.Thread | None = None
        self._server_thread_started = False
        self._server_error: BaseException | None = None
        self._server_done = threading.Event()
        self._runner: RunnerLike | None = None
        self._runner_started = False
        self._watcher_thread: threading.Thread | None = None
        self._watcher_thread_started = False
        self._watcher_error: BaseException | None = None

    @property
    def phase(self) -> ApplicationPhase:
        with self._lifecycle_lock:
            return self._phase

    @property
    def runtime(self) -> GatewayRuntime:
        with self._lifecycle_lock:
            if self._runtime is None:
                raise RuntimeError("gateway application runtime is not initialized")
            return self._runtime

    def request_shutdown(self) -> None:
        self.stop_event.set()

    def start(self) -> None:
        """Start every component transactionally or roll back acquired resources."""
        with self._lifecycle_lock:
            if self._phase != "new":
                raise RuntimeError(f"gateway application cannot start from {self._phase}")
            self._phase = "starting"
            try:
                self._runtime = self._runtime_factory(self.config)
                self._ensure_startup_active()

                node = self._node_factory()
                self._ensure_startup_active()
                self._runner = self._runner_factory(
                    runtime=self._runtime,
                    node=node,
                    stop_event=self.stop_event,
                )
                self._runner_started = True
                self._runner.start()
                self._ensure_startup_active()
                self._ensure_runner_healthy()

                self._watcher_thread = threading.Thread(
                    target=self._run_watcher,
                    name="forge_launcher_watch",
                    daemon=True,
                )
                self._watcher_thread_started = True
                self._watcher_thread.start()
                self._ensure_startup_active()

                self._server = self._server_factory(self._runtime, self.stop_event)
                self._server_thread = threading.Thread(
                    target=self._run_server,
                    name="gateway_uvicorn",
                    daemon=True,
                )
                self._server_thread_started = True
                self._server_thread.start()
                setattr(self._server, "thread", self._server_thread)
                self._wait_for_server_startup()
                self._ensure_startup_active()
                self._ensure_server_healthy()
                self._ensure_runner_healthy()
            except BaseException as error:
                self._phase = "stopping"
                result = self._close_resources()
                self._close_result = result
                self._phase = "stopped" if result.quiescent else "incomplete"
                if not result.quiescent:
                    error.add_note(
                        "Gateway startup rollback was incomplete: "
                        f"{result.describe_failures()}"
                    )
                raise
            self._phase = "running"

    def run(self) -> DoraRunReason:
        with self._run_condition:
            if self._phase != "running" or self._runner is None:
                raise RuntimeError(f"gateway application cannot run from {self._phase}")
            if self._run_active:
                raise RuntimeError("gateway application run loop is already active")
            self._run_active = True
            self._run_started = True
            runner = self._runner
        try:
            return runner.run()
        finally:
            with self._run_condition:
                self._run_active = False
                self._run_condition.notify_all()

    def close(self) -> ApplicationCloseResult:
        """Stop all owned components, attempting every cleanup step."""
        self.stop_event.set()
        owner = False
        with self._lifecycle_lock:
            if self._phase == "stopped" and self._close_result is not None:
                return self._close_result
            if self._phase == "new":
                self._close_result = ApplicationCloseResult.empty()
                self._phase = "stopped"
                return self._close_result
            if self._close_in_progress:
                attempt = self._close_attempt
                if attempt is None:
                    raise RuntimeError("gateway application close attempt is missing")
            else:
                self._phase = "stopping"
                self._close_in_progress = True
                attempt = _ApplicationCloseAttempt()
                self._close_attempt = attempt
                owner = True

        if not owner:
            attempt.completed.wait()
            return self._completed_close_attempt(attempt)

        try:
            result = self._close_resources()
        except BaseException as error:
            with self._lifecycle_lock:
                self._phase = "incomplete"
                self._close_in_progress = False
                attempt.error = error
                attempt.completed.set()
            return self._completed_close_attempt(attempt)

        with self._lifecycle_lock:
            self._close_result = result
            self._phase = "stopped" if result.quiescent else "incomplete"
            self._close_in_progress = False
            attempt.result = result
            attempt.completed.set()
        return result

    def _ensure_startup_active(self) -> None:
        if self.stop_event.is_set():
            raise GatewayStartupError("gateway startup was cancelled")

    def _wait_for_server_startup(self) -> None:
        deadline = time.monotonic() + self._startup_timeout_sec
        while True:
            error = self._get_server_error()
            if error is not None:
                raise GatewayStartupError(
                    f"Uvicorn failed during startup: {error}"
                ) from error
            if self._server_done.is_set():
                raise GatewayStartupError("Uvicorn exited before startup completed")
            server = self._server
            if server is not None and server.started:
                self._ensure_server_healthy()
                return
            if self.stop_event.is_set():
                raise GatewayStartupError("gateway startup was cancelled")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise GatewayStartupError(
                    "Uvicorn did not start before the startup timeout"
                )
            self._server_done.wait(timeout=min(0.01, remaining))

    def _ensure_server_healthy(self) -> None:
        error = self._get_server_error()
        if error is not None:
            raise GatewayStartupError(
                f"Uvicorn failed during startup: {error}"
            ) from error
        if self._server_done.is_set():
            raise GatewayStartupError("Uvicorn exited before startup completed")
        if self._server_thread is None or not self._server_thread.is_alive():
            raise GatewayStartupError("Uvicorn thread is not running")

    def _ensure_runner_healthy(self) -> None:
        runner = self._runner
        error = None if runner is None else runner.reader_error
        if error is not None:
            raise GatewayStartupError(
                f"Dora reader failed during startup: {error}"
            ) from error

    def _run_server(self) -> None:
        error: BaseException | None = None
        try:
            server = self._server
            if server is None:
                raise RuntimeError("Uvicorn server is not initialized")
            server.run()
        except BaseException as caught:
            error = caught
        finally:
            if error is None and not self.stop_event.is_set():
                error = RuntimeError("Uvicorn exited unexpectedly")
            if error is not None:
                with self._background_lock:
                    self._server_error = error
            self.stop_event.set()
            self._server_done.set()

    def _run_watcher(self) -> None:
        try:
            self._watcher_target(self.stop_event)
        except BaseException as error:
            with self._background_lock:
                self._watcher_error = error
            self.stop_event.set()

    def _close_resources(self) -> ApplicationCloseResult:
        deadline = time.monotonic() + self._thread_join_timeout_sec
        runtime_errors: list[BaseException] = []
        if self._runtime is not None:
            try:
                self._runtime.begin_close()
            except BaseException as error:
                runtime_errors.append(error)
        self.stop_event.set()

        server_signal_errors: list[BaseException] = []
        if self._server is not None:
            try:
                self._server.should_exit = True
            except BaseException as error:
                server_signal_errors.append(error)

        try:
            run_loop = self._wait_for_run_loop(deadline)
        except BaseException as error:
            run_loop = CloseStepResult(
                attempted=self._run_started,
                completed=False,
                error=error,
            )
        server = self._close_server(deadline, server_signal_errors)
        dora_reader = self._close_dora_reader(deadline)
        launcher_watcher = self._close_watcher(deadline)
        if run_loop.completed and server.completed:
            runtime = self._close_runtime(runtime_errors)
        else:
            runtime = CloseStepResult(
                attempted=True,
                completed=False,
                error=self._combine_errors(
                    "Gateway runtime admission close failed",
                    runtime_errors,
                ),
            )
        return ApplicationCloseResult(
            run_loop=run_loop,
            server=server,
            dora_reader=dora_reader,
            launcher_watcher=launcher_watcher,
            runtime=runtime,
        )

    def _wait_for_run_loop(self, deadline: float) -> CloseStepResult:
        with self._run_condition:
            if not self._run_started:
                return CloseStepResult.skipped()
            while self._run_active:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._run_condition.wait(timeout=remaining)
            return CloseStepResult(attempted=True, completed=not self._run_active)

    def _close_server(
        self,
        deadline: float,
        signal_errors: list[BaseException],
    ) -> CloseStepResult:
        if self._server is None:
            return CloseStepResult.skipped()
        completed = True
        errors = list(signal_errors)
        try:
            if self._server_thread_started and self._server_thread is not None:
                if self._server_thread.is_alive():
                    self._server_thread.join(timeout=self._remaining(deadline))
                completed = not self._server_thread.is_alive()
        except BaseException as error:
            completed = False
            errors.append(error)
        background_error = self._get_server_error()
        if background_error is not None:
            errors.append(background_error)
        return CloseStepResult(
            attempted=True,
            completed=completed,
            error=self._combine_errors("Uvicorn cleanup failed", errors),
        )

    def _close_dora_reader(self, deadline: float) -> CloseStepResult:
        if self._runner is None or not self._runner_started:
            return CloseStepResult.skipped()
        errors: list[BaseException] = []
        try:
            completed = self._runner.join_reader(timeout=self._remaining(deadline))
        except BaseException as error:
            completed = False
            errors.append(error)
        reader_error = self._runner.reader_error
        if reader_error is not None:
            errors.append(reader_error)
        return CloseStepResult(
            attempted=True,
            completed=completed,
            error=self._combine_errors("Dora reader cleanup failed", errors),
        )

    def _close_watcher(self, deadline: float) -> CloseStepResult:
        if self._watcher_thread is None or not self._watcher_thread_started:
            return CloseStepResult.skipped()
        completed = True
        errors: list[BaseException] = []
        try:
            if self._watcher_thread.is_alive():
                self._watcher_thread.join(timeout=self._remaining(deadline))
            completed = not self._watcher_thread.is_alive()
        except BaseException as error:
            completed = False
            errors.append(error)
        with self._background_lock:
            background_error = self._watcher_error
        if background_error is not None:
            errors.append(background_error)
        return CloseStepResult(
            attempted=True,
            completed=completed,
            error=self._combine_errors("launcher watcher cleanup failed", errors),
        )

    def _close_runtime(
        self,
        begin_errors: list[BaseException],
    ) -> CloseStepResult:
        if self._runtime is None:
            return CloseStepResult.skipped()
        errors = list(begin_errors)
        try:
            completed = self._runtime.close()
        except BaseException as error:
            completed = False
            errors.append(error)
        return CloseStepResult(
            attempted=True,
            completed=completed,
            error=self._combine_errors("Gateway runtime cleanup failed", errors),
        )

    @staticmethod
    def _completed_close_attempt(
        attempt: _ApplicationCloseAttempt,
    ) -> ApplicationCloseResult:
        if attempt.error is not None:
            raise RuntimeError("gateway application close failed") from attempt.error
        if attempt.result is None:
            raise RuntimeError("gateway application close result is missing")
        return attempt.result

    def _get_server_error(self) -> BaseException | None:
        with self._background_lock:
            return self._server_error

    @staticmethod
    def _remaining(deadline: float) -> float:
        return max(0.0, deadline - time.monotonic())

    @staticmethod
    def _combine_errors(
        message: str,
        errors: list[BaseException],
    ) -> BaseException | None:
        if not errors:
            return None
        if len(errors) == 1:
            return errors[0]
        return BaseExceptionGroup(message, errors)
