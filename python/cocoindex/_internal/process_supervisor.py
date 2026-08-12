"""Lifecycle supervision for one persistent spawned worker process.

The supervisor deliberately owns both the process and its pipe.  This lets an
async caller cancel a live call and know that the child has been reaped before
the cancellation is propagated.  Policy such as timeouts and retries belongs
to the caller; this module performs exactly one execution attempt.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import pickle
import queue
import threading
import time
import traceback
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from typing import Any, NoReturn


_POLL_INTERVAL_SECONDS = 0.02
_TERMINATE_GRACE_SECONDS = 1.0
_KILL_GRACE_SECONDS = 1.0
_MANAGER_JOIN_SECONDS = 3.0

_READY = "ready"
_RUN = "run"
_RESULT = "result"
_ERROR = "error"
_STOP = "stop"
_STOPPED = "stopped"

_WorkerFunction = Callable[[bytes], bytes]
_WorkerInitializer = Callable[[int], None]


class _WorkerCrashedError(RuntimeError):
    """The worker exited without returning a result for the active call."""

    generation_id: int
    exit_code: int | None

    def __init__(self, generation_id: int, exit_code: int | None, detail: str) -> None:
        self.generation_id = generation_id
        self.exit_code = exit_code
        super().__init__(
            f"Worker generation {generation_id} crashed ({detail}); "
            f"exit code: {exit_code}"
        )


class _RemoteExecutionError(RuntimeError):
    """Fallback for a worker exception that could not be serialized."""


class _RemoteTraceback(Exception):
    """Traceback captured in the worker process."""

    def __init__(self, traceback_text: str) -> None:
        super().__init__(f'\n"""\n{traceback_text}"""')


class _ProcessSupervisorError(RuntimeError):
    """The supervisor itself can no longer execute requests safely."""


class _ProcessSupervisorClosedError(_ProcessSupervisorError):
    """The supervisor was closed while a request was pending."""


class _ProcessSupervisorPoisonedError(_ProcessSupervisorError):
    """A worker could not be reaped, so starting another would be unsafe."""


class _RequestCancelledError(Exception):
    """Internal acknowledgement that cancellation cleanup is complete."""


@dataclass(slots=True)
class _Request:
    call_id: int
    payload: bytes
    outcome: Future[bytes] = field(default_factory=Future)
    cancel_requested: threading.Event = field(default_factory=threading.Event)
    _state_lock: threading.Lock = field(default_factory=threading.Lock)
    _started: bool = False

    def mark_started(self) -> bool:
        """Claim this request for the manager, unless it was cancelled queued."""
        with self._state_lock:
            if self.outcome.done() or self.cancel_requested.is_set():
                if not self.outcome.done():
                    self.outcome.set_exception(_RequestCancelledError())
                return False
            self._started = True
            return True

    def request_cancellation(self) -> None:
        """Cancel queued work immediately; active work is reaped by the manager."""
        self.cancel_requested.set()
        with self._state_lock:
            if not self._started and not self.outcome.done():
                self.outcome.set_exception(_RequestCancelledError())

    def set_result(self, result: bytes) -> None:
        """Complete the request exactly once across manager/caller races."""
        with self._state_lock:
            if not self.outcome.done():
                self.outcome.set_result(result)

    def set_exception(self, error: BaseException) -> None:
        """Fail the request exactly once across manager/caller races."""
        with self._state_lock:
            if not self.outcome.done():
                self.outcome.set_exception(error)


@dataclass(slots=True)
class _WorkerGeneration:
    generation_id: int
    process: BaseProcess
    connection: Connection
    ready: bool = False


@dataclass(slots=True)
class _CallOutcome:
    result: bytes | None = None
    error: BaseException | None = None


_QUEUE_STOP = object()


def _encode_message(message: tuple[Any, ...]) -> bytes:
    return pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)


def _safe_exception_text(error: BaseException) -> str:
    try:
        return str(error)
    except BaseException:
        return "<exception string conversion failed>"


def _worker_main(
    connection: Connection,
    parent_pid: int,
    generation_id: int,
    worker_fn: _WorkerFunction,
    initializer: _WorkerInitializer | None,
) -> None:
    """Receive calls until the parent closes the pipe or requests shutdown."""
    try:
        if initializer is not None:
            initializer(parent_pid)
        connection.send_bytes(_encode_message((_READY, generation_id)))

        while True:
            message = pickle.loads(connection.recv_bytes())
            kind = message[0]
            if kind == _STOP:
                if message != (_STOP, generation_id):
                    raise RuntimeError("Invalid worker stop message")
                connection.send_bytes(_encode_message((_STOPPED, generation_id)))
                return
            if kind != _RUN or len(message) != 4:
                raise RuntimeError("Invalid worker request message")

            _, request_generation, call_id, payload = message
            if request_generation != generation_id or not isinstance(payload, bytes):
                raise RuntimeError("Worker request belongs to a different generation")

            try:
                result = worker_fn(payload)
            except BaseException as error:
                remote_traceback = traceback.format_exc()
                try:
                    serialized_error: bytes | None = pickle.dumps(
                        error, protocol=pickle.HIGHEST_PROTOCOL
                    )
                except BaseException:
                    serialized_error = None
                error_type = f"{type(error).__module__}.{type(error).__qualname__}"
                response: tuple[Any, ...] = (
                    _ERROR,
                    generation_id,
                    call_id,
                    serialized_error,
                    error_type,
                    _safe_exception_text(error),
                    remote_traceback,
                )
            else:
                response = (_RESULT, generation_id, call_id, result)
            connection.send_bytes(_encode_message(response))
    except (BrokenPipeError, EOFError, OSError):
        # The parent went away or retired this generation.
        return
    finally:
        connection.close()


class _SingleProcessSupervisor:
    """Own one persistent worker behind a small async execution interface.

    A dedicated daemon thread is the sole owner of the parent pipe endpoint and
    process handle.  Requests may therefore originate from different asyncio
    event loops without binding supervisor state to any one loop.
    """

    def __init__(
        self,
        worker_fn: _WorkerFunction,
        *,
        initializer: _WorkerInitializer | None = None,
        process_name: str = "cocoindex-isolated-worker",
    ) -> None:
        self._worker_fn = worker_fn
        self._initializer = initializer
        self._process_name = process_name
        self._context = mp.get_context("spawn")
        self._owner_pid = os.getpid()
        self._requests: queue.Queue[_Request | object] = queue.Queue()
        self._state_lock = threading.Lock()
        self._shutdown_requested = threading.Event()
        self._manager_thread: threading.Thread | None = None
        self._closed = False
        self._next_call_id = 1

    async def execute(self, payload: bytes) -> bytes:
        """Execute one payload, reaping active work before propagating cancellation."""
        request = self._enqueue(payload)
        wrapped_outcome = asyncio.wrap_future(request.outcome)
        try:
            return await asyncio.shield(wrapped_outcome)
        except asyncio.CancelledError as cancelled:
            request.request_cancellation()
            # asyncio.wait_for waits for this acknowledgement.  Do not let a
            # second cancellation release the caller before the child is gone.
            while not wrapped_outcome.done():
                try:
                    await asyncio.shield(wrapped_outcome)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break
            raise cancelled

    async def aclose(self) -> None:
        """Close the supervisor and wait without blocking the current event loop."""
        thread = self._begin_close()
        if thread is None or thread is threading.current_thread():
            return
        await asyncio.to_thread(thread.join, _MANAGER_JOIN_SECONDS)
        if thread.is_alive():
            raise _ProcessSupervisorError("Process supervisor did not stop in time")

    def close(self) -> None:
        """Synchronous, idempotent shutdown used by interpreter cleanup."""
        thread = self._begin_close()
        if thread is None or thread is threading.current_thread():
            return
        thread.join(_MANAGER_JOIN_SECONDS)
        if thread.is_alive():
            raise _ProcessSupervisorError("Process supervisor did not stop in time")

    def _enqueue(self, payload: bytes) -> _Request:
        if os.getpid() != self._owner_pid:
            raise _ProcessSupervisorError(
                "Process supervisor cannot be reused after the parent forks"
            )
        if not isinstance(payload, bytes):
            raise TypeError("Process supervisor payload must be bytes")

        with self._state_lock:
            if self._closed:
                raise _ProcessSupervisorClosedError("Process supervisor is closed")
            request = _Request(call_id=self._next_call_id, payload=payload)
            self._next_call_id += 1
            if self._manager_thread is None:
                self._manager_thread = threading.Thread(
                    target=self._run_manager,
                    name=f"{self._process_name}-supervisor",
                    daemon=True,
                )
                self._manager_thread.start()
            self._requests.put(request)
            return request

    def _begin_close(self) -> threading.Thread | None:
        with self._state_lock:
            if not self._closed:
                self._closed = True
                self._shutdown_requested.set()
                self._requests.put(_QUEUE_STOP)
            return self._manager_thread

    def _run_manager(self) -> None:
        worker: _WorkerGeneration | None = None
        generation_id = 0
        poisoned_error: _ProcessSupervisorPoisonedError | None = None
        try:
            while True:
                item = self._requests.get()
                if item is _QUEUE_STOP:
                    break
                assert isinstance(item, _Request)
                request = item
                if not request.mark_started():
                    continue
                if poisoned_error is not None:
                    self._set_request_exception(request, poisoned_error)
                    continue
                if self._shutdown_requested.is_set():
                    self._set_request_exception(
                        request,
                        _ProcessSupervisorClosedError("Process supervisor is closing"),
                    )
                    break

                try:
                    if worker is None:
                        generation_id += 1
                        worker = self._start_worker(generation_id)
                        self._wait_until_ready(worker, request)
                    outcome = self._execute_request(worker, request)
                except (_RequestCancelledError, _ProcessSupervisorClosedError) as error:
                    worker = None
                    self._set_request_exception(request, error)
                except _WorkerCrashedError as error:
                    worker = None
                    self._set_request_exception(request, error)
                except _ProcessSupervisorPoisonedError as error:
                    poisoned_error = error
                    self._set_request_exception(request, error)
                except BaseException as error:
                    if worker is not None:
                        try:
                            self._retire_worker(worker, graceful=False)
                        except _ProcessSupervisorPoisonedError as retire_error:
                            poisoned_error = retire_error
                    worker = None
                    self._set_request_exception(request, error)
                else:
                    if outcome.error is not None:
                        self._set_request_exception(request, outcome.error)
                    else:
                        assert outcome.result is not None
                        self._set_request_result(request, outcome.result)

                if self._shutdown_requested.is_set():
                    break
        finally:
            if worker is not None:
                try:
                    self._retire_worker(worker, graceful=True)
                except _ProcessSupervisorPoisonedError:
                    pass
            self._fail_queued_requests()

    def _start_worker(self, generation_id: int) -> _WorkerGeneration:
        parent_connection, child_connection = self._context.Pipe(duplex=True)
        process = self._context.Process(
            target=_worker_main,
            args=(
                child_connection,
                self._owner_pid,
                generation_id,
                self._worker_fn,
                self._initializer,
            ),
            name=self._process_name,
            daemon=False,
        )
        try:
            process.start()
        except BaseException:
            parent_connection.close()
            child_connection.close()
            raise
        child_connection.close()
        return _WorkerGeneration(generation_id, process, parent_connection)

    def _wait_until_ready(self, worker: _WorkerGeneration, request: _Request) -> None:
        while True:
            self._abort_if_requested(worker, request)
            message = self._receive_if_available(worker)
            if message is not None:
                if message == (_READY, worker.generation_id):
                    worker.ready = True
                    return
                self._raise_worker_crashed(worker, "invalid startup response")
            if not worker.process.is_alive():
                self._raise_worker_crashed(worker, "exited during startup")

    def _execute_request(
        self, worker: _WorkerGeneration, request: _Request
    ) -> _CallOutcome:
        assert worker.ready
        try:
            worker.connection.send_bytes(
                _encode_message(
                    (_RUN, worker.generation_id, request.call_id, request.payload)
                )
            )
        except (BrokenPipeError, EOFError, OSError):
            self._raise_worker_crashed(worker, "request pipe closed")

        while True:
            self._abort_if_requested(worker, request)
            message = self._receive_if_available(worker)
            if message is not None:
                # Cancellation wins if it raced with a just-arrived response.
                self._abort_if_requested(worker, request)
                return self._decode_call_response(worker, request, message)
            if not worker.process.is_alive():
                self._raise_worker_crashed(worker, "exited while executing")

    def _receive_if_available(
        self, worker: _WorkerGeneration
    ) -> tuple[Any, ...] | None:
        try:
            if not worker.connection.poll(_POLL_INTERVAL_SECONDS):
                return None
            message: object = pickle.loads(worker.connection.recv_bytes())
        except (BrokenPipeError, EOFError, OSError, ValueError):
            self._raise_worker_crashed(worker, "response pipe closed")
        except BaseException:
            self._raise_worker_crashed(worker, "invalid response payload")
        if not isinstance(message, tuple):
            self._raise_worker_crashed(worker, "invalid response message")
        return message

    def _decode_call_response(
        self,
        worker: _WorkerGeneration,
        request: _Request,
        message: tuple[Any, ...],
    ) -> _CallOutcome:
        if len(message) < 3:
            self._raise_worker_crashed(worker, "truncated response")
        kind, generation_id, call_id = message[:3]
        if generation_id != worker.generation_id or call_id != request.call_id:
            self._raise_worker_crashed(worker, "response identity mismatch")

        if kind == _RESULT and len(message) == 4 and isinstance(message[3], bytes):
            return _CallOutcome(result=message[3])
        if kind != _ERROR or len(message) != 7:
            self._raise_worker_crashed(worker, "unknown response kind")

        _, _, _, serialized_error, error_type, error_message, remote_traceback = message
        error: BaseException
        if isinstance(serialized_error, bytes):
            try:
                restored = pickle.loads(serialized_error)
            except BaseException:
                restored = None
            if isinstance(restored, BaseException):
                error = restored
            else:
                error = _RemoteExecutionError(f"Remote {error_type}: {error_message}")
        else:
            error = _RemoteExecutionError(f"Remote {error_type}: {error_message}")
        if isinstance(remote_traceback, str):
            error.__cause__ = _RemoteTraceback(remote_traceback)
        return _CallOutcome(error=error)

    def _abort_if_requested(self, worker: _WorkerGeneration, request: _Request) -> None:
        if request.cancel_requested.is_set():
            self._retire_worker(worker, graceful=False)
            raise _RequestCancelledError()
        if self._shutdown_requested.is_set():
            self._retire_worker(worker, graceful=False)
            raise _ProcessSupervisorClosedError("Process supervisor is closing")

    def _raise_worker_crashed(self, worker: _WorkerGeneration, detail: str) -> NoReturn:
        exit_code = self._retire_worker(worker, graceful=False)
        raise _WorkerCrashedError(worker.generation_id, exit_code, detail)

    def _retire_worker(
        self, worker: _WorkerGeneration, *, graceful: bool
    ) -> int | None:
        process = worker.process
        connection = worker.connection
        exit_code: int | None = None
        try:
            if graceful and process.is_alive():
                try:
                    connection.send_bytes(
                        _encode_message((_STOP, worker.generation_id))
                    )
                except (BrokenPipeError, EOFError, OSError):
                    pass
                deadline = time.monotonic() + _TERMINATE_GRACE_SECONDS
                while process.is_alive() and time.monotonic() < deadline:
                    try:
                        response_available = connection.poll(_POLL_INTERVAL_SECONDS)
                    except (OSError, ValueError):
                        break
                    if response_available:
                        try:
                            message = pickle.loads(connection.recv_bytes())
                        except BaseException:
                            break
                        if message == (_STOPPED, worker.generation_id):
                            break
                    process.join(timeout=0)
                process.join(timeout=max(0.0, deadline - time.monotonic()))

            if process.is_alive():
                process.terminate()
                process.join(_TERMINATE_GRACE_SECONDS)
            if process.is_alive():
                process.kill()
                process.join(_KILL_GRACE_SECONDS)
            if process.is_alive():
                raise _ProcessSupervisorPoisonedError(
                    f"Worker generation {worker.generation_id} could not be reaped"
                )
        finally:
            connection.close()
            if not process.is_alive():
                exit_code = process.exitcode
                process.close()
        return exit_code

    def _fail_queued_requests(self) -> None:
        while True:
            try:
                item = self._requests.get_nowait()
            except queue.Empty:
                return
            if isinstance(item, _Request):
                self._set_request_exception(
                    item,
                    _ProcessSupervisorClosedError("Process supervisor is closed"),
                )

    @staticmethod
    def _set_request_result(request: _Request, result: bytes) -> None:
        request.set_result(result)

    @staticmethod
    def _set_request_exception(request: _Request, error: BaseException) -> None:
        request.set_exception(error)
