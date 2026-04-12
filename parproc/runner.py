"""
Runner interface and implementations: MultiProcessRunner and SingleProcessRunner.
Runners abstract how tasks are executed (subprocess vs same process/thread).
"""

import queue
import time
from collections.abc import Callable
from typing import Any, Protocol

from .proc import Proc
from .types import ProcState

# Avoid circular import: runner is used by par. Runners only call methods on the manager.

_SYNC_REQUESTS = (
    'get-input',
    'create-proc',
    'run-proc',
    'start-procs',
    'check-complete',
    'get-results',
)


class SyncChannel:
    """Channel that implements put/get by calling a handler synchronously (single-process mode)."""

    def __init__(self, handler: Callable[[dict[str, Any]], dict[str, Any]]):
        self._handler = handler
        self._response: dict[str, Any] | None = None

    def put(self, msg: dict[str, Any]) -> None:
        self._response = self._handler(msg)

    def get(self) -> dict[str, Any]:
        assert self._response is not None  # nosec
        return self._response


class Runner(Protocol):
    """Protocol for task runners."""

    def start_task(self, manager: Any, proc: Any) -> None:
        """Start the given proc (async for multi-process, sync for single-process)."""
        ...  # pylint: disable=unnecessary-ellipsis  # Standard for Protocol abstract method body

    def collect(self, manager: Any) -> None:
        """Poll for completed tasks and handle messages (no-op for single-process)."""
        ...  # pylint: disable=unnecessary-ellipsis  # Standard for Protocol abstract method body


class MultiProcessRunner:
    """Runs tasks in separate processes using multiprocessing."""

    def __init__(self) -> None:
        import multiprocessing as mp  # pylint: disable=import-outside-toplevel

        self._ctx: Any
        # Python 3.14 changed POSIX defaults away from fork. This library relies on
        # fork-compatible semantics (local closures in tests/examples), so prefer a
        # fork context when available to preserve existing behavior.
        if 'fork' in mp.get_all_start_methods():
            self._ctx = mp.get_context('fork')
        else:
            self._ctx = mp.get_context()

    def start_task(self, manager: Any, proc: Any) -> None:
        context = {'args': proc.args, **manager.context}
        manager.logger.info(f'Exec "{proc.name}" with context {context}')

        proc.queue_to_proc = self._ctx.Queue()
        proc.queue_to_master = self._ctx.Queue()
        proc.state = ProcState.RUNNING
        proc.start_time = time.time()

        if manager.task_db_path is not None:
            manager.record_task_start(proc)

        for l in proc.locks:
            manager.locks[l] = proc

        if proc.name is not None:
            proc.log_filename = manager.get_log_filename(proc.name)
        manager.term.start_proc(proc)
        proc.process = self._ctx.Process(
            target=proc.func,
            name=f'parproc-child-{proc.name}',
            args=(proc.queue_to_proc, proc.queue_to_master, context, proc.name),
        )
        try:
            proc.process.start()
        except Exception as e:  # pylint: disable=broad-exception-caught
            # Do not leave proc in RUNNING state; that would stall wait loops forever.
            log_filename = manager.get_log_filename(proc.name)
            manager.complete_proc(
                proc,
                None,
                Proc.ERROR_EXCEPTION,
                log_filename,
                more_info=f'failed to start child process: {type(e).__name__}: {e}',
            )

    def _handle_proc_message(self, manager: Any, proc: Any, msg: dict[str, Any], prefix: str = '') -> bool:
        """Handle a single child->master message. Returns True when a proc completes."""
        name = proc.name
        manager.logger.debug(f'got {prefix}msg from proc "{name}": {msg}')
        req = msg['req']
        if req == 'proc-complete':
            log_filename = msg.get('log_filename') or manager.get_log_filename(name)
            more_info = msg.get('more_info')
            manager.complete_proc(proc, msg['value'], msg['error'], log_filename, more_info=more_info)
            return True
        if req in _SYNC_REQUESTS:
            resp = manager.handle_sync_request(msg)
            proc.queue_to_proc.put(resp)
            return False
        raise manager.raise_user_error(f'unknown call: {req}')

    def collect(self, manager: Any) -> None:
        found_any = False
        for name in list(manager.procs):
            p = manager.procs[name]
            if not p.is_running():
                continue
            assert p.queue_to_master is not None  # nosec
            assert p.queue_to_proc is not None  # nosec
            try:
                msg = p.queue_to_master.get_nowait()
            except queue.Empty:
                pass
            else:
                found_any = self._handle_proc_message(manager, p, msg) or found_any

            # Child exited without sending proc-complete: avoid hanging forever in RUNNING.
            if p.is_running() and p.process is not None and not p.process.is_alive():
                # Drain any pending completion message that may have raced with is_alive().
                while True:
                    try:
                        late_msg = p.queue_to_master.get_nowait()
                    except queue.Empty:
                        break
                    found_any = self._handle_proc_message(manager, p, late_msg, prefix='late ') or found_any
                    if not p.is_running():
                        break

                if p.is_running():
                    exit_code = p.process.exitcode
                    log_filename = manager.get_log_filename(name)
                    manager.complete_proc(
                        p,
                        None,
                        Proc.ERROR_EXCEPTION,
                        log_filename,
                        more_info=f'child exited before completion message (exit code: {exit_code})',
                    )
                    found_any = True

            if (
                p.is_running()
                and p.timeout is not None
                and p.start_time is not None
                and (time.time() - p.start_time) > p.timeout
            ):
                if p.process is not None:
                    p.process.terminate()
                log_filename = manager.get_log_filename(name)
                manager.logger.info(f'proc "{p.name}" timed out')
                manager.complete_proc(p, None, p.ERROR_TIMEOUT, log_filename)

        if found_any:
            manager.try_execute_any()


class SingleProcessRunner:
    """Runs tasks in the current process, current thread (no multiprocessing, no threading)."""

    def start_task(self, manager: Any, proc: Any) -> None:
        context = {'args': proc.args, **manager.context}
        manager.logger.info(f'Exec "{proc.name}" with context {context}')

        proc.state = ProcState.RUNNING
        proc.start_time = time.time()

        if manager.task_db_path is not None:
            manager.record_task_start(proc)

        for l in proc.locks:
            manager.locks[l] = proc

        if proc.name is not None:
            proc.log_filename = manager.get_log_filename(proc.name)
        manager.term.start_proc(proc)

        sync_channel = SyncChannel(manager.handle_sync_request)
        pc = manager.build_pc(proc, context, sync_channel, sync_channel)
        ret, error, log_filename, more_info = manager.run_task(proc, pc, context, redirect=False)
        manager.complete_proc(proc, ret, error, log_filename, more_info=more_info)
        manager.try_execute_any()

    def collect(self, manager: Any) -> None:
        pass  # no-op: tasks complete synchronously in start_task
