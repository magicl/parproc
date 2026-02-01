"""
Runner interface and implementations: MultiProcessRunner and SingleProcessRunner.
Runners abstract how tasks are executed (subprocess vs same process/thread).
"""

import queue
import time
from collections.abc import Callable
from typing import Any, Protocol

from .state import ProcState

# Avoid circular import: runner is used by par. Runners only call methods on the manager.


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

    def start_task(self, manager: Any, proc: Any) -> None:
        import multiprocessing as mp  # pylint: disable=import-outside-toplevel

        context = {'args': proc.args, **manager.context}
        manager.logger.info(f'Exec "{proc.name}" with context {context}')

        proc.queue_to_proc = mp.Queue()
        proc.queue_to_master = mp.Queue()
        proc.state = ProcState.RUNNING
        proc.start_time = time.time()

        if manager.task_db_path is not None:
            manager.record_task_start(proc)

        for l in proc.locks:
            manager.locks[l] = proc

        manager.term.start_proc(proc)
        proc.process = mp.Process(
            target=proc.func,
            name=f'parproc-child-{proc.name}',
            args=(proc.queue_to_proc, proc.queue_to_master, context, proc.name),
        )
        proc.process.start()

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
                manager.logger.debug(f'got msg from proc "{name}": {msg}')
                if msg['req'] == 'proc-complete':
                    log_filename = msg.get('log_filename') or manager.get_log_filename(name)
                    more_info = msg.get('more_info')
                    manager.complete_proc(p, msg['value'], msg['error'], log_filename, more_info=more_info)
                    found_any = True
                elif msg['req'] in (
                    'get-input',
                    'create-proc',
                    'run-proc',
                    'start-procs',
                    'check-complete',
                    'get-results',
                ):
                    resp = manager.handle_sync_request(msg)
                    p.queue_to_proc.put(resp)
                else:
                    raise manager.raise_user_error(f'unknown call: {msg["req"]}')

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

        manager.term.start_proc(proc)

        sync_channel = SyncChannel(manager.handle_sync_request)
        pc = manager.build_pc(proc, context, sync_channel, sync_channel)
        ret, error, log_filename = manager.run_task(proc, pc, context, redirect=False)
        manager.complete_proc(proc, ret, error, log_filename)
        manager.try_execute_any()

    def collect(self, manager: Any) -> None:
        pass  # no-op: tasks complete synchronously in start_task
