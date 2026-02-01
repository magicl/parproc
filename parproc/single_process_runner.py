"""
Single-process runner: runs tasks in the current process, current thread.
No multiprocessing, no threading; stdout/stderr stay on the console (no capture).
Log file handling (exception capture) is delegated to multi_process_runner.
"""

import time
from collections.abc import Callable
from typing import Any

from .multi_process_runner import write_task_exception_log
from .state import ProcState


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
        ret, error, exc_info = manager.run_task_body(proc, pc, context)
        log_filename = manager.get_log_filename(proc.name)
        write_task_exception_log(log_filename, exc_info)
        manager.complete_proc(proc, ret, error, log_filename)
        manager.try_execute_any()

    def collect(self, manager: Any) -> None:
        pass  # no-op: tasks complete synchronously in start_task
