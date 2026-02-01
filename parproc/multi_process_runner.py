"""
Multi-process runner: runs tasks in separate processes using multiprocessing.
Uses a pipe + reader thread so child stdout/stderr are drained to the log file
without blocking the child on slow disk I/O.
"""

import logging
import os
import queue
import threading
import time
from typing import Any, Callable

from .state import ProcState

def _log_reader_thread(read_fd: int, log_file: Any) -> None:
    """Read from read_fd and write to log_file until EOF."""
    try:
        while True:
            data = os.read(read_fd, 65536)
            if not data:
                break
            log_file.write(data.decode('utf-8', errors='replace'))
    finally:
        try:
            log_file.flush()
        except OSError:
            pass


def _join_log_reader(proc: Any) -> None:
    """Join the log reader thread and close associated fds/file. No-op if runner did not set them."""
    thread = getattr(proc, '_runner_log_reader_thread', None)
    if thread is None:
        return
    thread.join()
    try:
        read_fd = getattr(proc, '_runner_log_read_fd', None)
        if read_fd is not None:
            os.close(read_fd)
    except OSError:
        pass
    try:
        log_file = getattr(proc, '_runner_log_file', None)
        if log_file is not None:
            log_file.close()
    except OSError:
        pass
    for attr in ('_runner_log_reader_thread', '_runner_log_read_fd', '_runner_log_file'):
        if hasattr(proc, attr):
            delattr(proc, attr)


def write_task_exception_log(log_filename: str, exc_info: str | None) -> None:
    """Write exception info to the task log file. Used by SingleProcessRunner (no pipe capture)."""
    with open(log_filename, 'w', encoding='utf-8') as f:
        if exc_info:
            f.write(exc_info)


def _child_entry(
    queue_to_proc: Any,
    queue_to_master: Any,
    context: dict[str, Any],
    name: str,
    pipe_write_fd: int,
    inner_func: Callable[..., None],
) -> None:
    """Child process entry: redirect stdout/stderr to pipe, then run the real task (inner_func)."""
    try:
        with open(os.devnull, encoding='utf-8') as devnull:
            os.dup2(devnull.fileno(), 0)
    except OSError:
        pass
    os.environ['TERM'] = 'dumb'
    os.environ['NO_COLOR'] = '1'
    try:
        os.dup2(pipe_write_fd, 1)
        os.dup2(pipe_write_fd, 2)
    except OSError:
        pass
    try:
        os.close(pipe_write_fd)
    except OSError:
        pass
    # So the log file only has task output, not parproc's own logger (e.g. "proc started").
    _par_logger = logging.getLogger('par')
    _par_logger.propagate = False
    _par_logger.addHandler(logging.NullHandler())
    inner_func(queue_to_proc, queue_to_master, context, name)


class MultiProcessRunner:
    """Runs tasks in separate processes using multiprocessing."""

    def start_task(self, manager: Any, proc: Any) -> None:
        import fcntl  # pylint: disable=import-outside-toplevel
        import multiprocessing as mp  # pylint: disable=import-outside-toplevel

        context = {'args': proc.args, **manager.context}
        manager.logger.info(f'Exec "{proc.name}" with context {context}')

        # Pipe + reader thread: child writes stdout/stderr to pipe; reader thread drains to log file.
        # Wrapper in child does dup2 before proc.func, so par only sees "stdout already redirected".
        log_filename = manager.get_log_filename(proc.name)
        read_fd, write_fd = os.pipe()
        try:
            fcntl.fcntl(read_fd, fcntl.F_SETPIPE_SZ, 1024 * 1024)
        except (OSError, AttributeError):
            pass
        log_file = open(log_filename, 'w', encoding='utf-8')
        reader = threading.Thread(
            target=_log_reader_thread,
            args=(read_fd, log_file),
            name=f'parproc-log-{proc.name}',
            daemon=False,
        )
        reader.start()
        proc._runner_log_reader_thread = reader
        proc._runner_log_read_fd = read_fd
        proc._runner_log_file = log_file

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
            target=_child_entry,
            name=f'parproc-child-{proc.name}',
            args=(proc.queue_to_proc, proc.queue_to_master, context, proc.name, write_fd, proc.func),
        )
        proc.process.start()
        os.close(write_fd)  # only child holds write end now

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
                    _join_log_reader(p)
                    log_filename = manager.get_log_filename(name)
                    manager.complete_proc(
                        p, msg['value'], msg['error'], log_filename, msg.get('more_info')
                    )
                    found_any = True
                elif msg['req'] in (
                    'get-input',
                    'create-proc',
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
                _join_log_reader(p)
                log_filename = manager.get_log_filename(name)
                manager.logger.info(f'proc "{p.name}" timed out')
                manager.complete_proc(p, None, p.ERROR_TIMEOUT, log_filename)

        if found_any:
            manager.try_execute_any()
