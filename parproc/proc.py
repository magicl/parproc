"""Proc and ProcContext. Kept in a separate module so term (and others) can import Proc.ERROR_* without importing par."""

import logging
import os
from collections.abc import Callable
from typing import Any, TypeVar

BaseModel: type[Any] | None = None
try:
    from pydantic import BaseModel as _PydanticBaseModel

    BaseModel = _PydanticBaseModel
except ImportError:
    pass

from .types import (
    FAILED_STATES,
    SUCCEEDED_STATES,
    ProcessError,
    ProcState,
    SpecialDep,
    UserError,
)

logger = logging.getLogger('par')

F = TypeVar('F', bound=Callable[..., Any])


class ProcContext:
    """Context passed into a running proc (lives inside the proc process/thread)."""

    def __init__(
        self,
        proc_name: str,
        context: dict[str, Any],
        queue_to_proc: Any,
        queue_to_master: Any,
    ):
        self.proc_name = proc_name
        self.results = context['results']
        self.params = context['params']
        self.args = context['args']
        self.queue_to_proc = queue_to_proc
        self.queue_to_master = queue_to_master

    def _cmd(self, **kwargs: Any) -> Any:
        self.queue_to_master.put(kwargs)
        logger.debug(f'ProcContext request to master: {kwargs}')
        resp = self.queue_to_proc.get()
        logger.debug(f'ProcContext response from master: {resp}')
        return resp

    def get_input(self, message: str = '', password: bool = False) -> Any:
        return self._cmd(req='get-input', message=message, password=password)['resp']

    def create(self, proto_name: str, proc_name: str | None = None) -> list[str]:
        resp = self._cmd(req='create-proc', proto_name=proto_name, proc_name=proc_name)
        return list(resp['proc_names'])

    def run(self, proto_name: str, proc_name: str | None = None) -> None:
        """Create and start a proc (single round-trip run-proc command)."""
        self._cmd(req='run-proc', proto_name=proto_name, proc_name=proc_name)

    def start(self, *names: str) -> None:
        if names:
            self._cmd(req='start-procs', names=list(names))

    def wait(self, *names: str) -> None:
        import time  # pylint: disable=import-outside-toplevel

        while True:
            res = self._cmd(req='check-complete', names=list(names))
            if res['failure']:
                raise ProcessError('Process error [3]')
            if res['complete']:
                break
            logger.info('waiting for sub-proc')
            time.sleep(0.01)
        logger.info(f'wait done. results pre: {self.results}')
        self.results.update(self._cmd(req='get-results', names=list(names))['results'])
        logger.info(f'wait done. results post: {self.results}')


class Proc:
    """
    Decorator for processes.
    name   - identified name of process
    deps   - process dependencies (proc names and/or SpecialDep). will not be run until these are satisfied
    locks  - list of locks. only one process can own a lock at any given time
    """

    # Set by par on load so Proc never imports par (avoids circular import).
    _default_manager_getter: Callable[[], Any] | None = None
    _default_run_task: Callable[..., Any] | None = None

    @classmethod
    def set_defaults(
        cls,
        manager_getter: Callable[[], Any],
        run_task_fn: Callable[..., Any],
    ) -> None:
        cls._default_manager_getter = manager_getter
        cls._default_run_task = run_task_fn

    ERROR_NONE = 0
    ERROR_EXCEPTION = 1
    ERROR_DEP_FAILED = 2
    ERROR_TIMEOUT = 3
    ERROR_NOT_PICKLEABLE = 4
    ERROR_FAILED = 5  # Proc raised ProcFailedError
    ERROR_SKIPPED = 6  # Proc raised ProcSkippedError

    def __init__(
        self,
        name: str | None = None,
        f: F | None = None,
        *,
        deps: list[str] | list[str | SpecialDep] | None = None,
        rdeps: list[str] | None = None,
        locks: list[str] | None = None,
        now: bool = False,
        args: dict[str, Any] | None = None,
        proto: Any = None,
        timeout: float | None = None,
        wave: int = 0,
        special_deps: list[SpecialDep] | None = None,
    ):
        if special_deps is not None:
            self.deps = deps if deps is not None else []
            self.special_deps = special_deps
        else:
            _deps = deps if deps is not None else []
            self.deps = [d for d in _deps if isinstance(d, str)]
            self.special_deps = [d for d in _deps if isinstance(d, SpecialDep)]
            for d in _deps:
                if not isinstance(d, (str, SpecialDep)):
                    raise UserError(f'Proc deps must contain str or SpecialDep values, got {type(d).__name__!r}.')
        self.name = name
        self.rdeps = rdeps if rdeps is not None else []
        self.locks = locks if locks is not None else []
        self.now = now
        self.args = args if args is not None else {}
        self.proto = proto
        self.timeout = timeout
        self.wave = proto.wave if proto is not None else wave
        self.log_filename = ''
        import uuid  # pylint: disable=import-outside-toplevel

        self.run_id = str(uuid.uuid4())
        self.func: Any = None
        self.user_func: Any = None
        self.start_time: float | None = None
        self.end_time: float | None = None
        self.process: Any = None
        self.queue_to_proc: Any = None
        self.queue_to_master: Any = None
        self.state = ProcState.IDLE
        self.error = Proc.ERROR_NONE
        self.more_info = ''
        self.output: Any = None
        if f is not None:
            self.__call__(f)

    def is_running(self) -> bool:
        return self.state == ProcState.RUNNING

    def is_complete(self) -> bool:
        return self.state in SUCCEEDED_STATES or self.state in FAILED_STATES

    def is_failed(self) -> bool:
        return self.state in FAILED_STATES

    def __call__(self, f: F) -> F:
        run_task_fn = Proc._default_run_task
        if run_task_fn is None:
            raise RuntimeError('Proc.set_defaults() was not called (par not loaded?)')

        def func(queue_to_proc: Any, queue_to_master: Any, context: dict[str, Any], name: str) -> None:
            # Run in child process: no manager here, build ProcContext and run task directly.
            logger.info(f'proc "{name}" started')
            pc = ProcContext(name, context, queue_to_proc, queue_to_master)
            ret, error, _exc_info = run_task_fn(f, pc, context)  # pylint: disable=not-callable
            log_filename = os.path.join(str(context['logdir']), name + '.log')
            logger.info(f'proc "{name}" ended: ret = {ret}')
            if BaseModel is not None and isinstance(ret, BaseModel):
                ret = ret.model_dump()
            import pickle as _pickle  # pylint: disable=import-outside-toplevel  # nosec: B403

            msg = {'req': 'proc-complete', 'value': ret, 'log_filename': log_filename, 'error': error}
            try:
                _pickle.dumps(msg)
            except (TypeError, AttributeError, OSError, _pickle.PicklingError) as e:
                err_msg = str(e) if isinstance(e, _pickle.PicklingError) else f'{type(e).__name__}: {e}'
                msg = {
                    'req': 'proc-complete',
                    'value': None,
                    'log_filename': log_filename,
                    'error': Proc.ERROR_NOT_PICKLEABLE,
                    'more_info': f'Return value is not pickleable: {err_msg}',
                }
            queue_to_master.put(msg)
            queue_to_master.close()
            queue_to_master.join_thread()

        if self.name is None:
            self.name = f.__name__
        self.user_func = f
        self.func = func
        manager_getter = Proc._default_manager_getter
        if manager_getter is not None:
            manager_getter().add_proc(self)
        return f
