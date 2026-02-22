import datetime
import fnmatch
import inspect
import itertools
import logging
import multiprocessing as mp
import os
import re
import signal
import sys
import tempfile
import time
import traceback
from collections import OrderedDict
from collections.abc import Callable, Sequence
from typing import Any, Optional, TypeVar, Union

BaseModel: type[Any] | None = None
try:
    from pydantic import BaseModel as _PydanticBaseModel

    BaseModel = _PydanticBaseModel
except ImportError:
    pass

from . import task_db
from .proc import Proc, ProcContext
from .runner import MultiProcessRunner, SingleProcessRunner
from .term import TermDynamic, TermSimple
from .types import (
    FAILED_STATES,
    SUCCEEDED_STATES,
    ProcessError,
    ProcFailedError,
    ProcSkippedError,
    ProcState,
    RdepRule,
    SpecialDep,
    UserError,
    WhenScheduled,
    WhenTargetScheduled,
)

# pylint: disable=too-many-positional-arguments

# Type variable for the decorated function
F = TypeVar('F', bound=Callable[..., Any])

# Types for dependency specifications
# A dependency can be:
# - A string (proc name, proto pattern, or filled-out name like "foo-a-2" or "foo::a::2")
# - A callable (lambda) that returns str or list[str]
DepSpec = str
DepSpecOrList = Union[str, list[str]]
DepInput = Union[str, Callable[..., DepSpecOrList]]

# Default separator between proto name and params (and between params). The actual value is
# the ProcManager option name_param_separator (set via set_options); this constant is the default.
NAME_PARAM_SEP = '::'

logger = logging.getLogger('par')

# Signal handlers installed once so we don't replace user handlers on every ProcManager()
_signal_handlers_installed = False  # pylint: disable=invalid-name


def _install_signal_handlers() -> None:
    """Install handlers for SIGHUP, SIGTERM, SIGINT to abort all tasks and exit."""
    global _signal_handlers_installed  # pylint: disable=global-statement
    if _signal_handlers_installed:
        return
    _signal_handlers_installed = True

    def _abort_handler(signum: int, _frame: Any) -> None:
        manager = ProcManager.inst
        if manager is not None:
            for p in manager.procs.values():
                if p.is_running() and p.process is not None:
                    try:
                        p.process.terminate()
                    except (OSError, AttributeError):
                        # OSError: process already gone; AttributeError: Process._popen can be
                        # None in Python 3.14+ when the child has already exited been reaped
                        pass
            # Restore terminal (cursor, stop Live) so shell is usable after Ctrl+C
            try:
                manager.term.cleanup_on_interrupt()
            except Exception:  # pylint: disable=broad-except  # nosec B110
                ...
        sys.stderr.write('Tasks Aborted\n')
        sys.stderr.flush()
        sys.exit(128 + signum)

    for sig_name in ('SIGHUP', 'SIGINT', 'SIGTERM'):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                signal.signal(sig, _abort_handler)
            except (ValueError, OSError):
                pass  # e.g. SIGKILL cannot be caught, or not main thread


def _flatten_names(names: list[str] | list[str | list[str]]) -> list[str]:
    """Flatten names so that wait(*create(...), *create(...)) and wait(create(...)) work.
    Each element may be a proc name (str) or a list of names from create().
    """
    out: list[str] = []
    for n in names:
        if isinstance(n, list):
            out.extend(n)
        else:
            out.append(n)
    return out


class ProcManager:  # pylint: disable=too-many-public-methods

    inst: Optional['ProcManager'] = None  # Singleton instance

    def __init__(self):
        _install_signal_handlers()
        self.logger = logger
        self.pending_now: list[str] = []
        self.clear()
        self.term = TermDynamic() if sys.stdout.isatty() else TermSimple()

        # Options are set in set_options. Defaults:
        self.parallel = 100
        self.dynamic = sys.stdout.isatty()
        self.mode = 'mp'
        self.debug = False
        self.runner: Any = MultiProcessRunner()
        self.allow_missing_deps = True
        self.task_db_path: str | None = None
        self.name_param_separator = '::'

    def clear(self):
        logger.debug('----------------CLEAR----------------------')
        self.parallel = 100
        self.procs: OrderedDict[str, 'Proc'] = OrderedDict()  # For consistent execution order
        self.protos: dict[str, 'Proto'] = {}
        self.locks: dict[str, 'Proc'] = {}

        fmt_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')
        self.context: dict[str, Any] = {
            'logdir': tempfile.mkdtemp(prefix=f'parproc_{fmt_time}_'),
            'results': {},  # Context passed to processes
            'params': {},
        }
        self.missing_deps: dict[str, bool] = {}
        self.allow_missing_deps = True
        self.pending_now = []  # Procs with now=True to be started on next _step

        if hasattr(self, 'term') and self.term is not None:
            self.term.clear()

        if getattr(self, 'debug', False):
            self.parallel = 1
            self.term = TermSimple()
        if getattr(self, 'mode', 'mp') == 'single':
            self.runner = SingleProcessRunner()
        else:
            self.runner = MultiProcessRunner()

    _TASK_DB_PATH_UNSET: Any = object()  # Sentinel for "task_db_path not passed"

    def set_options(
        self,
        parallel: int | None = None,
        dynamic: bool | None = None,
        mode: str | None = None,
        debug: bool | None = None,
        allow_missing_deps: bool | None = None,
        task_db_path: str | None = _TASK_DB_PATH_UNSET,
        name_param_separator: str | None = None,
    ) -> None:
        """
        parallel: Number of parallel running processes
        dynamic: If True, terminal updates in place; if False, static output.
        mode: "mp" (default, multiprocessing) or "single" (single process, single thread).
        debug: If True, sets mode="single", parallel=1, dynamic=False.
        allow_missing_deps: If False, raise error when missing dependency is detected (default: False)
        task_db_path: Path to SQLite DB for task run history (progress estimates). None to disable.
        name_param_separator: Separator between proto name and params (and between params). Default '::'.
          Param values may not contain this string. Patterns must use this separator between [param] placeholders.
        """
        if parallel is not None:
            self.parallel = parallel
        if dynamic is not None:
            self.term = TermDynamic() if dynamic else TermSimple()
        if mode is not None:
            if mode not in ('mp', 'single'):
                raise UserError(f'mode must be "mp" or "single", got {mode!r}')
            self.mode = mode
            self.runner = SingleProcessRunner() if mode == 'single' else MultiProcessRunner()
        if debug is not None:
            self.debug = debug
            if debug:
                self.mode = 'single'
                self.parallel = 1
                self.term = TermSimple()
                self.runner = SingleProcessRunner()
        if allow_missing_deps is not None:
            self.allow_missing_deps = allow_missing_deps
        if task_db_path is not ProcManager._TASK_DB_PATH_UNSET:
            self.task_db_path = task_db_path
            task_db.set_path(task_db_path)
        if name_param_separator is not None:
            self.name_param_separator = name_param_separator

    def set_params(self, **params: Any) -> None:
        for k, v in params.items():
            self.context['params'][k] = v

    @classmethod
    def get_inst(cls) -> 'ProcManager':
        # Only make inst available in parent process
        if mp.current_process().name.startswith('parproc-child'):
            raise UserError('Use context when calling parproc from sub-process')

        if cls.inst is None:
            cls.inst = ProcManager()

        return cls.inst

    def add_proc(self, p: 'Proc') -> None:
        logger.debug(f'ADD: "{p.name}"')
        if p.name is None:
            raise UserError('Proc name cannot be None')
        if p.name in self.procs:
            raise UserError(f'Proc "{p.name}" already created')

        # Validate that proc name doesn't contain proto pattern placeholders
        param_pattern = r'\[([^\]]+)\]'
        if re.search(param_pattern, p.name):
            raise UserError(
                f'Proc name "{p.name}" contains proto pattern placeholders. '
                f'Proc names must be fully resolved (no [param] placeholders).'
            )

        self.procs[p.name] = p

        if p.now or p.name in self.missing_deps:
            # Defer start until next _step so that other procs (e.g. deps) can be registered first
            self.pending_now.append(p.name)

    def add_proto(self, p: 'Proto') -> None:
        logger.debug(f'ADD PROTO: "{p.name}"')
        if p.name is None:
            raise UserError('Proto name cannot be None')
        if p.name in self.protos:
            raise UserError(f'Proto "{p.name}" already created')

        self.protos[p.name] = p

    def _match_rdep_pattern(self, rdep_pattern: str, proc_name: str) -> bool:
        """
        Check if a proc name matches an rdep pattern.

        An rdep pattern can contain:
        - Placeholders like [a] that match any value
        - Literal values that must match exactly

        Examples (with default separator "::"):
        - Pattern "B::[a]::[b]" matches "B::something::2" (a="something", b="2")
        - Pattern "B::[a]::[b]" matches "B::my-cluster::2" (a="my-cluster", b="2")
        - Pattern "B::1::[b]" matches "B::1::2" (b="2") but NOT "B::something::2"
        - Pattern "B::[a]::3" matches "B::something::3" but NOT "B::something::2"
        - Pattern "B::1::2" matches "B::1::2" exactly

        Args:
            rdep_pattern: Pattern with optional [param] placeholders
            proc_name: Actual proc name to match against

        Returns:
            True if pattern matches proc_name, False otherwise
        """
        # Find all [param] patterns
        param_pattern = r'\[([^\]]+)\]'
        params = re.findall(param_pattern, rdep_pattern)

        if not params:
            # No placeholders, exact match required
            return rdep_pattern == proc_name

        # Build regex using configured separator (same param-boundary logic as Proto._build_regex_pattern)
        sep = self.name_param_separator
        parts = re.split(r'(\[[^\]]+\])', rdep_pattern)

        pattern_parts = []
        param_index = 0

        for part in parts:
            if not part:
                continue
            if part.startswith('[') and part.endswith(']'):
                if param_index == len(params) - 1:
                    # Last parameter: match everything to end
                    pattern_parts.append('.*')
                else:
                    # Match non-empty sequence that does not contain the separator
                    pattern_parts.append(f'(?:(?!{re.escape(sep)}).)+')
                param_index += 1
            else:
                # Literal text, escape it
                pattern_parts.append(re.escape(part))

        regex_str = '^' + ''.join(pattern_parts) + '$'
        regex = re.compile(regex_str)
        return bool(regex.match(proc_name))

    def _extract_from_rdep_pattern(self, rdep_pattern: str, proc_name: str) -> dict[str, str] | None:
        """
        Extract parameter values from proc_name using an rdep pattern.

        When the rdep pattern matches proc_name, returns a dict of param name -> value.
        E.g. _extract_from_rdep_pattern("k8s.build-image::[target]::frontend", "k8s.build-image::stage::frontend")
        returns {"target": "stage"}.

        Returns None if the pattern does not match proc_name.

        Note: If the rdep pattern repeats a placeholder name (e.g. B::[x]::[x]), only one value per
        name is returned (the last match), as per Python regex groupdict().
        """
        param_pattern = r'\[([^\]]+)\]'
        params = re.findall(param_pattern, rdep_pattern)

        if not params:
            if rdep_pattern == proc_name:
                return {}
            return None

        sep = self.name_param_separator
        parts = re.split(r'(\[[^\]]+\])', rdep_pattern)

        pattern_parts = []
        param_index = 0

        for part in parts:
            if not part:
                continue
            if part.startswith('[') and part.endswith(']'):
                param_name = params[param_index]
                if param_index == len(params) - 1:
                    pattern_parts.append(f'(?P<{re.escape(param_name)}>.*)')
                else:
                    pattern_parts.append(f'(?P<{re.escape(param_name)}>(?:(?!{re.escape(sep)}).)+)')
                param_index += 1
            else:
                pattern_parts.append(re.escape(part))

        regex_str = '^' + ''.join(pattern_parts) + '$'
        regex = re.compile(regex_str)
        match = regex.match(proc_name)
        if not match:
            return None
        return match.groupdict()

    def _resolve_rdeps(self, proc_name: str) -> list[str]:
        """
        Find all protos and procs that have rdeps matching the given proc name.

        When an rdep matches:
        - If it's a proto: try to create a proc from it (if proc_name matches proto pattern)
        - If it's an existing proc: add it as a dependency

        Args:
            proc_name: Name of the proc being started

        Returns:
            List of proc names that should be injected as dependencies
        """
        matching_rdeps: list[str] = []

        # Check all protos for matching rdeps
        for proto in self.protos.values():
            for rdep in proto.rdeps:
                # Skip WhenScheduled -- handled by _process_conditional_rdeps
                if isinstance(rdep, WhenScheduled):
                    continue
                # WhenTargetScheduled activates when the target is scheduled (like normal rdeps)
                if isinstance(rdep, WhenTargetScheduled):
                    rdep_str = rdep.pattern
                elif isinstance(rdep, RdepRule):
                    continue
                else:
                    rdep_str = rdep
                if self._match_rdep_pattern(rdep_str, proc_name):
                    # Found a matching rdep - need to create a proc from this proto
                    if proto.name is None:
                        continue

                    # Try to match proc_name against proto pattern to extract args
                    # This allows creating a proc from proto when proc_name matches proto's pattern
                    extracted = proto.match_and_extract(proc_name)
                    if extracted is not None:
                        # proc_name matches proto pattern - create proc using proc_name directly
                        if proc_name not in self.procs:
                            try:
                                created_names = self.create_proc(proc_name)
                                matching_rdeps.extend(
                                    created_names if isinstance(created_names, list) else [created_names]
                                )
                            except UserError:
                                # Failed to create proc (e.g., missing args), skip it
                                logger.debug(
                                    f'Failed to create proc from proto "{proto.name}" with rdep "{rdep_str}" matching "{proc_name}"'
                                )
                    else:
                        # proc_name doesn't match proto pattern (rdep pattern differs from proto pattern).
                        # Extract args from proc_name using the rdep pattern; param names in the proto
                        # pattern must appear in the rdep pattern (and thus in rdep_args) for injection.
                        # E.g. rdep "k8s.build-image::[target]::frontend" matching "k8s.build-image::stage::frontend"
                        # gives target=stage; then create proto "next.build::[target]" as "next.build::stage".
                        rdep_args = self._extract_from_rdep_pattern(rdep_str, proc_name)
                        if rdep_args is not None:
                            param_pattern = r'\[([^\]]+)\]'
                            proto_params = re.findall(param_pattern, proto.name)
                            if proto_params:
                                if all(p in rdep_args for p in proto_params):
                                    generated_name = proto.name
                                    for param in proto_params:
                                        generated_name = generated_name.replace(f'[{param}]', str(rdep_args[param]))
                                    if generated_name not in self.procs:
                                        try:
                                            created_names = self.create_proc(generated_name)
                                            matching_rdeps.extend(
                                                created_names if isinstance(created_names, list) else [created_names]
                                            )
                                        except UserError:
                                            logger.debug(
                                                f'Failed to create proc from proto "{proto.name}" with rdep "{rdep_str}" matching "{proc_name}"'
                                            )
                            elif proto.name not in self.procs:
                                try:
                                    created_names = self.create_proc(proto.name)
                                    matching_rdeps.extend(
                                        created_names if isinstance(created_names, list) else [created_names]
                                    )
                                except UserError:
                                    logger.debug(
                                        f'Failed to create proc from proto "{proto.name}" with rdep "{rdep_str}" matching "{proc_name}"'
                                    )
                        elif not re.search(r'\[([^\]]+)\]', proto.name):
                            # Proto name is exact (no pattern) - create proc from it
                            if proto.name not in self.procs:
                                try:
                                    created_names = self.create_proc(proto.name)
                                    matching_rdeps.extend(
                                        created_names if isinstance(created_names, list) else [created_names]
                                    )
                                except UserError:
                                    logger.debug(
                                        f'Failed to create proc from proto "{proto.name}" with rdep "{rdep_str}" matching "{proc_name}"'
                                    )

        # Check all existing procs for matching rdeps
        for proc in self.procs.values():
            for rdep in proc.rdeps:
                # Skip WhenScheduled -- handled by _process_conditional_rdeps
                if isinstance(rdep, WhenScheduled):
                    continue
                if isinstance(rdep, WhenTargetScheduled):
                    rdep_str = rdep.pattern
                elif isinstance(rdep, RdepRule):
                    continue
                else:
                    rdep_str = rdep
                if self._match_rdep_pattern(rdep_str, proc_name):
                    # Found a matching rdep - add this proc as a dependency
                    if proc.name is not None and proc.name not in matching_rdeps:
                        matching_rdeps.append(proc.name)

        return matching_rdeps

    def _inject_rdeps(self, name: str) -> None:
        """Resolve rdeps for proc name and inject them as dependencies. Run when proc becomes WANTED."""
        p = self.procs[name]
        matching_rdeps = self._resolve_rdeps(name)
        for rdep_proc_name in matching_rdeps:
            if rdep_proc_name not in p.deps:
                logger.debug(f'Injecting rdep "{rdep_proc_name}" as dependency of "{p.name}"')
                p.deps.append(rdep_proc_name)

        # Reverse direction: check if any already-WANTED procs have WhenScheduled rdeps
        # matching this proc name. If so, inject those procs as dependencies of `name`.
        # This handles the case where B (with WhenScheduled('A')) became WANTED before A.
        for other_name, other_proc in list(self.procs.items()):
            if other_name == name:
                continue
            if other_proc.state not in (ProcState.WANTED, ProcState.RUNNING):
                continue
            for rdep in other_proc.rdeps:
                if isinstance(rdep, WhenScheduled) and self._match_rdep_pattern(rdep.pattern, name):
                    if other_name not in p.deps:
                        logger.debug(
                            f'Injecting conditional rdep "{other_name}" as dependency of "{name}" '
                            f'(reverse direction)'
                        )
                        p.deps.append(other_name)

        # Check if any dependencies have a higher wave than current proc - this could cause deadlock
        for d in p.deps:
            if isinstance(d, str) and d in self.procs:
                dep_proc = self.procs[d]
                if dep_proc.wave > p.wave:
                    raise UserError(
                        f'Proc "{p.name}" (wave {p.wave}) cannot depend on proc "{dep_proc.name}" (wave {dep_proc.wave}). '
                        f'Dependencies must have equal or lower wave number to avoid deadlock.'
                    )

    def _resolve_crdep_target_name(self, name: str, target_pattern: str) -> str | None:
        """Resolve a conditional-rdep target pattern to a concrete name.

        Fills ``[param]`` placeholders using params extracted from *name*
        (via its proto pattern).  Returns ``None`` when placeholders remain
        unresolved.
        """
        proc = self.procs[name]
        proto = proc.proto
        proc_args: dict[str, str] = {}
        if proto is not None and proto.name is not None:
            extracted = proto.match_and_extract(name)
            if extracted is not None:
                proc_args = extracted

        param_pattern = r'\[([^\]]+)\]'
        target_params = re.findall(param_pattern, target_pattern)
        if target_params:
            target_name = target_pattern
            for tp in target_params:
                if tp in proc_args:
                    target_name = target_name.replace(f'[{tp}]', str(proc_args[tp]))
            if re.search(param_pattern, target_name):
                return None
            return target_name
        return target_pattern

    def _process_conditional_rdeps(self, name: str) -> None:
        """Process conditional rdeps for a proc that just became WANTED.

        **WhenScheduled(pattern)**: the declaring proc is scheduled →
        1. Resolve the concrete target proc name from the pattern.
        2. Create the target proc from a proto if it does not exist yet.
        3. Inject *name* as a dependency of the target.
        4. Schedule the target (set to WANTED) if it is IDLE.
        5. Raise ``UserError`` if the target is already RUNNING or finished.

        **WhenTargetScheduled(pattern)**: the target is already scheduled →
        1. Resolve the concrete target proc name from the pattern.
        2. If the target exists and is WANTED, inject *name* as a dependency.
        3. If the target is RUNNING or finished, raise ``UserError``.
        4. If the target is not scheduled (IDLE or missing), do nothing.
        """
        proc = self.procs[name]

        ws_rdeps: list[WhenScheduled] = [r for r in proc.rdeps if isinstance(r, WhenScheduled)]
        wts_rdeps: list[WhenTargetScheduled] = [r for r in proc.rdeps if isinstance(r, WhenTargetScheduled)]
        if not ws_rdeps and not wts_rdeps:
            return

        # --- WhenScheduled rdeps ---
        for crdep in ws_rdeps:
            target_name = self._resolve_crdep_target_name(name, crdep.pattern)
            if target_name is None:
                logger.debug(
                    f'Cannot resolve WhenScheduled target "{crdep.pattern}" for proc "{name}" '
                    f'(unresolved placeholders)'
                )
                continue

            # Create the target proc from a proto if it doesn't exist
            if target_name not in self.procs:
                match = self._find_matching_proto(target_name)
                if match is not None:
                    try:
                        self.create_proc(target_name)
                    except UserError:
                        logger.debug(f'Failed to create target proc "{target_name}" for WhenScheduled rdep of "{name}"')
                        continue
                else:
                    logger.debug(f'No proto found for WhenScheduled target "{target_name}" (from proc "{name}")')
                    continue

            target_proc = self.procs[target_name]

            # Check target state -- fail hard if already past WANTED
            if target_proc.state in (
                ProcState.RUNNING,
                ProcState.SUCCEEDED,
                ProcState.FAILED,
                ProcState.FAILED_DEP,
                ProcState.SKIPPED,
            ):
                raise UserError(
                    f'Proc "{name}" has a WhenScheduled rdep on "{target_name}", but "{target_name}" '
                    f'is already {target_proc.state.name}. The ordering contract (run "{name}" before '
                    f'"{target_name}") cannot be satisfied because "{name}" was scheduled too late.'
                )

            # Inject name as a dependency of the target
            if name not in target_proc.deps:
                logger.debug(f'Injecting conditional rdep "{name}" as dependency of "{target_name}"')
                target_proc.deps.append(name)

            # Wave validation
            if target_proc.wave < proc.wave:
                raise UserError(
                    f'Proc "{target_name}" (wave {target_proc.wave}) cannot depend on proc "{name}" '
                    f'(wave {proc.wave}). Dependencies must have equal or lower wave number to avoid deadlock.'
                )

            # Schedule the target if still IDLE
            if target_proc.state == ProcState.IDLE:
                logger.debug(f'SCHED (via WhenScheduled): "{target_name}"')
                target_proc.state = ProcState.WANTED
                self._inject_rdeps(target_name)
                # Recursively process conditional rdeps of the target too
                self._process_conditional_rdeps(target_name)
                self.sched_deps(target_proc)

        # --- WhenTargetScheduled rdeps (reverse direction) ---
        for wts_rdep in wts_rdeps:
            target_name = self._resolve_crdep_target_name(name, wts_rdep.pattern)
            if target_name is None:
                logger.debug(
                    f'Cannot resolve WhenTargetScheduled target "{wts_rdep.pattern}" for proc "{name}" '
                    f'(unresolved placeholders)'
                )
                continue

            if target_name not in self.procs:
                continue

            target_proc = self.procs[target_name]

            # Only activate if the target is already scheduled
            if target_proc.state == ProcState.IDLE:
                continue

            # Fail hard if the target is already past WANTED
            if target_proc.state in (
                ProcState.RUNNING,
                ProcState.SUCCEEDED,
                ProcState.FAILED,
                ProcState.FAILED_DEP,
                ProcState.SKIPPED,
            ):
                raise UserError(
                    f'Proc "{name}" has a WhenTargetScheduled rdep on "{target_name}", but "{target_name}" '
                    f'is already {target_proc.state.name}. The ordering contract (run "{name}" before '
                    f'"{target_name}") cannot be satisfied because "{name}" was scheduled too late.'
                )

            # Target is WANTED -- inject name as a dependency
            if name not in target_proc.deps:
                logger.debug(
                    f'Injecting WhenTargetScheduled rdep "{name}" as dependency of "{target_name}" '
                    f'(reverse direction)'
                )
                target_proc.deps.append(name)

            # Wave validation
            if target_proc.wave < proc.wave:
                raise UserError(
                    f'Proc "{target_name}" (wave {target_proc.wave}) cannot depend on proc "{name}" '
                    f'(wave {proc.wave}). Dependencies must have equal or lower wave number to avoid deadlock.'
                )

    # Schedules one or more procs for execution
    def start_proc(self, *names: str) -> None:
        # Two-pass approach: first set all procs to WANTED and resolve rdeps,
        # then schedule deps and start execution.  This ensures that when
        # start_proc('A', 'B') is called and B has a WhenScheduled('A'),
        # both A and B are WANTED before any execution begins.
        newly_wanted: list[tuple[str, 'Proc']] = []
        for name in names:
            p = self.procs[name]
            # No-op if already running, wanted, or complete; only start when IDLE
            if p.state == ProcState.IDLE:
                logger.debug(f'SCHED: "{p.name}"')
                p.state = ProcState.WANTED
                self._inject_rdeps(name)
                self._process_conditional_rdeps(name)
                newly_wanted.append((name, p))

        for _name, p in newly_wanted:
            # Set dependencies as wanted or missing
            if not self.sched_deps(p):  # If no unresolved or unfinished dependencies
                self.try_execute_one(p)  # See if proc can be executed now

    def _cast_args_by_signature(self, func: Callable, filtered_args: dict[str, Any]) -> dict[str, Any]:
        """Cast arguments based on function signature type annotations."""
        try:
            sig = inspect.signature(func)
            cast_args: dict[str, Any] = {}
            for param_name, param_value in filtered_args.items():
                if param_name in sig.parameters:
                    param = sig.parameters[param_name]
                    param_type = param.annotation

                    # If type annotation exists and is not Any/empty, try to cast
                    if param_type not in (inspect.Parameter.empty, Any):
                        cast_args[param_name] = self._cast_single_arg(param_type, param_value)
                    else:
                        # No type annotation, use as-is
                        cast_args[param_name] = param_value
                else:
                    # Parameter not in signature, use as-is
                    cast_args[param_name] = param_value
            return cast_args
        except (ValueError, TypeError):
            # If signature inspection fails, use args as-is
            return filtered_args

    def _cast_single_arg(self, param_type: type, param_value: Any) -> Any:
        """Cast a single argument value to the specified type."""
        # Handle common built-in types
        if param_type == int:
            return int(param_value)
        if param_type == float:
            return float(param_value)
        if param_type == str:
            return str(param_value)
        if param_type == bool:
            # Handle string booleans
            if isinstance(param_value, str):
                return param_value.lower() in ('true', '1', 'yes', 'on')
            return bool(param_value)

        # For other types, try to construct from string
        try:
            return param_type(param_value)
        except (ValueError, TypeError):
            # If casting fails, use original value
            return param_value

    def _process_pattern_args_and_generate(
        self, pattern: str, all_args: dict[str, Any], func: Callable | None = None, generate_name: bool = False
    ) -> tuple[dict[str, Any], str | None]:
        """
        Unified function that processes pattern, filters args, casts types, and optionally generates name.

        This combines:
        - Extracting parameter names from pattern (e.g., "proto::[x]::[y]" -> {"x", "y"})
        - Filtering to only include parameters that match the pattern's field names
        - Validating that all required parameters are present
        - Casting args to appropriate types based on function signature (if func provided)
        - Optionally generating the final name from pattern and args

        Args:
            pattern: Name pattern with [field] placeholders (e.g., "proto::[x]::[y]"). Separator is configurable via set_options(name_param_separator=...), default "::".
            all_args: All available arguments
            func: Optional function to use for type casting based on signature
            generate_name: If True, generate and return the final name

        Returns:
            Tuple of (filtered_and_cast_args, generated_name_or_none)
        """
        # Validate func is not None - proto.func must always be defined
        # This check helps type checkers understand func is not None after this point
        if func is None:
            raise UserError(f'Function must be provided for pattern "{pattern}"')

        # Extract parameter names from pattern
        param_pattern = r'\[([^\]]+)\]'
        params = set(re.findall(param_pattern, pattern))

        # Filter to only include parameters that match the pattern's field names
        filtered_args: dict[str, Any] = {}
        if params:
            # Only include args that match the pattern's parameters
            filtered_args = {k: v for k, v in all_args.items() if k in params}
            # Check that all required parameters are present
            for param in params:
                if param not in filtered_args:
                    raise UserError(f'Pattern "{pattern}" requires argument "{param}" but was not provided')
        # If no params in pattern, filtered_args remains empty

        # Cast args based on function signature if func provided
        filtered_args = self._cast_args_by_signature(func, filtered_args)

        # Optionally generate the final name from pattern and filtered args
        generated_name: str | None = None
        if generate_name:
            generated_name = pattern
            for param in params:
                if param in filtered_args:
                    # Replace [param] with the value from filtered_args
                    generated_name = generated_name.replace(f'[{param}]', str(filtered_args[param]))
                else:
                    # This shouldn't happen due to validation above, but just in case
                    raise UserError(f'Pattern "{pattern}" requires argument "{param}" but was not provided')

        return (filtered_args, generated_name)

    def _resolve_dependency(self, dep: str, all_args: dict[str, Any]) -> tuple[str, dict[str, Any], Optional['Proto']]:
        """
        Resolve a dependency specification to (dep_name_or_pattern, filtered_args, matched_proto).

        Note: Caller should check if dep is already in self.procs before calling this.

        - dep can be a proto pattern or filled-out name
        - Returns (dep_name_or_pattern, filtered_args, matched_proto_or_none)
        """
        # Check if dep contains placeholders - if so, fill them in first before matching
        # This prevents patterns like "foo-[a]-2" from matching "foo-[a]-[b]" incorrectly
        param_pattern = r'\[([^\]]+)\]'
        params = set(re.findall(param_pattern, dep))

        if params:
            # Dep has placeholders - fill them in first, then try to match
            filtered_args: dict[str, Any] = {k: v for k, v in all_args.items() if k in params}
            for param in params:
                if param not in filtered_args:
                    raise UserError(f'Pattern "{dep}" requires argument "{param}" but was not provided')

            # Fill in the pattern with available args
            filled_name = dep
            for param in params:
                if param in filtered_args:
                    filled_name = filled_name.replace(f'[{param}]', str(filtered_args[param]))

            logger.debug('Filled pattern "%s" to "%s" with args %s', dep, filled_name, filtered_args)

            # Try to match the filled-in name against protos
            match_result = self._find_matching_proto(filled_name)
            if match_result is not None:
                proto, extracted_args = match_result
                if proto.name is None:
                    raise UserError('Proto has no name')

                # Combine extracted args with all_args, then process
                combined_args = {**all_args, **extracted_args}
                # Process pattern, filter args, and cast types
                final_args, _ = self._process_pattern_args_and_generate(proto.name, combined_args, proto.func)
                return (filled_name, final_args, proto)

            # Filled name didn't match - return as-is (might be a future proc)
            return (filled_name, filtered_args, None)

        # No placeholders - try to match directly
        match_result = self._find_matching_proto(dep)
        if match_result is not None:
            proto, extracted_args = match_result
            if proto.name is None:
                raise UserError('Proto has no name')

            # Combine extracted args with all_args, then process
            combined_args = {**all_args, **extracted_args}
            # Process pattern, filter args, and cast types
            final_args, _ = self._process_pattern_args_and_generate(proto.name, combined_args, proto.func)
            return (dep, final_args, proto)

        # No match found - return as-is (might be a future proc or will error later)
        return (dep, {}, None)

    def _expand_dependencies(self, deps: list[DepInput], all_args: dict[str, Any], proc_name: str) -> list[DepSpec]:
        """Expand callable dependencies into a list of dependency specifications."""
        # Create DepProcContext for lambda dependencies
        dep_context = DepProcContext(
            proc_name=proc_name,
            params=self.context['params'],
            args=all_args,
        )

        expanded_deps: list[DepSpec] = []
        for dep in deps:
            if callable(dep):
                # Lambda dependency: call with DepProcContext and filtered proc params
                # Inspect lambda signature to only pass expected parameters
                sig = inspect.signature(dep)
                param_names = list(sig.parameters.keys())
                # Always ignore the first parameter (it's passed as first positional arg)
                if param_names:
                    param_names = param_names[1:]
                # Filter all_args to only include parameters the lambda expects
                filtered_lambda_args = {k: v for k, v in all_args.items() if k in param_names}
                dep_result = dep(dep_context, **filtered_lambda_args)
                # Expand result into deps array
                if isinstance(dep_result, list):
                    # Validate list items are strings
                    for item in dep_result:
                        if not isinstance(item, str):
                            raise UserError(f'Lambda dependency list item must be str, got {type(item)}')
                    expanded_deps.extend(dep_result)
                elif isinstance(dep_result, str):
                    expanded_deps.append(dep_result)
                else:
                    raise UserError(f'Lambda dependency must return str or list[str], got {type(dep_result)}')
            elif isinstance(dep, str):
                expanded_deps.append(dep)
            else:
                raise UserError(f'Dependency must be str or callable, got {type(dep)}')
        return expanded_deps

    def _resolve_expanded_dependencies(self, expanded_deps: list[DepSpec], all_args: dict[str, Any]) -> list[str]:
        """Resolve expanded dependency specifications into proc names."""
        resolved_deps: list[str] = []
        for dep in expanded_deps:
            # Check if it's already an existing proc
            if dep in self.procs:
                resolved_deps.append(dep)
                continue

            # Try to resolve dependency
            dep_name_or_pattern, filtered_args, matched_proto = self._resolve_dependency(dep, all_args)

            matched_proto_name = matched_proto.name if matched_proto else None
            logger.debug(
                f'Resolved dependency "{dep}" -> name="{dep_name_or_pattern}", matched_proto={matched_proto_name}'
            )

            if matched_proto is not None:
                # Found matching proto, create proc using the resolved name
                # dep_name_or_pattern is already the filled-out name (either from pattern match or partial replacement)
                if matched_proto.name is None:
                    raise UserError('Proto has no name')

                # If dep_name_or_pattern is already a filled-out name (no [param] placeholders), use it directly
                # Otherwise, generate the name from the proto pattern
                param_pattern = r'\[([^\]]+)\]'
                if re.search(param_pattern, dep_name_or_pattern):
                    # Still has placeholders, generate name from proto pattern
                    _, generated_dep_name = self._process_pattern_args_and_generate(
                        matched_proto.name, filtered_args, matched_proto.func, generate_name=True
                    )
                    if generated_dep_name is None:
                        raise UserError(f'Failed to generate name for dependency "{dep_name_or_pattern}"')
                    resolved_dep_name = self.create_proc(generated_dep_name)
                else:
                    # Already a filled-out name, use it directly
                    logger.debug('Creating proc from filled-out name: "%s"', dep_name_or_pattern)
                    resolved_dep_name = self.create_proc(dep_name_or_pattern)
                resolved_deps.extend(resolved_dep_name if isinstance(resolved_dep_name, list) else [resolved_dep_name])
            else:
                # No proto match - validate that it's not a proto pattern
                param_pattern = r'\[([^\]]+)\]'
                if re.search(param_pattern, dep_name_or_pattern):
                    raise UserError(
                        f'Dependency "{dep}" resolved to proto pattern "{dep_name_or_pattern}" but no matching proto was found. '
                        f'Dependencies must be either existing proc names or filled-out names that match a proto pattern.'
                    )
                # Use as-is (might be a future proc or will error later)
                resolved_deps.append(dep_name_or_pattern)

        return resolved_deps

    def _resolve_proto_dependencies(
        self, proto: 'Proto', all_args: dict[str, Any], proc_name: str
    ) -> tuple[list[str], list[SpecialDep]]:
        """
        Resolve all dependencies for a proto.

        Deps may contain normal deps (str, callable) and special deps (SpecialDep).
        Normal deps are expanded and resolved to proc names; special deps are returned as-is.

        Two-pass resolution for normal deps:
        1. First pass: expand all callable (lambda) dependencies - they can return DepSpec or list[DepSpec]
        2. Second pass: resolve each dep (extract fields, filter args, handle tuples, generate names),
           match against proto patterns, and create procs if needed

        Args:
            proto: The proto to resolve dependencies for
            all_args: All available arguments (proto defaults + extracted args)
            proc_name: Name of the proc being created

        Returns:
            Tuple of (resolved proc names, special deps)
        """
        normal_deps = [d for d in proto.deps if not isinstance(d, SpecialDep)]
        special_deps = [d for d in proto.deps if isinstance(d, SpecialDep)]
        # First pass: expand all callable (lambda) dependencies
        expanded_deps = self._expand_dependencies(normal_deps, all_args, proc_name)
        # Second pass: resolve each dependency
        resolved = self._resolve_expanded_dependencies(expanded_deps, all_args)
        return (resolved, special_deps)

    def _find_matching_proto(self, name: str) -> tuple['Proto', dict[str, Any]] | None:
        """
        Find a proto that matches the given name (either exact match or pattern match).

        Args:
            name: Either exact proto name or filled-out name like "foo::a::2"

        Returns:
            Tuple of (proto, extracted_args) if found, None otherwise
        """
        # First, try exact match
        if name in self.protos:
            return (self.protos[name], {})

        # Then, try pattern matching against all protos
        matches: list[tuple['Proto', dict[str, Any]]] = []
        for proto in self.protos.values():
            extracted = proto.match_and_extract(name)
            if extracted is not None:
                matches.append((proto, extracted))

        if len(matches) > 1:
            proto_names = [m[0].name for m in matches]
            raise UserError(f'Name "{name}" matches multiple protos: {proto_names}')
        if len(matches) == 1:
            return matches[0]
        return None

    @staticmethod
    def _is_glob_value(value: Any) -> bool:
        """Return True if the string value contains glob characters * or ?."""
        if not isinstance(value, str):
            return False
        return '*' in value or '?' in value

    def _resolve_arg_choices(
        self,
        proto: 'Proto',
        request_name: str,
        all_args: dict[str, Any],
    ) -> dict[str, Sequence[Any]] | None:
        """
        Resolve proto.arg_choices to a dict. If arg_choices is a callable, call it with
        DepProcContext and filtered args (same as dependency lambdas); it must return a dict.
        """
        if proto.arg_choices is None:
            return None
        if callable(proto.arg_choices):
            dep_context = DepProcContext(
                proc_name=request_name,
                params=self.context['params'],
                args=all_args,
            )
            sig = inspect.signature(proto.arg_choices)
            param_names = list(sig.parameters.keys())
            if not param_names:
                result = proto.arg_choices()
            else:
                param_names = param_names[1:]  # first is context
                filtered = {k: all_args[k] for k in param_names if k in all_args}
                result = proto.arg_choices(dep_context, **filtered)
            if not isinstance(result, dict):
                raise UserError(f'arg_choices callable must return a dict, got {type(result).__name__}')
            # Validate keys are subset of pattern params when we have them
            if proto.regex_params:
                pattern_params = set(proto.regex_params)
                for key in result:
                    if key not in pattern_params:
                        raise UserError(
                            f'arg_choices key "{key}" is not a parameter in pattern "{proto.name}". '
                            f'Pattern parameters: {list(proto.regex_params)}'
                        )
            return result
        return dict(proto.arg_choices)

    def _get_arg_choices(
        self,
        proto: 'Proto',
        param: str,
        request_name: str,
        all_args: dict[str, Any],
        resolved_arg_choices: dict[str, Sequence[Any]] | None = None,
    ) -> list[Any]:
        """
        Resolve arg_choices for a param to a list. If the value is callable (lambda),
        call it with DepProcContext and filtered args (same as dependency lambdas).
        """
        if resolved_arg_choices is None:
            resolved_arg_choices = self._resolve_arg_choices(proto, request_name, all_args)
        if resolved_arg_choices is None:
            raise UserError(f'No arg_choices for param "{param}"')
        choices_val = resolved_arg_choices[param]
        if callable(choices_val):
            dep_context = DepProcContext(
                proc_name=request_name,
                params=self.context['params'],
                args=all_args,
            )
            sig = inspect.signature(choices_val)
            param_names = list(sig.parameters.keys())
            if param_names:
                param_names = param_names[1:]  # first is context
            filtered = {k: all_args[k] for k in param_names if k in all_args}
            result = choices_val(dep_context, **filtered)
            if isinstance(result, (list, tuple)):
                return list(result)
            raise UserError(
                f'arg_choices lambda for "{param}" must return a sequence (list or tuple), got {type(result).__name__}'
            )
        return list(choices_val)

    def _expand_glob_args(
        self,
        proto: 'Proto',
        extracted_args: dict[str, Any],
        request_name: str,
        all_args: dict[str, Any],
        resolved_arg_choices: dict[str, Sequence[Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Expand glob patterns in extracted_args against proto.arg_choices.
        Returns a list of concrete arg dicts (one per combination).
        Raises UserError if a glob pattern matches no allowed value.
        """
        if resolved_arg_choices is None:
            resolved_arg_choices = self._resolve_arg_choices(proto, request_name, all_args)
        # For each param: either a single value (no glob) or list of choices matching the glob
        value_lists: list[list[Any]] = []
        for param in proto.regex_params:
            raw = extracted_args.get(param)
            if resolved_arg_choices is not None and param in resolved_arg_choices and self._is_glob_value(raw):
                pattern = str(raw)
                choices = self._get_arg_choices(proto, param, request_name, all_args, resolved_arg_choices)
                matched = [c for c in choices if fnmatch.fnmatch(str(c), pattern)]
                if not matched:
                    raise UserError(
                        f'Glob pattern "{pattern}" for argument "{param}" matched no allowed values. '
                        f'Allowed: {list(choices)}'
                    )
                value_lists.append(matched)
            else:
                value_lists.append([raw])

        # Cartesian product
        combos = list(itertools.product(*value_lists))
        return [dict(zip(proto.regex_params, combo)) for combo in combos]

    def _create_single_proc_from_args(
        self,
        proto: 'Proto',
        all_args: dict[str, Any],
        proc_name: str | None = None,
    ) -> str:
        """
        Create one proc from a proto with the given args. Used for single creation
        and for each combination when expanding globs.
        """
        if proto.name is None:
            raise UserError("Proto must have a name to create a proc")
        proc_args, generated_name = self._process_pattern_args_and_generate(
            proto.name, all_args, proto.func, generate_name=(proc_name is None)
        )
        if proc_name is None:
            if generated_name is None:
                raise UserError(f'Failed to generate name for proto "{proto.name}"')
            proc_name = generated_name

        param_pattern = r'\[([^\]]+)\]'
        if re.search(param_pattern, proc_name):
            raise UserError(
                f'Proc name "{proc_name}" contains proto pattern placeholders. '
                f'Proc names must be fully resolved (no [param] placeholders).'
            )

        if proc_name in self.procs:
            return proc_name

        resolved_deps, special_deps = self._resolve_proto_dependencies(proto, all_args, proc_name)
        proc = Proc(
            name=proc_name,
            deps=resolved_deps,
            rdeps=proto.rdeps,
            locks=proto.locks,
            now=proto.now,
            args=proc_args,
            proto=proto,
            timeout=proto.timeout,
            wave=proto.wave,
            special_deps=special_deps,
        )
        proc(proto.func)
        return proc_name

    def create_proc(self, proto_name: str, proc_name: str | None = None) -> list[str]:
        """
        Create a proc from a proto.

        If proto_name (or proc_name when provided) is already an existing proc name,
        returns that name in a list (idempotent).

        Otherwise, proto_name can be either:
        - An exact proto name pattern (e.g., "foo::[x]::[y]") - proc_name must be provided
        - A filled-out name (e.g., "foo::a::2") - automatically matches proto pattern "foo::[x]::[y]"
          and extracts x='a', y='2', then generates proc_name
        - A filled-out name with globs (e.g., "foo::*::2") when the proto has arg_choices -
          expands to one proc per matching combination.

        Args:
            proto_name: Either existing proc name, proto pattern, or filled-out name (may contain * or ?)
            proc_name: Optional explicit proc name (required if proto_name is a pattern)

        Returns:
            List of created or existing proc names (always a list, one or more elements).

        Raises:
            UserError: If no matching proto found, multiple protos match, required args missing,
                glob used without arg_choices, or value not in arg_choices
        """
        # If already an existing proc, return it (idempotent; no proto required)
        if proto_name in self.procs:
            return [proto_name]
        if proc_name is not None and proc_name in self.procs:
            return [proc_name]
        # Try to find matching proto (exact match or pattern match)
        match_result = self._find_matching_proto(proto_name)
        if match_result is None:
            raise UserError(f'No proto found matching "{proto_name}"')

        proto, extracted_args = match_result

        if proto.func is None or proto.name is None:
            raise UserError('Proto has no function or name')

        # Combine proto defaults with extracted args from pattern match
        all_args: dict[str, Any] = {}
        if proto.args:
            all_args.update(proto.args)
        all_args.update(extracted_args)

        resolved_arg_choices = self._resolve_arg_choices(proto, proto_name, all_args)

        # Check for glob in any extracted arg value
        has_glob = any(self._is_glob_value(extracted_args.get(p)) for p in proto.regex_params)
        if has_glob:
            # Glob is only allowed when arg_choices is set for each globbed param
            if resolved_arg_choices is None:
                raise UserError(
                    'Glob patterns (* and ?) in argument values are only allowed when '
                    'the proto defines arg_choices for that argument.'
                )
            for param in proto.regex_params:
                val = extracted_args.get(param)
                if self._is_glob_value(val) and param not in resolved_arg_choices:
                    raise UserError(
                        f'Glob pattern in argument "{param}" is only allowed when '
                        f'the proto defines arg_choices for that argument.'
                    )
            expanded_list = self._expand_glob_args(proto, extracted_args, proto_name, all_args, resolved_arg_choices)
            names: list[str] = []
            for concrete_args in expanded_list:
                single_all = {}
                if proto.args:
                    single_all.update(proto.args)
                single_all.update(concrete_args)
                names.append(self._create_single_proc_from_args(proto, single_all, proc_name=None))
            return names

        # Single creation: validate literal values against arg_choices if set
        if resolved_arg_choices is not None:
            for param in resolved_arg_choices:
                if param not in extracted_args:
                    continue
                val = extracted_args[param]
                choices = self._get_arg_choices(proto, param, proto_name, all_args, resolved_arg_choices)
                allowed_strs = [str(c) for c in choices]
                if str(val) not in allowed_strs:
                    raise UserError(f'Argument "{param}" value {val!r} is not in allowed choices: {list(choices)}')

        single_name = self._create_single_proc_from_args(proto, all_args, proc_name)
        return [single_name]

    # Schedule proc dependencies. Returns True if no new deps are found idle
    def sched_deps(self, proc):
        new_deps = False
        for d in proc.deps:
            if isinstance(d, str) and d in self.procs:
                if self.procs[d].state == ProcState.IDLE:
                    # Resolve rdeps for this proc so it runs before procs that depend on it
                    self._inject_rdeps(d)
                    self.procs[d].state = ProcState.WANTED
                    self._process_conditional_rdeps(d)
                    new_deps = True

                    # Schedule dependencies of this proc
                    if not self.sched_deps(self.procs[d]):
                        # Try to kick off dependency
                        self.try_execute_one(self.procs[d], False)

            else:
                # Dependency not yet known
                if not self.allow_missing_deps:
                    raise UserError(
                        f'Proc "{proc.name}" depends on "{d}" which does not exist. '
                        f'Set allow_missing_deps=True to allow missing dependencies.'
                    )
                if isinstance(d, str):
                    self.missing_deps[d] = True

        return new_deps

    # Tries to execute any proc
    def try_execute_any(self) -> None:
        for _, p in self.procs.items():
            if p.state == ProcState.WANTED:
                self.try_execute_one(p, False)  # Do not go deeper while iterating

    def _try_execute_any_single_wait(self) -> None:
        """Run one WANTED proc, skipping wave and parallel limits. For single-mode context.wait() re-entry."""
        for _, p in self.procs.items():
            if p.state == ProcState.WANTED:
                self._try_execute_one_skip_limits(p)
                return

    def _try_execute_one_skip_limits(self, proc: 'Proc') -> bool:
        """Execute one proc if deps/locks ok, skipping wave and parallel checks (single-mode wait re-entry)."""
        for l in proc.locks:
            if l in self.locks:
                return False
        for d in proc.deps:
            if isinstance(d, str):
                if d not in self.procs or not self.procs[d].is_complete():
                    if d in self.procs and self.procs[d].is_failed():
                        proc.state = ProcState.FAILED_DEP
                        proc.error = Proc.ERROR_DEP_FAILED
                        proc.more_info = f'dependency "{self.procs[d].name}" failed'
                        self.term.completed_proc(proc)
                    return False
        # Check special dependencies (global conditions)
        for special_dep in proc.special_deps:
            if special_dep == SpecialDep.NO_FAILURES:
                if any(p.state in FAILED_STATES for p in self.procs.values()):
                    logger.debug(f'Proc "{proc.name}" not started: a task has failed')
                    proc.state = ProcState.FAILED_DEP
                    proc.error = Proc.ERROR_DEP_FAILED
                    proc.more_info = 'run aborted: a task has failed'
                    self.term.completed_proc(proc)
                    return False
            else:
                raise UserError(
                    f'Unrecognized special dependency {special_dep!r} on proc "{proc.name}". '
                    f'Supported: {[m.name for m in SpecialDep]}.'
                )
        if proc.state == ProcState.WANTED:
            self.execute(proc)
        return True

    # Executes proc now if possible. Returns false if not possible
    def try_execute_one(self, proc: 'Proc', collect: bool = True) -> bool:

        # Check if any other WANTED procs have a lower wave and are ready to run - they must run first
        # Don't block on dependencies - they will be handled by the dependency check below
        for name, p in self.procs.items():
            if p.state in [ProcState.WANTED, ProcState.RUNNING] and p.wave < proc.wave and name not in proc.deps:
                # Check if this lower wave proc is ready to run (all its dependencies are complete)
                can_run = True
                for dep in p.deps:
                    if not isinstance(dep, str) or dep not in self.procs or not self.procs[dep].is_complete():
                        can_run = False
                        break
                if can_run:
                    logger.debug(
                        f'Proc "{proc.name}" not started due to lower wave proc "{p.name}" (wave {p.wave} < {proc.wave})'
                    )
                    return False

        # If all dependencies are met, and none of the locks are taken, execute proc
        for l in proc.locks:
            if l in self.locks:
                logger.debug(f'Proc "{proc.name}" not started due to lock "{l}"')
                return False

        for d in proc.deps:
            if not isinstance(d, str):
                continue
            if d not in self.procs:
                if not self.allow_missing_deps:
                    raise UserError(
                        f'Proc "{proc.name}" depends on "{d}" which does not exist. '
                        f'Set allow_missing_deps=True to allow missing dependencies.'
                    )
                logger.debug(f'Proc "{proc.name}" not started due to unknown dependency "{d}"')
                proc.state = ProcState.FAILED_DEP
                proc.error = Proc.ERROR_DEP_FAILED
                proc.more_info = f'dependency "{d}" missing'
                self.term.completed_proc(proc)
                return False

            if self.procs[d].is_failed():
                logger.debug(f'Proc "{proc.name}" canceled due to failed dependency "{d}"')
                proc.state = ProcState.FAILED_DEP
                proc.error = Proc.ERROR_DEP_FAILED
                proc.more_info = f'canceled due to failure of "{self.procs[d].name}"'
                self.term.completed_proc(proc)

            elif not self.procs[d].is_complete():
                logger.debug(f'Proc "{proc.name}" not started due to unfinished dependency "{d}"')
                return False

        # Check special dependencies (global conditions)
        for special_dep in proc.special_deps:
            if special_dep == SpecialDep.NO_FAILURES:
                if any(p.state in FAILED_STATES for p in self.procs.values()):
                    logger.debug(f'Proc "{proc.name}" not started: a task has failed')
                    proc.state = ProcState.FAILED_DEP
                    proc.error = Proc.ERROR_DEP_FAILED
                    proc.more_info = 'run aborted: a task has failed'
                    self.term.completed_proc(proc)
                    return False
            else:
                raise UserError(
                    f'Unrecognized special dependency {special_dep!r} on proc "{proc.name}". '
                    f'Supported: {[m.name for m in SpecialDep]}.'
                )

        # If number of parallel processes limit has not been reached
        if sum(1 for name, p in self.procs.items() if p.is_running()) >= self.parallel:
            logger.debug(f'Proc "{proc.name}" not started due to parallel process limit of {self.parallel}')
            return False

        # All good. Execute process TODO: In a separate thread
        if proc.state == ProcState.WANTED:
            self.execute(proc)
        else:
            logger.debug(f'Proc "{proc.name}" not started due to wrong state "{proc.state}"')

        # Try execute other procs
        if collect:
            self.collect()

        return False

    def execute(self, proc: 'Proc') -> None:
        self.runner.start_task(self, proc)

    # Finds any procs that have completed their execution, and moves them on. Tries to execute other
    # procs if any procs were collected
    def collect(self) -> None:
        self.runner.collect(self)

    # Wait for all procs and locks
    def wait_for_all(self, exception_on_failure: bool = True) -> bool:
        """Wait for all procs and locks. Returns True if all succeeded, False if any failed.
        If exception_on_failure is True, raises ProcessError on failure instead of returning False."""
        logger.debug('WAIT FOR COMPLETION')
        last_term_refresh = time.time()
        while (
            self.pending_now
            or any(p.state != ProcState.IDLE and not p.is_complete() for name, p in self.procs.items())
            or self.locks
        ):
            self._step()
            # Refresh live progress bar every 1/10 s so the user sees task status updates
            if time.time() - last_term_refresh >= 0.1:
                self.term.update(force=True)
                last_term_refresh = time.time()

        # Do final update. Force update
        self.term.update(force=True)

        failed = self.check_failure(list(self.procs))
        if exception_on_failure and failed:
            raise ProcessError('Process error [1]')
        return not failed

    # Wait for procs or locks
    def wait(self, names: list[str] | list[str | list[str]]) -> None:
        names = _flatten_names(names)
        logger.debug(f'WAIT FOR {names}')
        last_term_refresh = time.time()
        while not self.check_complete(names):
            self._step()
            # Refresh live progress bar every 1/10 s so the user sees task status updates
            if time.time() - last_term_refresh >= 0.1:
                self.term.update(force=True)
                last_term_refresh = time.time()

        # Do final update. Force update
        self.term.update(force=True)

        # Raise on issue
        if self.check_failure(names):
            raise ProcessError('Process error [2]')

    def check_complete(self, names: list[str]) -> bool:
        # If proc does not exist, waits for proc to be created
        return all(self.procs[name].is_complete() if name in self.procs else False for name in names) and not any(
            name in self.locks for name in names
        )

    def check_failure(self, names: list[str]) -> bool:
        return any(self.procs[name].state in FAILED_STATES for name in names if name in self.procs)

    # Move things forward
    def _step(self) -> None:
        # Start any procs that were added with now=True (deferred so deps can be registered first)
        # Process in registration order (list) so e.g. p0 is scheduled before p1 before p2.
        # Clear pending_now after processing: each name only needs start_proc once; re-adding
        # names whose state is no longer IDLE would keep them in pending_now forever and stall the loop.
        to_start = [
            name for name in self.pending_now if name in self.procs and self.procs[name].state == ProcState.IDLE
        ]
        if to_start:
            self.start_proc(*to_start)
        self.pending_now = []
        # Move things forward
        self.collect()
        # Try to execute any WANTED procs
        self.try_execute_any()
        # Wait for a bit
        time.sleep(0.01)
        # Update terminal
        self.term.update()

    # def getData(self):
    #    return {p.name: p.output for key, p in self.procs.items()}

    def wait_clear(self, exception_on_failure: bool = False) -> bool:
        ret = self.wait_for_all(exception_on_failure=exception_on_failure)
        self.clear()
        return ret

    def build_pc(
        self, proc: 'Proc', context: dict[str, Any], queue_to_proc: Any, queue_to_master: Any
    ) -> 'ProcContext':
        """Build ProcContext for a task (used by runners)."""
        if proc.name is None:
            raise UserError('Proc has no name')
        return ProcContext(proc.name, context, queue_to_proc, queue_to_master)

    def run_task(
        self, proc: 'Proc', pc: 'ProcContext', context: dict[str, Any], redirect: bool = True
    ) -> tuple[Any, int, str]:
        """Run task; return (ret, error, log_filename). redirect=False for single/debug (logs to console)."""
        if proc.user_func is None:
            raise UserError('Proc has no user function')
        return run_task_with_redirect(proc.user_func, pc, context, redirect=redirect)

    def record_task_start(self, proc: 'Proc') -> None:
        """Record task start in task DB (used by runners)."""
        if proc.name is not None:
            task_db.on_task_start(proc.name, datetime.datetime.now(datetime.UTC), proc.run_id)

    def get_log_filename(self, name: str) -> str:
        """Return log file path for a proc name (used by runners)."""
        return os.path.join(str(self.context['logdir']), name + '.log')

    def raise_user_error(self, msg: str) -> UserError:
        """Return UserError for message (used by runners)."""
        return UserError(msg)

    def handle_sync_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """Handle a request from a task synchronously; return response dict (used by SyncChannel and collect)."""
        msg = dict(request)
        if msg['req'] == 'get-input':
            input_ = self.term.get_input(message=msg.get('message', ''), password=msg.get('password', False))
            msg['resp'] = input_
            return msg
        if msg['req'] == 'create-proc':
            msg['proc_names'] = self.create_proc(msg['proto_name'], msg.get('proc_name'))
            return msg
        if msg['req'] == 'run-proc':
            result = self.create_proc(msg['proto_name'], msg.get('proc_name'))
            self.start_proc(*result)
            msg['proc_names'] = result
            return msg
        if msg['req'] == 'start-procs':
            self.start_proc(*_flatten_names(msg['names']))
            return msg
        if msg['req'] == 'check-complete':
            names = _flatten_names(msg['names'])
            # Ensure requested procs are started (task may have called wait(*create()) without start())
            to_start = [n for n in names if n in self.procs and self.procs[n].state == ProcState.IDLE]
            if to_start:
                self.start_proc(*to_start)
            # In single-process mode, run pending tasks until requested names are complete or failed
            # (otherwise the waiting task would deadlock: other procs only run when current one returns)
            if self.mode == 'single':
                while not self.check_complete(names) and not self.check_failure(names):
                    self._try_execute_any_single_wait()
            msg['complete'] = self.check_complete(names)
            msg['failure'] = self.check_failure(names)
            return msg
        if msg['req'] == 'get-results':
            msg['results'] = self.context['results']
            return msg
        raise UserError(f'unknown call: {msg["req"]}')

    def complete_proc(
        self, p: 'Proc', output: Any, error: int, log_filename: str, more_info: str | None = None
    ) -> None:
        """Apply completion state (used by collect and SingleProcessRunner)."""
        p.process = None
        # Convert pydantic models to dict for consistent results in single- and multi-process mode
        if BaseModel is not None and isinstance(output, BaseModel):
            output = output.model_dump()
        p.output = output
        p.error = error
        if error == Proc.ERROR_NONE:
            p.state = ProcState.SUCCEEDED
        elif error == Proc.ERROR_SKIPPED:
            p.state = ProcState.SKIPPED
        else:
            p.state = ProcState.FAILED
        p.log_filename = log_filename
        if more_info is not None:
            p.more_info = more_info
        logger.info(f'proc "{p.name}" collected: ret = {p.output}')
        self.context['results'][p.name] = p.output
        logger.info(f'new context: {self.context}')
        for l in p.locks:
            del self.locks[l]
        if self.task_db_path is not None:
            status = (
                "success" if p.state in SUCCEEDED_STATES else ("timeout" if p.error == Proc.ERROR_TIMEOUT else "failed")
            )
            task_db.on_task_end(p.run_id, datetime.datetime.now(datetime.UTC), status)
        self.term.end_proc(p)


def run_task_with_redirect(
    user_func: Callable[..., Any],
    pc: 'ProcContext',
    context: dict[str, Any],
    redirect: bool = True,
) -> tuple[Any, int, str]:
    """Run user task. If redirect=True, stdout/stderr go to log file; if False (debug/single), logs to console."""
    name = pc.proc_name
    log_filename = os.path.join(str(context['logdir']), name + '.log')
    error = Proc.ERROR_NONE
    ret = None
    with open(log_filename, 'w', encoding='utf-8') as log_file:
        if redirect:
            # Ensure the task does not see a TTY (e.g. isatty() is False). Redirect stdin from
            # /dev/null; stdout/stderr are redirected to the log file below.
            try:
                with open(os.devnull, encoding='utf-8') as devnull:
                    os.dup2(devnull.fileno(), 0)
            except OSError:
                pass
            # Env vars so apps treat this as non-interactive: TERM=dumb (traditional),
            # NO_COLOR (https://no-color.org) for tools that respect it.
            os.environ['TERM'] = 'dumb'
            os.environ['NO_COLOR'] = '1'
            saved_stdout_fd = os.dup(1)
            saved_stderr_fd = os.dup(2)
            try:
                log_fd = log_file.fileno()
                os.dup2(log_fd, 1)
                os.dup2(log_fd, 2)
                sys.stdout = log_file
                sys.stderr = log_file
            except OSError:
                pass  # restore below
        try:
            try:
                ret = user_func(pc, **pc.args)
            except ProcSkippedError:
                # Ignore exception info
                error = Proc.ERROR_SKIPPED
            except ProcFailedError:
                # Ignore exception info
                error = Proc.ERROR_FAILED
            except Exception as e:  # pylint: disable=broad-exception-caught
                # Detect sh library ErrorReturnCode (non-zero exit) without importing sh
                exc_name = type(e).__name__
                exit_code = getattr(e, 'exit_code', None)
                is_sh_exit = (
                    exc_name == 'ErrorReturnCode' or exc_name.startswith('ErrorReturnCode_')
                ) and exit_code is not None
                if is_sh_exit:
                    error = Proc.ERROR_FAILED
                    log_file.write(f'Command failed with exit code {exit_code}\n')
                else:
                    _, _, tb = sys.exc_info()
                    info = str(e) + '\n' + ''.join(traceback.format_tb(tb))
                    stderr = getattr(e, 'stderr', None)
                    if stderr is not None and isinstance(stderr, bytes):
                        info += f'\nSTDERR_FULL:\n{stderr.decode("utf-8")}'
                    log_file.write(info)
                    if not redirect:
                        sys.stderr.write(info)
                    error = Proc.ERROR_EXCEPTION
        finally:
            if redirect:
                try:
                    os.dup2(saved_stdout_fd, 1)
                    os.dup2(saved_stderr_fd, 2)
                    os.close(saved_stdout_fd)
                    os.close(saved_stderr_fd)
                except (OSError, NameError):
                    pass
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
    return (ret, error, log_filename)


def run_task(user_func: Callable[..., Any], pc: 'ProcContext', context: dict[str, Any]) -> tuple[Any, int, Any]:
    """Run user task with redirect to log file. Returns (ret, error, exc_info)."""
    ret, error, _log_filename = run_task_with_redirect(user_func, pc, context, redirect=True)
    return (ret, error, None)


class DepProcContext:
    """Context object passed to dependency lambda functions.

    Similar to ProcContext but only contains the essential fields needed for dependency resolution:
    - proc_name: Name of the proc being created
    - params: Global parameters from ProcManager context
    - args: Arguments specific to this proc (filtered from proto args)
    """

    def __init__(self, proc_name: str, params: dict[str, Any], args: dict[str, Any]):
        self.proc_name = proc_name
        self.params = params
        self.args = args


class Proto:
    """Decorator for process prototypes. These can be parameterized and instantiated again and again.

    Proto names can contain [field] placeholders (e.g., "foo::[x]::[y]") which are replaced with
    actual values when creating procs. The separator between placeholders is configurable via
    set_options(name_param_separator=...) and defaults to "::" (param values may not contain it).
    You can create procs using either:
    - The proto pattern: create_proc('foo::[x]::[y]') - requires proc_name to be provided
    - A filled-out name: create_proc('foo::a::2') - automatically extracts x='a', y='2' from the name

    Dependencies (deps) can be:
    - Existing proc names (strings)
    - Proto patterns (e.g., "dep-[x]-[y]") - extracts matching args from parent proc's args
    - Filled-out names (e.g., "dep-test-42") - automatically matches proto pattern and creates proc
    - Callables (lambdas) that return str or list[str] - called with manager and proc args
    - Special dep values (e.g. pp.NO_FAILURES) - global conditions, passed in the same deps list

    Dependencies are automatically matched against proto patterns and created if not found.
    Type casting is performed automatically based on the proto function's type annotations.

    Argument choices and glob expansion:
    - arg_choices: optional dict mapping argument name to a sequence of allowed values,
      or a callable that returns such a dict. The callable is evaluated when creating
      procs and receives the same context as dependency lambdas: DepProcContext
      (proc_name, params, args) plus filtered keyword args, or no args if it takes none
      (e.g. arg_choices={'env': ['dev', 'prod']} or arg_choices=lambda ctx: {'env': ['dev', 'prod']}).
      Dict values may also be callables returning a sequence. Keys must be parameter names from the proto name pattern.
    - When arg_choices is set, creating a proc with a concrete value for that arg requires
      the value to be in the allowed set; otherwise UserError is raised.
    - When arg_choices is set, you can use glob patterns in a filled-out name: * and ?
      (fnmatch-style). * expands to all allowed values for that arg; other patterns match
      allowed values. Multiple globbed args yield a Cartesian product; create_proc then
      returns a list of proc names (or a single str when one proc). Example:
      create('foo::*::2') with arg_choices for first arg ['a','b'] creates foo::a::2 and
      foo::b::2 and returns list[str]. Use wait(*create('foo::*::2')) to wait for all.
    - Using * or ? in an argument value when the proto does not define arg_choices for
      that argument raises UserError.
    """

    def __init__(
        self,
        name: str | None = None,
        f: F | None = None,
        deps: Sequence[DepInput | SpecialDep] | None = None,
        rdeps: list[str | RdepRule] | None = None,
        locks: list[str] | None = None,
        now: bool = False,
        args: dict[str, Any] | None = None,
        timeout: float | None = None,
        wave: int = 0,
        arg_choices: dict[str, Sequence[Any]] | Callable[..., dict[str, Sequence[Any]]] | None = None,
    ):
        # Input properties
        self.name = name
        # Validate deps is a sequence (not a function or other type)
        if deps is not None and not isinstance(deps, (list, tuple)):
            raise UserError(
                f'Proto deps must be a list or tuple, got {type(deps).__name__}. '
                f'If you want to use a lambda dependency, wrap it in a list: deps=[lambda ...]'
            )
        _deps = list(deps) if deps is not None else []
        for d in _deps:
            if not isinstance(d, (str, SpecialDep)) and not callable(d):
                raise UserError(
                    f'Proto deps must contain str, callable, or SpecialDep values, got {type(d).__name__!r}.'
                )
        self.deps = _deps
        self.rdeps = rdeps if rdeps is not None else []
        self.locks = locks if locks is not None else []
        self.now = now  # Whether proc will start once created
        self.args = args if args is not None else {}
        self.timeout = timeout
        self.wave = wave
        if arg_choices is None:
            self.arg_choices: dict[str, Sequence[Any]] | Callable[..., dict[str, Sequence[Any]]] | None = None
        elif callable(arg_choices):
            self.arg_choices = arg_choices
        else:
            self.arg_choices = dict(arg_choices)
        # special_deps are stored inside deps; extracted when resolving
        self.special_deps: list[SpecialDep] = [d for d in _deps if isinstance(d, SpecialDep)]

        # Initialize regex attributes (will be set in _build_regex_pattern)
        self.regex_pattern: re.Pattern[str] | None = None
        self.regex_params: list[str] = []

        if f is not None:
            # Created using short-hand
            self.__call__(f)

    # Called immediately after initialization
    def __call__(self, f: F) -> F:

        if self.name is None:
            self.name = f.__name__

        self.func = f

        # Generate regex pattern for matching filled-out names
        # Convert pattern like "foo::[x]::[y]" to regex that can match "foo::a::2" and extract x="a", y="2"
        self._build_regex_pattern()

        # Validate arg_choices keys are subset of pattern params (only when a dict; callable is validated when resolved)
        if self.arg_choices is not None and not callable(self.arg_choices):
            pattern_params = set(self.regex_params)
            for key in self.arg_choices:
                if key not in pattern_params:
                    raise UserError(
                        f'Proto arg_choices key "{key}" is not a parameter in pattern "{self.name}". '
                        f'Pattern parameters: {list(self.regex_params)}'
                    )

        ProcManager.get_inst().add_proto(self)

        # Return the original function to preserve type information
        return f

    def _build_regex_pattern(self) -> None:
        """Build a regex pattern from the proto name pattern for matching filled-out names."""
        if self.name is None:
            return

        # Find all [param] patterns in the original name
        param_pattern = r'\[([^\]]+)\]'
        params = re.findall(param_pattern, self.name)

        if params:
            sep = ProcManager.get_inst().name_param_separator
            # Build regex by splitting on [param] markers
            parts = re.split(r'(\[[^\]]+\])', self.name)

            pattern_parts = []
            param_index = 0

            for idx, part in enumerate(parts):
                if not part:
                    continue
                if part.startswith('[') and part.endswith(']'):
                    # This is a parameter marker
                    param = part[1:-1]  # Remove [ and ]
                    # Find next non-empty literal (must equal configured separator)
                    next_literal = None
                    for p in parts[idx + 1 :]:
                        if p and not (p.startswith('[') and p.endswith(']')):
                            next_literal = p
                            break
                    if next_literal is not None and next_literal != sep:
                        raise UserError(
                            f'Pattern "{self.name}" must use the configured separator "{sep}" between '
                            f'placeholders; found "{next_literal}"'
                        )
                    if next_literal is None:
                        # Last parameter: match everything to end
                        pattern_parts.append(f'(?P<{param}>.*)')
                    else:
                        # Match any sequence that does not contain the separator
                        pattern_parts.append(f'(?P<{param}>(?:(?!{re.escape(sep)}).)+)')
                    param_index += 1
                else:
                    # Literal text, escape it
                    pattern_parts.append(re.escape(part))

            regex_str = '^' + ''.join(pattern_parts) + '$'
            self.regex_pattern = re.compile(regex_str)
            self.regex_params = params
        else:
            # No parameters, exact match only
            self.regex_pattern = re.compile(f'^{re.escape(self.name)}$')
            self.regex_params = []

    def match_and_extract(self, name: str) -> dict[str, Any] | None:
        """
        Try to match a filled-out name against this proto's pattern and extract parameters.

        Args:
            name: Filled-out name like "foo::a::2" for pattern "foo::[x]::[y]"

        Returns:
            Dict of extracted parameters if match, None otherwise
        """
        if self.regex_pattern is None:
            return None
        match = self.regex_pattern.match(name)
        if not match:
            return None

        # Extract parameter values from match groups
        extracted = {}
        for param in self.regex_params:
            value_str = match.group(param)
            # Type casting will be done later based on function signature
            extracted[param] = value_str
        return extracted


def wait_for_all(exception_on_failure: bool = True) -> bool:
    """Wait for all procs and locks. Returns True if all succeeded, False if any failed.
    If exception_on_failure is True, raises ProcessError on failure instead of returning False."""
    return ProcManager.get_inst().wait_for_all(exception_on_failure=exception_on_failure)


def results() -> dict[str, Any]:
    return dict(ProcManager.get_inst().context['results'])


def set_params(**params: Any) -> None:
    ProcManager.get_inst().set_params(**params)


# Waits for any previous job to complete, then clears state
def wait_clear(exception_on_failure: bool = False) -> bool:
    return ProcManager.get_inst().wait_clear(exception_on_failure=exception_on_failure)


def clear() -> None:
    ProcManager.get_inst().clear()


def start(*names: str) -> None:
    if names:
        ProcManager.get_inst().start_proc(*names)


def _fill_proto_pattern(pattern: str, params: dict[str, Any]) -> str:
    """Replace [param] placeholders in pattern with values from params."""
    result = pattern
    for key, value in params.items():
        result = result.replace(f'[{key}]', str(value))
    return result


def create(proto_name: str, proc_name: str | None = None, **kwargs: Any) -> list[str]:
    """Create a proc from a proto. Pass pattern params as keyword args (e.g. create('foo::[x]', x=1)).
    Returns list of proc names (one or more). Use wait(*create(...)) or start(*create(...)).
    """
    if kwargs:
        proto_name = _fill_proto_pattern(proto_name, kwargs)
        proc_name = None
    return ProcManager.get_inst().create_proc(proto_name, proc_name)


def run(proto_name: str, proc_name: str | None = None, **kwargs: Any) -> None:
    """Create and start proc(s). When using glob with arg_choices, starts all matching procs."""
    if kwargs:
        proto_name = _fill_proto_pattern(proto_name, kwargs)
        proc_name = None
    names = ProcManager.get_inst().create_proc(proto_name, proc_name)
    if names:
        ProcManager.get_inst().start_proc(*names)


def set_options(**kwargs: Any) -> None:
    return ProcManager.get_inst().set_options(**kwargs)


def get_procs() -> dict[str, Proc]:
    return ProcManager.get_inst().procs


def get_protos() -> dict[str, Proto]:
    return ProcManager.get_inst().protos


# Wait for given proc or lock names
def wait(*names: str) -> None:
    return ProcManager.get_inst().wait(list(names))


# So Proc can use manager and run_task without proc importing par (avoids circular import)
Proc.set_defaults(ProcManager.get_inst, run_task)
