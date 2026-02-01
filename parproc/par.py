import datetime
import inspect
import logging
import multiprocessing as mp
import os
import re
import signal
import sys
import tempfile
import time
import traceback
import uuid
from collections import OrderedDict
from collections.abc import Callable, Sequence
from enum import Enum
from typing import Any, Optional, TypeVar, Union

BaseModel: type[Any] | None = None
try:
    from pydantic import BaseModel as _PydanticBaseModel

    BaseModel = _PydanticBaseModel
except ImportError:
    pass

from . import task_db
from .runner import MultiProcessRunner, SingleProcessRunner
from .state import FAILED_STATES, SUCCEEDED_STATES, ProcState
from .term import TermDynamic, TermSimple

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


class SpecialDep(Enum):
    """Special dependency kinds: global conditions that are not other tasks."""

    NO_FAILURES = 'no_failures'  # Satisfied only when no proc in the run has failed


# Individual special deps exported on parproc (e.g. pp.NO_FAILURES)
NO_FAILURES = SpecialDep.NO_FAILURES


class UserError(Exception):
    pass


class ProcessError(Exception):
    pass


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
                    except OSError:
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

    def start_procs(self, names: list[str]) -> None:
        for n in names:
            self.start_proc(n)

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
                if self._match_rdep_pattern(rdep, proc_name):
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
                                created_name = self.create_proc(proc_name)
                                matching_rdeps.append(created_name)
                            except UserError:
                                # Failed to create proc (e.g., missing args), skip it
                                logger.debug(
                                    f'Failed to create proc from proto "{proto.name}" with rdep "{rdep}" matching "{proc_name}"'
                                )
                    else:
                        # proc_name doesn't match proto pattern (rdep pattern differs from proto pattern).
                        # Extract args from proc_name using the rdep pattern; param names in the proto
                        # pattern must appear in the rdep pattern (and thus in rdep_args) for injection.
                        # E.g. rdep "k8s.build-image::[target]::frontend" matching "k8s.build-image::stage::frontend"
                        # gives target=stage; then create proto "next.build::[target]" as "next.build::stage".
                        rdep_args = self._extract_from_rdep_pattern(rdep, proc_name)
                        if rdep_args is not None:
                            param_pattern = r'\[([^\]]+)\]'
                            proto_params = re.findall(param_pattern, proto.name)
                            if proto_params and all(p in rdep_args for p in proto_params):
                                generated_name = proto.name
                                for param in proto_params:
                                    generated_name = generated_name.replace(f'[{param}]', str(rdep_args[param]))
                                if generated_name not in self.procs:
                                    try:
                                        created_name = self.create_proc(generated_name)
                                        matching_rdeps.append(created_name)
                                    except UserError:
                                        logger.debug(
                                            f'Failed to create proc from proto "{proto.name}" with rdep "{rdep}" matching "{proc_name}"'
                                        )
                        elif not re.search(r'\[([^\]]+)\]', proto.name):
                            # Proto name is exact (no pattern) - create proc from it
                            if proto.name not in self.procs:
                                try:
                                    created_name = self.create_proc(proto.name)
                                    matching_rdeps.append(created_name)
                                except UserError:
                                    logger.debug(
                                        f'Failed to create proc from proto "{proto.name}" with rdep "{rdep}" matching "{proc_name}"'
                                    )

        # Check all existing procs for matching rdeps
        for proc in self.procs.values():
            for rdep in proc.rdeps:
                if self._match_rdep_pattern(rdep, proc_name):
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
        # Check if any dependencies have a higher wave than current proc - this could cause deadlock
        for d in p.deps:
            if isinstance(d, str) and d in self.procs:
                dep_proc = self.procs[d]
                if dep_proc.wave > p.wave:
                    raise UserError(
                        f'Proc "{p.name}" (wave {p.wave}) cannot depend on proc "{dep_proc.name}" (wave {dep_proc.wave}). '
                        f'Dependencies must have equal or lower wave number to avoid deadlock.'
                    )

    # Schedules a proc for execution
    def start_proc(self, name: str) -> None:
        p = self.procs[name]

        if p.state == ProcState.IDLE:
            logger.debug(f'SCHED: "{p.name}"')
            p.state = ProcState.WANTED

            self._inject_rdeps(name)

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
                resolved_deps.append(resolved_dep_name)
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

    def create_proc(self, proto_name: str, proc_name: str | None = None) -> str:
        """
        Create a proc from a proto.

        The proto_name can be either:
        - An exact proto name pattern (e.g., "foo::[x]::[y]") - proc_name must be provided
        - A filled-out name (e.g., "foo::a::2") - automatically matches proto pattern "foo::[x]::[y]"
          and extracts x='a', y='2', then generates proc_name

        Args:
            proto_name: Either proto pattern or filled-out name that matches a proto pattern
            proc_name: Optional explicit proc name (required if proto_name is a pattern)

        Returns:
            The created proc name (either provided or generated)

        Raises:
            UserError: If no matching proto found, or if multiple protos match, or if required args missing
        """
        # Try to find matching proto (exact match or pattern match)
        match_result = self._find_matching_proto(proto_name)
        if match_result is None:
            raise UserError(f'No proto found matching "{proto_name}"')

        proto, extracted_args = match_result

        if proto.func is None or proto.name is None:
            raise UserError('Proto has no function or name')

        # Use the proto's actual name pattern for processing
        actual_proto_name = proto.name

        # Combine proto defaults with extracted args from pattern match
        all_args: dict[str, Any] = {}
        if proto.args:
            all_args.update(proto.args)
        # Extracted args from pattern matching override proto defaults
        all_args.update(extracted_args)

        # Process pattern, filter args, cast types, and generate name
        proc_args, generated_name = self._process_pattern_args_and_generate(
            actual_proto_name, all_args, proto.func, generate_name=(proc_name is None)
        )

        # Use generated name if proc_name was not provided
        if proc_name is None:
            if generated_name is None:
                raise UserError(f'Failed to generate name for proto "{proto_name}"')
            proc_name = generated_name

        # Validate that proc_name doesn't contain proto pattern placeholders
        param_pattern = r'\[([^\]]+)\]'
        if re.search(param_pattern, proc_name):
            raise UserError(
                f'Proc name "{proc_name}" contains proto pattern placeholders. '
                f'Proc names must be fully resolved (no [param] placeholders).'
            )

        # If proc_name already exists after substitution, return existing proc
        if proc_name in self.procs:
            return proc_name

        # Resolve dependencies (normal deps -> proc names; special deps passed through)
        resolved_deps, special_deps = self._resolve_proto_dependencies(proto, all_args, proc_name)

        # Create proc based on prototype
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

        # Add new proc, by calling procs __call__ function
        proc(proto.func)

        # Return proc name as reference
        return proc_name

    # Schedule proc dependencies. Returns True if no new deps are found idle
    def sched_deps(self, proc):
        new_deps = False
        for d in proc.deps:
            if isinstance(d, str) and d in self.procs:
                if self.procs[d].state == ProcState.IDLE:
                    # Resolve rdeps for this proc so it runs before procs that depend on it
                    self._inject_rdeps(d)
                    self.procs[d].state = ProcState.WANTED
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
    def wait_for_all(self, exception_on_failure: bool = True) -> None:
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

        # Raise on issue
        if exception_on_failure and self.check_failure(list(self.procs)):
            raise ProcessError('Process error [1]')

    # Wait for procs or locks
    def wait(self, names: list[str]) -> None:
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
        for name in self.pending_now:
            if name in self.procs and self.procs[name].state == ProcState.IDLE:
                self.start_proc(name)
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

    def wait_clear(self, exception_on_failure: bool = False) -> None:
        self.wait_for_all(exception_on_failure=exception_on_failure)
        self.clear()

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
            proc_name = self.create_proc(msg['proto_name'], msg.get('proc_name'))
            msg['proc_name'] = proc_name
            return msg
        if msg['req'] == 'start-procs':
            self.start_procs(msg['names'])
            return msg
        if msg['req'] == 'check-complete':
            names = msg['names']
            # In single-process mode, run pending tasks until requested names are complete or failed
            # (otherwise the waiting task would deadlock: other procs only run when current one returns)
            if self.mode == 'single':
                # Ensure requested procs are started (task may have called wait(create(), create()) without start())
                for name in names:
                    if name in self.procs and self.procs[name].state == ProcState.IDLE:
                        self.start_proc(name)
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
        p.state = ProcState.SUCCEEDED if error == Proc.ERROR_NONE else ProcState.FAILED
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
            except Exception as e:  # pylint: disable=broad-exception-caught
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


# Objects of this class only live inside the individual proc threads
class ProcContext:

    def __init__(self, proc_name: str, context: dict[str, Any], queue_to_proc: Any, queue_to_master: Any):
        self.proc_name = proc_name
        self.results = context['results']
        self.params = context['params']
        self.args = context['args']
        self.queue_to_proc = queue_to_proc
        self.queue_to_master = queue_to_master

    def _cmd(self, **kwargs: Any) -> Any:
        # Pass request to master
        self.queue_to_master.put(kwargs)
        # Get and return response
        logger.debug(f'ProcContext request to master: {kwargs}')
        resp = self.queue_to_proc.get()
        logger.debug(f'ProcContext response from master: {resp}')
        return resp

    def get_input(self, message='', password=False):
        return self._cmd(req='get-input', message=message, password=password)['resp']

    def create(self, proto_name: str, proc_name: str | None = None) -> str:
        resp = self._cmd(req='create-proc', proto_name=proto_name, proc_name=proc_name)
        return str(resp['proc_name'])

    def start(self, *names: str) -> None:
        self._cmd(req='start-procs', names=list(names))

    def wait(self, *names: str) -> None:
        # Periodically poll for completion
        logger.info('waiting to wait')
        while True:
            res = self._cmd(req='check-complete', names=list(names))
            if res['failure']:
                raise ProcessError('Process error [3]')
            if res['complete']:
                break
            logger.info('waiting for sub-proc')
            time.sleep(0.01)

        # At this point, everything is complete
        logger.info(f'wait done. results pre: {self.results}')
        self.results.update(self._cmd(req='get-results', names=list(names))['results'])
        logger.info(f'wait done. results post: {self.results}')


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
    """

    def __init__(
        self,
        name: str | None = None,
        f: F | None = None,
        deps: Sequence[DepInput | SpecialDep] | None = None,
        rdeps: list[str] | None = None,
        locks: list[str] | None = None,
        now: bool = False,
        args: dict[str, Any] | None = None,
        timeout: float | None = None,
        wave: int = 0,
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


class Proc:
    """
    Decorator for processes
    name   - identified name of process
    deps   - process dependencies (proc names and/or SpecialDep). will not be run until these are satisfied
    locks  - list of locks. only one process can own a lock at any given time
    """

    ERROR_NONE = 0
    ERROR_EXCEPTION = 1
    ERROR_DEP_FAILED = 2
    ERROR_TIMEOUT = 3
    ERROR_NOT_PICKLEABLE = 4

    # Called on intitialization
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
        proto: Proto | None = None,
        timeout: float | None = None,
        wave: int = 0,
        special_deps: list[SpecialDep] | None = None,
    ):
        # special_deps is only set when creating from a proto; user puts special deps in deps array
        # Input properties
        self.name = name
        self.rdeps = rdeps if rdeps is not None else []
        self.locks = locks if locks is not None else []
        self.now = now
        self.args = args if args is not None else {}
        self.proto = proto
        self.timeout = timeout
        # Wave defaults to proto's wave if proto exists, otherwise use provided wave (default 0)
        self.wave = proto.wave if proto is not None else wave
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

        # Utils
        self.log_filename = ''
        self.run_id = str(uuid.uuid4())  # Unique id for this run; used by task DB to match start/end

        # Main function (wrapper for multiprocessing); user_func is the raw decorated callable
        self.func: Any | None = None
        self.user_func: Any | None = None

        # State
        self.start_time: float | None = None
        self.end_time: float | None = None
        self.process: mp.Process | None = None
        self.queue_to_proc: mp.Queue | None = None
        self.queue_to_master: mp.Queue | None = None
        self.state = ProcState.IDLE
        self.error = Proc.ERROR_NONE
        self.more_info = ''
        self.output: Any | None = None

        if f is not None:
            # Created using short-hand
            self.__call__(f)

    def is_running(self) -> bool:
        return self.state == ProcState.RUNNING

    def is_complete(self) -> bool:
        return self.state in SUCCEEDED_STATES or self.state in FAILED_STATES

    def is_failed(self) -> bool:
        return self.state in FAILED_STATES

    # Called immediately after initialization
    def __call__(self, f: F) -> F:
        def func(queue_to_proc: mp.Queue, queue_to_master: mp.Queue, context: dict[str, Any], name: str) -> None:
            logger.info(f'proc "{name}" started')
            pc = ProcContext(name, context, queue_to_proc, queue_to_master)
            ret, error, _exc_info = run_task(f, pc, context)
            log_filename = os.path.join(str(context['logdir']), name + '.log')
            logger.info(f'proc "{name}" ended: ret = {ret}')

            # Convert pydantic models to dicts so they are pickleable
            if BaseModel is not None and isinstance(ret, BaseModel):
                ret = ret.model_dump()

            import pickle as _pickle  # pylint: disable=import-outside-toplevel  # nosec: B403

            msg = {'req': 'proc-complete', 'value': ret, 'log_filename': log_filename, 'error': error}
            # multiprocessing.Queue.put() pickles in a background feeder thread, so pickle
            # errors there are not raised to the caller (see bpo-40195). Pre-validate in this
            # thread so we can catch PicklingError and send a fallback completion message.
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
            # Ensure completion message is flushed before child exits (Queue uses a feeder thread)
            queue_to_master.close()
            queue_to_master.join_thread()

        if self.name is None:
            self.name = f.__name__

        self.user_func = f
        self.func = func
        ProcManager.get_inst().add_proc(self)

        # Return the original function to preserve type information
        return f


def wait_for_all(exception_on_failure: bool = True) -> None:
    return ProcManager.get_inst().wait_for_all(exception_on_failure=exception_on_failure)


def results() -> dict[str, Any]:
    return dict(ProcManager.get_inst().context['results'])


def set_params(**params: Any) -> None:
    ProcManager.get_inst().set_params(**params)


# Waits for any previous job to complete, then clears state
def wait_clear(exception_on_failure: bool = False) -> None:
    return ProcManager.get_inst().wait_clear(exception_on_failure=exception_on_failure)


def clear() -> None:
    ProcManager.get_inst().clear()


def start(*names: str) -> None:
    return ProcManager.get_inst().start_procs(list(names))


def _fill_proto_pattern(pattern: str, params: dict[str, Any]) -> str:
    """Replace [param] placeholders in pattern with values from params."""
    result = pattern
    for key, value in params.items():
        result = result.replace(f'[{key}]', str(value))
    return result


def create(proto_name: str, proc_name: str | None = None, **kwargs: Any) -> str:
    """Create a proc from a proto. Pass pattern params as keyword args (e.g. create('foo::[x]', x=1))."""
    if kwargs:
        proto_name = _fill_proto_pattern(proto_name, kwargs)
        proc_name = None
    return ProcManager.get_inst().create_proc(proto_name, proc_name)


def run(proto_name: str, proc_name: str | None = None, **kwargs: Any) -> None:
    """Create and start a proc. Pass pattern params as keyword args (e.g. run('foo::[x]', x=1))."""
    if kwargs:
        proto_name = _fill_proto_pattern(proto_name, kwargs)
        proc_name = None
    proc_name = ProcManager.get_inst().create_proc(proto_name, proc_name)
    ProcManager.get_inst().start_proc(proc_name)


def set_options(**kwargs: Any) -> None:
    return ProcManager.get_inst().set_options(**kwargs)


def get_procs() -> dict[str, Proc]:
    return ProcManager.get_inst().procs


def get_protos() -> dict[str, Proto]:
    return ProcManager.get_inst().protos


# Wait for given proc or lock names
def wait(*names: str) -> None:
    return ProcManager.get_inst().wait(list(names))
