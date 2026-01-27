import datetime
import inspect
import logging
import multiprocessing as mp
import os
import queue
import re
import sys
import tempfile
import time
import traceback
from collections import OrderedDict
from collections.abc import Callable
from typing import Any, Optional, TypeVar, Union

from .state import ProcState
from .term import Term

# pylint: disable=too-many-positional-arguments

# Type variable for the decorated function
F = TypeVar('F', bound=Callable[..., Any])

# Types for dependency specifications
# A dependency can be:
# - A string (proc name, proto pattern, or filled-out name like "foo-a-2")
# - A callable (lambda) that returns str or list[str]
DepSpec = str
DepSpecOrList = Union[str, list[str]]
DepInput = Union[str, Callable[..., DepSpecOrList]]


class UserError(Exception):
    pass


class ProcessError(Exception):
    pass


logger = logging.getLogger('par')


class ProcManager:

    inst: Optional['ProcManager'] = None  # Singleton instance

    def __init__(self):

        self.clear()
        self.term = Term(dynamic=sys.stdout.isatty())

        # Options are set in set_options. Defaults:
        self.parallel = 100
        self.dynamic = sys.stdout.isatty()

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

    def set_options(self, parallel: int | None = None, dynamic: bool | None = None) -> None:
        """
        Parallel: Number of parallel running processes
        """
        if parallel is not None:
            self.parallel = parallel
        if dynamic is not None:
            self.term.dynamic = dynamic

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

        self.procs[p.name] = p

        if p.now or p.name in self.missing_deps:
            # Requested to run by script or dependent
            self.start_proc(p.name)

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

    # Schedules a proc for execution
    def start_proc(self, name: str) -> None:
        p = self.procs[name]

        if p.state == ProcState.IDLE:
            logger.debug(f'SCHED: "{p.name}"')
            p.state = ProcState.WANTED

            # Check if any dependencies have a higher wave than current proc - this could cause deadlock
            for d in p.deps:
                if d in self.procs:
                    dep_proc = self.procs[d]
                    if dep_proc.wave > p.wave:
                        raise UserError(
                            f'Proc "{p.name}" (wave {p.wave}) cannot depend on proc "{dep_proc.name}" (wave {dep_proc.wave}). '
                            f'Dependencies must have equal or lower wave number to avoid deadlock.'
                        )

            # Set dependencies as wanted or missing
            if not self.sched_deps(p):  # If no unresolved or unfinished dependencies
                self.try_execute_one(p)  # See if proc can be executed now

    def _process_pattern_args_and_generate(
        self, pattern: str, all_args: dict[str, Any], func: Callable | None = None, generate_name: bool = False
    ) -> tuple[dict[str, Any], str | None]:
        """
        Unified function that processes pattern, filters args, casts types, and optionally generates name.
        
        This combines:
        - Extracting parameter names from pattern (e.g., "proto-[x]-[y]" -> {"x", "y"})
        - Filtering to only include parameters that match the pattern's field names
        - Validating that all required parameters are present
        - Casting args to appropriate types based on function signature (if func provided)
        - Optionally generating the final name from pattern and args
        
        Args:
            pattern: Name pattern with [field] placeholders (e.g., "proto-[x]-[y]")
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
        if func is not None:
            # Type assertion: func is not None here, helps type checkers
            assert func is not None  # For type checking
            try:
                sig = inspect.signature(func)
                cast_args = {}
                for param_name, param_value in filtered_args.items():
                    if param_name in sig.parameters:
                        param = sig.parameters[param_name]
                        param_type = param.annotation
                        
                        # If type annotation exists and is not Any/empty, try to cast
                        if param_type != inspect.Parameter.empty and param_type != Any:
                            # Handle common types
                            if param_type == int:
                                cast_args[param_name] = int(param_value)
                            elif param_type == float:
                                cast_args[param_name] = float(param_value)
                            elif param_type == bool:
                                # Handle string booleans
                                if isinstance(param_value, str):
                                    cast_args[param_name] = param_value.lower() in ('true', '1', 'yes', 'on')
                                else:
                                    cast_args[param_name] = bool(param_value)
                            elif param_type == str:
                                cast_args[param_name] = str(param_value)
                            else:
                                # For other types, try to construct from string
                                try:
                                    cast_args[param_name] = param_type(param_value)
                                except (ValueError, TypeError):
                                    # If casting fails, use original value
                                    cast_args[param_name] = param_value
                        else:
                            # No type annotation, use as-is
                            cast_args[param_name] = param_value
                    else:
                        # Parameter not in signature, use as-is
                        cast_args[param_name] = param_value
                filtered_args = cast_args
            except (ValueError, TypeError):
                # If signature inspection fails, use args as-is
                pass

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

    def _resolve_dependency(self, dep: str, all_args: dict[str, Any]) -> tuple[str, dict[str, Any], 'Proto' | None]:
        """
        Resolve a dependency specification to (dep_name_or_pattern, filtered_args, matched_proto).
        
        Note: Caller should check if dep is already in self.procs before calling this.
        
        - dep can be a proto pattern or filled-out name
        - Returns (dep_name_or_pattern, filtered_args, matched_proto_or_none)
        """
        # Try to find matching proto
        match_result = self._find_matching_proto(dep)
        if match_result is not None:
            proto, extracted_args = match_result
            if proto.name is None:
                raise UserError(f'Proto has no name')
            
            # Combine extracted args with all_args, then process
            combined_args = {**all_args, **extracted_args}
            # Process pattern, filter args, and cast types
            final_args, _ = self._process_pattern_args_and_generate(proto.name, combined_args, proto.func)
            return (dep, final_args, proto)

        # No match found - could be a pattern that needs args filled in
        # Extract params and filter args manually (no proto, so no func for type casting)
        param_pattern = r'\[([^\]]+)\]'
        params = set(re.findall(param_pattern, dep))
        filtered_args: dict[str, Any] = {}
        if params:
            filtered_args = {k: v for k, v in all_args.items() if k in params}
            for param in params:
                if param not in filtered_args:
                    raise UserError(f'Pattern "{dep}" requires argument "{param}" but was not provided')
        return (dep, filtered_args, None)

    def _resolve_proto_dependencies(self, proto: 'Proto', all_args: dict[str, Any]) -> list[str]:
        """
        Resolve all dependencies for a proto.
        
        Two-pass resolution:
        1. First pass: expand all callable (lambda) dependencies - they can return DepSpec or list[DepSpec]
        2. Second pass: resolve each dep (extract fields, filter args, handle tuples, generate names), 
           match against proto patterns, and create procs if needed
        
        Returns:
            List of resolved dependency names (proc names, not proto patterns)
        """
        # First pass: expand all callable (lambda) dependencies
        expanded_deps: list[DepSpec] = []
        for dep in proto.deps:
            if callable(dep):
                # Lambda dependency: call with ProcManager context and filtered proc params
                # Inspect lambda signature to only pass expected parameters
                sig = inspect.signature(dep)
                param_names = set(sig.parameters.keys())
                # Remove 'manager' if present (it's passed as first positional arg)
                param_names.discard('manager')
                # Filter all_args to only include parameters the lambda expects
                filtered_lambda_args = {k: v for k, v in all_args.items() if k in param_names}
                dep_result = dep(self, **filtered_lambda_args)
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

        # Second pass: resolve each dependency
        resolved_deps: list[str] = []
        for dep in expanded_deps:
            # Check if it's already an existing proc
            if dep in self.procs:
                resolved_deps.append(dep)
                continue

            # Try to resolve dependency
            dep_name_or_pattern, filtered_args, matched_proto = self._resolve_dependency(dep, all_args)

            if matched_proto is not None:
                # Found matching proto, create proc
                resolved_dep_name = self.create_proc(dep_name_or_pattern)
                resolved_deps.append(resolved_dep_name)
            else:
                # No proto match - use as-is (might be a future proc or will error later)
                resolved_deps.append(dep_name_or_pattern)

        return resolved_deps


    def _find_matching_proto(self, name: str) -> tuple['Proto', dict[str, Any]] | None:
        """
        Find a proto that matches the given name (either exact match or pattern match).
        
        Args:
            name: Either exact proto name or filled-out name like "foo-a-2"
        
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
        elif len(matches) == 1:
            return matches[0]
        else:
            return None

    # Create a proc from a proto
    def create_proc(self, proto_name: str, proc_name: str | None = None) -> str:
        # Try to find matching proto (exact match or pattern match)
        match_result = self._find_matching_proto(proto_name)
        if match_result is None:
            raise UserError(f'No proto found matching "{proto_name}"')
        
        proto, extracted_args = match_result

        if proto.func is None or proto.name is None:
            raise UserError(f'Proto has no function or name')
        
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

        # If proc_name already exists after substitution, return existing proc
        if proc_name in self.procs:
            return proc_name

        # Resolve dependencies
        resolved_deps = self._resolve_proto_dependencies(proto, all_args)

        # Create proc based on prototype
        proc = Proc(
            name=proc_name,
            deps=resolved_deps,
            locks=proto.locks,
            now=proto.now,
            args=proc_args,
            proto=proto,
            timeout=proto.timeout,
            wave=proto.wave,
        )

        # Add new proc, by calling procs __call__ function
        proc(proto.func)

        # Return proc name as reference
        return proc_name

    # Schedule proc dependencies. Returns True if no new deps are found idle
    def sched_deps(self, proc):
        new_deps = False
        for d in proc.deps:
            if d in self.procs:
                if self.procs[d].state == ProcState.IDLE:
                    self.procs[d].state = ProcState.WANTED
                    new_deps = True

                    # Schedule dependencies of this proc
                    if not self.sched_deps(self.procs[d]):
                        # Try to kick off dependency
                        self.try_execute_one(self.procs[d], False)

            else:
                # Dependency not yet known
                self.missing_deps[d] = True

        return new_deps

    # Tries to execute any proc
    def try_execute_any(self) -> None:
        for _, p in self.procs.items():
            if p.state == ProcState.WANTED:
                self.try_execute_one(p, False)  # Do not go deeper while iterating

    # Executes proc now if possible. Returns false if not possible
    def try_execute_one(self, proc: 'Proc', collect: bool = True) -> bool:

        # Check if any other WANTED procs have a lower wave and are ready to run - they must run first
        # Don't block on dependencies - they will be handled by the dependency check below
        for name, p in self.procs.items():
            if p.state in [ProcState.WANTED, ProcState.RUNNING] and p.wave < proc.wave and name not in proc.deps:
                # Check if this lower wave proc is ready to run (all its dependencies are complete)
                can_run = True
                for dep in p.deps:
                    if dep not in self.procs or not self.procs[dep].is_complete():
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
            if d not in self.procs:
                logger.debug(f'Proc "{proc.name}" not started due to unknown dependency "{d}"')
                return False

            if self.procs[d].is_failed():
                logger.debug(f'Proc "{proc.name}" canceled due to failed dependency "{d}"')
                proc.state = ProcState.FAILED
                proc.error = Proc.ERROR_DEP_FAILED
                proc.more_info = f'canceled due to failure of "{self.procs[d].name}"'
                self.term.completed_proc(proc)

            elif not self.procs[d].is_complete():
                logger.debug(f'Proc "{proc.name}" not started due to unfinished dependency "{d}"')
                return False

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
        # Add context for specific process
        context = {'args': proc.args, **self.context}
        logger.info(f'Exec "{proc.name}" with context {context}')

        # Queues for bidirectional communication
        proc.queue_to_proc = mp.Queue()
        proc.queue_to_master = mp.Queue()
        proc.state = ProcState.RUNNING
        proc.start_time = time.time()

        # Set locks
        for l in proc.locks:
            self.locks[l] = proc

        # Kick off process
        self.term.start_proc(proc)
        proc.process = mp.Process(
            target=proc.func,
            name=f'parproc-child-{proc.name}',
            args=(proc.queue_to_proc, proc.queue_to_master, context, proc.name),
        )
        proc.process.start()

    # Finds any procs that have completed their execution, and moves them on. Tries to execute other
    # procs if any procs were collected
    def collect(self) -> None:
        found_any = False
        for name in list(self.procs):
            p = self.procs[name]  # Might mutate procs list, so iterate pregenerated list

            if p.is_running():
                assert p.queue_to_master is not None  # nosec
                assert p.queue_to_proc is not None  # nosec

                # Try to get output
                try:
                    # logger.debug('collect: looking')
                    msg = p.queue_to_master.get_nowait()
                except queue.Empty:  # Not done yet
                    # logger.debug('collect: empty')
                    pass
                else:
                    logger.debug(f'got msg from proc "{name}": {msg}')
                    # Process sent us data
                    if msg['req'] == 'proc-complete':
                        # Process is done
                        # logger.debug('collect: done')
                        p.process = None
                        p.output = msg['value']
                        p.error = msg['error']
                        p.state = ProcState.SUCCEEDED if p.error == Proc.ERROR_NONE else ProcState.FAILED

                        found_any = True
                        p.log_filename = os.path.join(str(self.context['logdir']), name + '.log')

                        logger.info(f'proc "{p.name}" collected: ret = {p.output}')

                        self.context['results'][p.name] = p.output

                        logger.info(f'new context: {self.context}')

                        # Release locks
                        for l in p.locks:
                            del self.locks[l]

                        self.term.end_proc(p)

                    elif msg['req'] == 'get-input':
                        # Proc is requesting input. Provide it
                        input_ = self.term.get_input(message=msg['message'], password=msg['password'])

                        msg.update({'resp': input_})
                        p.queue_to_proc.put(msg)

                    elif msg['req'] == 'create-proc':
                        proc_name = self.create_proc(msg['proto_name'], msg.get('proc_name'))
                        msg.update({'proc_name': proc_name})  # In case we created new name
                        p.queue_to_proc.put(msg)  # Respond with same msg. No new data

                    elif msg['req'] == 'start-procs':
                        self.start_procs(msg['names'])
                        p.queue_to_proc.put(msg)  # Respond with same msg. No new data

                    elif msg['req'] == 'check-complete':
                        msg.update(
                            {'complete': self.check_complete(msg['names']), 'failure': self.check_failure(msg['names'])}
                        )
                        if p.queue_to_proc is not None:
                            p.queue_to_proc.put(msg)

                    elif msg['req'] == 'get-results':
                        msg.update({'results': self.context['results']})
                        if p.queue_to_proc is not None:
                            p.queue_to_proc.put(msg)

                    else:
                        raise UserError(f'unknown call: {msg["req"]}')

            # If still running after processing messages, check for timeout
            if (
                p.is_running()
                and p.timeout is not None
                and p.start_time is not None
                and (time.time() - p.start_time) > p.timeout
            ):

                if p.process is not None:
                    p.process.terminate()
                p.process = None
                p.output = None
                p.error = Proc.ERROR_TIMEOUT
                p.state = ProcState.FAILED
                p.log_filename = os.path.join(str(self.context['logdir']), name + '.log')

                logger.info(f'proc "{p.name}" timed out')

                self.context['results'][p.name] = None

                logger.info(f'new context: {self.context}')

                # Release locks
                for l in p.locks:
                    del self.locks[l]

                self.term.end_proc(p)

        if found_any:
            self.try_execute_any()

    # Wait for all procs and locks
    def wait_for_all(self, exception_on_failure: bool = True) -> None:
        logger.debug('WAIT FOR COMPLETION')
        while any(p.state != ProcState.IDLE and not p.is_complete() for name, p in self.procs.items()) or self.locks:
            self._step()

        # Do final update. Force update
        self.term.update(force=True)

        # Raise on issue
        if exception_on_failure and self.check_failure(list(self.procs)):
            raise ProcessError('Process error [1]')

    # Wait for procs or locks
    def wait(self, names: list[str]) -> None:
        logger.debug(f'WAIT FOR {names}')
        while not self.check_complete(names):
            self._step()

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
        return any(self.procs[name].state == ProcState.FAILED for name in names if name in self.procs)

    # Move things forward
    def _step(self) -> None:
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


# Objects of this class only live inside the individual proc threads
class ProcContext:

    def __init__(self, proc_name: str, context: dict[str, Any], queue_to_proc: mp.Queue, queue_to_master: mp.Queue):
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


class Proto:
    """Decorator for process prototypes. These can be parameterized and instantiated again and again
    deps: Can be str (proc name, proto pattern, or filled-out name like "foo-a-2"), or callable returning str or list[str].
         Dependencies are matched against proto patterns and created automatically if not found.
    """

    def __init__(
        self,
        name: str | None = None,
        f: F | None = None,
        deps: list[DepInput] | None = None,
        locks: list[str] | None = None,
        now: bool = False,
        args: dict[str, Any] | None = None,
        timeout: float | None = None,
        wave: int = 0,
    ):
        # Input properties
        self.name = name
        self.deps = deps if deps is not None else []
        self.locks = locks if locks is not None else []
        self.now = now  # Whether proc will start once created
        self.args = args if args is not None else {}
        self.timeout = timeout
        self.wave = wave

        if f is not None:
            # Created using short-hand
            self.__call__(f)

    # Called immediately after initialization
    def __call__(self, f: F) -> F:

        if self.name is None:
            self.name = f.__name__

        self.func = f
        
        # Generate regex pattern for matching filled-out names
        # Convert pattern like "foo-[x]-[y]" to regex that can match "foo-a-2" and extract x="a", y="2"
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
            # Build regex by splitting on [param] markers
            # e.g., "foo-[x]-[y]" -> ["foo-", "[x]", "-", "[y]", ""]
            parts = re.split(r'(\[[^\]]+\])', self.name)
            
            pattern_parts = []
            param_index = 0
            
            for part in parts:
                if not part:
                    continue
                if part.startswith('[') and part.endswith(']'):
                    # This is a parameter marker
                    param = part[1:-1]  # Remove [ and ]
                    if param_index == len(params) - 1:
                        # Last parameter: match everything to end
                        pattern_parts.append(f'(?P<{param}>.*)')
                    else:
                        # Not last: match non-empty sequence (greedy, will be constrained by next literal)
                        pattern_parts.append(f'(?P<{param}>[^\\-]+)')
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
            name: Filled-out name like "foo-a-2" for pattern "foo-[x]-[y]"
        
        Returns:
            Dict of extracted parameters if match, None otherwise
        """
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
    deps   - process dependencies. will not be run until these have run
    locks  - list of locks. only one process can own a lock at any given time
    """

    ERROR_NONE = 0
    ERROR_EXCEPTION = 1
    ERROR_DEP_FAILED = 2
    ERROR_TIMEOUT = 3

    # Called on intitialization
    def __init__(
        self,
        name: str | None = None,
        f: F | None = None,
        *,
        deps: list[str] | None = None,
        locks: list[str] | None = None,
        now: bool = False,
        args: dict[str, Any] | None = None,
        proto: Proto | None = None,
        timeout: float | None = None,
        wave: int = 0,
    ):
        # Input properties
        self.name = name
        self.deps = deps if deps is not None else []
        self.locks = locks if locks is not None else []
        self.now = now
        self.args = args if args is not None else {}
        self.proto = proto
        self.timeout = timeout
        # Wave defaults to proto's wave if proto exists, otherwise use provided wave (default 0)
        self.wave = proto.wave if proto is not None else wave

        # Utils
        self.log_filename = ''

        # Main function
        self.func: Any | None = None

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
        return self.state in {ProcState.SUCCEEDED, ProcState.FAILED}

    def is_failed(self) -> bool:
        return self.state == ProcState.FAILED

    # Called immediately after initialization
    def __call__(self, f: F) -> F:
        # Queue is bi-directional queue to provide return value on exit (and maybe other things in the future
        def func(queue_to_proc: mp.Queue, queue_to_master: mp.Queue, context: dict[str, Any], name: str) -> None:
            # FIX: Wrap function and replace sys.stdout and sys.stderr to capture output
            # https://stackoverflow.com/questions/30793624/grabbing-stdout-of-a-function-with-multiprocessing
            logger.info(f'proc "{name}" started')

            pc = ProcContext(name, context, queue_to_proc, queue_to_master)
            error = Proc.ERROR_NONE
            ret = None

            # Redirect output to file, one for each process, to keep the output in sequence
            log_filename = os.path.join(str(context['logdir']), name + '.log')
            with open(log_filename, 'w', encoding='utf-8') as log_file:
                sys.stdout = log_file  # Redirect stdout
                sys.stderr = log_file

                try:
                    ret = f(pc, **pc.args)  # Execute process
                except Exception as e:  # Catch all exceptions, so pylint: disable=broad-exception-caught
                    _, _, tb = sys.exc_info()
                    info = str(e) + '\n' + ''.join(traceback.format_tb(tb))

                    # Exceptions from 'sh' sometimes have a separate stderr field
                    stderr = getattr(e, 'stderr', None)
                    if stderr is not None and isinstance(stderr, bytes):
                        info += f'\nSTDERR_FULL:\n{stderr.decode("utf-8")}'

                    log_file.write(info)
                    error = Proc.ERROR_EXCEPTION

            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__

            msg = {'req': 'proc-complete', 'value': ret, 'log_filename': log_filename, 'error': error}

            logger.info(f'proc "{name}" ended: ret = {ret}')

            queue_to_master.put(msg)  # Provide return value from function

        if self.name is None:
            self.name = f.__name__

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


def create(proto_name: str, proc_name: str | None = None) -> str:
    return ProcManager.get_inst().create_proc(proto_name, proc_name)


def run(proto_name: str, proc_name: str | None = None) -> None:
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
