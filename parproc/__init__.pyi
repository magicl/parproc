from collections.abc import Callable, Sequence
from enum import Enum
from os import PathLike
from typing import Any, TypeVar

F = TypeVar('F', bound=Callable[..., Any])
DepSpecOrList = str | list[str]
DepInput = str | Callable[..., DepSpecOrList]
FileSpec = str | Callable[..., list[str]]
ArgChoices = dict[str, Sequence[Any]]
ArgChoicesFn = Callable[..., ArgChoices]

class SpecialDep(Enum):
    NO_FAILURES = 'no_failures'

NO_FAILURES: SpecialDep

class RdepRule:
    pattern: str
    def __init__(self, pattern: str) -> None: ...

class WhenScheduled(RdepRule): ...
class WhenTargetScheduled(RdepRule): ...

class LogIssueRule:
    pattern: str
    def __init__(self, pattern: str) -> None: ...
    def applies(self, *, task_succeeded: bool) -> bool: ...

class IgnoreLogAlways(LogIssueRule): ...

class IgnoreLogIfSucceeded(LogIssueRule):
    def applies(self, *, task_succeeded: bool) -> bool: ...

class UserError(Exception): ...
class ProcessError(Exception): ...
class ProcFailedError(Exception): ...
class ProcSkippedError(Exception): ...

class ProcContext:
    proc_name: str
    params: dict[str, Any]
    results: dict[str, Any]
    args: dict[str, Any]
    deps_changed: bool
    changed_deps: list[str]
    input_fingerprint: dict[str, float] | None

    def create(self, proto_name: str, proc_name: str | None = None) -> list[str]: ...
    def run(self, proto_name: str, proc_name: str | None = None) -> None: ...
    def start(self, *names: str) -> None: ...
    def wait(self, *names: str) -> None: ...
    def get_input(self, message: str = '', password: bool = False) -> Any: ...

class ProcManager:
    inst: ProcManager | None

    @classmethod
    def get_inst(cls) -> ProcManager: ...
    def set_options(
        self,
        parallel: int | None = None,
        dynamic: bool | None = None,
        mode: str | None = None,
        debug: bool | None = None,
        allow_missing_deps: bool | None = None,
        task_db_path: str | None = ...,
        name_param_separator: str | None = None,
        watch: bool | None = None,
        watch_debounce_seconds: float | None = None,
        full_log_on_failure: bool | None = None,
    ) -> None: ...
    def set_params(self, **params: Any) -> None: ...
    def clear(self) -> None: ...
    def create_proc(self, proto_name: str, proc_name: str | None = None) -> list[str]: ...
    def start_proc(self, *name: str) -> None: ...
    def wait_for_all(self, exception_on_failure: bool = True) -> bool: ...
    def wait(self, names: list[str] | list[str | list[str]]) -> None: ...
    def wait_clear(self, exception_on_failure: bool = False) -> bool: ...
    def check_complete(self, names: list[str]) -> bool: ...
    def check_failure(self, names: list[str]) -> bool: ...

def get_procs() -> dict[str, Proc]: ...
def get_protos() -> dict[str, Proto]: ...
def set_options(
    parallel: int | None = None,
    dynamic: bool | None = None,
    mode: str | None = None,
    debug: bool | None = None,
    allow_missing_deps: bool | None = None,
    task_db_path: str | None = None,
    name_param_separator: str | None = None,
    watch: bool | None = None,
    watch_debounce_seconds: float | None = None,
    full_log_on_failure: bool | None = None,
) -> None: ...
def set_params(**params: Any) -> None: ...
def create(proto_name: str, proc_name: str | None = None, **kwargs: Any) -> list[str]: ...
def run(proto_name: str, proc_name: str | None = None, *, full: bool = False, **kwargs: Any) -> None: ...
def start(*names: str) -> None: ...
def wait(*names: str) -> None: ...
def clear() -> None: ...
def results() -> dict[str, Any]: ...
def wait_for_all(exception_on_failure: bool = True) -> bool: ...
def wait_clear(exception_on_failure: bool = False) -> bool: ...
def watch(*watch_names: str) -> None: ...
def regex_files(root: str | PathLike[str], pattern: str) -> Callable[..., list[str]]: ...

class Proc:
    ERROR_NONE: int
    ERROR_EXCEPTION: int
    ERROR_DEP_FAILED: int
    ERROR_TIMEOUT: int
    ERROR_NOT_PICKLEABLE: int
    ERROR_FAILED: int
    ERROR_SKIPPED: int
    ERROR_OUTPUTS_NOT_REFRESHED: int

    def __init__(
        self,
        name: str | None = None,
        f: F | None = None,
        *,
        deps: Sequence[str | SpecialDep] | None = None,
        rdeps: list[str | RdepRule] | None = None,
        locks: list[str] | None = None,
        now: bool = False,
        args: dict[str, Any] | None = None,
        proto: Proto | None = None,
        timeout: float | None = None,
        wave: int = 0,
        special_deps: list[SpecialDep] | None = None,
        inputs: Sequence[FileSpec] | Callable[..., list[str]] | None = None,
        inputs_ignore: Sequence[FileSpec] | Callable[..., list[str]] | None = None,
        outputs: Sequence[FileSpec] | Callable[..., list[str]] | None = None,
        log_ignore: list[str | LogIssueRule] | str | LogIssueRule | None = None,
        no_skip: bool = False,
    ) -> None: ...
    def __call__(self, f: F) -> F: ...

class Proto:
    def __init__(
        self,
        name: str | None = None,
        f: F | None = None,
        deps: Sequence[DepInput | SpecialDep] | None = None,
        rdeps: list[str | RdepRule] | None = None,
        locks: list[str] | None = None,
        now: bool = False,
        args: dict[str, Any] | None = None,
        arg_choices: ArgChoices | ArgChoicesFn | None = None,
        timeout: float | None = None,
        wave: int = 0,
        inputs: Sequence[FileSpec] | Callable[..., list[str]] | None = None,
        inputs_ignore: Sequence[FileSpec] | Callable[..., list[str]] | None = None,
        outputs: Sequence[FileSpec] | Callable[..., list[str]] | None = None,
        log_ignore: list[str | LogIssueRule] | str | LogIssueRule | None = None,
        no_skip: bool = False,
    ) -> None: ...
    def __call__(self, f: F) -> F: ...
