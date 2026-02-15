"""Shared types and constants to avoid circular imports (e.g. term importing par)."""

from enum import Enum


class ProcState(Enum):
    IDLE = 0  # Defined, but will not run until it is the dependency of another proc
    WANTED = 1  # Dependency of another proc, or specified to run immediately
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4
    FAILED_DEP = 5  # Canceled because a dependency is missing or failed
    SKIPPED = 6  # Proc raised ProcSkippedError; counts as success for dependencies


SUCCEEDED_STATES = {ProcState.SUCCEEDED, ProcState.SKIPPED}
FAILED_STATES = {ProcState.FAILED, ProcState.FAILED_DEP}


class SpecialDep(Enum):
    """Special dependency kinds: global conditions that are not other tasks."""

    NO_FAILURES = 'no_failures'  # Satisfied only when no proc in the run has failed


# Individual special deps exported on parproc (e.g. pp.NO_FAILURES)
NO_FAILURES = SpecialDep.NO_FAILURES


class RdepRule:
    """Base class for rdep behavior modifiers.

    Subclasses can be placed alongside plain strings in a Proto/Proc ``rdeps`` list
    to alter when and how the reverse dependency is injected.
    """

    def __init__(self, pattern: str):
        self.pattern = pattern

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.pattern!r})'


class WhenScheduled(RdepRule):
    """Conditional reverse dependency that activates only when the declaring proc is scheduled.

    When used in a Proto/Proc ``rdeps`` list, the declaring proc is injected as a
    dependency of the *target* (the pattern) only when the declaring proc itself is
    scheduled.  If the declaring proc is not scheduled, the target is unaffected.

    Additionally, scheduling the declaring proc will also schedule the target
    (creating it from a proto if necessary).

    Example::

        @pp.Proto(name='B', rdeps=[pp.WhenScheduled('A')])
        def b_proc(context): ...

    If only A is scheduled, B is *not* involved.  If B is scheduled, B is
    injected as a dependency of A (and A is scheduled too), so B runs first.
    """


class UserError(Exception):
    """Raised by the framework when configuration or API usage is invalid."""


class ProcessError(Exception):
    """Raised when wait_for_all/wait sees a failure and exception_on_failure is True."""


class ProcFailedError(Exception):
    """Raise from inside a proc to mark the task as failed (ERROR_FAILED)."""


class ProcSkippedError(Exception):
    """Raise from inside a proc to mark the task as skipped (ERROR_SKIPPED)."""
