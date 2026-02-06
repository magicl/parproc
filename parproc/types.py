"""Shared types and constants to avoid circular imports (e.g. term importing par)."""

from enum import Enum


class ProcState(Enum):
    IDLE = 0  # Defined, but will not run until it is the dependency of another proc
    WANTED = 1  # Dependency of another proc, or specified to run immediately
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4
    FAILED_DEP = 5  # Canceled because a dependency is missing or failed


SUCCEEDED_STATES = {ProcState.SUCCEEDED}
FAILED_STATES = {ProcState.FAILED, ProcState.FAILED_DEP}


class SpecialDep(Enum):
    """Special dependency kinds: global conditions that are not other tasks."""

    NO_FAILURES = 'no_failures'  # Satisfied only when no proc in the run has failed


# Individual special deps exported on parproc (e.g. pp.NO_FAILURES)
NO_FAILURES = SpecialDep.NO_FAILURES


class UserError(Exception):
    """Raised by the framework when configuration or API usage is invalid."""


class ProcessError(Exception):
    """Raised when wait_for_all/wait sees a failure and exception_on_failure is True."""


class ProcFailedError(Exception):
    """Raise from inside a proc to mark the task as failed (ERROR_FAILED)."""


class ProcSkippedError(Exception):
    """Raise from inside a proc to mark the task as skipped (ERROR_SKIPPED)."""
