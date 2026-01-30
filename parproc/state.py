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
