from enum import Enum


class ProcState(Enum):
    IDLE = 0  # Defined, but will not run until it is the dependency of another proc
    WANTED = 1  # Dependency of another proc, or specified to run immediately
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4
