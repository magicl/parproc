"""
Runner interface and implementations.
Runners abstract how tasks are executed (subprocess vs same process/thread).
"""

from typing import Any, Protocol

from .multi_process_runner import MultiProcessRunner
from .single_process_runner import SingleProcessRunner

__all__ = [
    'MultiProcessRunner',
    'Runner',
    'SingleProcessRunner',
]


class Runner(Protocol):
    """Protocol for task runners."""

    def start_task(self, manager: Any, proc: Any) -> None:
        """Start the given proc (async for multi-process, sync for single-process)."""
        ...  # pylint: disable=unnecessary-ellipsis

    def collect(self, manager: Any) -> None:
        """Poll for completed tasks and handle messages (no-op for single-process)."""
        ...  # pylint: disable=unnecessary-ellipsis
