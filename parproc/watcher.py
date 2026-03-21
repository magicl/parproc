"""File system watcher for watch mode.

Monitors declared input paths for changes and maps them back to proc names
so the scheduler knows which procs have become dirty.
"""

import errno
import logging
import os
import threading
from collections.abc import Iterable
from typing import Any

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver
from watchdog.observers.polling import PollingObserver

logger = logging.getLogger('par')


class _ChangeHandler(FileSystemEventHandler):
    """Watchdog handler that records which proc names are affected by file events."""

    def __init__(self, path_to_procs: dict[str, set[str]], lock: threading.Lock):
        super().__init__()
        self._path_to_procs = path_to_procs
        self._dirty: set[str] = set()
        self._lock = lock
        self._event = threading.Event()

    def _handle(self, event: Any) -> None:
        src = getattr(event, 'src_path', '')
        if not isinstance(src, str):
            return
        with self._lock:
            for watched_path, proc_names in self._path_to_procs.items():
                if src == watched_path or src.startswith(watched_path + '/'):
                    self._dirty.update(proc_names)
            if self._dirty:
                self._event.set()

    def on_modified(self, event: Any) -> None:
        self._handle(event)

    def on_created(self, event: Any) -> None:
        self._handle(event)

    def on_deleted(self, event: Any) -> None:
        self._handle(event)

    def on_moved(self, event: Any) -> None:
        self._handle(event)

    def drain(self) -> set[str]:
        with self._lock:
            dirty = self._dirty.copy()
            self._dirty.clear()
            self._event.clear()
        return dirty

    def wait(self, timeout: float | None = None) -> None:
        self._event.wait(timeout=timeout)


class FileWatcher:
    """Watches file system paths and maps changes back to proc names."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._path_to_procs: dict[str, set[str]] = {}
        self._handler = _ChangeHandler(self._path_to_procs, self._lock)
        self._observer: BaseObserver | None = None

    def add_proc_inputs(self, proc_name: str, paths: list[str]) -> None:
        """Register resolved input paths for a proc."""
        with self._lock:
            for path in paths:
                normalized_path = os.path.abspath(path)
                if normalized_path not in self._path_to_procs:
                    self._path_to_procs[normalized_path] = set()
                self._path_to_procs[normalized_path].add(proc_name)

    @staticmethod
    def _is_subpath(candidate: str, parent: str) -> bool:
        """Return True when candidate is inside parent (or equal)."""
        try:
            return os.path.commonpath([candidate, parent]) == parent
        except ValueError:
            # Different drives/roots cannot have a parent-child relationship.
            return False

    @classmethod
    def _compute_watch_dirs(cls, paths: Iterable[str]) -> list[str]:
        """Compute a minimal set of directories to watch recursively."""
        candidate_dirs = sorted(
            {os.path.abspath(path if os.path.isdir(path) else os.path.dirname(path) or '.') for path in paths},
            key=lambda p: (len(p), p),
        )

        roots: list[str] = []
        for directory in candidate_dirs:
            if any(cls._is_subpath(directory, root) for root in roots):
                continue
            roots.append(directory)
        return roots

    def _start_with_observer(self, observer: BaseObserver, watched_dirs: list[str]) -> None:
        """Schedule dirs on given observer and start it."""
        for watch_dir in watched_dirs:
            observer.schedule(self._handler, watch_dir, recursive=True)
        observer.start()
        self._observer = observer

    def start(self) -> None:
        """Start watching all registered paths (background thread)."""
        with self._lock:
            watched_dirs = self._compute_watch_dirs(self._path_to_procs.keys())

        if not watched_dirs:
            logger.info('FileWatcher started with no watched directories')
            return

        try:
            self._start_with_observer(Observer(), watched_dirs)
            logger.info('FileWatcher started (inotify), watching %d directories', len(watched_dirs))
        except OSError as exc:
            if exc.errno != errno.EMFILE:
                raise
            logger.warning(
                'Inotify instance limit reached; falling back to polling watcher for %d directories',
                len(watched_dirs),
            )
            self._start_with_observer(PollingObserver(), watched_dirs)

    def stop(self) -> None:
        """Stop the watcher."""
        if self._observer is not None:
            self._observer.stop()
            self._observer.join()
            self._observer = None

    def get_dirty_procs(self) -> set[str]:
        """Return proc names whose inputs changed since last call, then clear."""
        return self._handler.drain()

    def wait_for_changes(self, timeout: float | None = None) -> set[str]:
        """Block until at least one input changes (or timeout). Return dirty proc names."""
        self._handler.wait(timeout=timeout)
        return self._handler.drain()
