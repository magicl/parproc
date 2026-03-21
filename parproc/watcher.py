"""File system watcher for watch mode.

Monitors declared input paths for changes and maps them back to proc names
so the scheduler knows which procs have become dirty.
"""

import logging
import os
import threading
from typing import Any

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

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
        self._observer: Observer | None = None

    def add_proc_inputs(self, proc_name: str, paths: list[str]) -> None:
        """Register resolved input paths for a proc."""
        with self._lock:
            for path in paths:
                if path not in self._path_to_procs:
                    self._path_to_procs[path] = set()
                self._path_to_procs[path].add(proc_name)

    def start(self) -> None:
        """Start watching all registered paths (background thread)."""
        self._observer = Observer()
        watched_dirs: set[str] = set()
        with self._lock:
            for path in self._path_to_procs:
                watch_dir = path if os.path.isdir(path) else os.path.dirname(path) or '.'
                watch_dir = os.path.abspath(watch_dir)
                if watch_dir not in watched_dirs:
                    watched_dirs.add(watch_dir)
                    self._observer.schedule(self._handler, watch_dir, recursive=True)
        self._observer.start()
        logger.info('FileWatcher started, watching %d directories', len(watched_dirs))

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
