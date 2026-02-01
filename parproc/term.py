import getpass
import os
import re
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.live import Live
from rich.markup import escape
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.syntax import Syntax

from . import task_db
from .state import FAILED_STATES, SUCCEEDED_STATES, ProcState

if TYPE_CHECKING:
    from .par import Proc


@dataclass
class LogChunk:
    """Represents a chunk of log output with line number information."""

    content: str  # The actual log content (may contain newlines)
    start_line: int  # Line number where chunk starts (1-indexed)
    end_line: int  # Line number where chunk ends (1-indexed)


class Displayable:
    def __init__(self, proc: "Proc"):
        self.proc = proc
        self.chunks: list[LogChunk] = []  # Log chunks to display for proc
        self.completed = False
        self.start_time: float | None = None
        self.task_id: TaskID | None = None  # Rich Progress task ID
        self.execution_time: str = ""  # Execution time string for completed tasks
        self.expected_duration: float | None = None  # Seconds; set when task DB has history

    # Get number of lines to display for item
    def height(self) -> int:
        return 1 + sum(chunk.end_line - chunk.start_line + 1 for chunk in self.chunks)


def _get_error_type_message(disp: Displayable) -> str:
    """Short message for error type when task failed; empty when succeeded."""
    if disp.proc.state in SUCCEEDED_STATES:
        return ""
    if disp.proc.state == ProcState.FAILED_DEP:
        return " dependency failed"
    # ProcState.FAILED with error code
    proc_class = type(disp.proc)
    err = getattr(disp.proc, "error", None)
    if err == proc_class.ERROR_TIMEOUT:
        return " timeout"
    if err == proc_class.ERROR_EXCEPTION:
        return " exception"
    if err == getattr(proc_class, "ERROR_NOT_PICKLEABLE", -1):
        return " not pickleable"
    return " failed"


# Base class for terminal display. Use TermSimple or TermDynamic.
class Term:

    updatePeriod = 0.1  # Seconds between each update
    context_lines = 2  # Number of lines before and after keyword matches to include

    def __init__(self) -> None:
        self.active: OrderedDict["Proc", Displayable] = OrderedDict()
        self.inactive: list[Displayable] = []
        self.last_update: float = 0.0
        self.console = Console()
        self.progress: Progress | None = None
        self.live: Live | None = None

    def clear(self) -> None:
        """Reset display state (e.g. when ProcManager.clear() starts a new session)."""
        if self.live is not None:
            self.live.stop()
            self.live = None
        self.progress = None
        self.active = OrderedDict()
        self.inactive = []

    def start_proc(self, p: "Proc") -> None:
        """Called when a process starts. Override in subclasses."""
        raise NotImplementedError

    def end_proc(self, p: "Proc") -> None:
        """Called when a process ends. Override in subclasses."""
        raise NotImplementedError

    def _ensure_progress(self) -> None:
        """Initialize Progress and Live display if needed. No-op in TermSimple."""

    def update(self, force: bool = False) -> None:
        """Refresh display. Override in subclasses."""

    def get_input(self, message: str, password: bool) -> str:
        """Get input from user. Override in subclasses."""
        self.console.print(message)
        if password:
            return getpass.getpass()
        return sys.stdin.readline()

    def _get_description(self, proc: "Proc") -> str:
        """Get description text for a process."""
        name = proc.name or ""
        if proc.more_info:
            name += f" - {proc.more_info}"
        return name

    def completed_proc(self, p: "Proc") -> None:
        self.start_proc(p)
        self.end_proc(p)

    def _print_completed_task(self, disp: Displayable) -> None:
        """Print a single completed task to the console (scrollback). Not part of the live update area."""
        if disp.proc.state in SUCCEEDED_STATES:
            status = "[bold green]✓[/bold green]"
        elif disp.proc.state == ProcState.FAILED_DEP:
            status = "[bold red]🚫[/bold red]"
        else:
            status = "[bold red]✗[/bold red]"

        name_escaped = escape(disp.proc.name or "")
        time_escaped = escape(disp.execution_time)
        err_msg = _get_error_type_message(disp)
        err_escaped = escape(err_msg)
        completion_line = f"{status} {name_escaped}{time_escaped}{err_escaped}"
        self.console.print(completion_line)

        if disp.chunks:
            chunk_parts = []
            for i, chunk in enumerate(disp.chunks):
                line_range = f"lines {chunk.start_line}-{chunk.end_line}"
                if i > 0:
                    chunk_parts.append(f"\n--- {line_range} ---\n")
                else:
                    chunk_parts.append(f"--- {line_range} ---\n")
                chunk_parts.append(chunk.content)
            log_content_str = "".join(chunk_parts)
            lexer = "python" if disp.proc.state in FAILED_STATES else "text"
            log_content = Syntax(
                log_content_str,
                lexer=lexer,
                theme="monokai",
                line_numbers=False,
                word_wrap=True,
            )
            panel_title = None
            if disp.proc.state in FAILED_STATES and disp.proc.log_filename:
                abs_path = os.path.abspath(disp.proc.log_filename)
                panel_title = f"[link=file://{abs_path}]{disp.proc.log_filename}[/link]"
            log_panel = Panel.fit(
                log_content,
                title=panel_title,
                border_style="red" if disp.proc.state in FAILED_STATES else "dim",
            )
            self.console.print(log_panel)

    def _render_display(self) -> Any:
        """Render only the live-updated area. Override in TermDynamic."""
        return ""

    def _render_proc_static(self, disp: Displayable) -> None:
        """Render a process in static (non-dynamic) mode."""
        if disp.proc.state in SUCCEEDED_STATES:
            status = "[bold green]✓[/bold green]"
        elif disp.proc.state == ProcState.FAILED_DEP:
            status = "[bold red]🚫[/bold red]"
        elif disp.proc.state in FAILED_STATES:
            status = "[bold red]✗[/bold red]"
        else:
            status = "[yellow]•[/yellow]"

        more_info = disp.proc.more_info
        if disp.proc.state in FAILED_STATES:
            if more_info == '':
                more_info = f'logfile: {disp.proc.log_filename}'

        if more_info != '':
            more_info = ' - ' + more_info

        err_msg = _get_error_type_message(disp)
        self.console.print(f'{status} {disp.proc.name}{more_info}{err_msg}')

        if disp.chunks:
            for chunk in disp.chunks:
                self.console.print(f'  --- lines {chunk.start_line}-{chunk.end_line} ---')
                for line in chunk.content.split('\n'):
                    self.console.print(f'  {line}')

    @staticmethod
    def extract_error_log(text: str, task_failed: bool) -> list[LogChunk]:
        """
        Extract relevant log chunks from text.

        Args:
            text: The full log text
            task_failed: Whether the task failed (if True, MUST return at least one chunk)

        Returns:
            List of LogChunk objects with content and line number ranges
        """
        lines = text.split('\n')
        total_lines = len(lines)
        chunks: list[LogChunk] = []

        # If task failed, try parsers first
        if task_failed:
            parsers = [
                (
                    'python.sh-full',  # For when e.stderr is available
                    re.compile(r'^.*(RAN:.*STDOUT:.*STDERR:).*STDERR_FULL:(.*)$', re.DOTALL),
                    lambda m: f'  {m.group(1)}{m.group(2)}',
                ),
                ('python.sh', re.compile(r'^.*(RAN:.*STDOUT:.*STDERR:.*)$', re.DOTALL), lambda m: f'  {m.group(1)}'),
            ]

            # Find the matching parser
            for _, reg, output in parsers:
                m = re.match(reg, text)
                if m:
                    # Match with parser - return as single chunk
                    content = output(m)
                    # Estimate line numbers (approximate based on content position)
                    # For parser matches, we'll use the full range since we matched the whole text
                    return [LogChunk(content=content, start_line=1, end_line=total_lines)]

        # Search for keywords in the log (convert to lowercase for matching)
        keywords = ['exception', 'error', 'warning', 'notice', 'deprecated', 'deprecation']
        lines_lower = [line.lower() for line in lines]

        # Find line numbers where keywords appear
        cutout_ranges: list[tuple[int, int]] = []  # List of (start_line, end_line) ranges

        for keyword in keywords:
            for line_idx, line in enumerate(lines_lower, start=1):
                if keyword in line:
                    # Add range: line number ± context_lines
                    start = max(1, line_idx - Term.context_lines)
                    end = min(total_lines, line_idx + Term.context_lines)
                    cutout_ranges.append((start, end))

        # Merge overlapping ranges
        if cutout_ranges:
            # Sort by start line
            cutout_ranges.sort(key=lambda x: x[0])
            merged: list[tuple[int, int]] = []
            for start, end in cutout_ranges:
                if merged and start <= merged[-1][1] + 1:  # Overlapping or adjacent
                    # Merge with previous range
                    merged[-1] = (merged[-1][0], max(merged[-1][1], end))
                else:
                    merged.append((start, end))
            cutout_ranges = merged

        # Extract chunks from cutout ranges
        if cutout_ranges:
            for start, end in cutout_ranges:
                # Extract lines (convert back to 0-indexed for list access)
                chunk_lines = lines[start - 1 : end]
                content = '\n'.join(chunk_lines)
                chunks.append(LogChunk(content=content, start_line=start, end_line=end))
        elif task_failed:
            # If task failed but no keywords found, grab bottom 16 lines
            # (MUST output something if task failed)
            start = max(1, total_lines - 15)  # -15 because we want 16 lines total
            end = total_lines
            chunk_lines = lines[start - 1 : end]
            content = '\n'.join(chunk_lines)
            chunks.append(LogChunk(content=content, start_line=start, end_line=end))
        # If task didn't fail and no keywords found, return empty list

        return chunks


class TermSimple(Term):
    """Static terminal display: one line per task, no live updates."""

    def start_proc(self, p: "Proc") -> None:
        disp = Displayable(p)
        disp.start_time = time.time()
        self.active[p] = disp
        self._render_proc_static(disp)

    def end_proc(self, p: "Proc") -> None:
        if p not in self.active:
            return
        disp = self.active[p]
        disp.completed = True
        if disp.proc.log_filename != '':
            with open(disp.proc.log_filename, encoding='utf-8') as f:
                log_text = f.read()
                task_failed = disp.proc.state in FAILED_STATES
                disp.chunks = Term.extract_error_log(log_text, task_failed)
        self._render_proc_static(disp)
        del self.active[p]


class TermDynamic(Term):
    """Dynamic terminal display: live progress bars and in-place updates."""

    def _ensure_progress(self) -> None:
        if self.progress is None:
            self.progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=self.console,
                transient=False,
            )
            self.live = Live(
                self._render_display(),
                refresh_per_second=10,
                console=self.console,
                vertical_overflow="visible",
            )
            self.live.start()

    def start_proc(self, p: "Proc") -> None:
        disp = Displayable(p)
        disp.start_time = time.time()
        self.active[p] = disp
        self._ensure_progress()
        if self.progress is not None:
            description = self._get_description(p)
            expected = task_db.get_expected_duration(p.name) if task_db.get_current() and p.name is not None else None
            if expected is not None and expected > 0:
                disp.expected_duration = expected
                task_id = self.progress.add_task(description, total=expected)
            else:
                disp.expected_duration = None
                task_id = self.progress.add_task(description, total=None)
            disp.task_id = task_id
            if self.live is not None:
                self.live.update(self._render_display())

    def end_proc(self, p: "Proc") -> None:
        if p not in self.active:
            return
        disp = self.active[p]
        disp.completed = True
        if disp.proc.log_filename != '':
            with open(disp.proc.log_filename, encoding='utf-8') as f:
                log_text = f.read()
                task_failed = disp.proc.state in FAILED_STATES
                disp.chunks = Term.extract_error_log(log_text, task_failed)
        if disp.start_time is not None:
            elapsed = time.time() - disp.start_time
            disp.execution_time = f" ({elapsed:.2f}s)"
        else:
            disp.execution_time = ""
        if self.progress is not None and disp.task_id is not None:
            self.progress.remove_task(disp.task_id)
        self._print_completed_task(disp)
        self.inactive.append(disp)
        del self.active[p]
        if len(self.active) == 0:
            if self.live is not None:
                self.live.stop()
                self.live = None
            self.inactive = []
            self.progress = None
        else:
            if self.live is not None:
                self.live.update(self._render_display())

    def update(self, force: bool = False) -> None:
        if force or time.time() - self.last_update > Term.updatePeriod:
            self.last_update = time.time()
            if self.progress is not None:
                for proc, disp in self.active.items():
                    if disp.task_id is not None and not disp.completed:
                        description = self._get_description(proc)
                        if disp.expected_duration is not None and disp.start_time is not None:
                            elapsed = time.time() - disp.start_time
                            completed = min(elapsed, disp.expected_duration)
                            self.progress.update(
                                disp.task_id,
                                description=description,
                                completed=completed,
                                refresh=True,
                            )
                        else:
                            self.progress.update(disp.task_id, description=description, refresh=True)
            if self.live is not None:
                self.live.update(self._render_display())
            elif len(self.active) > 0:
                self._ensure_progress()
            elif len(self.active) == 0:
                self.progress = None

    def get_input(self, message: str, password: bool) -> str:
        if self.live is not None:
            self.live.stop()
            self.live = None
        self.console.print(message)
        if password:
            result = getpass.getpass()
        else:
            result = sys.stdin.readline()
        if len(self.active) > 0:
            if self.progress is None:
                self._ensure_progress()
            else:
                self.live = Live(
                    self._render_display(),
                    refresh_per_second=10,
                    console=self.console,
                    vertical_overflow="visible",
                )
                self.live.start()
        return result

    def _render_display(self) -> Any:
        if self.progress is not None:
            return self.progress
        return ""
