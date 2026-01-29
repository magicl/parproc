import getpass
import os
import re
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rich.console import Console, Group
from rich.live import Live
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

from .state import ProcState
from . import task_db

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


# Manages terminal session.
class Term:

    updatePeriod = 0.1  # Seconds between each update
    context_lines = 2  # Number of lines before and after keyword matches to include

    def __init__(self, dynamic: bool = True):
        # Keep track of active lines in terminal, i.e. lines we will go back and change
        self.active: OrderedDict["Proc", Displayable] = OrderedDict()
        self.inactive: list[Displayable] = []  # Completed processes
        self.rendered_inactive: set["Proc"] = set()  # Track which inactive procs have been rendered (by proc object)
        self.dynamic = dynamic  # True to dynamically update shell
        self.last_update: float = 0.0  # To limit update rate
        self.console = Console()
        self.progress: Progress | None = None
        self.live: Live | None = None

    def _ensure_progress(self) -> None:
        """Initialize Progress and Live display if needed."""
        if self.progress is None and self.dynamic:
            self.progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=self.console,
                transient=False,  # Keep completed tasks visible
            )
            self.live = Live(self._render_display(), refresh_per_second=10, console=self.console)
            self.live.start()

    def start_proc(self, p: "Proc") -> None:
        disp = Displayable(p)
        disp.start_time = time.time()
        self.active[p] = disp

        if not self.dynamic:
            self._render_proc_static(disp)
        else:
            self._ensure_progress()
            if self.progress is not None:
                description = self._get_description(p)
                expected = task_db.get_expected_duration(p.name) if task_db.get_current() else None
                if expected is not None and expected > 0:
                    disp.expected_duration = expected
                    task_id = self.progress.add_task(description, total=expected)
                else:
                    disp.expected_duration = None
                    task_id = self.progress.add_task(description, total=None)  # indeterminate
                disp.task_id = task_id
                # Update the display
                if self.live is not None:
                    self.live.update(self._render_display())

    def end_proc(self, p: "Proc") -> None:
        if p not in self.active:
            return

        disp = self.active[p]
        disp.completed = True

        # Always analyze log if it exists (for both success and failure)
        if disp.proc.log_filename != '':
            with open(disp.proc.log_filename, encoding='utf-8') as f:
                log_text = f.read()
                task_failed = disp.proc.state == ProcState.FAILED
                disp.chunks = Term.extract_error_log(log_text, task_failed)

        if not self.dynamic:
            self._render_proc_static(disp)
            del self.active[p]
        else:
            # Calculate execution time
            if disp.start_time is not None:
                elapsed = time.time() - disp.start_time
                disp.execution_time = f" ({elapsed:.2f}s)"
            else:
                disp.execution_time = ""

            # Remove task from progress immediately
            if self.progress is not None and disp.task_id is not None:
                self.progress.remove_task(disp.task_id)

            # Move to inactive (will be displayed at top of live display)
            self.inactive.append(disp)
            del self.active[p]

            # If this was the last task, render final display and clean up
            if len(self.active) == 0:
                if self.live is not None:
                    # Clear rendered tracking so all items are shown in final display
                    self.rendered_inactive = set()
                    # Render final display showing all completed tasks (no progress bar)
                    self.live.update(self._render_display())
                    self.live.stop()
                    self.live = None
                    # Clear inactive after Live display is stopped
                    self.inactive = []
                    self.rendered_inactive = set()
                elif self.progress is not None:
                    # Clear rendered tracking so all items are shown
                    self.rendered_inactive = set()
                    # If live wasn't running but progress exists, render final state directly
                    self.console.print(self._render_display())
                    # Clear inactive and rendered tracking after printing
                    self.inactive = []
                    self.rendered_inactive = set()
                self.progress = None
            else:
                # Update live display to show completed task at top, with remaining progress below
                if self.live is not None:
                    self.live.update(self._render_display())

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

    # Call to notify of proc e.g. being canceled. Will show up as completed with message depending
    # on the state of the proc
    def completed_proc(self, p: "Proc") -> None:
        self.start_proc(p)
        self.end_proc(p)

    def _get_description(self, proc: "Proc") -> str:
        """Get description text for a process."""
        name = proc.name or ""
        if proc.more_info:
            name += f" - {proc.more_info}"
        return name

    # force: force update (e.g. when caller wants to refresh every 1/10 s)
    def update(self, force: bool = False) -> None:
        # Refresh task statuses at most every updatePeriod (0.1 s) unless force=True
        if self.dynamic and (force or time.time() - self.last_update > Term.updatePeriod):
            self.last_update = time.time()

            # Update active tasks in progress
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
                # Restart live if we have active processes but no live display
                self._ensure_progress()
            elif len(self.active) == 0:
                # Stop live display when no active processes remain
                # Note: self.live is already None here (we would have taken the first branch otherwise)
                self.progress = None

        # self.console.flush()

    def get_input(self, message: str, password: bool) -> str:
        # Temporarily stop live display if active
        if self.live is not None:
            self.live.stop()
            self.live = None
            # Keep progress but stop live updates

        self.console.print(message)
        if password:
            result = getpass.getpass()
        else:
            result = sys.stdin.readline()

        # Restart live display
        if self.dynamic and len(self.active) > 0:
            if self.progress is None:
                self._ensure_progress()
            else:
                self.live = Live(self._render_display(), refresh_per_second=10, console=self.console)
                self.live.start()

        return result

    def _render_display(self) -> Group | str:
        """Render the complete display with completed tasks at top, then active progress below."""
        display_parts = []

        # Render all inactive items to ensure they remain visible
        # Track which ones are new to prevent duplication in the final output
        inactive_to_render = list(self.inactive)

        # Mark new items as rendered (using proc object as key)
        new_items = [disp for disp in inactive_to_render if disp.proc not in self.rendered_inactive]
        self.rendered_inactive.update(disp.proc for disp in new_items)

        # Render completed tasks at the top
        for disp in inactive_to_render:
            # Status with checkmark, task name, and execution time
            if disp.proc.state == ProcState.SUCCEEDED:
                status = "[bold green]✓[/bold green]"
            else:
                status = "[bold red]✗[/bold red]"

            completion_line = f"{status} {disp.proc.name}{disp.execution_time}"
            display_parts.append(completion_line)

            # If there's log output, show it in its own box
            if disp.chunks:
                # Combine all chunks with separators
                chunk_parts = []
                for i, chunk in enumerate(disp.chunks):
                    if i > 0:
                        # Add separator between chunks
                        line_range = f"lines {chunk.start_line}-{chunk.end_line}"
                        chunk_parts.append(f"\n--- {line_range} ---\n")
                    else:
                        # First chunk - add line range at the start
                        line_range = f"lines {chunk.start_line}-{chunk.end_line}"
                        chunk_parts.append(f"--- {line_range} ---\n")

                    chunk_parts.append(chunk.content)

                log_content_str = "".join(chunk_parts)

                # Use Python syntax highlighting for error logs (works well for tracebacks)
                # For other content, try to auto-detect or use text
                lexer = "python" if disp.proc.state == ProcState.FAILED else "text"
                log_content = Syntax(
                    log_content_str,
                    lexer=lexer,
                    theme="monokai",
                    line_numbers=False,
                    word_wrap=True,
                )

                # Add filename as title if it's an error with a log file
                panel_title = None
                if disp.proc.state == ProcState.FAILED and disp.proc.log_filename:
                    # Make filename clickable using file:// protocol
                    abs_path = os.path.abspath(disp.proc.log_filename)
                    # Use Rich markup to create clickable link
                    panel_title = f"[link=file://{abs_path}]{disp.proc.log_filename}[/link]"

                log_panel = Panel(
                    log_content, title=panel_title, border_style="red" if disp.proc.state == ProcState.FAILED else "dim"
                )
                display_parts.append(log_panel)

        # Add separator if we have both completed tasks and active progress
        # if inactive_to_render and self.progress is not None:
        #    display_parts.append("")  # Empty line separator

        # Add active progress area below completed tasks
        if self.progress is not None:
            display_parts.append(self.progress)

        if not display_parts:
            return ""  # Empty display

        return Group(*display_parts)

    def _render_proc_static(self, disp: Displayable) -> None:
        """Render a process in static (non-dynamic) mode."""
        if disp.proc.state == ProcState.SUCCEEDED:
            status = "[bold green]✓[/bold green]"
        elif disp.proc.state == ProcState.FAILED:
            status = "[bold red]✗[/bold red]"
        else:
            status = "[yellow]•[/yellow]"

        more_info = disp.proc.more_info
        if disp.proc.state == ProcState.FAILED:
            if more_info == '':
                more_info = f'logfile: {disp.proc.log_filename}'

        if more_info != '':
            more_info = ' - ' + more_info

        self.console.print(f'{status} {disp.proc.name}{more_info}')

        if disp.chunks:
            for chunk in disp.chunks:
                # Print line range header
                self.console.print(f'  --- lines {chunk.start_line}-{chunk.end_line} ---')
                # Print chunk content (may contain multiple lines)
                for line in chunk.content.split('\n'):
                    self.console.print(f'  {line}')
