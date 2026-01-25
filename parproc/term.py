import getpass
import re
import sys
import time
from collections import OrderedDict
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

if TYPE_CHECKING:
    from .par import Proc


class Displayable:
    def __init__(self, proc: "Proc"):
        self.proc = proc
        self.text: list[str] = []  # Additional lines of text to display for proc
        self.completed = False
        self.start_time: float | None = None
        self.task_id: TaskID | None = None  # Rich Progress task ID
        self.execution_time: str = ""  # Execution time string for completed tasks

    # Get number of lines to display for item
    def height(self) -> int:
        return 1 + len(self.text)


# Manages terminal session.
class Term:

    updatePeriod = 0.1  # Seconds between each update

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
                # Create a task for this process (indeterminate progress)
                description = self._get_description(p)
                task_id = self.progress.add_task(description, total=None)  # None = indeterminate
                disp.task_id = task_id
                # Update the display
                if self.live is not None:
                    self.live.update(self._render_display())

    def end_proc(self, p: "Proc") -> None:
        if p not in self.active:
            return

        disp = self.active[p]
        disp.completed = True

        # On failure show N last lines of log
        if disp.proc.state == ProcState.FAILED and disp.proc.log_filename != '':
            with open(disp.proc.log_filename, encoding='utf-8') as f:
                disp.text = Term.extract_error_log(f.read())

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
    def extract_error_log(text: str) -> list[str]:
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
                # Match with parser
                return output(m).split('\n')

        return text.split('\n')[-16:]

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

    # force: force update
    def update(self, force: bool = False) -> None:
        # Only make updates every so often
        if self.dynamic and (force or time.time() - self.last_update > Term.updatePeriod):
            self.last_update = time.time()

            # Update active tasks in progress
            if self.progress is not None:
                for proc, disp in self.active.items():
                    if disp.task_id is not None and not disp.completed:
                        description = self._get_description(proc)
                        # Update description but keep progress at 0 (indeterminate)
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
            if disp.text:
                # Join log lines and use syntax highlighting
                log_content_str = "\n".join(disp.text)

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
                    panel_title = disp.proc.log_filename

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

        if disp.text:
            for line in disp.text:
                self.console.print(f'  {line}')
