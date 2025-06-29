import getpass
import re
import sys
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from .state import ProcState


class Displayable:
    def __init__(self, proc: Any):
        self.proc = proc
        self.text: list[str] = []  # Additional lines of text to display for proc
        self.completed = False

    # Get number of lines to display for item
    def height(self) -> int:
        return 1 + len(self.text)


# Manages terminal session.
class Term:

    procStatus = {
        ProcState.IDLE: 'IDLE   ',
        ProcState.WANTED: 'WANT   ',
        ProcState.RUNNING: 'RUNNING',
        ProcState.SUCCEEDED: '  OK!  ',
        ProcState.FAILED: 'FAILED ',
    }
    procStateAnim = [
        '*      ',
        ' *     ',
        '  *    ',
        '   *   ',
        '    *  ',
        '     * ',
        '      *',
        '     * ',
        '    *  ',
        '   *   ',
        '  *    ',
        ' *     ',
    ]

    procStatusWidth = 7

    updatePeriod = 0.1  # Seconds between each update

    def __init__(self, dynamic: bool = True):
        # Keep track of active lines in terminal, i.e. lines we will go back and change
        self.active: OrderedDict[Any, Displayable] = OrderedDict()
        self.dynamic = dynamic  # True to dynamically update shell
        self.last_update: float = 0.0  # To limit update rate
        self.anim_state = 0

        # self.extraLines = 0 #Set to number of extra lines output to shell whenever printing something custom
        self.height = 0  # Number of lines currently active

    def start_proc(self, p: Any) -> None:
        disp = Displayable(p)  # FIX: Will give issues if multiple instances of same proc
        self.active[p] = disp

        if not self.dynamic:
            stars = '*' * Term.procStatusWidth
            print(f'[{stars}] {p.name} started')
        else:
            # Add line to active lines, and print it
            self._print_line(self._proc_lines(disp))

    def end_proc(self, p: Any) -> None:
        disp = self.active[p]
        disp.completed = True

        # On failure show N last lines of log
        if disp.proc.state == ProcState.FAILED and disp.proc.log_filename != '':
            with open(disp.proc.log_filename, encoding='utf-8') as f:
                disp.text = Term.extract_error_log(f.read())

        if not self.dynamic:
            lines = self._proc_lines(self.active[p])
            for l in lines:
                print(l)
            del self.active[p]
        else:
            # No need to make any changes, as 'update' will take care of it
            pass

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
    def completed_proc(self, p: Any) -> None:
        self.start_proc(p)
        self.end_proc(p)

    # force: force update
    def update(self, force: bool = False) -> None:
        if len(self.active) == 0:
            return

        # Only make updates every so often
        if self.dynamic and (force or time.time() - self.last_update > Term.updatePeriod):
            self.last_update = time.time()
            old_height = self.height
            del_height = 0
            self.height = 0

            # Move cursor up, to get ready for update
            self._move_cursor_vertical_offset(-old_height)

            # Find all completed processes. Draw these first, and delete them
            for proc, disp in [(proc, disp) for proc, disp in self.active.items() if disp.completed]:
                self._print_lines(self._proc_lines(disp))
                del_height += disp.height()
                del self.active[proc]

            # Redraw all active procs
            for proc, disp in self.active.items():
                self._print_lines(self._proc_lines(disp))

            # Draw over any extra lines
            while self.height < old_height:
                self._print_line('')

            # Deleted items are now static, and can be removed from active height
            self.height -= del_height

            # Progress animation
            self.anim_state += 1

        sys.stdout.flush()

    def get_input(self, message: str, password: bool) -> str:
        self.height += 3

        print(message)
        if password:
            return getpass.getpass()

        return sys.stdin.readline()

    # Returns status line for a process
    def _proc_lines(self, disp: Displayable) -> list[str]:
        # Main status line
        if disp.proc.state == ProcState.RUNNING and self.dynamic:
            status = Term.procStateAnim[self.anim_state % len(Term.procStateAnim)]
        else:
            status = Term.procStatus[disp.proc.state]

        more_info = disp.proc.more_info
        if disp.proc.state == ProcState.FAILED:
            if more_info == '':
                more_info = f'logfile: {disp.proc.log_filename}'

        if more_info != '':
            more_info = ' - ' + more_info

        lines = [f'[{status}] {disp.proc.name}{more_info}']

        if disp.text:
            lines += ['------------------------------', *disp.text, '------------------------------']

        return lines

    # Move cursor up or down with an offset
    def _move_cursor_vertical_offset(self, dy: int) -> None:
        if dy < 0:
            print(f'\033[{-dy}A', end='')  # Move cursor up
        else:
            print(f'\033[{dy}B', end='')  # Move cursor down

    # Print a line, covering up whatever was on that line before
    def _print_line(self, text: str) -> None:
        print(f'\033[0K\r{text}')
        self.height += 1

    def _print_lines(self, lines: list[str]) -> None:
        for l in lines:
            self._print_line(l)
