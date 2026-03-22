# pylint: disable=unused-argument

import logging
import os
import queue
import sys
import tempfile
import time
from typing import Any
from unittest import TestCase

from parameterized import parameterized  # pylint: disable=import-error

import parproc as pp
from parproc.runner import MultiProcessRunner
from parproc.types import ProcState


class SimpleTest(TestCase):

    def marked_delay(self, seconds):
        """Delays for 'seconds' seconds, and returns a tuple containing start-time and end-time"""
        start = time.time()
        time.sleep(seconds)
        return (start, time.time())

    def assert_proc_start_order(self, proc_names):
        """Checks that the processes are in the given order based on (start, end) tuples"""
        val = ', '.join([f'{name}: {pp.results()[name][0]}' for name in proc_names])
        logging.debug(f'Checking start order: {val}')
        start = 0
        for name in proc_names:
            tt = pp.results()[name]
            self.assertGreater(tt[0], start)
            start = tt[0]

    def assert_proc_non_overlapping(self, proc_names):
        """Checks that the processes are not overlapping, based on their (start, end) tuples"""
        end = 0
        for name in proc_names:
            tt = pp.results()[name]
            self.assertGreater(tt[0], end)
            end = tt[1]

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        mode = os.environ.get('PARPROC_TEST_MODE', 'mp')
        pp.ProcManager.get_inst().set_options(mode=mode, dynamic=False)
        if mode == 'single':
            pp.ProcManager.get_inst().set_options(parallel=1)

    @parameterized.expand(
        [
            (1,),
            (10,),
        ]
    )
    def test_simple(self, parallel=None):
        if parallel is None:
            return

        pp.wait_clear()
        pp.ProcManager.get_inst().set_options(parallel=parallel)

        @pp.Proc(name='t1', deps=['t2'], now=True)
        def t1(context):
            print(context)
            return 'You' + context.results['t2']

        @pp.Proc(name='t2', now=True)
        def t2(context):
            return 'Me'

        pp.wait_for_all()

        self.assertEqual(pp.results()['t2'], 'Me')
        self.assertEqual(pp.results()['t1'], 'YouMe')

    @parameterized.expand(
        [
            (1,),
            (10,),
        ]
    )
    def test_locks(self, parallel=None):
        if parallel is None:
            return

        pp.wait_clear()
        pp.ProcManager.get_inst().set_options(parallel=parallel)

        @pp.Proc(name='t1', locks=['lock1'], now=True)
        def t1(context):
            return self.marked_delay(0.1)

        @pp.Proc(name='t2', locks=['lock1'], now=True)  # Same lock as t1. Will wait for t1
        def t2(context):
            return self.marked_delay(0.1)

        @pp.Proc(name='t3', now=True)
        def t3(context):
            return self.marked_delay(0.1)

        pp.wait_for_all()

        mode = os.environ.get('PARPROC_TEST_MODE', 'mp')
        if parallel > 1 and mode != 'single':
            self.assert_proc_start_order(['t1', 't2'])  # t1 or t3 could start first, but both before t2
            self.assert_proc_start_order(['t3', 't2'])
        else:
            # Sequential (parallel=1 or single mode: only one task runs at a time)
            self.assert_proc_start_order(['t1', 't2', 't3'])

        self.assert_proc_non_overlapping(['t1', 't2'])  # Share lock, so should not overlap

    def test_wanted(self):
        """
        Verifies that if a proc has not been set to run immediately, it is not run until needed by another process, or until
        a command has been sent to run it
        """

        pp.wait_clear()

        @pp.Proc(name='x1')
        def x1(context):
            return True

        @pp.Proc(name='x2', deps=['x1'])
        def x2(context):
            return True

        @pp.Proc(name='x3')
        def x3(context):
            return True

        @pp.Proc(name='x4', deps=['x3'], now=True)
        def x4(context):
            return True

        pp.wait_for_all()  # Should only execute x3 and x4

        self.assertEqual(pp.results(), {'x3': True, 'x4': True})

        pp.start('x2')  # Should kick off 'x1', then 'x2

        pp.wait_for_all()

        self.assertEqual(pp.results(), {'x1': True, 'x2': True, 'x3': True, 'x4': True})

    def test_input(self):

        # Mock stdin
        readline: Any = sys.stdin.readline
        sys.stdin.readline = lambda: 'the input'  # type: ignore

        pp.wait_clear()

        @pp.Proc(name='input0', now=True)
        def input0(context):
            return context.get_input()

        pp.wait_for_all()

        self.assertEqual(pp.results(), {'input0': 'the input'})

        # Restore stdin
        sys.stdin.readline = readline  # type: ignore

    def test_log(self):

        pp.wait_clear()

        @pp.Proc(name='logger', now=True)
        def input0(context):
            print('this is the log output')  # print to stdout
            print('this is an error', file=sys.stderr)  # print to stderr

        pp.wait_for_all()

        log_file = os.path.join(pp.ProcManager.get_inst().context['logdir'], 'logger.log')
        with open(log_file, encoding='utf-8') as f:
            content = f.read()
        # In single mode we don't capture stdout/stderr to the log file (logs go to console)
        if os.environ.get('PARPROC_TEST_MODE') != 'single':
            self.assertEqual(content, 'this is the log output\nthis is an error\n')

    def test_dependency_failure(self):
        """Verifies that dependent procs fail if a dependency fails"""

        pp.wait_clear()

        @pp.Proc(now=True)
        def p0(context):
            time.sleep(0.1)
            raise Exception('error')  # pylint: disable=broad-exception-raised

        @pp.Proc(now=True, deps=['p0'])
        def p1(context):
            return True

        @pp.Proc(now=True, deps=['p1'])
        def p2(context):
            return True

        with self.assertRaises(pp.ProcessError):
            pp.wait_for_all()

        # Only one proc (the first) should run
        self.assertEqual(pp.results(), {'p0': None})

    def test_collector(self):
        """Tests proc with no function, can still have deps, and act as a collector"""
        pp.wait_clear()

        # Two simple procs
        pp.Proc('f0', lambda c: 'ay')
        pp.Proc('f1', lambda c: 'ya')

        # Proc used simply as collector
        pp.Proc('f', lambda c: None, deps=['f0', 'f1'], now=True)

        pp.wait('f')
        self.assertEqual(pp.results(), {'f0': 'ay', 'f1': 'ya', 'f': None})

    def test_unpickleable_return_value(self):
        """In multiprocess mode, a task that returns a non-pickleable value is detected and reported."""
        if os.environ.get('PARPROC_TEST_MODE') == 'single':
            self.skipTest('only relevant for multiprocess mode (return value is sent over a queue)')

        pp.wait_clear()

        @pp.Proc(name='unpickleable', now=True)
        def unpickleable(context):
            # Lambdas are not pickleable
            return lambda x: x  # noqa: E731

        pp.wait_for_all(exception_on_failure=False)

        procs = pp.get_procs()
        self.assertIn('unpickleable', procs)
        p = procs['unpickleable']
        self.assertEqual(p.error, pp.Proc.ERROR_NOT_PICKLEABLE)
        self.assertIn('not pickleable', p.more_info)
        self.assertIsNone(pp.results()['unpickleable'])

    def test_pydantic_return_value(self):
        """A task that returns a pydantic BaseModel has it converted to dict and returned in results."""
        try:
            from pydantic import BaseModel  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest('pydantic not installed')

        pp.wait_clear()

        class ResultModel(BaseModel):
            """Simple pydantic model for test."""

            value: str
            count: int = 0

        @pp.Proc(name='pydantic_task', now=True)
        def pydantic_task(context):
            return ResultModel(value='ok', count=42)

        pp.wait_for_all()

        procs = pp.get_procs()
        self.assertIn('pydantic_task', procs)
        self.assertEqual(procs['pydantic_task'].error, pp.Proc.ERROR_NONE)
        results_dict = pp.results()
        self.assertIn('pydantic_task', results_dict)
        # Should be converted to dict (pickleable), not the raw model
        got = results_dict['pydantic_task']
        self.assertIsInstance(got, dict)
        self.assertEqual(got, {'value': 'ok', 'count': 42})

    def test_run_existing_proc(self):
        """run(existing_proc_name) starts an already-registered proc without requiring a matching proto."""
        pp.wait_clear()

        @pp.Proc(name='run_existing')
        def run_existing(context):
            return 'ran'

        # Proc is already in manager (from decorator). run() should just start it.
        pp.run('run_existing')
        pp.wait_for_all()

        self.assertEqual(pp.results(), {'run_existing': 'ran'})

    def test_start_and_run_already_wanted_or_running_noop(self):
        """start() and run() on an already-wanted or already-running proc are no-ops (single completion)."""
        pp.wait_clear()

        @pp.Proc(name='noop_target')
        def noop_target(context):
            return 1

        pp.start('noop_target')
        pp.start('noop_target')  # No-op: already WANTED or RUNNING
        pp.run('noop_target')  # No-op: create_proc returns name, start_proc sees non-IDLE
        pp.wait_for_all()

        # Proc ran once, not three times
        self.assertEqual(pp.results(), {'noop_target': 1})

    def test_collect_marks_failed_when_child_exits_without_completion_message(self):
        """If a child exits without proc-complete, collect() must fail it instead of hanging."""

        class _QueueEmpty:
            @staticmethod
            def get_nowait():
                raise queue.Empty

            @staticmethod
            def put(msg):
                del msg

        class _DeadProcess:
            exitcode = 17

            @staticmethod
            def is_alive():
                return False

        class _FakeProc:
            def __init__(self):
                self.name = 'orphaned-child'
                self.state = ProcState.RUNNING
                self.queue_to_master = _QueueEmpty()
                self.queue_to_proc = _QueueEmpty()
                self.process = _DeadProcess()
                self.timeout = None
                self.start_time = None
                self.error = pp.Proc.ERROR_NONE

            def is_running(self):
                return self.state == ProcState.RUNNING

        class _Logger:
            @staticmethod
            def debug(msg):
                del msg

            @staticmethod
            def info(msg):
                del msg

        class _FakeManager:
            def __init__(self, proc):
                self.logger = _Logger()
                self.procs = {proc.name: proc}
                self.completed_calls = []
                self.try_execute_any_calls = 0

            @staticmethod
            def handle_sync_request(msg):
                del msg
                raise AssertionError('No sync request expected in this test')

            @staticmethod
            def raise_user_error(msg):
                return pp.UserError(msg)

            @staticmethod
            def get_log_filename(name):
                return os.path.join(tempfile.gettempdir(), f'{name}.log')

            def complete_proc(self, *args, **kwargs):
                proc, output, error, log_filename = args
                more_info = kwargs.get('more_info')
                proc.state = ProcState.FAILED
                proc.error = error
                self.completed_calls.append((proc, output, error, log_filename, more_info))

            def try_execute_any(self):
                self.try_execute_any_calls += 1

        proc = _FakeProc()
        manager = _FakeManager(proc)
        runner = MultiProcessRunner()

        runner.collect(manager)

        self.assertEqual(len(manager.completed_calls), 1)
        _proc, output, error, _log_filename, more_info = manager.completed_calls[0]
        self.assertIsNone(output)
        self.assertEqual(error, pp.Proc.ERROR_EXCEPTION)
        self.assertIsNotNone(more_info)
        more_info = str(more_info)
        self.assertIn('exit code: 17', more_info)
        self.assertEqual(proc.state, ProcState.FAILED)
        self.assertEqual(proc.error, pp.Proc.ERROR_EXCEPTION)
        self.assertEqual(manager.try_execute_any_calls, 1)
