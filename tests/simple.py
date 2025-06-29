# pylint: disable=unused-argument

import logging
import os
import sys
import time
from typing import Any
from unittest import TestCase

from parameterized import parameterized  # pylint: disable=import-error

import parproc as pp


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
        pp.ProcManager.get_inst().set_options(dynamic=False)

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

        if parallel > 1:
            self.assert_proc_start_order(['t1', 't2'])  # t1 or t3 could start first, but both before t2
            self.assert_proc_start_order(['t3', 't2'])
        else:
            self.assert_proc_start_order(['t1', 't2', 't3'])  # In non-parallel mode, these go in sequence

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
            self.assertEqual(f.read(), 'this is the log output\nthis is an error\n')

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
