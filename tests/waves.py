# pylint: disable=unused-argument
import logging
import multiprocessing
import time
from unittest import TestCase

import parproc as pp


class WaveTest(TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        pp.ProcManager.get_inst().set_options(dynamic=False)

    def test_wave_ordering(self):
        """Test that procs with lower waves run before higher waves, even when started together"""

        pp.wait_clear()

        # Create a multiprocessing queue to track execution order across processes
        execution_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Create procs:
        # A (wave=0)
        # B (wave=0), depends on A
        # C (wave=2), depends on B
        # D (wave=1)

        @pp.Proc(name='A', wave=0)
        def proc_a(context: pp.ProcContext) -> str:
            time.sleep(0.05)
            execution_queue.put('A')
            return 'A'

        @pp.Proc(name='B', wave=0, deps=['A'])
        def proc_b(context: pp.ProcContext) -> str:
            time.sleep(0.05)
            execution_queue.put('B')
            return 'B'

        @pp.Proc(name='C', wave=2, deps=['B'])
        def proc_c(context: pp.ProcContext) -> str:
            time.sleep(0.05)
            execution_queue.put('C')
            return 'C'

        @pp.Proc(name='D', wave=1)
        def proc_d(context: pp.ProcContext) -> str:
            time.sleep(0.05)
            execution_queue.put('D')
            return 'D'

        # Start C and D in the same call
        pp.start('C', 'D')

        # Wait for all to complete
        pp.wait_for_all()

        # Verify results
        results = pp.results()
        self.assertEqual(results['A'], 'A')
        self.assertEqual(results['B'], 'B')
        self.assertEqual(results['C'], 'C')
        self.assertEqual(results['D'], 'D')

        # Read execution order from queue
        execution_order = []
        while not execution_queue.empty():
            execution_order.append(execution_queue.get())

        # Verify execution order: A, B, D, C
        self.assertEqual(execution_order, ['A', 'B', 'D', 'C'])

    def test_wave_dependency_validation(self):
        """Test that start_proc fails when a dependency has a higher wave"""

        pp.wait_clear()

        # Create a proc with higher wave
        @pp.Proc(name='high_wave', wave=2)
        def high_wave(context: pp.ProcContext) -> str:
            return 'high'

        # Create a proc with lower wave that depends on high_wave (wave 2)
        # This should fail because dependencies cannot have higher wave (would cause deadlock)
        @pp.Proc(name='low_wave', wave=0, deps=['high_wave'])
        def low_wave(context: pp.ProcContext) -> str:
            return 'low'

        # Starting low_wave should raise an error because high_wave has higher wave
        with self.assertRaises(pp.UserError) as context:
            pp.start('low_wave')

        self.assertIn('cannot depend on proc "high_wave"', str(context.exception))
        self.assertIn('Dependencies must have equal or lower wave number to avoid deadlock', str(context.exception))

        pp.clear()
