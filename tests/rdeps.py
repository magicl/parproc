# pylint: disable=unused-argument
import logging
import multiprocessing as mp
import time
from typing import cast
from unittest import TestCase

import parproc as pp


class RdepTest(TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        pp.ProcManager.get_inst().set_options(dynamic=False)

    def test_rdep_basic_proc(self):
        """Test basic rdeps functionality with a proc having an rdep"""

        pp.wait_clear()

        @pp.Proc(name='setup', rdeps=['B::[a]::[b]'])
        def setup_proc(context: pp.ProcContext) -> str:
            return 'setup_done'

        @pp.Proto(name='B::[a]::[b]')
        def b_proc(context: pp.ProcContext, a: str, b: int) -> str:
            return f'B_{a}_{b}'

        # Create and start B proc - setup should be injected as dependency
        proc_name = pp.create('B::test::2')
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        # Verify setup ran first (it's a dependency of B)
        self.assertIn('setup', results)
        self.assertIn('B::test::2', results)
        self.assertEqual(results['setup'], 'setup_done')
        self.assertEqual(results['B::test::2'], 'B_test_2')

    def test_rdep_basic_proto(self):
        """Test basic rdeps functionality with a proto having an rdep"""

        pp.wait_clear()

        @pp.Proto(name='setup::[env]', rdeps=['B::[a]::[b]'])
        def setup_proto(context: pp.ProcContext, env: str) -> str:
            return f'setup_{env}'

        @pp.Proto(name='B::[a]::[b]')
        def b_proc(context: pp.ProcContext, a: str, b: int) -> str:
            return f'B_{a}_{b}'

        # Create setup proc first, then start B - setup should be injected
        setup_name = pp.create('setup::test')
        proc_name = pp.create('B::test::2')
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        # Verify setup ran first
        self.assertIn('setup::test', results)
        self.assertIn('B::test::2', results)
        self.assertEqual(results['setup::test'], 'setup_test')
        self.assertEqual(results['B::test::2'], 'B_test_2')

    def test_rdep_multiple_matches(self):
        """Test that multiple rdeps can match the same proc"""

        pp.wait_clear()

        @pp.Proc(name='setup1', rdeps=['B::[a]::[b]'])
        def setup1(context: pp.ProcContext) -> str:
            return 'setup1'

        @pp.Proc(name='setup2', rdeps=['B::[a]::[b]'])
        def setup2(context: pp.ProcContext) -> str:
            return 'setup2'

        @pp.Proto(name='B::[a]::[b]')
        def b_proc(context: pp.ProcContext, a: str, b: int) -> str:
            return f'B_{a}_{b}'

        proc_name = pp.create('B::test::2')
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        # Both setups should be injected
        self.assertIn('setup1', results)
        self.assertIn('setup2', results)
        self.assertIn('B::test::2', results)

    def test_rdep_multiple_patterns(self):
        """Test that a proc can have multiple rdep patterns"""

        pp.wait_clear()

        @pp.Proc(name='setup', rdeps=['B::[a]::[b]', 'C::[x]::[y]'])
        def setup_proc(context: pp.ProcContext) -> str:
            return 'setup_done'

        @pp.Proto(name='B::[a]::[b]')
        def b_proc(context: pp.ProcContext, a: str, b: int) -> str:
            return f'B_{a}_{b}'

        @pp.Proto(name='C::[x]::[y]')
        def c_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'C_{x}_{y}'

        # Start B - setup should be injected
        proc_name_b = pp.create('B::test::2')
        pp.start(proc_name_b)
        pp.wait(proc_name_b)

        results = pp.results()
        self.assertIn('setup', results)
        self.assertIn('B::test::2', results)

        # Start C - setup should be injected again
        pp.wait_clear()
        @pp.Proc(name='setup', rdeps=['B::[a]::[b]', 'C::[x]::[y]'])
        def setup_proc2(context: pp.ProcContext) -> str:
            return 'setup_done'

        @pp.Proto(name='B::[a]::[b]')
        def b_proc2(context: pp.ProcContext, a: str, b: int) -> str:
            return f'B_{a}_{b}'

        @pp.Proto(name='C::[x]::[y]')
        def c_proc2(context: pp.ProcContext, x: str, y: int) -> str:
            return f'C_{x}_{y}'

        proc_name_c = pp.create('C::foo::42')
        pp.start(proc_name_c)
        pp.wait(proc_name_c)

        results = pp.results()
        self.assertIn('setup', results)
        self.assertIn('C::foo::42', results)

    def test_rdep_execution_order(self):
        """Test that rdeps execute before the proc that matches them"""

        pp.wait_clear()

        # Use Manager().list() so subprocesses can append and main process can read
        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proc(name='setup', rdeps=['B::[a]::[b]'])
            def setup_proc(context: pp.ProcContext) -> str:
                execution_order.append('setup')
                time.sleep(0.05)  # Longer delay to ensure ordering
                return 'setup_done'

            @pp.Proto(name='B::[a]::[b]')
            def b_proc(context: pp.ProcContext, a: str, b: int) -> str:
                # Check that setup has already run
                if 'setup' not in execution_order:
                    execution_order.append('B_before_setup')  # This should not happen
                execution_order.append('B')
                return f'B_{a}_{b}'

            proc_name = pp.create('B::test::2')
            pp.start(proc_name)
            pp.wait(proc_name)

            # Copy to list for assertion (manager.list() is a proxy)
            order = list(execution_order)
            # Verify setup ran before B (setup is a dependency, so it must complete first)
            self.assertIn('setup', order)
            self.assertIn('B', order)
            self.assertNotIn('B_before_setup', order)
            # Verify setup comes before B in the order
            setup_idx = order.index('setup')
            b_idx = order.index('B')
            self.assertLess(setup_idx, b_idx, 'setup should execute before B')

    def test_rdep_with_regular_deps(self):
        """Test that rdeps work together with regular dependencies"""

        pp.wait_clear()

        @pp.Proc(name='setup', rdeps=['B::[a]::[b]'])
        def setup_proc(context: pp.ProcContext) -> str:
            return 'setup_done'

        @pp.Proto(name='A::[x]')
        def a_proc(context: pp.ProcContext, x: str) -> str:
            return f'A_{x}'

        @pp.Proto(name='B::[a]::[b]', deps=['A::[x]'], args={'x': 'test'})
        def b_proc(context: pp.ProcContext, a: str, b: int) -> str:
            a_result = context.results.get('A::test')
            return f'B_{a}_{b}_{a_result}'

        proc_name = pp.create('B::test::2')
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        # Both setup (rdep) and A::test (regular dep) should run before B
        self.assertIn('setup', results)
        self.assertIn('A::test', results)
        self.assertIn('B::test::2', results)
        self.assertEqual(results['B::test::2'], 'B_test_2_A_test')


class RdepPatternMatchingTest(TestCase):
    """Detailed unit tests for rdep pattern matching"""

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        pp.ProcManager.get_inst().set_options(dynamic=False)
        self.manager = pp.ProcManager.get_inst()

    def test_match_exact_pattern(self):
        """Test matching exact pattern (no placeholders)"""
        self.assertTrue(self.manager._match_rdep_pattern('B::1::2', 'B::1::2'))
        self.assertFalse(self.manager._match_rdep_pattern('B::1::2', 'B::1::3'))
        self.assertFalse(self.manager._match_rdep_pattern('B::1::2', 'B::2::2'))

    def test_match_single_placeholder(self):
        """Test matching pattern with single placeholder"""
        # Pattern: B::[a]
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]', 'B::test'))
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]', 'B::123'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]', 'C::test'))
        # Note: B::[a] with last param uses .* which matches everything, so B::test::extra would match
        # This is expected behavior - the last placeholder matches to the end
        # If you want to restrict it, use a pattern like B::[a]::[b] instead

    def test_match_two_placeholders(self):
        """Test matching pattern with two placeholders"""
        # Pattern: B::[a]::[b]
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::[b]', 'B::test::2'))
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::[b]', 'B::foo::42'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::[b]', 'B::test'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::[b]', 'C::test::2'))

    def test_match_literal_first(self):
        """Test matching pattern with literal value first"""
        # Pattern: B::1::[b]
        self.assertTrue(self.manager._match_rdep_pattern('B::1::[b]', 'B::1::2'))
        self.assertTrue(self.manager._match_rdep_pattern('B::1::[b]', 'B::1::42'))
        self.assertFalse(self.manager._match_rdep_pattern('B::1::[b]', 'B::2::2'))
        self.assertFalse(self.manager._match_rdep_pattern('B::1::[b]', 'B::something::2'))
        self.assertFalse(self.manager._match_rdep_pattern('B::1::[b]', 'B::1'))

    def test_match_literal_last(self):
        """Test matching pattern with literal value last"""
        # Pattern: B::[a]::2
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::2', 'B::test::2'))
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::2', 'B::foo::2'))
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::2', 'B::1::2'))  # Should match: [a] matches "1"
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::2', 'B::test::3'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::2', 'B::test'))

    def test_match_literal_middle(self):
        """Test matching pattern with literal value in the middle"""
        # Pattern: B::[a]::1::[b]
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::1::[b]', 'B::test::1::2'))
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::1::[b]', 'B::foo::1::42'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::1::[b]', 'B::test::2::2'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::1::[b]', 'B::test::1'))

    def test_match_multiple_literals(self):
        """Test matching pattern with multiple literal values"""
        # Pattern: B::1::2::[c]
        self.assertTrue(self.manager._match_rdep_pattern('B::1::2::[c]', 'B::1::2::test'))
        self.assertTrue(self.manager._match_rdep_pattern('B::1::2::[c]', 'B::1::2::foo'))
        self.assertFalse(self.manager._match_rdep_pattern('B::1::2::[c]', 'B::1::3::test'))
        self.assertFalse(self.manager._match_rdep_pattern('B::1::2::[c]', 'B::2::2::test'))
        self.assertFalse(self.manager._match_rdep_pattern('B::1::2::[c]', 'B::1::2'))

    def test_match_complex_patterns(self):
        """Test matching complex patterns with multiple placeholders and literals"""
        # Pattern: A::[x]::B::[y]::C
        self.assertTrue(self.manager._match_rdep_pattern('A::[x]::B::[y]::C', 'A::test::B::foo::C'))
        self.assertFalse(self.manager._match_rdep_pattern('A::[x]::B::[y]::C', 'A::test::B::foo::D'))
        self.assertFalse(self.manager._match_rdep_pattern('A::[x]::B::[y]::C', 'A::test::C::foo::B'))

    def test_match_double_colon_separator_params_may_contain_hyphen(self):
        """Test that "::" separator allows param values to contain "-" """
        # Pattern: B::[a]::[b] - param a can be "my-cluster", param b can be "us-east-1"
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::[b]', 'B::my-cluster::2'))
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::[b]', 'B::my-cluster::us-east-1'))
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::[b]', 'B::something::2'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::[b]', 'B::my-cluster'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::[b]', 'B-my-cluster-2'))

    def test_match_empty_placeholder(self):
        """Test that placeholders can match empty strings (edge case)"""
        # With "::" separator, param must have at least one char so B::::2 does not match B::[a]::[b]
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::[b]', 'B::::2'))

    def test_match_special_characters(self):
        """Test matching patterns with special characters in literals"""
        # Pattern: B::[a]::test::2 (param can contain hyphens when using ::)
        self.assertTrue(self.manager._match_rdep_pattern('B::[a]::test::2', 'B::foo::test::2'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::test::2', 'B::foo::test::3'))
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::test::2', 'B::foo::TEST::2'))

    def test_match_long_names(self):
        """Test matching with longer proc names"""
        # Pattern: task::[id]::[status]
        self.assertTrue(self.manager._match_rdep_pattern('task::[id]::[status]', 'task::12345::done'))
        self.assertTrue(self.manager._match_rdep_pattern('task::[id]::[status]', 'task::abc123::pending'))
        self.assertFalse(self.manager._match_rdep_pattern('task::[id]::[status]', 'task::12345'))

    def test_match_edge_cases(self):
        """Test various edge cases"""
        # Single character
        self.assertTrue(self.manager._match_rdep_pattern('A::[x]', 'A::x'))
        # A::[x] with last param uses .* which matches empty string too, so A:: would match
        # This is expected behavior for the last placeholder
        self.assertTrue(self.manager._match_rdep_pattern('A::[x]', 'A::'))

        # Very long placeholder match
        long_name = 'B::' + 'x' * 100 + '::2'
        long_pattern = 'B::[a]::2'
        self.assertTrue(self.manager._match_rdep_pattern(long_pattern, long_name))

        # Pattern longer than name
        self.assertFalse(self.manager._match_rdep_pattern('B::[a]::[b]::[c]', 'B::test::2'))

    def test_match_real_world_scenarios(self):
        """Test realistic scenarios from actual usage"""
        # Scenario 1: Build system - setup before any build
        self.assertTrue(self.manager._match_rdep_pattern('build::[project]::[version]', 'build::myapp::1.0.0'))
        self.assertTrue(self.manager._match_rdep_pattern('build::[project]::[version]', 'build::lib::2.3.4'))

        # Scenario 2: Test system - setup before specific test
        self.assertTrue(self.manager._match_rdep_pattern('test::[suite]::[case]', 'test::unit::math'))
        self.assertTrue(self.manager._match_rdep_pattern('test::[suite]::[case]', 'test::integration::api'))

        # Scenario 3: Deployment - pre-deploy before deploy
        self.assertTrue(self.manager._match_rdep_pattern('deploy::[env]::[version]', 'deploy::prod::1.0.0'))
        self.assertFalse(self.manager._match_rdep_pattern('deploy::[env]::[version]', 'deploy::prod'))

    def test_integration_pattern_matching(self):
        """Integration test: verify pattern matching works in actual rdep resolution"""

        pp.wait_clear()

        @pp.Proc(name='setup1', rdeps=['B::[a]::[b]'])
        def setup1(context: pp.ProcContext) -> str:
            return 'setup1'

        @pp.Proc(name='setup2', rdeps=['B::1::[b]'])
        def setup2(context: pp.ProcContext) -> str:
            return 'setup2'

        @pp.Proc(name='setup3', rdeps=['B::[a]::2'])
        def setup3(context: pp.ProcContext) -> str:
            return 'setup3'

        @pp.Proc(name='setup4', rdeps=['B::1::2'])
        def setup4(context: pp.ProcContext) -> str:
            return 'setup4'

        @pp.Proto(name='B::[a]::[b]')
        def b_proc(context: pp.ProcContext, a: str, b: int) -> str:
            return f'B_{a}_{b}'

        # Test 1: B::something::2 should match B::[a]::[b] and B::[a]::2
        proc_name = pp.create('B::something::2')
        pp.start(proc_name)
        pp.wait(proc_name)
        results = pp.results()
        self.assertIn('setup1', results)  # B::[a]::[b] matches
        self.assertNotIn('setup2', results)  # B::1::[b] doesn't match (1 != something)
        self.assertIn('setup3', results)  # B::[a]::2 matches (2 == 2)
        self.assertNotIn('setup4', results)  # B::1::2 doesn't match (1 != something)

        # Test 2: B::1::2 should match all patterns
        pp.wait_clear()
        @pp.Proc(name='setup1', rdeps=['B::[a]::[b]'])
        def setup1_v2(context: pp.ProcContext) -> str:
            return 'setup1'

        @pp.Proc(name='setup2', rdeps=['B::1::[b]'])
        def setup2_v2(context: pp.ProcContext) -> str:
            return 'setup2'

        @pp.Proc(name='setup3', rdeps=['B::[a]::2'])
        def setup3_v2(context: pp.ProcContext) -> str:
            return 'setup3'

        @pp.Proc(name='setup4', rdeps=['B::1::2'])
        def setup4_v2(context: pp.ProcContext) -> str:
            return 'setup4'

        @pp.Proto(name='B::[a]::[b]')
        def b_proc_v2(context: pp.ProcContext, a: str, b: int) -> str:
            return f'B_{a}_{b}'

        proc_name = pp.create('B::1::2')
        pp.start(proc_name)
        pp.wait(proc_name)
        results = pp.results()
        self.assertIn('setup1', results)  # B::[a]::[b] matches
        self.assertIn('setup2', results)  # B::1::[b] matches
        self.assertIn('setup3', results)  # B::[a]::2 matches
        self.assertIn('setup4', results)  # B::1::2 matches

        # Test 3: B::1::3 should match B::[a]::[b] and B::1::[b], but not B::[a]::2 or B::1::2
        pp.wait_clear()
        @pp.Proc(name='setup1', rdeps=['B::[a]::[b]'])
        def setup1_v3(context: pp.ProcContext) -> str:
            return 'setup1'

        @pp.Proc(name='setup2', rdeps=['B::1::[b]'])
        def setup2_v3(context: pp.ProcContext) -> str:
            return 'setup2'

        @pp.Proc(name='setup3', rdeps=['B::[a]::2'])
        def setup3_v3(context: pp.ProcContext) -> str:
            return 'setup3'

        @pp.Proc(name='setup4', rdeps=['B::1::2'])
        def setup4_v3(context: pp.ProcContext) -> str:
            return 'setup4'

        @pp.Proto(name='B::[a]::[b]')
        def b_proc_v3(context: pp.ProcContext, a: str, b: int) -> str:
            return f'B_{a}_{b}'

        proc_name = pp.create('B::1::3')
        pp.start(proc_name)
        pp.wait(proc_name)
        results = pp.results()
        self.assertIn('setup1', results)  # B::[a]::[b] matches
        self.assertIn('setup2', results)  # B::1::[b] matches
        self.assertNotIn('setup3', results)  # B::[a]::2 doesn't match (2 != 3)
        self.assertNotIn('setup4', results)  # B::1::2 doesn't match (2 != 3)
