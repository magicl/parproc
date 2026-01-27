# pylint: disable=unused-argument
import logging
import time
from typing import cast
from unittest import TestCase

import parproc as pp


class ProtoTest(TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        pp.ProcManager.get_inst().set_options(dynamic=False)

    def test_proto_from_base(self):
        """Proto creation from base"""

        pp.wait_clear()

        @pp.Proto(name='test-[p0]')
        def proto(context: pp.ProcContext, p0: str) -> str:
            return p0 + 'ba'

        # Create proc and run by handle
        pp.create('test-[p0]', 'test:1', p0='ha')
        pp.create('test-[p0]', 'test:2', p0='la')
        pp.start('test:1', 'test:2')

        pp.wait('test:1', 'test:2')
        self.assertEqual(pp.results(), {'test:1': 'haba', 'test:2': 'laba'})

    def test_proto_from_proc(self):
        """Creation of proto from inside a proc"""

        pp.wait_clear()

        @pp.Proto(name='proto-[x]-[y]')
        def proc1(context: pp.ProcContext, x: int, y: int) -> int:
            logging.info(f'PROTO FUNC: {x}, {y}')
            return x + cast(int, context.args['y'])

        @pp.Proto(name='base-[a]-[b]')
        def proc0(context: pp.ProcContext, a: int, b: int) -> int:
            # Create multiple procs from within proc
            context.create('proto-[x]-[y]', 'proto:1', x=1, y=a)
            context.create('proto-[x]-[y]', 'proto:2', x=2, y=b)

            context.start('proto:1', 'proto:2')
            context.wait('proto:1', 'proto:2')  # Automatically feeds results into context.results

            return cast(int, context.results['proto:1']) + cast(int, context.results['proto:2'])

        pp.create('base-[a]-[b]', 'base', a=1, b=2)
        pp.start('base')
        pp.wait('base')

        self.assertEqual(pp.results(), {'base': 6, 'proto:1': 2, 'proto:2': 4})

    def test_shorthands(self):
        """Tests shorthands for proto creation, starting, stopping, etc"""

        pp.wait_clear()

        # Proto short-hand
        pp.Proto('f0-[x]-[y]', lambda c, x, y: x * y, now=True)

        # Proc short-hand
        pp.Proc('f1', lambda c: 10, now=True)

        pp.wait_for_all()

        # f1 should have run, but not f0, as it has not yet been instantiated
        self.assertEqual(pp.results(), {'f1': 10})

        # Kick off a couple f0s. Starts immediately on creation due to 'now' setting
        pp.wait(*[pp.create('f0-[x]-[y]', x=1, y=2), pp.create('f0-[x]-[y]', x=3, y=4)])

        self.assertEqual(pp.results(), {'f1': 10, 'f0-1-2': 2, 'f0-3-4': 12})

        # Test kickoff from inside proc
        @pp.Proc(now=True)
        def f2(context):
            context.wait(*[context.create('f0-[x]-[y]', x=10, y=20), context.create('f0-[x]-[y]', x=30, y=40)])
            return context.results['f0-10-20'] + context.results['f0-30-40']

        pp.wait('f2')

        self.assertEqual(
            pp.results(), {'f0-1-2': 2, 'f0-10-20': 200, 'f0-3-4': 12, 'f0-30-40': 1200, 'f1': 10, 'f2': 1400}
        )

    def test_timeouts(self):
        """Check timeouts within procs and within procs within procs"""

        pp.Proto('sleepy', lambda c: time.sleep(10), timeout=1, now=True)

        with self.assertRaises(pp.ProcessError):
            pp.wait(pp.create('sleepy'))

        # Test timeout within proc
        @pp.Proc(now=True)
        def got_exception(context):
            try:
                context.wait(context.create('sleepy'))
            except pp.ProcessError:
                return True  # Expect the exception to happen

            return False

        pp.wait('got_exception')

        self.assertEqual(pp.results()['got_exception'], True)

    def test_proto_at_dependency(self):
        """Test that a proto with @ dependency automatically creates and runs the dependency first"""

        pp.wait_clear()

        # Define a proto that will be used as a dependency
        @pp.Proto(name='dep_proto-[value]')
        def dep_proto(context, value):
            time.sleep(0.1)  # Small delay to ensure ordering
            return f'dep_{value}'

        # Define a proto that depends on dep_proto using @ prefix
        @pp.Proto(name='main_proto-[value]', deps=['@dep_proto-[value]'])
        def main_proto(context, value):
            # This should only run after dep_proto completes. Value should be available
            # both under full name and template name
            dep_result = context.results.get('dep_proto-test')
            return f'main_{value}_{dep_result}'

        # Create a proc from main_proto - this should automatically create dep_proto:0
        proc_name = pp.create('main_proto-[value]', value='test')
        pp.start(proc_name)
        pp.wait(proc_name)

        # Verify both procs ran
        results = pp.results()
        self.assertIn('dep_proto-test', results)
        self.assertIn('main_proto-test', results)

        # Verify the dependency ran first
        self.assertEqual(results['dep_proto-test'], 'dep_test')
        self.assertEqual(results['main_proto-test'], 'main_test_dep_test')

        # Verify dependency was created automatically
        self.assertIn('dep_proto-test', pp.ProcManager.get_inst().procs)

    def test_proto_lambda_dependencies(self):
        """Test that proto deps can be lambdas that are called with context and proc params"""

        pp.wait_clear()

        # Define some base procs that will be used as dependencies
        @pp.Proto(name='base-[x]')
        def base_proc(context: pp.ProcContext, x: str) -> str:
            return f'base_{x}'

        @pp.Proto(name='helper-[y]')
        def helper_proc(context: pp.ProcContext, y: str) -> str:
            return f'helper_{y}'

        # Test 1: Lambda that returns a single tuple dependency with [value] substitution
        @pp.Proto(
            name='single_lambda-[value]',
            deps=[lambda manager, value: ('@base-[x]', {'x': '[value]'})]  # Lambda returns tuple with [value] substitution
        )
        def single_lambda_proc(context: pp.ProcContext, value: str) -> str:
            base_result = context.results.get(f'base-{value}')
            return f'single_{value}_{base_result}'

        # Test 2: Lambda that returns a list of tuple dependencies with [value] substitution
        @pp.Proto(
            name='list_lambda-[value]',
            deps=[lambda manager, value: [
                ('@base-[x]', {'x': '[value]'}),
                ('@helper-[y]', {'y': '[value]'})
            ]]
        )
        def list_lambda_proc(context: pp.ProcContext, value: str) -> str:
            base_result = context.results.get(f'base-{value}')
            helper_result = context.results.get(f'helper-{value}')
            return f'list_{value}_{base_result}_{helper_result}'

        # Test 3: Mix of string and lambda dependencies
        @pp.Proto(
            name='mixed_deps-[value]',
            deps=[
                'base-fixed',  # String dependency
                lambda manager, value: ('@helper-[y]', {'y': '[value]'}),  # Lambda dependency with [value] substitution
            ]
        )
        def mixed_deps_proc(context: pp.ProcContext, value: str) -> str:
            base_result = context.results.get('base-fixed')
            helper_result = context.results.get(f'helper-{value}')
            return f'mixed_{value}_{base_result}_{helper_result}'

        # Test 4: Lambda that uses manager context and returns tuple with [value] substitution
        @pp.Proto(
            name='manager_context-[value]',
            deps=[
                lambda manager, value: (
                    ('@base-[x]', {'x': '[value]'}) if 'base-[x]' in manager.protos else ('@base-[x]', {'x': 'fallback'})
                )
            ]
        )
        def manager_context_proc(context: pp.ProcContext, value: str) -> str:
            base_result = context.results.get(f'base-{value}')
            return f'manager_{value}_{base_result}'

        # Create and run procs
        pp.create('base-[x]', 'base-fixed', x='fixed')
        pp.create('base-[x]', 'base-test', x='test')
        pp.create('helper-[y]', 'helper-test', y='test')

        single_name = pp.create('single_lambda-[value]', value='test')
        list_name = pp.create('list_lambda-[value]', value='test')
        mixed_name = pp.create('mixed_deps-[value]', value='test')
        manager_name = pp.create('manager_context-[value]', value='test')

        pp.start('base-fixed', 'base-test', 'helper-test', single_name, list_name, mixed_name, manager_name)
        pp.wait('base-fixed', 'base-test', 'helper-test', single_name, list_name, mixed_name, manager_name)

        results = pp.results()

        # Verify single lambda dependency
        self.assertEqual(results['single_lambda-test'], 'single_test_base_test')

        # Verify list lambda dependencies
        self.assertEqual(results['list_lambda-test'], 'list_test_base_test_helper_test')

        # Verify mixed dependencies
        self.assertEqual(results['mixed_deps-test'], 'mixed_test_base_fixed_helper_test')

        # Verify manager context lambda
        self.assertEqual(results['manager_context-test'], 'manager_test_base_test')

    def test_proto_lambda_with_value_substitution(self):
        """Integration test: lambda dependencies with [value] substitution in override dict"""

        pp.wait_clear()

        @pp.Proto(name='dep-[x]-[y]')
        def dep_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'dep_{x}_{y}'

        @pp.Proto(
            name='main-[value]',
            deps=[
                lambda manager, value: ('@dep-[x]-[y]', {'x': '[value]', 'y': 123})
            ]
        )
        def main_proc(context: pp.ProcContext, value: str) -> str:
            dep_result = context.results.get('dep-test-123')
            return f'main_{value}_{dep_result}'

        proc_name = pp.create('main-[value]', value='test')
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        # Verify [value] was substituted with 'test'
        self.assertEqual(results['dep-test-123'], 'dep_test_123')
        self.assertEqual(results['main-test'], 'main_test_dep_test_123')

    def test_process_pattern_and_args_filters_correctly(self):
        """Test that _process_pattern_and_args only includes matching parameters"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        # Pattern requires x and y, but we provide x, y, a, b, c
        all_args = {'x': 'test', 'y': 42, 'a': 'A', 'b': 'B', 'c': 100}
        filtered = manager._process_pattern_and_args('dep-[x]-[y]', all_args)

        # Should only have x and y, not a, b, c
        self.assertEqual(filtered, {'x': 'test', 'y': 42})
        self.assertNotIn('a', filtered)
        self.assertNotIn('b', filtered)
        self.assertNotIn('c', filtered)

    def test_proto_dependency_field_filtering(self):
        """Integration test: dependencies with [field] patterns only receive matching args"""

        pp.wait_clear()

        @pp.Proto(name='dep-[x]-[y]')
        def dep_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'dep_{x}_{y}'

        @pp.Proto(
            name='main-[a]-[b]-[c]',
            deps=['@dep-[x]-[y]']  # Should only receive x and y from args, not a, b, c
        )
        def main_proc(context: pp.ProcContext, a: str, b: str, c: int) -> str:
            dep_result = context.results.get('dep-test-42')
            return f'main_{a}_{b}_{c}_{dep_result}'

        # Create main proc with all args - dep should only get x and y
        proc_name = pp.create('main-[a]-[b]-[c]', a='A', b='B', c=100, x='test', y=42)
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        # Verify dep only got x and y (not a, b, c)
        self.assertEqual(results['dep-test-42'], 'dep_test_42')
        self.assertEqual(results['main-A-B-100'], 'main_A_B_100_dep_test_42')

    def test_process_pattern_and_args_with_override(self):
        """Test that _process_pattern_and_args applies override_dict correctly"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        # Pattern requires x and y
        all_args = {'x': 'original', 'y': 0, 'value': 'test'}
        override_dict = {'x': 'override', 'y': 999}
        filtered = manager._process_pattern_and_args('dep-[x]-[y]', all_args, override_dict)

        # Should have overridden values, not original values
        self.assertEqual(filtered, {'x': 'override', 'y': 999})
        self.assertNotIn('value', filtered)

    def test_proto_dependency_tuple_override(self):
        """Integration test: tuple dependencies (dep_name, override_dict) override filtered args"""

        pp.wait_clear()

        @pp.Proto(name='dep-[x]-[y]')
        def dep_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'dep_{x}_{y}'

        @pp.Proto(
            name='main-[value]',
            deps=[
                ('@dep-[x]-[y]', {'x': 'override', 'y': 999})  # Override x and y
            ]
        )
        def main_proc(context: pp.ProcContext, value: str) -> str:
            dep_result = context.results.get('dep-override-999')
            return f'main_{value}_{dep_result}'

        # Create main proc - dep should get overridden values, not value
        proc_name = pp.create('main-[value]', value='test', x='ignored', y=0)
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        # Verify dep got overridden values
        self.assertEqual(results['dep-override-999'], 'dep_override_999')
        self.assertEqual(results['main-test'], 'main_test_dep_override_999')

    def test_proto_lambda_returns_tuple(self):
        """Test that lambdas can return tuple dependencies"""

        pp.wait_clear()

        @pp.Proto(name='dep-[x]-[y]')
        def dep_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'dep_{x}_{y}'

        @pp.Proto(
            name='main-[value]',
            deps=[
                lambda manager, value: ('@dep-[x]-[y]', {'x': f'lambda_{value}', 'y': 123})
            ]
        )
        def main_proc(context: pp.ProcContext, value: str) -> str:
            dep_result = context.results.get('dep-lambda_test-123')
            return f'main_{value}_{dep_result}'

        proc_name = pp.create('main-[value]', value='test')
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        self.assertEqual(results['dep-lambda_test-123'], 'dep_lambda_test_123')
        self.assertEqual(results['main-test'], 'main_test_dep_lambda_test_123')

    def test_proto_lambda_returns_list_with_tuples(self):
        """Test that lambdas can return lists containing tuples"""

        pp.wait_clear()

        @pp.Proto(name='dep1-[x]')
        def dep1_proc(context: pp.ProcContext, x: str) -> str:
            return f'dep1_{x}'

        @pp.Proto(name='dep2-[y]')
        def dep2_proc(context: pp.ProcContext, y: int) -> str:
            return f'dep2_{y}'

        @pp.Proto(
            name='main-[value]',
            deps=[
                lambda manager, value: [
                    ('@dep1-[x]', {'x': f'from_lambda_{value}'}),
                    ('@dep2-[y]', {'y': 456}),
                ]
            ]
        )
        def main_proc(context: pp.ProcContext, value: str) -> str:
            dep1_result = context.results.get('dep1-from_lambda_test')
            dep2_result = context.results.get('dep2-456')
            return f'main_{value}_{dep1_result}_{dep2_result}'

        proc_name = pp.create('main-[value]', value='test')
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        self.assertEqual(results['dep1-from_lambda_test'], 'dep1_from_lambda_test')
        self.assertEqual(results['dep2-456'], 'dep2_456')
        self.assertEqual(results['main-test'], 'main_test_dep1_from_lambda_test_dep2_456')

    def test_proto_mixed_dependency_types(self):
        """Test mixing string, tuple, and lambda dependencies"""

        pp.wait_clear()

        @pp.Proto(name='str_dep-[x]')
        def str_dep(context: pp.ProcContext, x: str) -> str:
            return f'str_{x}'

        @pp.Proto(name='tuple_dep-[y]')
        def tuple_dep(context: pp.ProcContext, y: int) -> str:
            return f'tuple_{y}'

        @pp.Proto(name='lambda_dep-[z]')
        def lambda_dep(context: pp.ProcContext, z: str) -> str:
            return f'lambda_{z}'

        @pp.Proto(
            name='main-[value]',
            deps=[
                '@str_dep-[x]',  # String dependency
                ('@tuple_dep-[y]', {'y': 789}),  # Tuple dependency
                lambda manager, value: ('@lambda_dep-[z]', {'z': f'lambda_{value}'}),  # Lambda returning tuple
            ]
        )
        def main_proc(context: pp.ProcContext, value: str) -> str:
            str_result = context.results.get('str_dep-test')
            tuple_result = context.results.get('tuple_dep-789')
            lambda_result = context.results.get('lambda_dep-lambda_test')
            return f'main_{value}_{str_result}_{tuple_result}_{lambda_result}'

        proc_name = pp.create('main-[value]', value='test', x='test', y=0, z='ignored')
        pp.start(proc_name)
        pp.wait(proc_name)

        results = pp.results()
        self.assertEqual(results['str_dep-test'], 'str_test')
        self.assertEqual(results['tuple_dep-789'], 'tuple_789')
        self.assertEqual(results['lambda_dep-lambda_test'], 'lambda_lambda_test')
        self.assertEqual(results['main-test'], 'main_test_str_test_tuple_789_lambda_lambda_test')

    def test_process_pattern_and_args_missing_field_error(self):
        """Test that _process_pattern_and_args raises error for missing required fields"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        # Pattern requires x and y, but only x is provided
        all_args = {'x': 'test', 'value': 'test'}

        with self.assertRaises(pp.UserError) as cm:
            manager._process_pattern_and_args('dep-[x]-[y]', all_args)
        self.assertIn('requires argument "y"', str(cm.exception))

    def test_process_pattern_and_args_missing_field_with_override(self):
        """Test that override_dict can provide missing required fields"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        # Pattern requires x and y, but only x is in args
        all_args = {'x': 'test'}
        override_dict = {'y': 42}  # Provide y via override

        filtered = manager._process_pattern_and_args('dep-[x]-[y]', all_args, override_dict)
        self.assertEqual(filtered, {'x': 'test', 'y': 42})

    def test_generate_name_from_pattern(self):
        """Test that _generate_name_from_pattern correctly replaces [field] placeholders"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        pattern = 'proto-[x]-[y]'
        args = {'x': 'test', 'y': 42}
        name = manager._generate_name_from_pattern(pattern, args)

        self.assertEqual(name, 'proto-test-42')

    def test_generate_name_from_pattern_missing_arg(self):
        """Test that _generate_name_from_pattern raises error for missing args"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        pattern = 'proto-[x]-[y]'
        args = {'x': 'test'}  # Missing y

        with self.assertRaises(pp.UserError) as cm:
            manager._generate_name_from_pattern(pattern, args)
        self.assertIn('requires argument "y"', str(cm.exception))

    def test_resolve_dependency_string(self):
        """Test _resolve_dependency with string dependency"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        all_args = {'x': 'test', 'y': 42, 'value': 'ignored'}
        dep_pattern, filtered_args, is_proto_ref = manager._resolve_dependency('dep-[x]-[y]', all_args)

        self.assertEqual(dep_pattern, 'dep-[x]-[y]')
        self.assertEqual(filtered_args, {'x': 'test', 'y': 42})
        self.assertFalse(is_proto_ref)

    def test_resolve_dependency_tuple(self):
        """Test _resolve_dependency with tuple dependency"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        all_args = {'x': 'original', 'y': 0}
        override_dict = {'x': 'override', 'y': 999}
        dep_pattern, filtered_args, is_proto_ref = manager._resolve_dependency(
            ('dep-[x]-[y]', override_dict), all_args
        )

        self.assertEqual(dep_pattern, 'dep-[x]-[y]')
        self.assertEqual(filtered_args, {'x': 'override', 'y': 999})
        self.assertFalse(is_proto_ref)

    def test_resolve_dependency_proto_ref(self):
        """Test _resolve_dependency with @ proto reference"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        all_args = {'x': 'test', 'y': 42}
        dep_pattern, filtered_args, is_proto_ref = manager._resolve_dependency('@dep-[x]-[y]', all_args)

        self.assertEqual(dep_pattern, '@dep-[x]-[y]')
        self.assertEqual(filtered_args, {'x': 'test', 'y': 42})
        self.assertTrue(is_proto_ref)

    def test_substitute_field_references_in_override(self):
        """Test that [field] references in override dict values are substituted"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        all_args = {'value': 'test_value', 'x': 'ignored'}
        override_dict = {'x': '[value]'}  # Should substitute [value] with 'test_value'

        filtered = manager._process_pattern_and_args('dep-[x]', all_args, override_dict)
        self.assertEqual(filtered, {'x': 'test_value'})

    def test_substitute_field_references_multiple(self):
        """Test that multiple [field] references in override dict values are substituted"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        all_args = {'prefix': 'pre', 'suffix': 'suf', 'x': 'ignored'}
        override_dict = {'x': '[prefix]-middle-[suffix]'}  # Should substitute both

        filtered = manager._process_pattern_and_args('dep-[x]', all_args, override_dict)
        self.assertEqual(filtered, {'x': 'pre-middle-suf'})

    def test_substitute_field_references_missing_field(self):
        """Test that missing field in [field] reference raises error"""
        manager = pp.ProcManager.get_inst()
        manager.clear()

        all_args = {'x': 'test'}
        override_dict = {'x': '[missing]'}  # Missing field

        with self.assertRaises(pp.UserError) as cm:
            manager._process_pattern_and_args('dep-[x]', all_args, override_dict)
        self.assertIn('Field reference "[missing]"', str(cm.exception))

    def test_proto_dependency_missing_field_error(self):
        """Integration test: missing required fields in dependencies raise errors"""

        pp.wait_clear()

        @pp.Proto(name='dep-[x]-[y]')
        def dep_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'dep_{x}_{y}'

        @pp.Proto(
            name='main-[value]',
            deps=['@dep-[x]-[y]']  # Requires x and y
        )
        def main_proc(context: pp.ProcContext, value: str) -> str:
            return 'main'

        # Should raise error because y is missing
        with self.assertRaises(pp.UserError) as cm:
            pp.create('main-[value]', value='test', x='test')
        self.assertIn('requires argument "y"', str(cm.exception))
