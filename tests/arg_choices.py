# pylint: disable=unused-argument
import logging
import os
from unittest import TestCase

import parproc as pp


class ArgChoicesTest(TestCase):
    """Tests for Proto arg_choices and glob expansion."""

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        mode = os.environ.get('PARPROC_TEST_MODE', 'mp')
        pp.ProcManager.get_inst().set_options(mode=mode, dynamic=False)

    def test_validation_allowed_value_succeeds(self):
        """Creating a proc with an allowed value succeeds."""
        pp.wait_clear()

        @pp.Proto(name='foo::[x]::[y]', arg_choices={'x': ['a', 'b'], 'y': [1, 2]})
        def foo_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'{x}_{y}'

        names = pp.create('foo::a::2')
        self.assertEqual(names, ['foo::a::2'])
        pp.start(*names)
        pp.wait(*names)
        self.assertEqual(pp.results(), {'foo::a::2': 'a_2'})

    def test_validation_disallowed_value_raises(self):
        """Creating a proc with a value not in arg_choices raises UserError."""
        pp.wait_clear()

        @pp.Proto(name='foo::[x]::[y]', arg_choices={'x': ['a', 'b']})
        def foo_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'{x}_{y}'

        with self.assertRaises(pp.UserError) as cm:
            pp.create('foo::c::2')
        self.assertIn('allowed choices', str(cm.exception))
        self.assertIn('a', str(cm.exception))
        self.assertIn('b', str(cm.exception))

    def test_star_expansion_returns_list(self):
        """create('foo::*::2') with arg_choices expands to all procs and returns list."""
        pp.wait_clear()

        @pp.Proto(name='foo::[x]::[y]', arg_choices={'x': ['a', 'b'], 'y': [1, 2]})
        def foo_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'{x}_{y}'

        result = pp.create('foo::*::2')
        self.assertIsInstance(result, list)
        self.assertEqual(set(result), {'foo::a::2', 'foo::b::2'})
        pp.start(*result)
        pp.wait(*result)
        self.assertEqual(pp.results(), {'foo::a::2': 'a_2', 'foo::b::2': 'b_2'})

    def test_star_expansion_single_match_returns_list_of_one(self):
        """When glob expands to one proc, create still returns list with one element."""
        pp.wait_clear()

        @pp.Proto(name='foo::[x]::[y]', arg_choices={'x': ['a'], 'y': [1, 2]})
        def foo_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'{x}_{y}'

        result = pp.create('foo::*::2')
        self.assertIsInstance(result, list)
        self.assertEqual(result, ['foo::a::2'])

    def test_question_mark_and_partial_glob(self):
        """? matches single char; a* matches prefix."""
        pp.wait_clear()

        @pp.Proto(name='foo::[x]::[y]', arg_choices={'x': ['a', 'ab', 'b'], 'y': [1]})
        def foo_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'{x}_{y}'

        # ? matches single-char: a and b
        result = pp.create('foo::?::1')
        self.assertIsInstance(result, list)
        self.assertEqual(set(result), {'foo::a::1', 'foo::b::1'})

        # a* matches a and ab (same proto, no wait_clear so proto still registered)
        result2 = pp.create('foo::a*::1')
        self.assertEqual(set(result2), {'foo::a::1', 'foo::ab::1'})

    def test_no_arg_choices_glob_raises(self):
        """Using * or ? when proto has no arg_choices raises UserError."""
        pp.wait_clear()

        @pp.Proto(name='foo::[x]::[y]')
        def foo_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'{x}_{y}'

        with self.assertRaises(pp.UserError) as cm:
            pp.create('foo::*::2')
        self.assertIn('arg_choices', str(cm.exception).lower())
        self.assertIn('Glob', str(cm.exception))

        with self.assertRaises(pp.UserError) as cm2:
            pp.create('foo::x?::2')
        self.assertIn('arg_choices', str(cm2.exception).lower())

    def test_multiple_globbed_args_cartesian_product(self):
        """create('foo::*::*') returns all combinations."""
        pp.wait_clear()

        @pp.Proto(name='foo::[x]::[y]', arg_choices={'x': ['a', 'b'], 'y': [1, 2]})
        def foo_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'{x}_{y}'

        result = pp.create('foo::*::*')
        self.assertIsInstance(result, list)
        self.assertEqual(
            set(result),
            {'foo::a::1', 'foo::a::2', 'foo::b::1', 'foo::b::2'},
        )
        pp.start(*result)
        pp.wait(*result)
        self.assertEqual(
            pp.results(),
            {
                'foo::a::1': 'a_1',
                'foo::a::2': 'a_2',
                'foo::b::1': 'b_1',
                'foo::b::2': 'b_2',
            },
        )

    def test_glob_matches_nothing_raises(self):
        """Glob pattern that matches no allowed value raises UserError."""
        pp.wait_clear()

        @pp.Proto(name='foo::[x]::[y]', arg_choices={'x': ['a', 'b'], 'y': [1]})
        def foo_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'{x}_{y}'

        with self.assertRaises(pp.UserError) as cm:
            pp.create('foo::z*::1')
        self.assertIn('matched no', str(cm.exception))
        self.assertIn('z*', str(cm.exception))

    def test_run_with_glob_creates_and_starts_all(self):
        """run('foo::*::2') creates and starts all matching procs."""
        pp.wait_clear()

        @pp.Proto(name='foo::[x]::[y]', arg_choices={'x': ['a', 'b'], 'y': [2]})
        def foo_proc(context: pp.ProcContext, x: str, y: int) -> str:
            return f'{x}_{y}'

        pp.run('foo::*::2')
        pp.wait('foo::a::2', 'foo::b::2')
        self.assertEqual(pp.results(), {'foo::a::2': 'a_2', 'foo::b::2': 'b_2'})

    def test_arg_choices_key_must_be_in_pattern(self):
        """arg_choices keys must be parameters in the proto name pattern."""
        pp.wait_clear()

        with self.assertRaises(pp.UserError) as cm:

            @pp.Proto(name='foo::[x]', arg_choices={'y': ['a']})
            def bad_proto(context: pp.ProcContext, x: str) -> str:
                return x

        self.assertIn('arg_choices', str(cm.exception))
        self.assertIn('not a parameter', str(cm.exception))
