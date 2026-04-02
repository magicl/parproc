# pylint: disable=unused-argument
import logging
import os
import tempfile
from unittest import TestCase

import parproc as pp


class OptionsTest(TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        pp.ProcManager.get_inst().set_options(dynamic=False)

    def test_allow_missing_deps_default_is_true(self):
        """Test that allow_missing_deps defaults to True"""
        manager = pp.ProcManager()
        self.assertTrue(manager.allow_missing_deps)

    def test_allow_missing_deps_false_raises_error(self):
        """Test that when allow_missing_deps=False, missing dependencies raise UserError"""
        pp.wait_clear()
        pp.ProcManager.get_inst().set_options(allow_missing_deps=False)

        @pp.Proc(name='proc_a', now=True)
        def proc_a(context):
            return 'result_a'

        @pp.Proc(name='proc_b', deps=['nonexistent_proc'], now=True)
        def proc_b(context):
            return 'result_b'

        # Should raise UserError when trying to schedule proc_b with missing dependency
        with self.assertRaises(pp.UserError) as cm:
            pp.wait_for_all()
        self.assertIn('depends on "nonexistent_proc" which does not exist', str(cm.exception))
        self.assertIn('Set allow_missing_deps=True', str(cm.exception))

    def test_allow_missing_deps_true_allows_missing(self):
        """Test that when allow_missing_deps=True, proc with missing dep is marked FAILED_DEP"""
        pp.wait_clear()
        pp.ProcManager.get_inst().set_options(allow_missing_deps=True)

        @pp.Proc(name='proc_a', now=True)
        def proc_a(context):
            return 'result_a'

        @pp.Proc(name='proc_b', deps=['nonexistent_proc'], now=True)
        def proc_b(context):
            return 'result_b'

        # proc_b should be created and marked FAILED_DEP (dependency missing)
        procs = pp.get_procs()
        self.assertIn('proc_a', procs)
        self.assertIn('proc_b', procs)
        self.assertEqual(procs['proc_b'].state.name, 'FAILED_DEP')

    def test_full_log_on_failure_option_propagates_to_term(self):
        """Setting full_log_on_failure should update both manager and term."""
        manager = pp.ProcManager.get_inst()
        manager.set_options(full_log_on_failure=True)
        self.assertTrue(manager.full_log_on_failure)
        self.assertTrue(manager.term.show_full_log_on_failure)

        manager.set_options(full_log_on_failure=False)
        self.assertFalse(manager.full_log_on_failure)
        self.assertFalse(manager.term.show_full_log_on_failure)

    def test_global_inputs_ignore_is_appended_to_proc_inputs_ignore(self):
        """Global ignore specs should be added alongside per-proc inputs_ignore."""
        pp.wait_clear()
        manager = pp.ProcManager.get_inst()
        manager.set_options(dynamic=False)

        with tempfile.TemporaryDirectory() as tmpdir:
            keep_file = os.path.join(tmpdir, 'keep.txt')
            local_ignore_file = os.path.join(tmpdir, 'local-ignore.txt')
            global_ignore_file = os.path.join(tmpdir, 'global-ignore.txt')
            for path in (keep_file, local_ignore_file, global_ignore_file):
                with open(path, 'w', encoding='utf-8') as handle:
                    handle.write('x')

            manager.set_options(global_inputs_ignore=[global_ignore_file])

            @pp.Proc(name='build', inputs=[os.path.join(tmpdir, '*.txt')], inputs_ignore=[local_ignore_file])
            def build(context):
                del context
                return 'ok'

            proc = pp.get_procs()['build']
            resolved_inputs = manager._resolve_inputs(proc)  # pylint: disable=protected-access

            self.assertIn(keep_file, resolved_inputs)
            self.assertNotIn(local_ignore_file, resolved_inputs)
            self.assertNotIn(global_ignore_file, resolved_inputs)

    def test_global_inputs_ignore_is_used_when_proto_has_no_local_ignore(self):
        """Global ignore specs should apply even when proto/proc defines no inputs_ignore."""
        pp.wait_clear()
        manager = pp.ProcManager.get_inst()
        manager.set_options(dynamic=False)

        with tempfile.TemporaryDirectory() as tmpdir:
            keep_file = os.path.join(tmpdir, 'keep.txt')
            global_ignore_file = os.path.join(tmpdir, 'global-ignore.txt')
            for path in (keep_file, global_ignore_file):
                with open(path, 'w', encoding='utf-8') as handle:
                    handle.write('x')

            manager.set_options(global_inputs_ignore=[global_ignore_file])

            @pp.Proto(name='build', inputs=[os.path.join(tmpdir, '*.txt')])
            def build(context):
                del context
                return 'ok'

            pp.create('build')
            proc = pp.get_procs()['build']
            resolved_inputs = manager._resolve_inputs(proc)  # pylint: disable=protected-access

            self.assertIn(keep_file, resolved_inputs)
            self.assertNotIn(global_ignore_file, resolved_inputs)
