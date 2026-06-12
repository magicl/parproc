"""Tests for transitive first-party Python import tracking (track_python_imports)."""

# pylint: disable=unused-argument,protected-access

import os
import tempfile
import time
import unittest

import parproc as pp
from parproc.imports import PythonImportResolver
from parproc.types import ProcState


def _test_mode() -> str:
    return os.environ.get('PARPROC_TEST_MODE', 'single')


class ImportFixtureMixin(unittest.TestCase):
    """Helpers for building temporary Python source trees."""

    def setUp(self) -> None:
        pp.clear()
        self._tmpdir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        pp.clear()

    def _write(self, rel: str, content: str = '') -> str:
        path = os.path.join(self._tmpdir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        return os.path.abspath(path)

    def _resolver(self) -> PythonImportResolver:
        return PythonImportResolver([self._tmpdir])


class TestResolver(ImportFixtureMixin):
    """Unit tests for PythonImportResolver."""

    def test_chained_imports(self) -> None:
        entry = self._write('entry.py', 'import a\n')
        a = self._write('a.py', 'import b\n')
        b = self._write('b.py', 'import c\n')
        c = self._write('c.py', '\n')

        deps = self._resolver().transitive_deps(entry)
        self.assertEqual(deps, sorted([a, b, c]))

    def test_input_file_excluded_from_results(self) -> None:
        entry = self._write('entry.py', 'import a\n')
        self._write('a.py', '\n')
        self.assertNotIn(entry, self._resolver().transitive_deps(entry))

    def test_stdlib_and_thirdparty_ignored(self) -> None:
        entry = self._write('entry.py', 'import os\nimport sys\nimport json\nimport definitely_not_installed_pkg\n')
        self.assertEqual(self._resolver().transitive_deps(entry), [])

    def test_relative_imports(self) -> None:
        self._write('pkg/__init__.py', '\n')
        mod_a = self._write('pkg/mod_a.py', 'from . import mod_b\nfrom .mod_c import thing\n')
        mod_b = self._write('pkg/mod_b.py', '\n')
        mod_c = self._write('pkg/mod_c.py', 'thing = 1\n')
        pkg_init = os.path.join(self._tmpdir, 'pkg', '__init__.py')

        deps = self._resolver().transitive_deps(mod_a)
        self.assertIn(mod_b, deps)
        self.assertIn(mod_c, deps)
        self.assertIn(os.path.abspath(pkg_init), deps)

    def test_dotted_import_includes_parent_packages(self) -> None:
        a_init = self._write('a/__init__.py', '\n')
        b_init = self._write('a/b/__init__.py', '\n')
        c_mod = self._write('a/b/c.py', '\n')
        entry = self._write('entry.py', 'import a.b.c\n')

        deps = self._resolver().transitive_deps(entry)
        self.assertEqual(deps, sorted([a_init, b_init, c_mod]))

    def test_from_package_import_submodule(self) -> None:
        self._write('pkg/__init__.py', '\n')
        sub = self._write('pkg/sub.py', '\n')
        entry = self._write('entry.py', 'from pkg import sub\n')

        deps = self._resolver().transitive_deps(entry)
        self.assertIn(sub, deps)

    def test_cycle_is_safe(self) -> None:
        a = self._write('a.py', 'import b\n')
        b = self._write('b.py', 'import a\n')
        self.assertEqual(self._resolver().transitive_deps(a), [b])
        self.assertEqual(self._resolver().transitive_deps(b), [a])

    def test_syntax_error_is_graceful(self) -> None:
        bad = self._write('bad.py', 'def (:\n')
        self.assertEqual(self._resolver().transitive_deps(bad), [])

    def test_missing_file_is_graceful(self) -> None:
        missing = os.path.join(self._tmpdir, 'nope.py')
        self.assertEqual(self._resolver().transitive_deps(missing), [])

    def test_mtime_cache_refreshes_on_change(self) -> None:
        entry = self._write('entry.py', 'import a\n')
        self._write('a.py', '\n')
        b = self._write('b.py', '\n')
        resolver = self._resolver()
        self.assertNotIn(b, resolver.transitive_deps(entry))

        time.sleep(0.05)
        self._write('entry.py', 'import a\nimport b\n')
        self.assertIn(b, resolver.transitive_deps(entry))


class ManagerIntegrationBase(ImportFixtureMixin):
    """Base for tests exercising input expansion through ProcManager."""

    def setUp(self) -> None:
        super().setUp()
        self._db_path = os.path.join(self._tmpdir, 'test.db')

    def _set_options(self, track_python_imports: bool = False) -> None:
        pp.set_options(
            mode=_test_mode(),
            dynamic=False,
            task_db_path=self._db_path,
            python_import_roots=[self._tmpdir],
            track_python_imports=track_python_imports,
        )


class TestManagerExpansion(ManagerIntegrationBase):
    """Test that ProcManager._resolve_inputs honors import tracking."""

    def test_global_enable_expands_inputs(self) -> None:
        entry = self._write('entry.py', 'import helper\n')
        helper = self._write('helper.py', '\n')
        self._set_options(track_python_imports=True)

        @pp.Proto(name='build', inputs=[entry])
        def build(ctx: pp.ProcContext) -> str:
            return 'ok'

        pp.create('build')
        proc = pp.get_procs()['build']
        resolved = pp.ProcManager.get_inst()._resolve_inputs(proc)
        self.assertIn(entry, resolved)
        self.assertIn(helper, resolved)

    def test_disabled_does_not_expand(self) -> None:
        entry = self._write('entry.py', 'import helper\n')
        helper = self._write('helper.py', '\n')
        self._set_options(track_python_imports=False)

        @pp.Proto(name='build', inputs=[entry])
        def build(ctx: pp.ProcContext) -> str:
            return 'ok'

        pp.create('build')
        proc = pp.get_procs()['build']
        resolved = pp.ProcManager.get_inst()._resolve_inputs(proc)
        self.assertIn(entry, resolved)
        self.assertNotIn(helper, resolved)

    def test_per_proc_override_enables(self) -> None:
        entry = self._write('entry.py', 'import helper\n')
        helper = self._write('helper.py', '\n')
        self._set_options(track_python_imports=False)

        @pp.Proto(name='build', inputs=[entry], track_imports=True)
        def build(ctx: pp.ProcContext) -> str:
            return 'ok'

        pp.create('build')
        proc = pp.get_procs()['build']
        resolved = pp.ProcManager.get_inst()._resolve_inputs(proc)
        self.assertIn(helper, resolved)

    def test_per_proc_override_disables(self) -> None:
        entry = self._write('entry.py', 'import helper\n')
        helper = self._write('helper.py', '\n')
        self._set_options(track_python_imports=True)

        @pp.Proto(name='build', inputs=[entry], track_imports=False)
        def build(ctx: pp.ProcContext) -> str:
            return 'ok'

        pp.create('build')
        proc = pp.get_procs()['build']
        resolved = pp.ProcManager.get_inst()._resolve_inputs(proc)
        self.assertNotIn(helper, resolved)

    def test_inputs_ignore_prunes_transitive_dep(self) -> None:
        entry = self._write('entry.py', 'import helper\n')
        helper = self._write('helper.py', '\n')
        self._set_options(track_python_imports=True)

        @pp.Proto(name='build', inputs=[entry], inputs_ignore=[helper])
        def build(ctx: pp.ProcContext) -> str:
            return 'ok'

        pp.create('build')
        proc = pp.get_procs()['build']
        resolved = pp.ProcManager.get_inst()._resolve_inputs(proc)
        self.assertIn(entry, resolved)
        self.assertNotIn(helper, resolved)


class TestStalenessThroughImports(ManagerIntegrationBase):
    """End-to-end: editing a transitively-imported file marks the proc stale."""

    def _run_build(self, ret: str) -> None:
        @pp.Proto(name='build', inputs=[os.path.join(self._tmpdir, 'entry.py')])
        def build(ctx: pp.ProcContext) -> str:
            return ret

        pp.run('build')
        pp.wait_for_all()

    def test_editing_imported_file_triggers_rerun(self) -> None:
        self._write('entry.py', 'import helper\n')
        self._write('helper.py', 'VALUE = 1\n')
        self._set_options(track_python_imports=True)

        self._run_build('v1')
        self.assertEqual(pp.results()['build'], 'v1')

        # Re-run with no changes: should be skipped (UP_TO_DATE).
        pp.clear()
        self._set_options(track_python_imports=True)
        self._run_build('v2')
        self.assertEqual(pp.get_procs()['build'].state, ProcState.UP_TO_DATE)
        self.assertEqual(pp.results()['build'], 'v1')

        # Edit the imported helper: should now be stale and re-run.
        time.sleep(0.05)
        self._write('helper.py', 'VALUE = 2\n')
        pp.clear()
        self._set_options(track_python_imports=True)
        self._run_build('v3')
        self.assertEqual(pp.get_procs()['build'].state, ProcState.SUCCEEDED)
        self.assertEqual(pp.results()['build'], 'v3')

    def test_editing_imported_file_ignored_when_disabled(self) -> None:
        self._write('entry.py', 'import helper\n')
        self._write('helper.py', 'VALUE = 1\n')
        self._set_options(track_python_imports=False)

        self._run_build('v1')
        self.assertEqual(pp.results()['build'], 'v1')

        time.sleep(0.05)
        self._write('helper.py', 'VALUE = 2\n')
        pp.clear()
        self._set_options(track_python_imports=False)
        self._run_build('v2')
        # helper.py is not tracked, only entry.py is; proc stays up to date.
        self.assertEqual(pp.get_procs()['build'].state, ProcState.UP_TO_DATE)
        self.assertEqual(pp.results()['build'], 'v1')


if __name__ == '__main__':
    unittest.main()
