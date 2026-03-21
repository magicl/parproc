"""Tests for incremental builds, staleness detection, result caching, and output verification."""

# pylint: disable=unused-argument

import os
import tempfile
import time
import unittest

import parproc as pp
from parproc.types import ProcState


def _test_mode() -> str:
    return os.environ.get('PARPROC_TEST_MODE', 'single')


class IncrementalBaseTest(unittest.TestCase):
    """Base class that sets up parproc with a temp task_db and temp dirs for file I/O."""

    def setUp(self) -> None:
        pp.clear()
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, 'test.db')
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)

    def tearDown(self) -> None:
        pp.clear()

    def _write_file(self, name: str, content: str = 'data') -> str:
        path = os.path.join(self._tmpdir, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        return path

    def _counter_path(self) -> str:
        return os.path.join(self._tmpdir, '_call_counter')

    def _increment_counter(self) -> None:
        """Write-based counter that works across multiprocessing boundaries."""
        path = self._counter_path()
        count = 0
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                count = int(f.read().strip())
        with open(path, 'w', encoding='utf-8') as f:
            f.write(str(count + 1))

    def _read_counter(self) -> int:
        path = self._counter_path()
        if not os.path.exists(path):
            return 0
        with open(path, encoding='utf-8') as f:
            return int(f.read().strip())

    def _reset_counter(self) -> None:
        path = self._counter_path()
        if os.path.exists(path):
            os.remove(path)


class TestStalenessWithInputs(IncrementalBaseTest):
    """Test that procs with declared inputs are auto-skipped when inputs haven't changed."""

    def test_first_run_always_executes(self) -> None:
        src = self._write_file('src.txt', 'v1')

        tmpdir = self._tmpdir

        @pp.Proto(name='build', inputs=[src])
        def build(ctx: pp.ProcContext) -> str:
            with open(os.path.join(tmpdir, '_ran'), 'w', encoding='utf-8') as f:
                f.write('yes')
            return 'built'

        pp.run('build')
        pp.wait_for_all()
        self.assertTrue(os.path.exists(os.path.join(self._tmpdir, '_ran')))
        self.assertEqual(pp.results()['build'], 'built')
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.SUCCEEDED)

    def test_second_run_skips_when_inputs_unchanged(self) -> None:
        src = self._write_file('src.txt', 'v1')

        @pp.Proto(name='build', inputs=[src])
        def build(ctx: pp.ProcContext) -> str:
            return 'built_v1'

        pp.run('build')
        pp.wait_for_all()
        self.assertEqual(pp.results()['build'], 'built_v1')

        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)

        @pp.Proto(name='build', inputs=[src])
        def build2(ctx: pp.ProcContext) -> str:
            return 'built_v2'

        pp.run('build')
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.UP_TO_DATE)
        # Cached result from first run
        self.assertEqual(pp.results()['build'], 'built_v1')

    def test_reruns_when_input_changes(self) -> None:
        src = self._write_file('src.txt', 'v1')

        @pp.Proto(name='build', inputs=[src])
        def build(ctx: pp.ProcContext) -> str:
            return 'built_v1'

        pp.run('build')
        pp.wait_for_all()
        self.assertEqual(pp.results()['build'], 'built_v1')

        # Modify the input file (need time gap for mtime_ns to differ)
        time.sleep(0.05)
        self._write_file('src.txt', 'v2')

        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)

        @pp.Proto(name='build', inputs=[src])
        def build2(ctx: pp.ProcContext) -> str:
            return 'built_v2'

        pp.run('build')
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.SUCCEEDED)
        self.assertEqual(pp.results()['build'], 'built_v2')

    def test_full_forces_rerun(self) -> None:
        src = self._write_file('src.txt', 'v1')

        @pp.Proto(name='build', inputs=[src])
        def build(ctx: pp.ProcContext) -> str:
            return 'built_v1'

        pp.run('build')
        pp.wait_for_all()

        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)

        @pp.Proto(name='build', inputs=[src])
        def build2(ctx: pp.ProcContext) -> str:
            return 'forced_v2'

        pp.run('build', full=True)
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.SUCCEEDED)
        self.assertEqual(pp.results()['build'], 'forced_v2')


class TestNoSkip(IncrementalBaseTest):
    """Test that no_skip=True procs always run."""

    def test_no_skip_always_runs(self) -> None:
        @pp.Proto(name='deploy', no_skip=True)
        def deploy(ctx: pp.ProcContext) -> str:
            return 'deployed_v1'

        pp.run('deploy')
        pp.wait_for_all()
        self.assertEqual(pp.results()['deploy'], 'deployed_v1')

        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)

        @pp.Proto(name='deploy', no_skip=True)
        def deploy2(ctx: pp.ProcContext) -> str:
            return 'deployed_v2'

        pp.run('deploy')
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['deploy'].state, ProcState.SUCCEEDED)
        # Function re-ran and returned new value
        self.assertEqual(pp.results()['deploy'], 'deployed_v2')


class TestAutoSkipWithoutInputs(IncrementalBaseTest):
    """Test that procs without inputs are auto-skipped when deps are UP_TO_DATE and cached result exists."""

    def test_skips_when_deps_up_to_date(self) -> None:
        src = self._write_file('src.txt', 'v1')

        @pp.Proto(name='build', inputs=[src])
        def build(ctx: pp.ProcContext) -> str:
            return 'built'

        @pp.Proto(name='deploy', deps=['build'])
        def deploy(ctx: pp.ProcContext) -> str:
            return 'deployed'

        pp.run('deploy')
        pp.wait_for_all()
        self.assertEqual(pp.results()['build'], 'built')
        self.assertEqual(pp.results()['deploy'], 'deployed')

        # Second run, no changes
        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)

        @pp.Proto(name='build', inputs=[src])
        def build2(ctx: pp.ProcContext) -> str:
            return 'built2'

        @pp.Proto(name='deploy', deps=['build'])
        def deploy2(ctx: pp.ProcContext) -> str:
            return 'deployed2'

        pp.run('deploy')
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.UP_TO_DATE)
        self.assertEqual(procs['deploy'].state, ProcState.UP_TO_DATE)
        # Cached results are loaded
        self.assertEqual(pp.results()['build'], 'built')
        self.assertEqual(pp.results()['deploy'], 'deployed')

    def test_downstream_reruns_when_upstream_changes(self) -> None:
        src = self._write_file('src.txt', 'v1')

        @pp.Proto(name='build', inputs=[src])
        def build(ctx: pp.ProcContext) -> str:
            return 'built_v1'

        @pp.Proto(name='deploy', deps=['build'])
        def deploy(ctx: pp.ProcContext) -> str:
            return 'deployed_v1'

        pp.run('deploy')
        pp.wait_for_all()

        # Modify the input file
        time.sleep(0.05)
        self._write_file('src.txt', 'v2')

        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)

        @pp.Proto(name='build', inputs=[src])
        def build2(ctx: pp.ProcContext) -> str:
            return 'built_v2'

        @pp.Proto(name='deploy', deps=['build'])
        def deploy2(ctx: pp.ProcContext) -> str:
            return 'deployed_v2'

        pp.run('deploy')
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.SUCCEEDED)
        self.assertEqual(procs['deploy'].state, ProcState.SUCCEEDED)
        self.assertEqual(pp.results()['build'], 'built_v2')
        self.assertEqual(pp.results()['deploy'], 'deployed_v2')


class TestDepsChanged(IncrementalBaseTest):
    """Test that deps_changed is correctly populated in ProcContext."""

    def test_deps_changed_false_when_dep_cached(self) -> None:
        src = self._write_file('src.txt', 'v1')
        marker_path = os.path.join(self._tmpdir, '_deps_changed')

        tmpdir = self._tmpdir

        @pp.Proto(name='build', inputs=[src])
        def build(ctx: pp.ProcContext) -> str:
            return 'built'

        @pp.Proto(name='test', deps=['build'], no_skip=True)
        def test_proc(ctx: pp.ProcContext) -> str:
            with open(os.path.join(tmpdir, '_deps_changed'), 'w', encoding='utf-8') as f:
                f.write('1' if ctx.deps_changed else '0')
            return 'tested'

        pp.run('test')
        pp.wait_for_all()
        with open(marker_path, encoding='utf-8') as f:
            self.assertEqual(f.read(), '1')

        # Second run - build should be UP_TO_DATE, test runs (no_skip) but deps_changed=False
        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)
        os.remove(marker_path)

        @pp.Proto(name='build', inputs=[src])
        def build2(ctx: pp.ProcContext) -> str:
            return 'built2'

        @pp.Proto(name='test', deps=['build'], no_skip=True)
        def test_proc2(ctx: pp.ProcContext) -> str:
            with open(os.path.join(tmpdir, '_deps_changed'), 'w', encoding='utf-8') as f:
                f.write('1' if ctx.deps_changed else '0')
            return 'tested2'

        pp.run('test')
        pp.wait_for_all()
        with open(marker_path, encoding='utf-8') as f:
            self.assertEqual(f.read(), '0')


class TestOutputVerification(IncrementalBaseTest):
    """Test that declared outputs are verified after proc runs."""

    def test_fails_when_output_not_created(self) -> None:
        output_path = os.path.join(self._tmpdir, 'missing_output.txt')

        @pp.Proto(name='build', outputs=[output_path])
        def build(ctx: pp.ProcContext) -> str:
            return 'built'

        pp.run('build')
        ok = pp.wait_for_all(exception_on_failure=False)
        self.assertFalse(ok)
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.FAILED)
        self.assertEqual(procs['build'].error, pp.Proc.ERROR_OUTPUTS_NOT_REFRESHED)

    def test_succeeds_when_output_created(self) -> None:
        output_path = os.path.join(self._tmpdir, 'output.txt')

        @pp.Proto(name='build', outputs=[output_path])
        def build(ctx: pp.ProcContext) -> str:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('result')
            return 'built'

        pp.run('build')
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.SUCCEEDED)


class TestNoTaskDb(IncrementalBaseTest):
    """Test that without task_db_path, everything always runs (no caching)."""

    def test_always_runs_without_db(self) -> None:
        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=None)

        @pp.Proto(name='build')
        def build(ctx: pp.ProcContext) -> str:
            return 'built'

        pp.run('build')
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.SUCCEEDED)
        self.assertEqual(pp.results()['build'], 'built')


class TestDeletedDbFullRerun(IncrementalBaseTest):
    """Test that deleting the DB causes everything to run again."""

    def test_deleted_db_causes_rerun(self) -> None:
        src = self._write_file('src.txt', 'v1')

        @pp.Proto(name='build', inputs=[src])
        def build(ctx: pp.ProcContext) -> str:
            return 'built_v1'

        pp.run('build')
        pp.wait_for_all()
        self.assertEqual(pp.results()['build'], 'built_v1')

        # Delete the DB
        os.remove(self._db_path)

        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)

        @pp.Proto(name='build', inputs=[src])
        def build2(ctx: pp.ProcContext) -> str:
            return 'rebuilt'

        pp.run('build')
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['build'].state, ProcState.SUCCEEDED)
        self.assertEqual(pp.results()['build'], 'rebuilt')


class TestCallableInputs(IncrementalBaseTest):
    """Test that callable inputs are properly resolved."""

    def test_lambda_inputs(self) -> None:
        tmpdir = self._tmpdir
        self._write_file('src_a.txt', 'v1')

        @pp.Proto(
            name='build::[target]',
            inputs=lambda ctx, target: [os.path.join(tmpdir, f'src_{target}.txt')],
        )
        def build(ctx: pp.ProcContext, target: str) -> str:
            return f'built_{target}'

        pp.run('build::a')
        pp.wait_for_all()
        self.assertEqual(pp.results()['build::a'], 'built_a')

        pp.clear()
        pp.set_options(mode=_test_mode(), dynamic=False, task_db_path=self._db_path)

        @pp.Proto(
            name='build::[target]',
            inputs=lambda ctx, target: [os.path.join(tmpdir, f'src_{target}.txt')],
        )
        def build2(ctx: pp.ProcContext, target: str) -> str:
            return f'built2_{target}'

        pp.run('build::a')
        pp.wait_for_all()
        procs = pp.get_procs()
        self.assertEqual(procs['build::a'].state, ProcState.UP_TO_DATE)
        self.assertEqual(pp.results()['build::a'], 'built_a')


class TestGeneration(IncrementalBaseTest):
    """Test generation tracking on Proc."""

    def test_generation_increments_on_dirty(self) -> None:
        @pp.Proto(name='A')
        def a_proc(ctx: pp.ProcContext) -> str:
            return 'a'

        @pp.Proto(name='B', deps=['A'])
        def b_proc(ctx: pp.ProcContext) -> str:
            return 'b'

        pp.run('B')
        pp.wait_for_all()

        mgr = pp.ProcManager.get_inst()
        procs = mgr.procs

        self.assertEqual(procs['A'].generation, 0)
        self.assertEqual(procs['B'].generation, 0)

        mgr._mark_dirty('A')  # pylint: disable=protected-access
        self.assertEqual(procs['A'].generation, 1)
        self.assertEqual(procs['B'].generation, 1)

    def test_completed_generation_set_on_success(self) -> None:
        @pp.Proto(name='A')
        def a_proc(ctx: pp.ProcContext) -> str:
            return 'a'

        pp.run('A')
        pp.wait_for_all()

        procs = pp.get_procs()
        self.assertEqual(procs['A'].completed_generation, 0)
        self.assertEqual(procs['A'].generation, 0)


if __name__ == '__main__':
    unittest.main()
