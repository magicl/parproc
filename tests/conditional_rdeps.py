# pylint: disable=unused-argument,protected-access
"""Tests for WhenScheduled (conditional reverse dependencies)."""

import logging
import multiprocessing as mp
import os
import time
from unittest import TestCase

import parproc as pp


class ConditionalRdepTest(TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        mode = os.environ.get('PARPROC_TEST_MODE', 'mp')
        pp.ProcManager.get_inst().set_options(mode=mode, dynamic=False)

    # ------------------------------------------------------------------
    # Basic: schedule B with WhenScheduled('A') -> both run, B before A
    # ------------------------------------------------------------------
    def test_basic_when_scheduled(self):
        """Scheduling B (with WhenScheduled('A')) should also schedule A, with B running first."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='A')
            def a_proc(context: pp.ProcContext) -> str:
                execution_order.append('A')
                return 'A_done'

            @pp.Proto(name='B', rdeps=[pp.WhenScheduled('A')])
            def b_proc(context: pp.ProcContext) -> str:
                execution_order.append('B')
                time.sleep(0.05)
                return 'B_done'

            proc_name = pp.create('B')
            pp.start(*proc_name)
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            # Both A and B should have run
            self.assertIn('A', results)
            self.assertIn('B', results)
            self.assertEqual(results['A'], 'A_done')
            self.assertEqual(results['B'], 'B_done')

            # B should execute before A
            self.assertIn('B', order)
            self.assertIn('A', order)
            b_idx = order.index('B')
            a_idx = order.index('A')
            self.assertLess(b_idx, a_idx, 'B should execute before A')

    # ------------------------------------------------------------------
    # Only A scheduled -> A runs alone, B is not involved
    # ------------------------------------------------------------------
    def test_only_target_scheduled(self):
        """When only A is scheduled, B (with WhenScheduled('A')) should NOT run."""

        pp.wait_clear()

        @pp.Proto(name='A')
        def a_proc(context: pp.ProcContext) -> str:
            return 'A_done'

        @pp.Proto(name='B', rdeps=[pp.WhenScheduled('A')])
        def b_proc(context: pp.ProcContext) -> str:
            return 'B_done'

        proc_name = pp.create('A')
        pp.start(*proc_name)
        pp.wait_for_all()

        results = pp.results()
        self.assertIn('A', results)
        self.assertEqual(results['A'], 'A_done')
        # B should NOT have been scheduled or run
        self.assertNotIn('B', results)

    # ------------------------------------------------------------------
    # Both scheduled explicitly -> B before A
    # ------------------------------------------------------------------
    def test_both_scheduled_explicitly(self):
        """When both A and B are scheduled, B (with WhenScheduled('A')) should run before A."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='A')
            def a_proc(context: pp.ProcContext) -> str:
                execution_order.append('A')
                return 'A_done'

            @pp.Proto(name='B', rdeps=[pp.WhenScheduled('A')])
            def b_proc(context: pp.ProcContext) -> str:
                execution_order.append('B')
                time.sleep(0.05)
                return 'B_done'

            pp.create('A')
            pp.create('B')
            pp.start('B', 'A')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('A', results)
            self.assertIn('B', results)
            self.assertIn('B', order)
            self.assertIn('A', order)
            b_idx = order.index('B')
            a_idx = order.index('A')
            self.assertLess(b_idx, a_idx, 'B should execute before A')

    # ------------------------------------------------------------------
    # Both scheduled explicitly, A first then B -> B still injected before A
    # ------------------------------------------------------------------
    def test_both_scheduled_a_then_b(self):
        """A listed before B in the same start() call. B should still be injected as dep of A."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='A')
            def a_proc(context: pp.ProcContext) -> str:
                execution_order.append('A')
                return 'A_done'

            @pp.Proto(name='B', rdeps=[pp.WhenScheduled('A')])
            def b_proc(context: pp.ProcContext) -> str:
                execution_order.append('B')
                time.sleep(0.05)
                return 'B_done'

            pp.create('A')
            pp.create('B')
            # A is listed first -- the two-pass start_proc ensures both are
            # WANTED before execution begins, so B's WhenScheduled('A') can
            # inject B as A's dep even though A appears first.
            pp.start('A', 'B')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('A', results)
            self.assertIn('B', results)
            self.assertIn('B', order)
            self.assertIn('A', order)
            b_idx = order.index('B')
            a_idx = order.index('A')
            self.assertLess(b_idx, a_idx, 'B should execute before A even when A is listed first')

    # ------------------------------------------------------------------
    # Late scheduling failure: A already RUNNING/SUCCEEDED when B scheduled
    # ------------------------------------------------------------------
    def test_late_scheduling_failure(self):
        """Scheduling B after A is already running/finished should raise UserError."""

        pp.wait_clear()

        @pp.Proto(name='A')
        def a_proc(context: pp.ProcContext) -> str:
            return 'A_done'

        @pp.Proto(name='B', rdeps=[pp.WhenScheduled('A')])
        def b_proc(context: pp.ProcContext) -> str:
            return 'B_done'

        # Schedule and complete A first
        proc_name = pp.create('A')
        pp.start(*proc_name)
        pp.wait_for_all()

        results = pp.results()
        self.assertIn('A', results)

        # Now schedule B -- A is already SUCCEEDED, so this should fail
        pp.create('B')
        with self.assertRaises(pp.UserError) as ctx:
            pp.start('B')

        self.assertIn('WhenScheduled', str(ctx.exception))
        self.assertIn('too late', str(ctx.exception).lower())

    # ------------------------------------------------------------------
    # Timely scheduling: B scheduled before A starts -> correct order
    # ------------------------------------------------------------------
    def test_timely_scheduling_success(self):
        """B is scheduled before A starts; both run in correct order (B first, A second)."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='A')
            def a_proc(context: pp.ProcContext) -> str:
                execution_order.append('A')
                return 'A_done'

            @pp.Proto(name='B', rdeps=[pp.WhenScheduled('A')])
            def b_proc(context: pp.ProcContext) -> str:
                execution_order.append('B')
                time.sleep(0.05)
                return 'B_done'

            # Create both, schedule B (which pulls in A) - before anything runs
            pp.create('B')
            pp.start('B')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('A', results)
            self.assertIn('B', results)
            b_idx = order.index('B')
            a_idx = order.index('A')
            self.assertLess(b_idx, a_idx, 'B should execute before A')

    # ------------------------------------------------------------------
    # Pattern matching: WhenScheduled('A::[env]')
    # ------------------------------------------------------------------
    def test_parameterized_when_scheduled(self):
        """WhenScheduled with parameterized patterns should extract and fill params."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='deploy::[env]')
            def deploy_proc(context: pp.ProcContext, env: str) -> str:
                execution_order.append(f'deploy::{env}')
                return f'deploy_{env}'

            @pp.Proto(name='build::[env]', rdeps=[pp.WhenScheduled('deploy::[env]')])
            def build_proc(context: pp.ProcContext, env: str) -> str:
                execution_order.append(f'build::{env}')
                time.sleep(0.05)
                return f'build_{env}'

            pp.create('build::prod')
            pp.start('build::prod')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            # Both should have run with env=prod
            self.assertIn('build::prod', results)
            self.assertIn('deploy::prod', results)
            self.assertEqual(results['build::prod'], 'build_prod')
            self.assertEqual(results['deploy::prod'], 'deploy_prod')

            # build should execute before deploy
            build_idx = order.index('build::prod')
            deploy_idx = order.index('deploy::prod')
            self.assertLess(build_idx, deploy_idx, 'build should execute before deploy')

    # ------------------------------------------------------------------
    # Cross-pattern: WhenScheduled pattern differs from proto pattern
    # ------------------------------------------------------------------
    def test_cross_pattern_when_scheduled(self):
        """WhenScheduled pattern can differ from the declaring proto's pattern."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='k8s.deploy::[target]::[component]')
            def deploy_proc(context: pp.ProcContext, target: str, component: str) -> str:
                execution_order.append(f'deploy::{target}::{component}')
                return f'deploy_{target}_{component}'

            # build has a different pattern but shares the [target] param.
            # WhenScheduled target fills in fixed values for other params.
            @pp.Proto(name='next.build::[target]', rdeps=[pp.WhenScheduled('k8s.deploy::[target]::frontend')])
            def build_proc(context: pp.ProcContext, target: str) -> str:
                execution_order.append(f'build::{target}')
                time.sleep(0.05)
                return f'build_{target}'

            pp.create('next.build::stage')
            pp.start('next.build::stage')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('next.build::stage', results)
            self.assertIn('k8s.deploy::stage::frontend', results)
            build_idx = order.index('build::stage')
            deploy_idx = order.index('deploy::stage::frontend')
            self.assertLess(build_idx, deploy_idx, 'build should execute before deploy')

    # ------------------------------------------------------------------
    # Multiple conditional rdeps on one proto
    # ------------------------------------------------------------------
    def test_multiple_conditional_rdeps(self):
        """A proto can have multiple WhenScheduled rdeps."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='target1')
            def target1(context: pp.ProcContext) -> str:
                execution_order.append('target1')
                return 'target1_done'

            @pp.Proto(name='target2')
            def target2(context: pp.ProcContext) -> str:
                execution_order.append('target2')
                return 'target2_done'

            @pp.Proto(name='setup', rdeps=[pp.WhenScheduled('target1'), pp.WhenScheduled('target2')])
            def setup_proc(context: pp.ProcContext) -> str:
                execution_order.append('setup')
                time.sleep(0.05)
                return 'setup_done'

            pp.create('setup')
            pp.start('setup')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            # All three should run: setup before target1 and target2
            self.assertIn('setup', results)
            self.assertIn('target1', results)
            self.assertIn('target2', results)
            setup_idx = order.index('setup')
            self.assertLess(setup_idx, order.index('target1'), 'setup should execute before target1')
            self.assertLess(setup_idx, order.index('target2'), 'setup should execute before target2')

    # ------------------------------------------------------------------
    # Mix of plain rdeps and WhenScheduled in same list
    # ------------------------------------------------------------------
    def test_mix_plain_and_conditional_rdeps(self):
        """Plain rdeps and WhenScheduled can coexist in the same rdeps list.

        When setup is pulled in via the plain rdep (because unconditional_target
        is scheduled), setup becomes WANTED. Because setup is now scheduled, its
        WhenScheduled('conditional_target') also activates, pulling in
        conditional_target. All three should run.
        """

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='unconditional_target')
            def unconditional_target(context: pp.ProcContext) -> str:
                execution_order.append('unconditional_target')
                return 'ut_done'

            @pp.Proto(name='conditional_target')
            def conditional_target(context: pp.ProcContext) -> str:
                execution_order.append('conditional_target')
                return 'ct_done'

            # Use a Proc so it already exists as a proc
            @pp.Proc(name='setup', rdeps=['unconditional_target', pp.WhenScheduled('conditional_target')])
            def setup_proc(context: pp.ProcContext) -> str:
                execution_order.append('setup')
                time.sleep(0.05)
                return 'setup_done'

            # Start unconditional_target -- setup is injected via the plain rdep.
            # Because setup becomes WANTED, its WhenScheduled rdep also fires,
            # pulling in conditional_target.
            pp.create('unconditional_target')
            pp.start('unconditional_target')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            # All three should have run
            self.assertIn('setup', results, 'setup should run (plain rdep)')
            self.assertIn('unconditional_target', results)
            self.assertIn('conditional_target', results, 'conditional_target should run (WhenScheduled activated)')
            setup_idx = order.index('setup')
            self.assertLess(
                setup_idx, order.index('unconditional_target'), 'setup should execute before unconditional_target'
            )
            self.assertLess(
                setup_idx, order.index('conditional_target'), 'setup should execute before conditional_target'
            )

    def test_conditional_rdep_not_activated_when_unscheduled(self):
        """WhenScheduled rdep does NOT activate when the declaring proc is not scheduled at all."""

        pp.wait_clear()

        @pp.Proto(name='target')
        def target_proc(context: pp.ProcContext) -> str:
            return 'target_done'

        # unrelated_setup has a WhenScheduled('target'), but is never scheduled
        @pp.Proc(name='unrelated_setup', rdeps=[pp.WhenScheduled('target')])
        def unrelated_setup(context: pp.ProcContext) -> str:
            return 'setup_done'

        # Start a completely independent proc
        @pp.Proc(name='independent', now=True)
        def independent_proc(context: pp.ProcContext) -> str:
            return 'independent_done'

        pp.wait_for_all()

        results = pp.results()
        self.assertIn('independent', results)
        # Neither target nor unrelated_setup should have run
        self.assertNotIn('target', results)
        self.assertNotIn('unrelated_setup', results)

    def test_mix_plain_and_conditional_rdeps_both_active(self):
        """When setup is also scheduled, the WhenScheduled rdep activates too."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='unconditional_target')
            def unconditional_target(context: pp.ProcContext) -> str:
                execution_order.append('unconditional_target')
                return 'ut_done'

            @pp.Proto(name='conditional_target')
            def conditional_target(context: pp.ProcContext) -> str:
                execution_order.append('conditional_target')
                return 'ct_done'

            # Use a Proc so it already exists
            @pp.Proc(name='setup', rdeps=['unconditional_target', pp.WhenScheduled('conditional_target')])
            def setup_proc(context: pp.ProcContext) -> str:
                execution_order.append('setup')
                time.sleep(0.05)
                return 'setup_done'

            # Schedule setup and unconditional_target -- setup activates its
            # WhenScheduled rdep (pulling in conditional_target) and its
            # plain rdep fires when unconditional_target is started.
            pp.create('unconditional_target')
            pp.start('setup', 'unconditional_target')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            # All three should have run
            self.assertIn('setup', results)
            self.assertIn('unconditional_target', results)
            self.assertIn('conditional_target', results)
            setup_idx = order.index('setup')
            self.assertLess(
                setup_idx, order.index('unconditional_target'), 'setup should execute before unconditional_target'
            )
            self.assertLess(
                setup_idx, order.index('conditional_target'), 'setup should execute before conditional_target'
            )

    # ------------------------------------------------------------------
    # WhenScheduled with Proc (not Proto)
    # ------------------------------------------------------------------
    def test_when_scheduled_with_proc(self):
        """WhenScheduled also works on Proc (not just Proto)."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='A')
            def a_proc(context: pp.ProcContext) -> str:
                execution_order.append('A')
                return 'A_done'

            @pp.Proc(name='B', rdeps=[pp.WhenScheduled('A')])
            def b_proc(context: pp.ProcContext) -> str:
                execution_order.append('B')
                time.sleep(0.05)
                return 'B_done'

            pp.start('B')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('A', results)
            self.assertIn('B', results)
            b_idx = order.index('B')
            a_idx = order.index('A')
            self.assertLess(b_idx, a_idx, 'B should execute before A')

    # ------------------------------------------------------------------
    # WhenScheduled does not interfere with normal rdeps
    # ------------------------------------------------------------------
    def test_normal_rdeps_still_work(self):
        """Plain string rdeps should continue to work as before."""

        pp.wait_clear()

        @pp.Proc(name='setup', rdeps=['target::[x]'])
        def setup_proc(context: pp.ProcContext) -> str:
            return 'setup_done'

        @pp.Proto(name='target::[x]')
        def target_proc(context: pp.ProcContext, x: str) -> str:
            return f'target_{x}'

        proc_name = pp.create('target::foo')
        pp.start(*proc_name)
        pp.wait_for_all()

        results = pp.results()
        self.assertIn('setup', results)
        self.assertIn('target::foo', results)
        self.assertEqual(results['setup'], 'setup_done')
        self.assertEqual(results['target::foo'], 'target_foo')


class WhenTargetScheduledTest(TestCase):
    """Tests for WhenTargetScheduled (conditional rdep that activates when the target is scheduled)."""

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        mode = os.environ.get('PARPROC_TEST_MODE', 'mp')
        pp.ProcManager.get_inst().set_options(mode=mode, dynamic=False)

    # ------------------------------------------------------------------
    # Basic: A scheduled, B has WhenTargetScheduled('A') -> B auto-created, B before A
    # ------------------------------------------------------------------
    def test_basic_when_target_scheduled(self):
        """Scheduling A should auto-create B (with WhenTargetScheduled('A')), with B running first."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='A')
            def a_proc(context: pp.ProcContext) -> str:
                execution_order.append('A')
                return 'A_done'

            @pp.Proto(name='B', rdeps=[pp.WhenTargetScheduled('A')])
            def b_proc(context: pp.ProcContext) -> str:
                execution_order.append('B')
                time.sleep(0.05)
                return 'B_done'

            proc_name = pp.create('A')
            pp.start(*proc_name)
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('A', results)
            self.assertIn('B', results)
            self.assertEqual(results['A'], 'A_done')
            self.assertEqual(results['B'], 'B_done')

            b_idx = order.index('B')
            a_idx = order.index('A')
            self.assertLess(b_idx, a_idx, 'B should execute before A')

    # ------------------------------------------------------------------
    # Only B scheduled -> B runs alone, A is NOT involved
    # ------------------------------------------------------------------
    def test_only_declaring_scheduled(self):
        """When only B is scheduled, A (the target) should NOT be scheduled."""

        pp.wait_clear()

        @pp.Proto(name='A')
        def a_proc(context: pp.ProcContext) -> str:
            return 'A_done'

        @pp.Proto(name='B', rdeps=[pp.WhenTargetScheduled('A')])
        def b_proc(context: pp.ProcContext) -> str:
            return 'B_done'

        proc_name = pp.create('B')
        pp.start(*proc_name)
        pp.wait_for_all()

        results = pp.results()
        self.assertIn('B', results)
        self.assertEqual(results['B'], 'B_done')
        self.assertNotIn('A', results)

    # ------------------------------------------------------------------
    # Both scheduled explicitly -> B before A
    # ------------------------------------------------------------------
    def test_both_scheduled_explicitly(self):
        """When both A and B are scheduled, B (with WhenTargetScheduled('A')) should run before A."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='A')
            def a_proc(context: pp.ProcContext) -> str:
                execution_order.append('A')
                return 'A_done'

            @pp.Proto(name='B', rdeps=[pp.WhenTargetScheduled('A')])
            def b_proc(context: pp.ProcContext) -> str:
                execution_order.append('B')
                time.sleep(0.05)
                return 'B_done'

            pp.create('A')
            pp.create('B')
            pp.start('B', 'A')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('A', results)
            self.assertIn('B', results)
            b_idx = order.index('B')
            a_idx = order.index('A')
            self.assertLess(b_idx, a_idx, 'B should execute before A')

    # ------------------------------------------------------------------
    # Both scheduled, A first -> B still injected before A
    # ------------------------------------------------------------------
    def test_both_scheduled_a_then_b(self):
        """A listed before B in start(). B should still be injected as dep of A."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='A')
            def a_proc(context: pp.ProcContext) -> str:
                execution_order.append('A')
                return 'A_done'

            @pp.Proto(name='B', rdeps=[pp.WhenTargetScheduled('A')])
            def b_proc(context: pp.ProcContext) -> str:
                execution_order.append('B')
                time.sleep(0.05)
                return 'B_done'

            pp.create('A')
            pp.create('B')
            pp.start('A', 'B')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('A', results)
            self.assertIn('B', results)
            b_idx = order.index('B')
            a_idx = order.index('A')
            self.assertLess(b_idx, a_idx, 'B should execute before A even when A is listed first')

    # ------------------------------------------------------------------
    # Late scheduling failure: A already finished when B is scheduled
    # ------------------------------------------------------------------
    def test_late_scheduling_failure(self):
        """Scheduling B after A is already finished should raise UserError."""

        pp.wait_clear()

        @pp.Proto(name='A')
        def a_proc(context: pp.ProcContext) -> str:
            return 'A_done'

        # Schedule and complete A first
        proc_name = pp.create('A')
        pp.start(*proc_name)
        pp.wait_for_all()

        results = pp.results()
        self.assertIn('A', results)

        # Now define B with WhenTargetScheduled('A') and schedule it
        @pp.Proc(name='B', rdeps=[pp.WhenTargetScheduled('A')])
        def b_proc(context: pp.ProcContext) -> str:
            return 'B_done'

        with self.assertRaises(pp.UserError) as ctx:
            pp.start('B')

        self.assertIn('WhenTargetScheduled', str(ctx.exception))
        self.assertIn('too late', str(ctx.exception).lower())

    # ------------------------------------------------------------------
    # Parameterized: WhenTargetScheduled('deploy::[env]')
    # ------------------------------------------------------------------
    def test_parameterized_when_target_scheduled(self):
        """WhenTargetScheduled with parameterized patterns should extract and fill params."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='deploy::[env]')
            def deploy_proc(context: pp.ProcContext, env: str) -> str:
                execution_order.append(f'deploy::{env}')
                return f'deploy_{env}'

            @pp.Proto(name='build::[env]', rdeps=[pp.WhenTargetScheduled('deploy::[env]')])
            def build_proc(context: pp.ProcContext, env: str) -> str:
                execution_order.append(f'build::{env}')
                time.sleep(0.05)
                return f'build_{env}'

            pp.create('deploy::prod')
            pp.start('deploy::prod')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('build::prod', results)
            self.assertIn('deploy::prod', results)
            self.assertEqual(results['build::prod'], 'build_prod')
            self.assertEqual(results['deploy::prod'], 'deploy_prod')

            build_idx = order.index('build::prod')
            deploy_idx = order.index('deploy::prod')
            self.assertLess(build_idx, deploy_idx, 'build should execute before deploy')

    # ------------------------------------------------------------------
    # Parameterized: only declaring scheduled, target NOT involved
    # ------------------------------------------------------------------
    def test_parameterized_only_declaring_scheduled(self):
        """When only build::prod is scheduled, deploy::prod should NOT run."""

        pp.wait_clear()

        @pp.Proto(name='deploy::[env]')
        def deploy_proc(context: pp.ProcContext, env: str) -> str:
            return f'deploy_{env}'

        @pp.Proto(name='build::[env]', rdeps=[pp.WhenTargetScheduled('deploy::[env]')])
        def build_proc(context: pp.ProcContext, env: str) -> str:
            return f'build_{env}'

        pp.create('build::prod')
        pp.start('build::prod')
        pp.wait_for_all()

        results = pp.results()
        self.assertIn('build::prod', results)
        self.assertNotIn('deploy::prod', results)

    # ------------------------------------------------------------------
    # Cross-pattern: WhenTargetScheduled pattern differs from proto pattern
    # ------------------------------------------------------------------
    def test_cross_pattern_when_target_scheduled(self):
        """WhenTargetScheduled pattern can differ from the declaring proto's pattern."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='k8s.deploy::[target]::[component]')
            def deploy_proc(context: pp.ProcContext, target: str, component: str) -> str:
                execution_order.append(f'deploy::{target}::{component}')
                return f'deploy_{target}_{component}'

            @pp.Proto(name='next.build::[target]', rdeps=[pp.WhenTargetScheduled('k8s.deploy::[target]::frontend')])
            def build_proc(context: pp.ProcContext, target: str) -> str:
                execution_order.append(f'build::{target}')
                time.sleep(0.05)
                return f'build_{target}'

            pp.create('k8s.deploy::stage::frontend')
            pp.start('k8s.deploy::stage::frontend')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('next.build::stage', results)
            self.assertIn('k8s.deploy::stage::frontend', results)
            build_idx = order.index('build::stage')
            deploy_idx = order.index('deploy::stage::frontend')
            self.assertLess(build_idx, deploy_idx, 'build should execute before deploy')

    # ------------------------------------------------------------------
    # Multiple WhenTargetScheduled rdeps on one proto
    # ------------------------------------------------------------------
    def test_multiple_when_target_scheduled_rdeps(self):
        """A proto can have multiple WhenTargetScheduled rdeps."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='target1')
            def target1(context: pp.ProcContext) -> str:
                execution_order.append('target1')
                return 'target1_done'

            @pp.Proto(name='target2')
            def target2(context: pp.ProcContext) -> str:
                execution_order.append('target2')
                return 'target2_done'

            @pp.Proto(name='setup', rdeps=[pp.WhenTargetScheduled('target1'), pp.WhenTargetScheduled('target2')])
            def setup_proc(context: pp.ProcContext) -> str:
                execution_order.append('setup')
                time.sleep(0.05)
                return 'setup_done'

            # Schedule both targets -- setup should be injected as dep of both
            pp.create('target1')
            pp.create('target2')
            pp.start('target1', 'target2')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('setup', results)
            self.assertIn('target1', results)
            self.assertIn('target2', results)
            setup_idx = order.index('setup')
            self.assertLess(setup_idx, order.index('target1'), 'setup should execute before target1')
            self.assertLess(setup_idx, order.index('target2'), 'setup should execute before target2')

    # ------------------------------------------------------------------
    # Only one of multiple targets scheduled
    # ------------------------------------------------------------------
    def test_multiple_targets_only_one_scheduled(self):
        """When setup has WhenTargetScheduled for two targets but only one is scheduled."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='target1')
            def target1(context: pp.ProcContext) -> str:
                execution_order.append('target1')
                return 'target1_done'

            @pp.Proto(name='target2')
            def target2(context: pp.ProcContext) -> str:
                execution_order.append('target2')
                return 'target2_done'

            @pp.Proto(name='setup', rdeps=[pp.WhenTargetScheduled('target1'), pp.WhenTargetScheduled('target2')])
            def setup_proc(context: pp.ProcContext) -> str:
                execution_order.append('setup')
                time.sleep(0.05)
                return 'setup_done'

            # Only schedule target1 -- setup injected as dep of target1 only
            pp.create('target1')
            pp.start('target1')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('setup', results)
            self.assertIn('target1', results)
            self.assertNotIn('target2', results, 'target2 should NOT run')
            setup_idx = order.index('setup')
            self.assertLess(setup_idx, order.index('target1'), 'setup should execute before target1')

    # ------------------------------------------------------------------
    # WhenTargetScheduled with Proc (not Proto)
    # ------------------------------------------------------------------
    def test_when_target_scheduled_with_proc(self):
        """WhenTargetScheduled also works on Proc (not just Proto)."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='A')
            def a_proc(context: pp.ProcContext) -> str:
                execution_order.append('A')
                return 'A_done'

            @pp.Proc(name='B', rdeps=[pp.WhenTargetScheduled('A')])
            def b_proc(context: pp.ProcContext) -> str:
                execution_order.append('B')
                time.sleep(0.05)
                return 'B_done'

            pp.create('A')
            pp.start('A')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('A', results)
            self.assertIn('B', results)
            b_idx = order.index('B')
            a_idx = order.index('A')
            self.assertLess(b_idx, a_idx, 'B should execute before A')

    # ------------------------------------------------------------------
    # Mix of WhenScheduled and WhenTargetScheduled
    # ------------------------------------------------------------------
    def test_mix_when_scheduled_and_when_target_scheduled(self):
        """WhenScheduled and WhenTargetScheduled can coexist on different procs."""

        pp.wait_clear()

        with mp.Manager() as manager:
            execution_order = manager.list()

            @pp.Proto(name='deploy')
            def deploy_proc(context: pp.ProcContext) -> str:
                execution_order.append('deploy')
                return 'deploy_done'

            @pp.Proto(name='build', rdeps=[pp.WhenScheduled('deploy')])
            def build_proc(context: pp.ProcContext) -> str:
                execution_order.append('build')
                time.sleep(0.05)
                return 'build_done'

            @pp.Proto(name='lint', rdeps=[pp.WhenTargetScheduled('deploy')])
            def lint_proc(context: pp.ProcContext) -> str:
                execution_order.append('lint')
                time.sleep(0.05)
                return 'lint_done'

            # Schedule build (pulls in deploy via WhenScheduled).
            # deploy is scheduled, which triggers lint via WhenTargetScheduled.
            pp.create('build')
            pp.start('build')
            pp.wait_for_all()

            results = pp.results()
            order = list(execution_order)

            self.assertIn('build', results)
            self.assertIn('deploy', results)
            self.assertIn('lint', results)
            deploy_idx = order.index('deploy')
            self.assertLess(order.index('build'), deploy_idx, 'build should execute before deploy')
            self.assertLess(order.index('lint'), deploy_idx, 'lint should execute before deploy')

    # ------------------------------------------------------------------
    # WhenTargetScheduled does not interfere with normal rdeps
    # ------------------------------------------------------------------
    def test_normal_rdeps_still_work_with_wts(self):
        """Plain string rdeps should continue to work alongside WhenTargetScheduled."""

        pp.wait_clear()

        @pp.Proc(name='setup', rdeps=['target::[x]'])
        def setup_proc(context: pp.ProcContext) -> str:
            return 'setup_done'

        @pp.Proto(name='target::[x]')
        def target_proc(context: pp.ProcContext, x: str) -> str:
            return f'target_{x}'

        proc_name = pp.create('target::foo')
        pp.start(*proc_name)
        pp.wait_for_all()

        results = pp.results()
        self.assertIn('setup', results)
        self.assertIn('target::foo', results)
        self.assertEqual(results['setup'], 'setup_done')
        self.assertEqual(results['target::foo'], 'target_foo')
