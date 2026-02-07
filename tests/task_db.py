# pylint: disable=unused-argument
import logging
import os
import sqlite3
import tempfile
import time
from typing import cast
from unittest import TestCase

import parproc as pp
from parproc.task_db import get_expected_duration


class TaskDBTest(TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        pp.ProcManager.get_inst().set_options(dynamic=False)

    def test_task_db_records_runs_and_returns_expected_duration(self):
        """With task_db_path set, runs are recorded and get_expected_duration returns a float."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            pp.wait_clear()
            pp.ProcManager.get_inst().set_options(dynamic=False, task_db_path=path)

            @pp.Proc(name='proc_a', now=True)
            def proc_a(context):
                time.sleep(0.05)
                return 'ok'

            pp.wait_for_all()

            conn = sqlite3.connect(path)
            try:
                rows = conn.execute('SELECT task_name, started_at, ended_at, status FROM runs').fetchall()
                conn.close()
            except Exception:
                conn.close()
                raise
            self.assertEqual(len(rows), 1)
            task_name, started_at, ended_at, status = rows[0]
            self.assertEqual(task_name, 'proc_a')
            self.assertIsNotNone(started_at)
            self.assertIsNotNone(ended_at)
            self.assertEqual(status, 'success')
            # ISO format with optional timezone
            self.assertIn('T', started_at)
            self.assertIn('T', ended_at)

            eta = get_expected_duration('proc_a')
            self.assertIsInstance(eta, float)
            self.assertIsNotNone(eta)
            eta = cast(float, eta)
            self.assertGreater(eta, 0)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    def test_task_db_disabled_when_path_none(self):
        """When task_db_path is set then set to None, no DB is used."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            pp.wait_clear()
            pp.ProcManager.get_inst().set_options(dynamic=False, task_db_path=path)

            @pp.Proc(name='proc_b', now=True)
            def proc_b(context):
                return 'ok'

            pp.wait_for_all()

            # Disable task DB
            pp.ProcManager.get_inst().set_options(task_db_path=None)

            # When disabled, get_expected_duration returns None for any task
            self.assertIsNone(get_expected_duration('proc_b'))
            self.assertIsNone(get_expected_duration('proc_c'))
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    def test_get_expected_duration_decay_weighted_toward_recent(self):
        """Two runs with different durations: expected duration is weighted toward the most recent."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            pp.wait_clear()
            pp.ProcManager.get_inst().set_options(dynamic=False, task_db_path=path)

            @pp.Proc(name='slow_first', now=True)
            def slow_first(context):
                time.sleep(0.15)
                return 'ok'

            pp.wait_for_all()

            # Second run: same proc name (simulate re-run), shorter
            pp.wait_clear()
            pp.ProcManager.get_inst().set_options(dynamic=False, task_db_path=path)

            @pp.Proc(name='slow_first', now=True)
            def slow_first_again(context):
                time.sleep(0.05)
                return 'ok'

            pp.wait_for_all()

            # With decay=0.8, most recent (0.05s) has weight 1, previous (0.15s) has weight 0.8
            # weighted avg = (0.05*1 + 0.15*0.8) / (1 + 0.8) ≈ 0.094
            eta = get_expected_duration('slow_first', window=10, decay=0.8)
            self.assertIsInstance(eta, float)
            self.assertIsNotNone(eta)
            eta = cast(float, eta)
            self.assertGreater(eta, 0.03)
            self.assertLess(eta, 0.2)
            # Should be closer to 0.05 than to 0.15
            self.assertLess(eta, 0.12)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
