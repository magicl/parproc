# pylint: disable=unused-argument
import logging
from unittest import TestCase

import parproc as pp
from parproc.term import Term


class ErrorFormatTest(TestCase):

    def test_python_sh(self):
        """Errors from 'sh' in subcommands"""

        error_msg = """
  foooooo
  RAN: /bin/bash -c pip3 install flycheck

  STDOUT:
  def
  STDERR:
  abc
"""

        expected_output = """  RAN: /bin/bash -c pip3 install flycheck

  STDOUT:
  def
  STDERR:
  abc
"""

        chunks = Term.extract_error_log(error_msg, task_failed=True)
        # Extract content from chunks and join
        output = '\n'.join(chunk.content for chunk in chunks)
        logging.info(output)

        self.assertEqual(output, expected_output)

    def test_python_sh_full(self):
        """Errors from 'sh' in subcommands where e.stderr is available"""

        error_msg = """
  foooooo
  RAN: /bin/bash -c pip3 install flycheck

  STDOUT:
  def
  STDERR:
  abc
  STDERR_FULL:
  fooo
"""

        expected_output = """  RAN: /bin/bash -c pip3 install flycheck

  STDOUT:
  def
  STDERR:
  fooo
"""

        chunks = Term.extract_error_log(error_msg, task_failed=True)
        # Extract content from chunks and join
        output = '\n'.join(chunk.content for chunk in chunks)
        logging.info(output)

        self.assertEqual(output, expected_output)

    def test_full_log_on_failure_returns_complete_log(self):
        """When enabled, failed tasks should print the complete log output."""
        error_msg = """line 1
line 2 warning
line 3
line 4"""

        chunks = Term.extract_error_log(error_msg, task_failed=True, full_log_on_failure=True)
        output = '\n'.join(chunk.content for chunk in chunks)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(output, error_msg)

    def test_log_ignore_always_filters_keyword_lines(self):
        """Always-ignore rules remove matching warning/error lines from extracted snippets."""
        error_msg = """line 1
warning: benign deprecation warning
line 3
error: real issue"""

        chunks = Term.extract_error_log(
            error_msg,
            task_failed=False,
            log_ignore=[pp.IgnoreLogAlways(r'benign deprecation warning')],
            task_succeeded=True,
        )
        output = '\n'.join(chunk.content for chunk in chunks)

        self.assertNotIn('benign deprecation warning', output)
        self.assertIn('real issue', output)

    def test_log_ignore_if_succeeded_applies_conditionally(self):
        """Ignore-on-success filters only when task_succeeded is True."""
        log_text = """line 1
warning: flaky but acceptable
line 3"""
        success_chunks = Term.extract_error_log(
            log_text,
            task_failed=False,
            log_ignore=[pp.IgnoreLogIfSucceeded(r'flaky but acceptable')],
            task_succeeded=True,
        )
        self.assertEqual(success_chunks, [])

        failed_chunks = Term.extract_error_log(
            log_text,
            task_failed=True,
            log_ignore=[pp.IgnoreLogIfSucceeded(r'flaky but acceptable')],
            task_succeeded=False,
        )
        failed_output = '\n'.join(chunk.content for chunk in failed_chunks)
        self.assertIn('flaky but acceptable', failed_output)

    def test_log_ignore_applies_to_parser_output(self):
        """Parser-derived output should also honor ignore rules."""
        error_msg = """
  foooooo
  RAN: /bin/bash -c pip3 install flycheck

  STDOUT:
  def
  STDERR:
  abc
"""
        chunks = Term.extract_error_log(
            error_msg,
            task_failed=True,
            log_ignore=[pp.IgnoreLogAlways(r'^\s*STDOUT:$')],
        )
        output = '\n'.join(chunk.content for chunk in chunks)
        self.assertNotIn('STDOUT:', output)
        self.assertIn('STDERR:', output)
