# pylint: disable=unused-argument
import logging
from unittest import TestCase

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

        output = '\n'.join(Term.extract_error_log(error_msg))
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

        output = '\n'.join(Term.extract_error_log(error_msg))
        logging.info(output)

        self.assertEqual(output, expected_output)
