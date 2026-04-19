#!/usr/bin/env python
# pylint: disable=unused-argument

import argparse
import os
import tempfile

import parproc as pp  # pylint: disable=import-error


def define_procs(tmpdir: str) -> None:
    """Define a task that intentionally misses declared outputs."""
    missing_a = os.path.join(tmpdir, 'missing-a.txt')
    missing_b = os.path.join(tmpdir, 'missing-b.txt')

    @pp.Proto(name='outputs-not-refreshed-error', now=True, outputs=[missing_a, missing_b])
    def outputs_not_refreshed_error(context):
        # Intentionally do not create declared outputs, so terminal output can be previewed.
        return 'intentional-output-miss'


def main() -> None:
    parser = argparse.ArgumentParser(description='Example that triggers ERROR_OUTPUTS_NOT_REFRESHED.')
    parser.add_argument(
        '--no-live', action='store_false', dest='live', default=True, help='Disable live/dynamic terminal output'
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix='parproc_outputs_not_refreshed_') as tmpdir:
        pp.set_options(dynamic=args.live, task_db_path='.output/parproc.db')
        define_procs(tmpdir)
        pp.run('outputs-not-refreshed-error')
        pp.wait_for_all(exception_on_failure=False)


if __name__ == '__main__':
    main()
