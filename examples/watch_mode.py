#!/usr/bin/env python
# pylint: disable=unused-argument

"""Watch-mode demo: two input-driven tasks and one that depends on both."""

import argparse
import os

import parproc as pp  # pylint: disable=import-error

_EXAMPLES_DIR = os.path.dirname(os.path.abspath(__file__))
FILE1 = os.path.join(_EXAMPLES_DIR, 'file1.txt')
FILE2 = os.path.join(_EXAMPLES_DIR, 'file2.txt')


def define_procs() -> None:
    """Register procs: one per input file, plus a merge task depending on both."""

    @pp.Proto(name='task_file1', inputs=[FILE1], now=True)
    def task_file1(context):
        print('[task_file1] ran (watched input: file1.txt)')

    @pp.Proto(name='task_file2', inputs=[FILE2], now=True)
    def task_file2(context):
        print('[task_file2] ran (watched input: file2.txt)')

    @pp.Proto(name='task_both', deps=['task_file1', 'task_file2'], now=True)
    def task_both(context):
        print('[task_both] ran (depends on task_file1 and task_file2)')


def main() -> None:
    parser = argparse.ArgumentParser(description='Watch file1.txt / file2.txt and re-run dependent tasks')
    parser.add_argument(
        '--no-live', action='store_false', dest='live', default=True, help='Disable live/dynamic terminal output'
    )
    args = parser.parse_args()

    print()
    print('While this script is running, edit and save the text files below to trigger tasks:')
    print(f'  {FILE1}')
    print(f'  {FILE2}')
    print()
    print('  task_file1 — input: file1.txt')
    print('  task_file2 — input: file2.txt')
    print('  task_both  — depends on task_file1 and task_file2')
    print()
    print('Starting initial run, then watch mode (Ctrl+C to exit).')
    print()

    pp.set_options(dynamic=args.live, task_db_path='.output/parproc.db')
    define_procs()

    pp.run('task_file1', full=True)
    pp.run('task_file2', full=True)
    pp.run('task_both', full=True)
    pp.watch()


if __name__ == '__main__':
    main()
