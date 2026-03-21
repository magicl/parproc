#!/usr/bin/env python
# pylint: disable=unused-argument

import argparse
import time

import parproc as pp  # pylint: disable=import-error


def _add_failing_task(delay: int) -> None:
    """Register one proc; *delay* is bound in the caller (no loop closure)."""

    @pp.Proc(name=f'task_{delay}s', now=True)
    def task(context, sleep_s=delay):
        time.sleep(sleep_s)
        for line in range(10):
            print(f'Error in task completing after {sleep_s}s: failure detail line {line + 1} of 10')
        raise Exception(f'Task failed after {sleep_s}s')  # pylint: disable=broad-exception-raised


def define_procs():
    """Define 10 tasks that all start at once and fail with 10-line error messages."""

    for i in range(10):
        _add_failing_task(i + 1)


def main():
    parser = argparse.ArgumentParser(description='10 tasks with long error output finishing at staggered times')
    parser.add_argument(
        '--no-live', action='store_false', dest='live', default=True, help='Disable live/dynamic terminal output'
    )
    args = parser.parse_args()

    pp.set_options(dynamic=args.live, task_db_path='.output/parproc.db')
    define_procs()

    try:
        pp.wait_for_all()
    except:  # pylint: disable=bare-except # nosec try_except_pass
        pass


if __name__ == '__main__':
    main()
