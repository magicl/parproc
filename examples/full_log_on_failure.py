#!/usr/bin/env python
# pylint: disable=unused-argument

import argparse
import time

import parproc as pp  # pylint: disable=import-error


def define_procs() -> None:
    """Define tasks where one failing task emits a lot of output."""

    @pp.Proc(name='chatty_failure', now=True)
    def chatty_failure(context):
        for line_no in range(1, 201):
            print(f'chatty_failure log line {line_no:03d}: simulated noisy output before failure')
        raise Exception('Intentional failure after long output')  # pylint: disable=broad-exception-raised

    @pp.Proc(name='downstream', now=True, deps=['chatty_failure'])
    def downstream(context):
        time.sleep(0.2)
        print('This should not run because dependency failed')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Example that prints the full failing proc log with full_log_on_failure enabled'
    )
    parser.add_argument(
        '--no-live', action='store_false', dest='live', default=True, help='Disable live/dynamic terminal output'
    )
    args = parser.parse_args()

    pp.set_options(dynamic=args.live, task_db_path='.output/parproc.db', full_log_on_failure=True)
    define_procs()

    try:
        pp.wait_for_all()
    except:  # pylint: disable=bare-except # nosec try_except_pass
        pass


if __name__ == '__main__':
    main()
