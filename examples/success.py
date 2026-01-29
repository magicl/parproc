#!/usr/bin/env python
# pylint: disable=unused-argument,duplicate-code

import argparse
import time

import parproc as pp  # pylint: disable=import-error


def define_procs():
    """Define all processes."""

    @pp.Proc(now=True)
    def func0(context):
        time.sleep(1)

    @pp.Proc(now=True)
    def func1(context):
        print('some output')
        time.sleep(3)

    @pp.Proc(now=True, deps=['func0', 'func1'])
    def func2(context):
        time.sleep(2)

    @pp.Proc(now=True, deps=['func2'])
    def func3(context):
        time.sleep(1)

    @pp.Proc(now=True, deps=['func2', 'func3'])
    def func4(context):
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description='Multiple concurrent processes with dependencies')
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
