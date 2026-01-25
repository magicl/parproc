#!/usr/bin/env python
# pylint: disable=unused-argument

import argparse
import time

import parproc as pp  # pylint: disable=import-error


def define_procs():
    """Define all processes."""

    @pp.Proc(now=True)
    def a(context):
        time.sleep(1)

    @pp.Proc(now=True, deps=['a'])
    def a_a(context):
        time.sleep(3)

    @pp.Proc(now=True, deps=['a'])
    def a_b(context):
        time.sleep(2)

    @pp.Proc(now=True, deps=['a_a'])
    def a_a_a(context):
        time.sleep(1)

    @pp.Proc(now=True, deps=['a_a'])
    def a_a_b(context):
        time.sleep(1)

    @pp.Proc(now=True)
    def x(context):
        time.sleep(1)
        print('Internal error caused by x')
        raise Exception('Failure')  # pylint: disable=broad-exception-raised

    @pp.Proc(now=True, deps=['x'])
    def x_a(context):
        time.sleep(3)

    @pp.Proc(now=True, deps=['x'])
    def x_b(context):
        time.sleep(2)

    @pp.Proc(now=True, deps=['x_a'])
    def x_a_a(context):
        time.sleep(1)

    @pp.Proc(now=True, deps=['x_a'])
    def x_a_b(context):
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description='Shows behavior when dependencies fail')
    parser.add_argument(
        '--no-live', action='store_false', dest='live', default=True, help='Disable live/dynamic terminal output'
    )
    args = parser.parse_args()

    pp.set_options(dynamic=args.live)
    define_procs()

    try:
        pp.wait_for_all()
    except:  # pylint: disable=bare-except # nosec try_except_pass
        pass


if __name__ == '__main__':
    main()
