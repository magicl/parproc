#!/usr/bin/python3
# pylint: disable=unused-argument

import time

import parproc as pp  # pylint: disable=import-error

print('Multiple concurrent processes, where a couple are failing at various times')


@pp.Proc(now=True)
def func0(context):
    time.sleep(1)
    print('Internal error caused by X')
    raise Exception('Failure')  # pylint: disable=broad-exception-raised


@pp.Proc(now=True)
def func1(context):
    time.sleep(3)
    print('Another internal error message')
    raise Exception('Failure')  # pylint: disable=broad-exception-raised


@pp.Proc(now=True)
def func2(context):
    time.sleep(2)


@pp.Proc(now=True)
def func3(context):
    time.sleep(1)


@pp.Proc(now=True)
def func4(context):
    time.sleep(5)


try:
    pp.wait_for_all()
except:  # pylint: disable=bare-except # nosec try_except_pass
    pass
