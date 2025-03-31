#!/usr/bin/env python
# pylint: disable=unused-argument

import time

import parproc as pp  # pylint: disable=import-error

print('Multiple concurrent processes, where a couple are failing at various times')


@pp.Proc(now=True)
def func0(context):
    time.sleep(1)


@pp.Proc(now=True)
def func1(context):
    print('some output')
    time.sleep(3)


@pp.Proc(now=True)
def func2(context, deps=['func0', 'func1']):
    time.sleep(2)


@pp.Proc(now=True)
def func3(context, deps=['func2']):
    time.sleep(1)


@pp.Proc(now=True)
def func4(context, deps=['func2', 'func3']):
    time.sleep(5)


try:
    pp.wait_for_all()
except:  # pylint: disable=bare-except # nosec try_except_pass
    pass
