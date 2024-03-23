#!/usr/bin/python3
# pylint: disable=unused-argument

import time

import parproc as pp

print('Shows behavior when dependencies fail')


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


try:
    pp.wait_for_all()
except:  # pylint: disable=bare-except
    pass
