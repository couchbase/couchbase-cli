#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import math
import itertools

BIG_VALUE = 2 ** 60
SMALL_VALUE = - (2 ** 60)

def hostport(hoststring, default_port=8091):
    """ finds the host and port given a host:port string """
    try:
        host, port = hoststring.split(':')
        port = int(port)
    except ValueError:
        host = hoststring
        port = default_port

    return (host, port)

def time_label(s):
    # -(2**64) -> '-inf'
    # 2**64 -> 'inf'
    # 0 -> '0'
    # 4 -> '4us'
    # 838384 -> '838ms'
    # 8283852 -> '8s'
    if s > BIG_VALUE:
        return 'inf'
    elif s < SMALL_VALUE:
        return '-inf'
    elif s == 0:
        return '0'
    product = 1
    sizes = (('us', 1), ('ms', 1000), ('s', 1000), ('m', 60))
    sizeMap = []
    for l,sz in sizes:
        product = sz * product
        sizeMap.insert(0, (l, product))
    lbl, factor = itertools.dropwhile(lambda x: x[1] > s, sizeMap).next()
    return "%d %s" % (s / factor, lbl)

def size_label(s):
    if type(s) in (int, long, float, complex) :
        if s == 0:
            return "0"
        sizes=['', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
        e = math.floor(math.log(abs(s), 1024))
        suffix = sizes[int(e)]
        return "%d %s" % (s/(1024 ** math.floor(e)), suffix)
    else:
        return s

def linreg(X, Y):
    """
    Summary
        Linear regression of y = ax + b
    Usage
        real, real, real = linreg(list, list)
    Returns coefficients to the regression line "y=ax+b" from x[] and y[], and R^2 Value
    """
    if len(X) != len(Y):  raise ValueError, 'unequal length'
    N = len(X)
    Sx = Sy = Sxx = Syy = Sxy = 0.0
    for x, y in map(None, X, Y):
        Sx = Sx + x
        Sy = Sy + y
        Sxx = Sxx + x*x
        Syy = Syy + y*y
        Sxy = Sxy + x*y
    det = Sxx * N - Sx * Sx
    if det == 0:
        return 0, 0
    else:
        a, b = (Sxy * N - Sy * Sx)/det, (Sxx * Sy - Sx * Sxy)/det
        return a, b

def two_pass_variance(data):
    n    = 0
    sum1 = 0
    sum2 = 0

    for x in data:
        n    = n + 1
        sum1 = sum1 + x

    mean = sum1/n
    for x in data:
        sum2 = sum2 + (x - mean)*(x - mean)
    if n <= 1:
        return 0
    variance = sum2/(n - 1)
    return variance

def pretty_float(number, precision=2):
    return '%.*f' % (precision, number)

def pretty_print(obj):
    return json.dumps(obj, indent=4, sort_keys=True)
