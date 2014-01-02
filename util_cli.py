#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import itertools
import locale
import simplejson as json
import math
import os
import sys

try:
    locale.setlocale(locale.LC_ALL, '')
except:
    os.environ['LC_ALL'] = "en_US.UTF-8"
    try:
        locale.setlocale(locale.LC_ALL, '')
    except:
        sys.exit("Error: unsupported locale setting, please set LC_ALL to en_US.UTF-8")

BIG_VALUE = 2 ** 60
SMALL_VALUE = - (2 ** 60)

def devisible(a, b):
    if b == 0:
        return False
    return a % b == 0

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
    sizes = (('us', 1), ('ms', 1000), ('sec', 1000), ('min', 60))
    sizeMap = []
    for l,sz in sizes:
        product = sz * product
        sizeMap.insert(0, (l, product))
    try:
        lbl, factor = itertools.dropwhile(lambda x: x[1] > s, sizeMap).next()
    except StopIteration:
        lbl, factor = sizeMap[-1]
    if devisible(s, factor):
        return '%d %s' % (s / factor, lbl)
    else:
        return '%.*f %s' % (3, s * 1.0/factor, lbl)

def size_label(s):
    if type(s) in (int, long, float, complex) :
        if s == 0:
            return "0"
        sizes=['', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
        e = math.floor(math.log(abs(s), 1024))
        suffix = sizes[int(e)]
        if devisible(s, 1024 ** math.floor(e)):
            return '%d %s' % ( s / (1024 ** math.floor(e)), suffix)
        else:
            return "%.*f %s" % (3, s *1.0/(1024 ** math.floor(e)), suffix)
    else:
        return s

def size_convert(s, unit):
    if type(s) in (int, long, float, complex) :
        if s == 0:
            return s
        sizes=['', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
        try:
            e = sizes.index(unit.upper())
        except Exception:
            e = 0
        if devisible(s, 1024 ** math.floor(e)):
            return '%d' % (s / (1024 ** math.floor(e)))
        else:
            return "%.*f" % (3, s *1.0/(1024 ** math.floor(e)))
    else:
        return s

def number_label(s):
    if type(s) in (int, long, float, complex) :
        if s < 0:
            s = -s
            flag = "-"
        else:
            flag = ""
        if s < 1:
            return "0"
        sizes=['', 'thousand', 'million', 'billion', 'trillion', 'quadrillion', 'quintillion']
        e = math.floor(math.log(abs(s), 1000))
        if e < 0:
           e = 0
        suffix = sizes[int(e)]
        if devisible(s, 1000 ** math.floor(e)):
            return "%s%d %s" % (flag, s / (1000 ** math.floor(e)), suffix)
        else:
            return "%s%.*f %s" % (flag, 2, s *1.0/(1000 ** math.floor(e)), suffix)
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

def abnormal_extract(vals, threshold, op = '>='):
    abnormal = []
    begin_index = -1
    seg_count = 0

    for index, sample in enumerate(vals):
        ev = evalfunc(sample, threshold, op)
        if ev is None:
            return abnormal
        elif ev:
            if begin_index < 0:
                begin_index = index
            seg_count += 1
        else:
            if begin_index >= 0:
                abnormal.append((begin_index, seg_count))
                begin_index = -1
                seg_count = 0

    if begin_index >= 0:
        abnormal.append((begin_index, seg_count))
    return abnormal

def evalfunc(value, threshold, op):
    rt = None

    func = {'>=' : lambda x, y: x >= y,
            '>' : lambda x, y: x > y,
            '==' : lambda x, y: x == y,
            '<' : lambda x, y: x < y,
            '!=' : lambda x, y: x != y,
           }.get(op, None)
    if func is None:
        return rt
    return func(value, threshold)

def pretty_float(number, precision=2):
    return '%.*f' % (precision, number)

def pretty_print(obj):
    return json.dumps(obj, indent=4, sort_keys=True)

def pretty_datetime(number, timeonly=False):
    if timeonly:
        return str(datetime.datetime.fromtimestamp(number/1000).time())
    else:
        timestamp = datetime.datetime.fromtimestamp(number/1000)
        return timestamp.strftime('%x') + ' ' + str(timestamp.time())
