#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
util.py

This script will contain helper methods for the membase command line
tools to reduce code redundancy

"""

def hostport(hoststring):
    """ hostport() Used to find the host and port of a host:port string """
    try:
        host, port = hoststring.split(':')
        port = int(port)
    except ValueError:
        host = hoststring
        port = 8080

    return [ host, port ]
