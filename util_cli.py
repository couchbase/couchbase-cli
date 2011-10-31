#!/usr/bin/env python
# -*- coding: utf-8 -*-

def hostport(hoststring, default_port=8091):
    """ finds the host and port given a host:port string """
    try:
        host, port = hoststring.split(':')
        port = int(port)
    except ValueError:
        host = hoststring
        port = default_port

    return (host, port)
