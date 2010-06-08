#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
  membase_info.py

  Contains functions such as usage()

"""

import sys


def usage(error_msg=''):
    print >> sys.stderr, ''
    if error_msg:
        print >> sys.stderr, 'ERROR: %s' % error_msg

    print >> sys.stderr, """Membase Commandline Tool Set

Command line interface to Membase cluster management via REST

Usage: membase command [OPTIONS]

commands:
  server-list   Provide a list of membase servers within a given cluster
  bucket-list   Provide a list of buckets within a given cluster
  bucket-flush  Flush a given bucket

OPTIONS:
  -c, --cluster [=host:port]
  -p, --password[=password]
  -u, --user[=username]

EXAMPLES:
  membase server-list -c 192.168.0.1:8080
"""

    sys.exit(2)


