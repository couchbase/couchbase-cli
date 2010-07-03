#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  membase_info.py

  This module contains informational functions such as usage()

"""

import sys


def usage(error_msg=''):
    print ''
    if error_msg:
        print "ERROR: %s\n" % error_msg

    print """Membase Commandline Tool Set

Command line interface to Membase cluster management via REST

Usage: membase command [OPTIONS]

commands:
  server-list       Provide a list of membase servers within a given cluster
  bucket-list       Provide a list of buckets within a given cluster
  bucket-flush      Flush a given bucket
  rebalance         Start a rebalance operation
  rebalance-stop    Stop a rebalance
  rebalance-status  Status of a rebalance
  server-add        Add a server
  server-info       Display information about a server

COMMON OPTIONS:
  -c, --cluster [=host:port]
  -d, --debug
  -o, --output [=json|standard]
  -p, --password[=password]
  -u, --user[=username]
  -v, --verbose

bucket-* OPTIONS:

  -b, --buckets[=buckets]

server-add and rebalance-* OPTIONS:

  --server-add[=server]                 The server being added
  --server-add-username[=user]          The user specified for the server being
                                        added
  --server-add-password[=password]      The password specified for the server
                                        being added
  --server-remove[=server]              The server being remove removed
  --server-remove-username[=user]       The user specified for the server being
                                        removed
  --server-remove-password[=password]   The password specified for the server
                                        being added


EXAMPLES:

  List servers in a cluster:
    membase server-list -c 192.168.0.1:8080

  List buckets in a cluster:
    membase bucket-list -c 192.168.0.1:8080

  Add a node to a cluster, but do not rebalance:
    membase server-add -c 192.168.0.1:8080 --server-add=192.168.0.2

  Add a node to a cluster and rebalance:
    membase rebalance -c 192.168.0.1:8080 --server-add=192.168.0.2
    membase rebalance -c 192.168.0.1:8080 --server-add=192.168.0.2 \\
    --server-add=192.168.0.3

  Remove a node from a cluster and rebalance:
    membase rebalance -c 192.168.0.1:8080 --server-remove=192.168.0.2

  Remove and add nodes from/to a cluster and rebalance:
    membase rebalance -c 192.168.0.1:8080 --server-remove=192.168.0.2 \\
    --server-add=192.168.0.4

  Cancel rebalance:
    membase rebalance-stop -c 192.168.0.1:8080

  Server Information:
    membase server-info -c 192.168.0.1:8080 [server information options]

"""

    sys.exit(2)
