#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

def commands_usage():
    return """
  server-list       list all servers in a cluster
  server-info       show details on one server
  server-add        add a server to cluster
  rebalance         start a cluster rebalancing
  rebalance-stop    stop current cluster rebalancing
  rebalance-status  show status of current cluster rebalancing
  bucket-list       list all buckets in a cluster
  bucket-flush      flush a given bucket
  help              show longer usage/help and examples
"""

def short_usage():
    print "usage: membase COMMAND CLUSTER [OPTIONS]"
    print ""
    print "CLUSTER is --cluster=HOST[:PORT] or -c HOST[:PORT]"
    print ""
    print "COMMAND's include" + commands_usage()

    sys.exit(2)

def usage(error_msg=''):
    if error_msg:
        print "ERROR: %s\n" % error_msg
        sys.exit(2)

    print """membase - command-line cluster administration tool

usage: membase COMMAND CLUSTER [OPTIONS]

COMMAND:""" + commands_usage() + """
CLUSTER:
  --cluster=HOST[:PORT] or -c HOST[:PORT]

OPTIONS:
  -u USERNAME, --user=USERNAME      admin username of the cluster
  -p PASSWORD, --password=PASSWORD  admin password of the cluster
  -o KIND, --output=KIND            KIND is json or standard
  -v, --verbose
  -d, --debug

server-add OPTIONS:
  --server-add=HOST[:PORT]          server to be added
  --server-add-username=USERNAME    admin username for the
                                    server to be added
  --server-add-password=PASSWORD    admin password for the
                                    server to be added

rebalance OPTIONS:
  --server-add*                     see server-add OPTIONS
  --server-remove=HOST[:PORT]       the server to be removed

bucket-* OPTIONS:
  --buckets[=buckets]

The default PORT number is 8080.

EXAMPLES:
  List servers in a cluster:
    membase server-list -c 192.168.0.1:8080

  Server information:
    membase server-info -c 192.168.0.1:8080

  Add a node to a cluster, but do not rebalance:
    membase server-add -c 192.168.0.1:8080 \\
       --server-add=192.168.0.2:8080

  Add a node to a cluster and rebalance:
    membase rebalance -c 192.168.0.1:8080 \\
       --server-add=192.168.0.2:8080

  Remove a node from a cluster and rebalance:
    membase rebalance -c 192.168.0.1:8080 \\
       --server-remove=192.168.0.2:8080

  Remove and add nodes from/to a cluster and rebalance:
    membase rebalance -c 192.168.0.1:8080 \\
      --server-remove=192.168.0.2 \\
      --server-add=192.168.0.4

  Stop the current rebalancing:
    membase rebalance-stop -c 192.168.0.1:8080

  List buckets in a cluster:
    membase bucket-list -c 192.168.0.1:8080
"""

    sys.exit(2)
