#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

def commands_usage():
    return """
  server-list       list all servers in a cluster
  server-info       show details on one server
  server-add        add one or more servers to the cluster
  rebalance         start a cluster rebalancing
  rebalance-stop    stop current cluster rebalancing
  rebalance-status  show status of current cluster rebalancing
  failover          failover one or more servers
  bucket-list       list all buckets in a cluster
  bucket-create     add a new bucket to the cluster
  bucket-edit       modify an existing bucket
  bucket-delete     delete an existing bucket
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
        print "ERROR: %s" % error_msg
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

failover OPTIONS:
  --server-failover=HOST[:PORT]     server to failover

bucket-* OPTIONS:
  --bucket=BUCKETNAME               bucket to act on
  --bucket-port=PORT                supports ASCII protocol and is auth-less
  --bucket-password=PASSWORD        standard port, exclusive with bucket-port
  --bucket-ramsize=RAMSIZEMB        ram quota in MB
  --bucket-hddsize=HDDSIZEGB        disk quota in GB
  --bucket-replica=COUNT            replication count

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

  Create a new dedicated port bucket:
    membase bucket-create -c 192.168.0.1:8080 \\
       --bucket=test_bucket \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-hddsize=1 \\
       --bucket-replica=1

  Modify a dedicated port bucket:
    membase bucket-edit -c 192.168.0.1:8080 \\
       --bucket=test_bucket \\
       --bucket-port=11222 \\
       --bucket-ramsize=400 \\
       --bucket-hddsize=1

  Delete a bucket:
    membase bucket-delete -c 192.168.0.1:8080 \\
       --bucket=test_bucket

"""

    sys.exit(2)
