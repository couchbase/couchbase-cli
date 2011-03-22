#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

def commands_usage():
    return """
  server-list       list all servers in a cluster
  server-info       show details on one server
  server-add        add one or more servers to the cluster
  server-readd      readd a server that was failed over
  rebalance         start a cluster rebalancing
  rebalance-stop    stop current cluster rebalancing
  rebalance-status  show status of current cluster rebalancing
  failover          failover one or more servers
  cluster-init      set the username,password and port of the cluster
  node-init         set node specific parameters
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
    print "COMMANDs include" + commands_usage()

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

server-readd OPTIONS:
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

cluster-init OPTIONS:
  --cluster-init-username=USER      new admin username
  --cluster-init-password=PASSWORD  new admin password
  --cluster-init-port=PORT          new cluster REST/http port
  --cluster-init-ramsize=RAMSIZEMB  per node ram quota in MB

node-init OPTIONS:
  --node-init-data-path=PATH        per node path to store data

bucket-* OPTIONS:
  --bucket=BUCKETNAME               bucket to act on
  --bucket-type=TYPE                memcached or membase
  --bucket-port=PORT                supports ASCII protocol and is auth-less
  --bucket-password=PASSWORD        standard port, exclusive with bucket-port
  --bucket-ramsize=RAMSIZEMB        ram quota in MB
  --bucket-replica=COUNT            replication count

The default PORT number is 8091.

EXAMPLES:
  List servers in a cluster:
    membase server-list -c 192.168.0.1:8091

  Server information:
    membase server-info -c 192.168.0.1:8091

  Add a node to a cluster, but do not rebalance:
    membase server-add -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091

  Add a node to a cluster and rebalance:
    membase rebalance -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091

  Remove a node from a cluster and rebalance:
    membase rebalance -c 192.168.0.1:8091 \\
       --server-remove=192.168.0.2:8091

  Remove and add nodes from/to a cluster and rebalance:
    membase rebalance -c 192.168.0.1:8091 \\
      --server-remove=192.168.0.2 \\
      --server-add=192.168.0.4

  Stop the current rebalancing:
    membase rebalance-stop -c 192.168.0.1:8091

  Change the username, password, port and ram quota:
    membase cluster-init -c 192.168.0.1:8091 \\
       --cluster-init-username=Administrator \\
       --cluster-init-password=password \\
       --cluster-init-port=8080 \\
       --cluster-init-ramsize=300

  Change the data path:
     membase node-init -c 192.168.0.1:8091 \\
       --node-init-data-path=/tmp

  List buckets in a cluster:
    membase bucket-list -c 192.168.0.1:8091

  Create a new dedicated port membase bucket:
    membase bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=membase \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-replica=1

  Create a new sasl memcached bucket:
    membase bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=memcached \\
       --bucket-password=password \\
       --bucket-ramsize=200

  Modify a dedicated port bucket:
    membase bucket-edit -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-port=11222 \\
       --bucket-ramsize=400

  Delete a bucket:
    membase bucket-delete -c 192.168.0.1:8091 \\
       --bucket=test_bucket

"""

    sys.exit(2)
