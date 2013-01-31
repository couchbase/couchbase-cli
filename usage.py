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
    print "usage: couchbase-cli COMMAND CLUSTER [OPTIONS]"
    print ""
    print "CLUSTER is --cluster=HOST[:PORT] or -c HOST[:PORT]"
    print ""
    print "COMMANDs include" + commands_usage()

    sys.exit(2)

def usage(error_msg=''):
    if error_msg:
        print "ERROR: %s" % error_msg
        sys.exit(2)

    print """couchbase-cli - command-line cluster administration tool

usage: couchbase-cli COMMAND CLUSTER [OPTIONS]

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
  --bucket-type=TYPE                memcached or couchbase
  --bucket-port=PORT                supports ASCII protocol and is auth-less
  --bucket-password=PASSWORD        standard port, exclusive with bucket-port
  --bucket-ramsize=RAMSIZEMB        ram quota in MB
  --bucket-replica=COUNT            replication count
  --wait                            wait for bucket create to be complete before returning

The default PORT number is 8091.

EXAMPLES:
  List servers in a cluster:
    couchbase-cli server-list -c 192.168.0.1:8091

  Server information:
    couchbase-cli server-info -c 192.168.0.1:8091

  Add a node to a cluster, but do not rebalance:
    couchbase-cli server-add -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091 \\
       --server-add-username=Administrator \\
       --server-add-password=password

  Add a node to a cluster and rebalance:
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091 \\
       --server-add-username=Administrator \\
       --server-add-password=password

  Remove a node from a cluster and rebalance:
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
       --server-remove=192.168.0.2:8091

  Remove and add nodes from/to a cluster and rebalance:
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
      --server-remove=192.168.0.2 \\
      --server-add=192.168.0.4 \\
      --server-add-username=Administrator \\
      --server-add-password=password

  Stop the current rebalancing:
    couchbase-cli rebalance-stop -c 192.168.0.1:8091

  Change the username, password, port and ram quota:
    couchbase-cli cluster-init -c 192.168.0.1:8091 \\
       --cluster-init-username=Administrator \\
       --cluster-init-password=password \\
       --cluster-init-port=8080 \\
       --cluster-init-ramsize=300

  Change the data path:
     couchbase-cli node-init -c 192.168.0.1:8091 \\
       --node-init-data-path=/tmp

  List buckets in a cluster:
    couchbase-cli bucket-list -c 192.168.0.1:8091

  Create a new dedicated port couchbase bucket:
    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=couchbase \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-replica=1

  Create a couchbase bucket and wait for bucket ready:
    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=couchbase \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-replica=1 \\
       --wait

  Create a new sasl memcached bucket:
    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=memcached \\
       --bucket-password=password \\
       --bucket-ramsize=200

  Modify a dedicated port bucket:
    couchbase-cli bucket-edit -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-port=11222 \\
       --bucket-ramsize=400

  Delete a bucket:
    couchbase-cli bucket-delete -c 192.168.0.1:8091 \\
       --bucket=test_bucket

"""

    sys.exit(2)
