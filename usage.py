#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

class Usage:
    def getCommandSummary(self, cmd):
        if cmd == "help":
            return "show longer usage/help and examples"
        return None

def command_error(err):
    """ Print an error and terminate with exit(2) """
    print "ERROR: %s" % (err)
    sys.exit(2)
    return

def usage_header():
    """ Print the CLI usage 'header' information. """
    cluster_options = """CLUSTER:
  --cluster=HOST[:PORT] or -c HOST[:PORT]"""

    global_options = """OPTIONS:
  -u USERNAME, --user=USERNAME      admin username of the cluster
  -p PASSWORD, --password=PASSWORD  admin password of the cluster
  -o KIND, --output=KIND            KIND is json or standard
  -d, --debug
  -s, --ssl                         uses SSL for communication with secure servers"""

    command_notes = """NOTE:
  USERNAME can be set in environment variable CB_REST_USERNAME and/or
  PASSWORD can be set in environment variable CB_REST_PASSWORD instead"""

    print "couchbase-cli - command-line cluster administration tool"
    print ""
    print "%s" % (cluster_options)
    print ""
    print "%s" % (global_options)
    print ""
    print "%s" % (command_notes)
    print ""

MIN_SPACES = 2

def command_usage(cmd, command_map):
    """ For the command 'cmd', use the command_map to find the class
        which owns that command and use getCommandHelp to obtain
        detailed parameter infomation for it.
    """
    c = command_map[cmd]()
    if hasattr(c, 'getCommandHelp'):
        # None is allowed as a return
        cmd_help = c.getCommandHelp(cmd)
        if cmd_help:

            longest_key = 0
            for arg_pair in cmd_help:
                if len(arg_pair[0]) > longest_key:
                    longest_key = len(arg_pair[0])

            print "%s OPTIONS:" % (cmd)
            for arg_pair in cmd_help:
                spaces = ' ' * (MIN_SPACES + (longest_key - len(arg_pair[0])))
                print "  %s%s%s" % (arg_pair[0], spaces, arg_pair[1])
            print ""



def command_summaries(command_map):
    """ For all commands in the command_map print a formatted
    block of text showing the command and one-line summary help.
    E.g.
    command1     Create a cluster.
    command2     Delete a cluster.
    """
    summary_help = {}
    longest_cmd = 0
    for cmd in command_map:
        c = command_map[cmd]()
        if hasattr(c, 'getCommandSummary'):
            if len(cmd) > longest_cmd:
                longest_cmd = len(cmd)
            summary =  c.getCommandSummary(cmd)
            # some commands have no summary (None returned)
            if summary:
                summary_help[cmd] = summary

    for cmd in sorted(summary_help):
        spaces = ' ' * (MIN_SPACES + (longest_cmd - len(cmd)))
        print "  %s%s%s" % (cmd, spaces, summary_help[cmd])


def one_command_usage(cmd, command_map):
    """ Print formatted usage information for the CLI and one chosen command.
    Function terminates the process with exit(2).
    """
    usage_header()
    command_usage(cmd, command_map)
    print "usage: couchbase-cli %s CLUSTER [OPTIONS]" % (cmd)
    sys.exit(2)

def short_usage(command_map):
    """ Print formatted usage information showing commands and one-line summaries.
    Function terminates the process with exit(2).
    """
    usage_header()
    print "usage: couchbase-cli COMMAND CLUSTER [OPTIONS]"
    print ""
    print "COMMANDs include"
    command_summaries(command_map)
    print ""
    sys.exit(2)

def all_help(command_map):
    """
        Print everything, summaries, command help and examples.
        Note: Function exits process.
    """
    usage_header()

    print """COMMAND:"""
    command_summaries(command_map)

    print ""

    for cmd in sorted(command_map):
        command_usage(cmd, command_map)

    # Now dump our examples
    print """
usage: couchbase-cli COMMAND CLUSTER [OPTIONS]"

The default PORT number is 8091.

EXAMPLES:
  Set data path and hostname for an unprovisioned cluster:
    couchbse-cli node-init -c 192.168.0.1:8091 \\
       --node-init-data-path=/tmp/data \\
       --node-init-index-path=/tmp/index \\
       --node-init-hostname=myhostname \\
       -u Administrator -p password

  List servers in a cluster:
    couchbase-cli server-list -c 192.168.0.1:8091

  Server information:
    couchbase-cli server-info -c 192.168.0.1:8091

  Add a node to a cluster, but do not rebalance:
    couchbase-cli server-add -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091 \\
       --server-add-username=Administrator1 \\
       --server-add-password=password1 \\
       --group-name=group1 \\
       -u Administrator -p password

  Add a node to a cluster and rebalance:
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091 \\
       --server-add-username=Administrator1 \\
       --server-add-password=password1 \\
       --group-name=group1 \\
       -u Administrator -p password

  Remove a node from a cluster and rebalance:
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
       --server-remove=192.168.0.2:8091 \\
       -u Administrator -p password

  Remove and add nodes from/to a cluster and rebalance:
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
      --server-remove=192.168.0.2 \\
      --server-add=192.168.0.4 \\
      --server-add-username=Administrator1 \\
      --server-add-password=password1 \\
      --group-name=group1 \\
      -u Administrator -p password

  Stop the current rebalancing:
    couchbase-cli rebalance-stop -c 192.168.0.1:8091 \\
       -u Administrator -p password

  Set recovery type to a server:
    couchbase-cli recovery -c 192.168.0.1:8091 \\
       --server-recovery=192.168.0.2 \\
       --recovery-type=full \\
       -u Administrator -p password

  Set a failover, readd, recovery and rebalance sequence operations \\
    couchbase-cli failover -c 192.168.0.1:8091 \\
       --server-failover=192.168.0.2 \\
       -u Administrator -p password

    couchbase-cli server-readd -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2 \\
       -u Administrator -p password

    couchbase-cli recovery -c 192.168.0.1:8091 \\
       --server-recovery=192.168.0.2 \\
       --recovery-type=delta \\
       -u Administrator -p password

    couchbase-cli rebalance -c 192.168.0.1:8091 \\
       --recovery-buckets="default,bucket1" \\
       -u Administrator -p password

  Set the username, password, port and data service ram quota:
    couchbase-cli cluster-init -c 192.168.0.1:8091 \\
       --cluster-username=Administrator \\
       --cluster-password=password \\
       --cluster-port=8080 \\
       --cluster-ramsize=300

  change the cluster username, password, port and data service ram quota:
    couchbase-cli cluster-edit -c 192.168.0.1:8091 \\
       --cluster-username=Administrator1 \\
       --cluster-password=password1 \\
       --cluster-port=8080 \\
       --cluster-ramsize=300 \\
       -u Administrator -p password

  Change the data path:
     couchbase-cli node-init -c 192.168.0.1:8091 \\
       --node-init-data-path=/tmp \\
       -u Administrator -p password

  List buckets in a cluster:
    couchbase-cli bucket-list -c 192.168.0.1:8091

  Create a new dedicated port couchbase bucket:
    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=couchbase \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-replica=1 \\
       --bucket-priority=high \\
       -u Administrator -p password

  Create a couchbase bucket and wait for bucket ready:
    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=couchbase \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-replica=1 \\
       --bucket-priority=low \\
       --wait \\
       -u Administrator -p password

  Create a new sasl memcached bucket:
    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=memcached \\
       --bucket-password=password \\
       --bucket-ramsize=200 \\
       --bucket-eviction-policy=valueOnly \\
       --enable-flush=1 \\
       -u Administrator -p password

  Modify a dedicated port bucket:
    couchbase-cli bucket-edit -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-port=11222 \\
       --bucket-ramsize=400 \\
       --bucket-eviction-policy=fullEviction \\
       --enable-flush=1 \\
       --bucket-priority=high \\
       -u Administrator -p password

  Delete a bucket:
    couchbase-cli bucket-delete -c 192.168.0.1:8091 \\
       --bucket=test_bucket

  Flush a bucket:
    couchbase-cli bucket-flush -c 192.168.0.1:8091 \\
       --force \\
       -u Administrator -p password

  Compact a bucket for both data and view
    couchbase-cli bucket-compact -c 192.168.0.1:8091 \\
        --bucket=test_bucket \\
        -u Administrator -p password

  Compact a bucket for data only
    couchbase-cli bucket-compact -c 192.168.0.1:8091 \\
        --bucket=test_bucket \\
        --data-only \\
        -u Administrator -p password

  Compact a bucket for view only
    couchbase-cli bucket-compact -c 192.168.0.1:8091 \\
        --bucket=test_bucket \\
        --view-only \\
        -u Administrator -p password

  Create a XDCR remote cluster
    couchbase-cli xdcr-setup -c 192.168.0.1:8091 \\
        --create \\
        --xdcr-cluster-name=test \\
        --xdcr-hostname=10.1.2.3:8091 \\
        --xdcr-username=Administrator1 \\
        --xdcr-password=password1 \\
        --xdcr-demand-encryption=1 \\
        --xdcr-certificate=/tmp/test.pem \\
        -u Administrator -p password

  Delete a XDCR remote cluster
    couchbase-cli xdcr-setup --delete -c 192.168.0.1:8091 \\
        --xdcr-cluster-name=test \\
        -u Administrator -p password

  List XDCR remote cluster
    couchbase-cli xdcr-setup --list -c 192.168.0.1:8091 \\
        -u Administrator -p password

  Start a replication stream in memcached protocol
    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --create \\
        --xdcr-cluster-name=test \\
        --xdcr-from-bucket=default \\
        --xdcr-to-bucket=default1 \\
        --xdcr-replication-mode=xmem \\
        -u Administrator -p password

  Start a replication stream in capi protocol
    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --create \\
        --xdcr-cluster-name=test \\
        --xdcr-from-bucket=default \\
        --xdcr-to-bucket=default1 \\
        --xdcr-replication-mode=capi \\
        -u Administrator -p password

  Delete a replication stream
    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --delete \\
        --xdcr-replicator=f4eb540d74c43fd3ac6d4b7910c8c92f/default/default \\
        -u Administrator -p password

  Pause a running replication stream
    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --pause \\
        --xdcr-replicator=f4eb540d74c43fd3ac6d4b7910c8c92f/default/default \\
        -u Administrator -p password

  Resume a paused replication stream
    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --resume \\
        --xdcr-replicator=f4eb540d74c43fd3ac6d4b7910c8c92f/default/default \\
        -u Administrator -p password

  Update settings for a replication stream
    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --settings \\
        --xdcr-replicator=f4eb540d74c43fd3ac6d4b7910c8c92f/default/default \\
        --checkpoint-interval=1800 \\
        --worker-batch-size=500    \\
        --doc-batch-size=2048      \\
        --failure-restart-interval=30 \\
        --optimistic-replication-threshold=256 \\
        --source-nozzle-per-node=5 \\
        --target-nozzle-per-node=6 \\
        --log-level=Debug \\
        --stats-interval=80 \\
        -u Administrator -p password

  List all xdcr replication streams
    couchbase-cli xdcr-replicate -c 192.168.0.1:8091 \\
        --list \\
        -u Administrator -p password

  List read only user in a cluster:
    couchbase-cli user-manage --list -c 192.168.0.1:8091 \\
           -u Administrator -p password

  Delete a read only user in a cluster
    couchbase-cli user-manage -c 192.168.0.1:8091 \\
        --delete --ro-username=readonlyuser \\
        -u Administrator -p password

  create/modify a read only user in a cluster
    couchbase-cli user-manage -c 192.168.0.1:8091 \\
        --set --ro-username=readonlyuser --ro-password=readonlypassword \\
        -u Administrator -p password

  Create a new group
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --create --group-name=group1 -u Administrator -p password

  Delete an empty group
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --delete --group-name=group1 -u Administrator -p password

  Rename an existed group
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --rename=newgroup --group-name=group1 -u Administrator -p password

  Show group/server map
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --list -u Administrator -p password

  Add a server to a group
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --add-servers=10.1.1.1:8091,10.1.1.2:8091 \\
        --group-name=group1 \\
        --server-add-username=Administrator1 \\
        --server-add-password=password1 \\
        --services=data,index,query \\
        -u Administrator -p password

  Move list of servers from group1 to group2
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --move-servers=10.1.1.1:8091,10.1.1.2:8091 \\
        --from-group=group1 \\
        --to-group=group2 \\
        -u Administrator -p password

  Download a cluster certificate
    couchbase-cli ssl-manage -c 192.168.0.1:8091 \\
        --retrieve-cert=/tmp/test.pem \\
        -u Administrator -p password

  Regenerate AND download a cluster certificate
    couchbase-cli ssl-manage -c 192.168.0.1:8091 \\
        --regenerate-cert=/tmp/test.pem \\
        -u Administrator -p password

  Start cluster-wide log collection for whole cluster
    couchbase-cli collect-logs-start -c 192.168.0.1:8091 \\
        -u Administrator -p password \\
        --all-nodes --upload --upload-host=host.upload.com \\
        --customer="example inc" --ticket=12345

  Start cluster-wide log collection for selected nodes
    couchbase-cli collect-logs-start -c 192.168.0.1:8091 \\
        -u Administrator -p password \\
        --nodes=10.1.2.3:8091,10.1.2.4 --upload --upload-host=host.upload.com \\
        --customer="example inc" --ticket=12345

  Stop cluster-wide log collection
    couchbase-cli collect-logs-stop -c 192.168.0.1:8091 \\
        -u Administrator -p password

  Show status of cluster-wide log collection
    couchbase-cli collect-logs-status -c 192.168.0.1:8091 \\
        -u Administrator -p password

"""

    sys.exit(2)



