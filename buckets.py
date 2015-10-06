#!/usr/bin/env python
# -*- coding: utf-8 -*-

from usage import command_error

import restclient
from timeout import timed_out
import time
import sys
import urllib
import util_cli as util

rest_cmds = {
    'bucket-list': '/pools/default/buckets',
    'bucket-flush': '/pools/default/buckets/',
    'bucket-delete': '/pools/default/buckets/',
    'bucket-create': '/pools/default/buckets/',
    'bucket-edit': '/pools/default/buckets/',
    'bucket-get': '/pools/default/buckets',
    'bucket-stats': '/pools/default/buckets/%s/stats?zoom=hour',
    'bucket-node-stats': '/pools/default/buckets/%s/stats/%s?zoom=%s',
    'bucket-info': '/pools/default/buckets/%s',
    'bucket-compact': '/pools/default/buckets/',
    'bucket-ddocs': '/pools/default/buckets/%s/ddocs',
    }
methods = {
    'bucket-list': 'GET',
    'bucket-delete': 'DELETE',
    'bucket-create': 'POST',
    'bucket-edit': 'POST',
    'bucket-flush': 'POST',
    'bucket-get': 'GET',
    'bucket-stats': 'GET',
    'bucket-node-stats': 'GET',
    'bucket-compact': 'POST',
    'bucket-ddocs': 'GET',
    }

priority = {
    'low': 3,
    'high': 8,
    }

class Buckets:
    def __init__(self):
        self.debug = False
        self.rest_cmd = rest_cmds['bucket-list']
        self.method = 'GET'
        self.ssl = False

    @timed_out(60)
    def runCmd(self, cmd, server, port,
               user, password, ssl, opts):
        self.user = user
        self.password = password
        self.ssl = ssl

        bucketname = ''
        buckettype = ''
        authtype = 'sasl'
        bucketport = '11211'
        bucketpassword = ''
        bucketramsize = ''
        bucketreplication = ''
        bucketpriority = None
        eviction_policy = None
        output = 'default'
        wait_for_bucket_ready = False
        enable_flush = None
        enable_replica_index = None
        force = False
        compact_data_only = False
        compact_view_only = False

        for (o, a) in opts:
            if o in ('-b', '--bucket'):
                bucketname = a
            elif o == '--bucket-type':
                buckettype = a
            elif o == '--bucket-port':
                bucketport = a
            elif o == '--bucket-password':
                bucketpassword = a
            elif o == '--bucket-ramsize':
                bucketramsize = a
            elif o == '--bucket-replica':
                bucketreplication = a
            elif o == '--bucket-eviction-policy':
                eviction_policy = a
            elif o == '--bucket-priority':
                bucketpriority = a
            elif o == '-d' or o == '--debug':
                self.debug = True
            elif o in ('-o', '--output'):
                output = a
            elif o == '--enable-flush':
                enable_flush = a
            elif o == '--enable-index-replica':
                enable_replica_index = a
            elif o == '--wait':
                wait_for_bucket_ready = True
            elif o == '--force':
                force = True
            elif o == '--data-only':
                compact_data_only = True
            elif o == '--view-only':
                compact_view_only = True

        self.rest_cmd = rest_cmds[cmd]
        rest = util.restclient_factory(server, port, {'debug':self.debug}, self.ssl)
        #rest = restclient.RestClient(server, port, {'debug':self.debug})
        # get the parameters straight
        opts = {}
        opts['error_msg'] = "unable to %s;" % cmd
        opts['success_msg'] = "%s" % cmd

        if cmd in ('bucket-create', 'bucket-edit'):
            if bucketname:
                rest.setParam('name', bucketname)
                if bucketname == "default":
                    if bucketport and bucketport != "11211":
                        command_error("default bucket must be on port 11211.")
                    if bucketpassword:
                        command_error("default bucket should only have empty password.")
                    authtype = 'sasl'
                else:
                    if bucketport == "11211":
                        authtype = 'sasl'
                    else:
                        authtype = 'none'
                        if bucketpassword:
                            command_error("a sasl bucket is supported only on port 11211.")
            if buckettype:
                rest.setParam('bucketType', buckettype)
            if authtype:
                rest.setParam('authType', authtype)
            if bucketport:
                rest.setParam('proxyPort', bucketport)
            if bucketpassword:
                rest.setParam('saslPassword', bucketpassword)
            if bucketramsize:
                rest.setParam('ramQuotaMB', bucketramsize)
            if bucketreplication:
                rest.setParam('replicaNumber', bucketreplication)
            if enable_flush:
                rest.setParam('flushEnabled', enable_flush)
            if eviction_policy:
                if eviction_policy in ['valueOnly', 'fullEviction']:
                    rest.setParam('evictionPolicy', eviction_policy)
                else:
                    command_error("eviction policy value should be either 'valueOnly' or 'fullEviction'.")
            if enable_replica_index and cmd == 'bucket-create':
                rest.setParam('replicaIndex', enable_replica_index)
            if bucketpriority:
                if bucketpriority in priority:
                    rest.setParam('threadsNumber', priority[bucketpriority])
                else:
                    command_error("bucket priority must be either low or high.")

        if cmd in ('bucket-delete', 'bucket-flush', 'bucket-edit'):
            self.rest_cmd = self.rest_cmd + bucketname
        if cmd == 'bucket-flush':
            self.rest_cmd = self.rest_cmd + '/controller/doFlush'
            if not force:
                question = "Running this command will totally PURGE database data from disk. " + \
                           "Do you really want to do it? (Yes/No)"
                confirm = raw_input(question)
                if confirm in ('Y', 'Yes'):
                    print "\nDatabase data will be purged from disk ..."
                else:
                    print "\nDatabase data will not be purged. Done."
                    return False
            else:
                print "Database data will be purged from disk ..."
            opts['error_msg'] = "unable to %s; please check if the bucket exists or not;" % cmd
        elif cmd == 'bucket-compact':
            if compact_data_only and compact_view_only:
                print "You cannot compact data only and view only at the same time."
                return False
            elif compact_data_only:
                self.rest_cmd = self.rest_cmd + bucketname + '/controller/compactDatabases'
            elif compact_view_only:
                self.compact_view(rest, server, port, bucketname)
                return True
            else:
                self.rest_cmd = self.rest_cmd + bucketname + '/controller/compactBucket'
        elif cmd == 'bucket-ddocs':
            self.rest_cmd = self.rest_cmd % bucketname

        data = rest.restCmd(methods[cmd], urllib.quote(self.rest_cmd),
                            self.user, self.password, opts)

        if cmd in ("bucket-get", "bucket-stats", "bucket-node-stats", "bucket-ddocs"):
            return rest.getJson(data)
        elif cmd == "bucket-list":
            if output == 'json':
                print data
            else:
                json = rest.getJson(data)
                for bucket in json:
                    print '%s' % bucket['name']
                    print ' bucketType: %s' % bucket['bucketType']
                    print ' authType: %s' % bucket['authType']
                    if bucket['authType'] == "sasl":
                        print ' saslPassword: %s' % bucket['saslPassword']
                    else:
                        print ' proxyPort: %s' % bucket['proxyPort']
                    print ' numReplicas: %s' % bucket['replicaNumber']
                    print ' ramQuota: %s' % bucket['quota']['ram']
                    print ' ramUsed: %s' % bucket['basicStats']['memUsed']
        elif cmd == "bucket-create" and wait_for_bucket_ready:
            rest_query = util.restclient_factory(server, port, {'debug':self.debug}, self.ssl)
            timeout_in_seconds = 120
            start = time.time()
            # Make sure the bucket exists before querying its status
            bucket_exist = False
            while (time.time() - start) <= timeout_in_seconds and not bucket_exist:
                buckets = rest_query.restCmd('GET', rest_cmds['bucket-list'],
                                             self.user, self.password, opts)
                for bucket in rest_query.getJson(buckets):
                    if bucket["name"] == bucketname:
                        bucket_exist = True
                        break
                if not bucket_exist:
                    sys.stderr.write(".")
                    time.sleep(2)

            if not bucket_exist:
                print "\nFail to create bucket '%s' within %s seconds" %\
                      (bucketname, timeout_in_seconds)
                return False

            #Query status for all bucket nodes
            while (time.time() - start) <= timeout_in_seconds:
                bucket_info = rest_query.restCmd('GET', urllib.quote(rest_cmds['bucket-info'] % bucketname),
                                                 self.user, self.password, opts)
                json = rest_query.getJson(bucket_info)
                all_node_ready = True
                for node in json["nodes"]:
                    if node["status"] != "healthy":
                        all_node_ready = False
                        break
                if all_node_ready:
                    if output == 'json':
                        print rest.jsonMessage(data)
                    else:
                        print data
                    return True
                else:
                    sys.stderr.write(".")
                    time.sleep(2)

            print "\nBucket '%s' is created but not ready to use within %s seconds" %\
                 (bucketname, timeout_in_seconds)
            return False
        else:
            if output == 'json':
                print rest.jsonMessage(data)
            else:
                print data

    def compact_view(self, rest, server, port, bucket_name):
        opts = {}
        rest_query = util.restclient_factory(server, port, {'debug':self.debug}, self.ssl)
        rest_cmd = '/pools/default/buckets/%s/ddocs' % bucket_name
        ddoc_info = rest_query.restCmd('GET', urllib.quote(rest_cmd),
                                       self.user, self.password, opts)
        json = rest_query.getJson(ddoc_info)
        for row in json["rows"]:
            cmd = row["controllers"]["compact"]
            opts['error_msg'] = "fail to run task:%s" % cmd
            opts['success_msg'] = "run task:%s" % cmd
            data = rest.restCmd('POST', cmd,
                                self.user, self.password, opts)
            print data

    def getCommandSummary(self, cmd):
        """Return one-line summary info for each supported command"""
        command_summary = {
            "bucket-list" : "list all buckets in a cluster",
            "bucket-create" : "add a new bucket to the cluster",
            "bucket-edit" : "modify an existing bucket",
            "bucket-delete" : "delete an existing bucket",
            "bucket-flush" : "flush all data from disk for a given bucket",
            "bucket-compact" : "compact database and index data"}
        if cmd in command_summary:
            return command_summary[cmd]
        else:
            return None

    def getCommandHelp(self, cmd):
        """ Obtain detailed parameter help for Bucket commands
        Returns a list of pairs (arg1, arg1-information) or None if there's
        no help or cmd is unknown.
        """
        bucket_name = [("--bucket=BUCKETNAME", "bucket to act on")]
        bucket_replica = [("--bucket-replica=COUNT", "replication count")]
        bucket_ramsize = [("--bucket-ramsize=RAMSIZEMB", "ram quota in MB")]
        bucket_type = [("--bucket-type=TYPE","memcached or couchbase")]
        bucket_priority = [("--bucket-priority=[low|high]",
                            "priority when compared to other buckets")]
        bucket_port = [("--bucket-port=PORT",
                        "supports ASCII protocol and is auth-less")]
        bucket_password = [("--bucket-password=PASSWORD",
                            "standard port, exclusive with bucket-port")]
        eviction_policy = [("--bucket-eviction-policy=[valueOnly|fullEviction]",
                            "policy how to retain meta in memory")]
        enable_flush = [("--enable-flush=[0|1]", "enable/disable flush")]
        enable_replica_idx = [("--enable-index-replica=[0|1]",
                               "enable/disable index replicas")]
        force = [("--force",
                  "force to execute command without asking for confirmation")]
        wait = [("--wait",
                 "wait for bucket create to be complete before returning")]
        compact_data = [("--data-only", "compact database data only")]
        compact_view = [("--view-only", "compact view data only")]

        create_edit = (bucket_name + bucket_ramsize + bucket_replica +
                      bucket_type + bucket_priority + bucket_password +
                      eviction_policy + enable_flush)

        if cmd == "bucket-create":
            return create_edit + enable_replica_idx + wait
        elif cmd == "bucket-edit":
            return create_edit
        elif cmd == "bucket-delete":
            return bucket_name
        elif cmd == "bucket-flush":
            return bucket_name + force
        elif cmd == "bucket-compact":
            return bucket_name + compact_data + compact_view

    def getCommandExampleHelp(self, cmd):
        """ Obtain detailed example help for command
        Returns a list of command examples to illustrate how to use command
        or None if there's no example help or cmd is unknown.
        """

        if cmd == "bucket-list":
            return [("List buckets in a cluster",
"""
    couchbase-cli bucket-list -c 192.168.0.1:8091""")]
        elif cmd == "bucket-create":
            return [("Create a new dedicated port couchbase bucket",
"""
    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=couchbase \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-replica=1 \\
       --bucket-priority=high \\
       --bucket-eviction-policy=valueOnly \\
       -u Administrator -p password"""),
                ("Create a couchbase bucket and wait for bucket ready",
"""
    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=couchbase \\
       --bucket-port=11222 \\
       --bucket-ramsize=200 \\
       --bucket-replica=1 \\
       --bucket-priority=low \\
       --wait \\
       -u Administrator -p password"""),
                ("Create a new sasl memcached bucket",
"""
    couchbase-cli bucket-create -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-type=memcached \\
       --bucket-password=password \\
       --bucket-ramsize=200 \\
       --bucket-eviction-policy=valueOnly \\
       --enable-flush=1 \\
       -u Administrator -p password""")]
        elif cmd == "bucket-edit":
            return [("Modify a dedicated port bucket",
"""
    couchbase-cli bucket-edit -c 192.168.0.1:8091 \\
       --bucket=test_bucket \\
       --bucket-port=11222 \\
       --bucket-ramsize=400 \\
       --bucket-eviction-policy=fullEviction \\
       --enable-flush=1 \\
       --bucket-priority=high \\
       -u Administrator -p password""")]
        elif cmd == "bucket-delete":
            return [("Delete a bucket",
"""
    couchbase-cli bucket-delete -c 192.168.0.1:8091 \\
       --bucket=test_bucket""")]
        elif cmd == "bucket-compact":
            return [("Compact a bucket for both data and view",
"""
    couchbase-cli bucket-compact -c 192.168.0.1:8091 \\
        --bucket=test_bucket \\
        -u Administrator -p password"""),
                    ("Compact a bucket for data only",
"""
    couchbase-cli bucket-compact -c 192.168.0.1:8091 \\
        --bucket=test_bucket \\
        --data-only \\
        -u Administrator -p password"""),
                    ("Compact a bucket for view only",
"""
    couchbase-cli bucket-compact -c 192.168.0.1:8091 \\
        --bucket=test_bucket \\
        --view-only \\
        -u Administrator -p password""")]
        elif cmd == "bucket-flush":
            return [("Flush a bucket",
"""
    couchbase-cli bucket-flush -c 192.168.0.1:8091 \\
       --force \\
       -u Administrator -p password""")]
        else:
            return None

class BucketStats:
    def __init__(self, bucket_name):
        self.debug = False
        self.rest_cmd = rest_cmds['bucket-stats'] % bucket_name
        self.method = 'GET'

    def runCmd(self, cmd, server, port,
               user, password, ssl, opts):
        opts = {}
        opts['error_msg'] = "unable to %s" % cmd
        opts['success_msg'] = "%s" % cmd

        #print server, port, cmd, self.rest_cmd
        rest = util.restclient_factory(server, port, {'debug':self.debug}, ssl)
        data = rest.restCmd(methods[cmd], self.rest_cmd,
                            user, password, opts)
        return rest.getJson(data)

class BucketNodeStats:
    def __init__(self, bucket_name, stat_name, scale):
        self.debug = False
        self.rest_cmd = rest_cmds['bucket-node-stats'] % (bucket_name, stat_name, scale)
        self.method = 'GET'
        #print self.rest_cmd

    def runCmd(self, cmd, server, port,
               user, password, ssl, opts):
        opts = {}
        opts['error_msg'] = "unable to %s" % cmd
        opts['success_msg'] = "%s" % cmd

        #print server, port, cmd, self.rest_cmd
        rest = util.restclient_factory(server, port, {'debug':self.debug}, ssl)
        data = rest.restCmd(methods[cmd], self.rest_cmd,
                            user, password, opts)
        return rest.getJson(data)
