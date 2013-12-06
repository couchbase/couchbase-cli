#!/usr/bin/env python
# -*- coding: utf-8 -*-

from usage import usage

import restclient
from timeout import timed_out
import time
import sys
import urllib

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

class Buckets:
    def __init__(self):
        self.debug = False
        self.rest_cmd = rest_cmds['bucket-list']
        self.method = 'GET'

    @timed_out(60)
    def runCmd(self, cmd, server, port,
               user, password, opts):
        self.user = user
        self.password = password

        bucketname = ''
        buckettype = ''
        authtype = 'sasl'
        bucketport = '11211'
        bucketpassword = ''
        bucketramsize = ''
        bucketreplication = '1'
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
        rest = restclient.RestClient(server, port, {'debug':self.debug})

        # get the parameters straight
        opts = {}
        opts['error_msg'] = "unable to %s; please check your username (-u) and password (-p);" % cmd
        opts['success_msg'] = "%s" % cmd

        if cmd in ('bucket-create', 'bucket-edit'):
            if bucketname:
                rest.setParam('name', bucketname)
                if bucketname == "default":
                    if bucketport and bucketport != "11211":
                        usage("default bucket must be on port 11211.")
                    if bucketpassword:
                        usage("default bucket should only have empty password.")
                    authtype = 'sasl'
                else:
                    if bucketport == "11211":
                        authtype = 'sasl'
                    else:
                        authtype = 'none'
                        if bucketpassword:
                            usage("a sasl bucket is supported only on port 11211.")
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
            if enable_replica_index and cmd == 'bucket-create':
                rest.setParam('replicaIndex', enable_replica_index)
        if cmd in ('bucket-delete', 'bucket-flush', 'bucket-edit'):
            self.rest_cmd = self.rest_cmd + bucketname
        if cmd == 'bucket-flush':
            self.rest_cmd = self.rest_cmd + '/controller/doFlush'
            if not force:
                question = "Running this command will totally PURGE database data from disk." + \
                           "Do you really want to do it? (Yes/No)"
                confirm = raw_input(question)
                if confirm in ('Y', 'Yes'):
                    print "Database data will be purged from disk ..."
                else:
                    print "Database data will not be purged. Done."
                    return False
            else:
                print "Database data will be purged from disk ..."
            opts['error_msg'] = "unable to %s; please check username/password or if the bucket exists or not;" % cmd
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
            rest_query = restclient.RestClient(server, port, {'debug':self.debug})
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
        opts['error_msg'] = "unable to compact view; please check your username (-u) and password (-p);"
        opts['success_msg'] = "compact view for bucket"

        rest_query = restclient.RestClient(server, port, {'debug':self.debug})
        rest_cmd = '/pools/default/buckets/%s/ddocs' % bucket_name
        ddoc_info = rest_query.restCmd('GET', urllib.quote(rest_cmd),
                                       self.user, self.password, opts)
        json = rest_query.getJson(ddoc_info)
        for row in json["rows"]:
            cmd = row["controllers"]["compact"]
            data = rest.restCmd('POST', cmd,
                                self.user, self.password, opts)
            print data

class BucketStats:
    def __init__(self, bucket_name):
        self.debug = False
        self.rest_cmd = rest_cmds['bucket-stats'] % bucket_name
        self.method = 'GET'

    def runCmd(self, cmd, server, port,
               user, password, opts):
        opts = {}
        opts['error_msg'] = "unable to %s" % cmd
        opts['success_msg'] = "%s" % cmd

        #print server, port, cmd, self.rest_cmd
        rest = restclient.RestClient(server, port, {'debug':self.debug})
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
               user, password, opts):
        opts = {}
        opts['error_msg'] = "unable to %s" % cmd
        opts['success_msg'] = "%s" % cmd

        #print server, port, cmd, self.rest_cmd
        rest = restclient.RestClient(server, port, {'debug':self.debug})
        data = rest.restCmd(methods[cmd], self.rest_cmd,
                            user, password, opts)
        return rest.getJson(data)
