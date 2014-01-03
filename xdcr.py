"""
  Implementation for xdcr management
"""

import time
import os
import sys
import socket
import urllib

from usage import usage
import restclient
import listservers
import util_cli as util

# Map of HTTP success code, success message and error message for
# handling HTTP response properly

class XDCR:
    def __init__(self):
        self.method = 'POST'
        self.debug = False
        self.cluster = ''
        self.server = ''
        self.port = ''
        self.user = ''
        self.password = ''
        self.remote_username = ''
        self.remote_password = ''
        self.remote_hostname = ''
        self.remote_cluster = ''
        self.replication_mode = ''
        self.cmd = 'create'
        self.replicator = ''
        self.params = {}
        self.output = 'standard'
        self.demand_encryption = ''
        self.certificate = ''

        self.from_bucket = ''
        self.to_bucket = ''
        self.type = ''

        self.max_stream = ''
        self.checkpoint_interval = ''
        self.worker_batch_size = ''
        self.doc_batch_size = ''
        self.failure_restart_interval = ''
        self.optimistic_replication_threshold = ''

        # the rest commands and associated URIs for various node operations
        self.REST_CMDS = {
            'xdcr-setup': '/pools/default/remoteClusters',
            'xdcr-replicate': '/controller/createReplication',
            'setting-xdcr': '/internalSettings'
        }

        # Map of operations and the HTTP methods used against the REST interface
        self.METHODS = {
            'xdcr-setup': 'POST',
            'xdcr-replicate': 'POST',
            'setting-xdcr': 'POST',
        }

    def runCmd(self, cmd, server, port,
               user, password, opts):
        self.rest_cmd = self.REST_CMDS[cmd]
        self.method = self.METHODS[cmd]
        self.server = server
        self.port = int(port)
        self.user = user
        self.password = password
        self.processOpts(cmd, opts)

        if self.debug:
            print "INFO: servers %s" % servers

        if cmd == 'xdcr-setup':
            if self.cmd in('create', 'edit'):
                self.setup_create(self.cmd)
            elif self.cmd == 'delete':
                self.setup_delete()
        if cmd == 'xdcr-replicate':
            if self.cmd == 'create':
                self.replicate_start()
            elif self.cmd == 'delete':
                self.replicate_stop()
            elif self.cmd == 'list':
                self.replicate_list()
            else:
                print "ERROR: unsupported replicate command:", cmd

        if cmd == 'setting-xdcr':
            self.setting()

    def processOpts(self, cmd, opts):
        """ Set standard opts.
            note: use of a server key keeps optional
            args aligned with server.
            """

        for o, a in opts:
            if o in ('-d', '--debug'):
                self.debug = True
            elif o == '--xdcr-cluster-name':
                self.remote_cluster = a
            elif o == '--xdcr-hostname':
                self.remote_hostname = a
            elif o == '--xdcr-username':
                self.remote_username = a
            elif o == '--xdcr-password':
                self.remote_password = a
            elif o == '--xdcr-from-bucket':
                self.from_bucket = a
            elif o == '--xdcr-to-bucket':
                self.to_bucket = a
            elif o == '--xdcr-type':
                self.type = a
            elif o == '--xdcr-replicator':
                self.replicator = a
            elif o == '--xdcr-replication-mode':
                self.replication_mode = a
            elif o == '--create':
                self.cmd = 'create'
            elif o == '--edit':
                self.cmd = 'edit'
            elif o == '--delete':
                self.cmd = 'delete'
            elif o == '--list':
                self.cmd = 'list'
            elif o == '--max-concurrent-reps':
                self.max_stream = int(a)
            elif o == '--checkpoint-interval':
                self.checkpoint_interval = int(a)
            elif o == '--worker-batch-size':
                self.worker_batch_size = int(a)
            elif o == '--doc-batch-size':
                self.doc_batch_size = int(a)
            elif o == '--failure-restart-interval':
                self.failure_restart_interval = int(a)
            elif o == '--optimistic-replication-threshold':
                self.optimistic_replication_threshold = int(a)
            elif o == '--xdcr-demand-encryption':
                self.demand_encryption = int(a)
            elif o == '--xdcr-certificate':
                self.certificate = a

    def setup_create(self, cmd):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})

        if self.remote_cluster:
            rest.setParam('name', self.remote_cluster)
            if cmd == 'edit':
                self.rest_cmd = self.rest_cmd + "/" + urllib.quote(self.remote_cluster)
        else:
            if cmd == 'edit':
                print "Error: Cluster name is needed to edit cluster connections"
                return
            else:
                rest.setParam('name', 'remote cluster')

        if self.remote_hostname:
            rest.setParam('hostname', self.remote_hostname)
        else:
            print "Error: hostname (ip) is missing"
            return

        if self.remote_username:
            rest.setParam('username', self.remote_username)
        else:
            rest.setParam('username', "username")
        if self.remote_password:
            rest.setParam('password', self.remote_password)
        else:
            rest.setParam('password', "password")

        if self.demand_encryption:
            rest.setParam("demandEncryption", self.demand_encryption)
            if self.certificate:
                if os.path.isfile(self.certificate):
                    try:
                        fp = open(self.certificate, 'r')
                        raw_data = fp.read()
                        fp.close()
                        rest.setParam("certificate", raw_data)
                    except IOError, error:
                        print "Error:", error
                        return
                else:
                    print "ERROR: Fail to open certificate file from %s" % self.certificate
                    return
            else:
                print "ERROR: certificate required if encryption is demanded."
                return
        else:
            rest.setParam("demandEncryption", str(int(0)))

        opts = {
            'error_msg': "unable to set up xdcr remote site %s" % self.remote_cluster,
            'success_msg': "init/edit %s" % self.remote_cluster
        }
        output_result = rest.restCmd('POST',
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def setup_delete(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        if self.remote_cluster:
            self.rest_cmd = self.rest_cmd + "/" + urllib.quote(self.remote_cluster)
        else:
            print "Error: Cluster name is needed to delete cluster connections"
            return

        opts = {
            'error_msg': "unable to delete xdcr remote site %s" % self.server,
            'success_msg': "delete %s" % self.remote_cluster
        }
        output_result = rest.restCmd('DELETE',
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def replicate_start(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        if self.to_bucket:
            rest.setParam('toBucket', self.to_bucket)

        if self.remote_cluster:
            rest.setParam('toCluster', self.remote_cluster)

        if self.from_bucket:
            rest.setParam('fromBucket', self.from_bucket)

        if self.replication_mode:
            rest.setParam('type', self.replication_mode)

        rest.setParam('replicationType', 'continuous')

        opts = {
            'error_msg': "unable to create replication",
            'success_msg': "start replication"
        }
        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def replicate_stop(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        if self.replicator:
            self.rest_cmd = '/controller/cancelXCDR/' + urllib.quote_plus(self.replicator)
        else:
            print "Error: option --xdcr-replicator is needed to delete a replication"
            return

        opts = {
            'error_msg': "unable to delete replication",
            'success_msg': "delete replication"
        }
        output_result = rest.restCmd('DELETE',
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def replicate_list(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})

        opts = {
            'error_msg': "unable to retrieve any replication streams",
            'success_msg': "list replication streams"
        }
        output_result = rest.restCmd('GET',
                                     '/pools/default/tasks',
                                     self.user,
                                     self.password,
                                     opts)
        tasks = rest.getJson(output_result)
        for task in tasks:
            if task["type"] == "xdcr":
                print 'stream id: %s' % task['id']
                print "   status: %s" % task["status"]
                print "   source: %s" % task["source"]
                print "   target: %s" % task["target"]

    def setting(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        opts = {
            'error_msg': "unable to set xdcr internal settings",
            'success_msg': "set xdcr settings"
        }

        if self.max_stream:
            rest.setParam('xdcrMaxConcurrentReps', self.max_stream)
            opts['success_msg'] += ' xdcrMaxConcurrentReps'

        if self.checkpoint_interval:
            rest.setParam('xdcrCheckpointInterval', self.checkpoint_interval)
            opts['success_msg'] += ' xdcrCheckpointInterval'

        if self.worker_batch_size:
            rest.setParam('xdcrWorkerBatchSize', self.worker_batch_size)
            opts['success_msg'] += ' xdcrWorkerBatchSize'

        if self.doc_batch_size:
            rest.setParam('xdcrDocBatchSizeKb', self.doc_batch_size)
            opts['success_msg'] += ' xdcrDocBatchSizeKb'

        if self.failure_restart_interval:
            rest.setParam('xdcrFailureRestartInterval', self.failure_restart_interval)
            opts['success_msg'] += ' xdcrFailureRestartInterval'

        if self.optimistic_replication_threshold:
            rest.setParam('xdcrOptimisticReplicationThreshold', self.optimistic_replication_threshold)
            opts['success_msg'] += ' xdcrOptimisticReplicationThreshold'

        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result
