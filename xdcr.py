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
        self.rest_cmd = rest_cmds['xdcr-setup']
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
        self.cmd = 'create'
        self.replicator = ''
        self.params = {}
        self.output = 'standard'

        self.from_bucket = ''
        self.to_bucket = ''
        self.type = ''

        self.max_stream = ''
        self.checkpoint_interval = ''
        self.worker_batch_size = ''
        self.doc_batch_size = ''
        self.failure_restart_interval = ''

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
            if self.cmd == 'create':
                self.setup_create()
            elif self.cmd == 'edit':
                self.setup_edit()
            elif self.cmd == 'delete':
                self.setup_delete()

        if cmd == 'xdcr-replicate':
            if self.cmd == 'create':
                self.replicate_start()
            elif self.cmd == 'delete':
                self.replicate_stop()

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
            elif o == '--create':
                self.cmd = 'create'
            elif o == '--edit':
                self.cmd = 'edit'
            elif o == '--delete':
                self.cmd = 'delete'
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

    def setup_create(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        if self.remote_cluster:
            rest.setParam('name', self.remote_cluster)
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

        opts = {
            'error_msg': "unable to set up xdcr remote site %s" % self.remote_cluster,
            'success_msg': "init %s" % self.remote_cluster
        }
        output_result = rest.restCmd('POST',
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def setup_edit(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        if self.remote_cluster:
            rest.setParam('name', self.remote_cluster)
            self.rest_cmd = self.rest_cmd + "/" + self.remote_cluster
        else:
            print "Error: Cluster name is needed to edit cluster connections"
            return

        if self.remote_username:
            rest.setParam('username', self.remote_username)
        else:
            rest.setParam('username', "username")
        if self.remote_password:
            rest.setParam('password', self.remote_password)
        else:
            rest.setParam('password', "password")

        opts = {
            'error_msg': "unable to edit xdcr remote site %s" % self.remote_cluster,
            'success_msg': "edit cluster %s" % self.remote_cluster
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
            self.rest_cmd = self.rest_cmd + "/" + self.remote_cluster
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
        json = rest.getJson(output_result)

        print "replicator id:", json["id"]

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

    def setting(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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

        opts = {
            'error_msg': "unable to set xdcr internal settings",
            'success_msg': "set xdcr settings"
        }
        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result
