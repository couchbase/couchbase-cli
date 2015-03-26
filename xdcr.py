"""
  Implementation for xdcr management
"""

import time
import os
import sys
import socket
import urllib

from usage import command_error
import restclient
import listservers
import util_cli as util
import node

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
        self.ssl = False
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
        self.regex = ''

        self.from_bucket = ''
        self.to_bucket = ''
        self.type = ''

        self.checkpoint_interval = ''
        self.worker_batch_size = ''
        self.doc_batch_size = ''
        self.failure_restart_interval = ''
        self.optimistic_replication_threshold = ''
        self.source_nozzle_per_node = ''
        self.target_nozzle_per_node = ''
        self.max_replication_lag = ''
        self.timeout_perc_cap = ''
        self.log_level = ''
        self.stats_interval = ''

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
               user, password, ssl, opts):
        self.rest_cmd = self.REST_CMDS[cmd]
        self.method = self.METHODS[cmd]
        self.server = server
        self.port = int(port)
        self.user = user
        self.password = password
        self.processOpts(cmd, opts)

        if self.debug:
            print "INFO: server %s" % server

        if cmd == 'xdcr-setup':
            if self.cmd in('create', 'edit'):
                self.setup_create(self.cmd)
            elif self.cmd == 'delete':
                self.setup_delete()
            elif self.cmd == 'list':
                self.setup_list()
        if cmd == 'xdcr-replicate':
            if self.cmd == 'create':
                self.replicate_start()
            elif self.cmd == 'delete':
                self.replicate_stop()
            elif self.cmd == 'list':
                self.replicate_list()
            elif self.cmd == 'pause' or self.cmd == 'resume':
                self.replicate_pause_resume()
            elif self.cmd == 'settings':
                self.replicate_settings()
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
            if o in ('-o', '--output'):
                self.output = a
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
            elif o == '--settings':
                self.cmd = 'settings'
            elif o == '--pause':
                self.cmd = 'pause'
            elif o == '--resume':
                self.cmd = 'resume'
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
            elif o == '--filterExpression':
                self.regex = a
            elif o == '--sourceNozzlePerNode':
                self.source_nozzle_per_node = int(a)
            elif o == '--targetNozzlePerNode':
                self.target_nozzle_per_node = int(a)
            elif o == '--maxExpectedReplicationLag':
                self.max_replication_lag = int(a)
            elif o == '--timeoutPercentageCap':
                self.timeout_perc_cap = int(a)
            elif o == '--logLevel':
                self.log_level = a
            elif o == '--statsInterval':
                self.stats_interval = int(a)

    def setup_create(self, cmd):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if not self.remote_cluster:
            print "Error: --xdcr-cluster-name is required to create/edit cluster connection"
            return
        else:
            rest.setParam('name', self.remote_cluster)
            if cmd == 'edit':
                self.rest_cmd = self.rest_cmd + "/" + urllib.quote(self.remote_cluster)

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
            if self.demand_encryption not in [0, 1]:
                print "ERROR: xdcr-demand-encryption can only be either 0 or 1."
                return
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
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
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

    def setup_list(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        opts = {
            'error_msg': "unable to list xdcr remote cluster",
            'success_msg': "list remote cluster successfully",
        }
        output_result = rest.restCmd('GET',
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        clusters = rest.getJson(output_result)
        if self.output == 'json':
            print output_result
        else:
            for cluster in clusters:
                if not cluster.get('deleted'):
                    print "cluster name: %s" % cluster["name"]
                    print "        uuid: %s" % cluster["uuid"]
                    print "   host name: %s" % cluster["hostname"]
                    print "   user name: %s" % cluster["username"]
                    print "         uri: %s" % cluster["uri"]

    def replicate_start(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if self.to_bucket:
            rest.setParam('toBucket', self.to_bucket)

        if self.remote_cluster:
            rest.setParam('toCluster', self.remote_cluster)

        if self.from_bucket:
            rest.setParam('fromBucket', self.from_bucket)

        if self.replication_mode:
            rest.setParam('type', self.replication_mode)

        if self.regex:
            rest.setParam('filterExpression', self.regex)

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
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
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

    def replicate_pause_resume(self):
        if not self.replicator:
            print "Error: option --xdcr-replicator is needed to pause/resume a replication"
            return

        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)

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
            if task["type"] == "xdcr" and task["id"] == self.replicator:
                if self.cmd == "pause" and task["status"] == "notRunning":
                    print "The replication is not running yet. Pause is not needed"
                    return
                if self.cmd == "resume" and task["status"] == "running":
                    print "The replication is running already. Resume is not needed"
                    return
                break

        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        self.rest_cmd = '/settings/replications/' + urllib.quote_plus(self.replicator)
        if self.cmd == "pause":
            rest.setParam('pauseRequested', node.bool_to_str(1))
        elif self.cmd == "resume":
            rest.setParam('pauseRequested', node.bool_to_str(0))

        opts = {
            'error_msg': "unable to %s replication" % self.cmd,
            'success_msg': "%s replication" % self.cmd
        }
        output_result = rest.restCmd('POST',
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def replicate_list(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)

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

    def replicate_settings(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        opts = {
            'error_msg': "unable to set xdcr replication settings",
            'success_msg': "set xdcr replication settings"
        }
        if self.replicator:
            self.rest_cmd = '/settings/replications/' + urllib.quote_plus(self.replicator)
        else:
            print "Error: option --xdcr-replicator is needed to update replication settings"
            return

        if self.checkpoint_interval:
            rest.setParam('checkpointInterval', self.checkpoint_interval)
            opts['success_msg'] += ' xdcrCheckpointInterval'

        if self.worker_batch_size:
            rest.setParam('workerBatchSize', self.worker_batch_size)
            opts['success_msg'] += ' xdcrWorkerBatchSize'

        if self.doc_batch_size:
            rest.setParam('docBatchSizeKb', self.doc_batch_size)
            opts['success_msg'] += ' xdcrDocBatchSizeKb'

        if self.failure_restart_interval:
            rest.setParam('failureRestartInterval', self.failure_restart_interval)
            opts['success_msg'] += ' xdcrFailureRestartInterval'

        if self.optimistic_replication_threshold:
            rest.setParam('optimisticReplicationThreshold', self.optimistic_replication_threshold)
            opts['success_msg'] += ' xdcrOptimisticReplicationThreshold'

        if self.source_nozzle_per_node:
            rest.setParam('sourceNozzlePerNode', self.source_nozzle_per_node)
            opts['success_msg'] += ' xdcrSourceNozzlePerNode'

        if self.target_nozzle_per_node:
            rest.setParam('targetNozzlePerNode', self.target_nozzle_per_node)
            opts['success_msg'] += ' xdcrTargetNozzlePerNode'

        if self.max_replication_lag:
            rest.setParam('maxExpectedReplicationLag', self.max_replication_lag)
            opts['success_msg'] += ' xdcrMaxExpectedReplicationLag'

        if self.timeout_perc_cap:
            rest.setParam('timeoutPercentageCap', self.timeout_perc_cap)
            opts['success_msg'] += ' xdcrTimeoutPercentageCap'

        if self.log_level:
            rest.setParam('logLevel', self.log_level)
            opts['success_msg'] += ' xdcrLogLevel'

        if self.stats_interval:
            rest.setParam('statsInterval', self.stats_interval)
            opts['success_msg'] += ' xdcrStatsInterval'

        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def setting(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        opts = {
            'error_msg': "unable to set xdcr internal settings",
            'success_msg': "set xdcr settings"
        }

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

        if self.source_nozzle_per_node:
            rest.setParam('sourceNozzlePerNode', self.source_nozzle_per_node)
            opts['success_msg'] += ' xdcrSourceNozzlePerNode'

        if self.target_nozzle_per_node:
            rest.setParam('targetNozzlePerNode', self.target_nozzle_per_node)
            opts['success_msg'] += ' xdcrTargetNozzlePerNode'

        if self.max_replication_lag:
            rest.setParam('maxExpectedReplicationLag', self.max_replication_lag)
            opts['success_msg'] += ' xdcrMaxExpectedReplicationLag'

        if self.timeout_perc_cap:
            rest.setParam('timeoutPercentageCap', self.timeout_perc_cap)
            opts['success_msg'] += ' xdcrTimeoutPercentageCap'

        if self.log_level:
            rest.setParam('logLevel', self.log_level)
            opts['success_msg'] += ' xdcrLogLevel'

        if self.stats_interval:
            rest.setParam('statsInterval', self.stats_interval)
            opts['success_msg'] += ' xdcrStatsInterval'

        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def getCommandSummary(self, cmd):
        """Return one-line summary info for each supported command"""
        command_summary = {
            "setting-xdcr" : "set xdcr related settings",
            "xdcr-replicate" : "xdcr operations",
            "xdcr-setup" : "set up XDCR connection"}
        if cmd in command_summary:
            return command_summary[cmd]
        else:
            return None

    def getCommandHelp(self, cmd):
        """ Obtain detailed parameter help for Xdcr commands
        Returns a list of pairs (arg1, arg1-information) or None if there's
        no help or cmd is unknown.
        """

        chkpoint_interval = [("--checkpoint-interval=[1800]",
                              "intervals between checkpoints, 60 to 14400 seconds.")]
        worker_bat_size = [("--worker-batch-size=[500]",
                            "doc batch size, 500 to 10000.")]
        doc_bat_size = [("--doc-batch-size=[2048]KB",
                         "document batching size, 10 to 100000 KB")]
        failure_restart = [("--failure-restart-interval=[30]",
                            "interval for restarting failed xdcr, 1 to 300 seconds")]
        opt_rep_theshold = [("--optimistic-replication-threshold=[256]",
                             ("document body size threshold (bytes) "
                              "to trigger optimistic replication"))]
        src_nozzle_node = [("--sourceNozzlePerNode=[1-10]",
                            "the number of source nozzles per source node")]
        tgt_nozzle_node = [("--targetNozzlePerNode=[1-100]",
                            "the number of outgoing nozzles per target node")]
        max_replication_log= [("--maxExpectedReplicationLag=MS",
                               ("the maximum replication lag (in millisecond) "
                                "that can be tolerated before it is considered timeout"))]
        timeout_perc_cap = [("--timeoutPercentageCap=[1-100]",
                             ("the maximum allowed timeout percentage."
"If this limit is exceeded, replication is considered as not healthy and may be restarted."))]
        log_level =[("--logLevel=[Error|Info|Debug|Trace]",
                     "logging level")]
        stats_interval = [("--statsInterval=[MS]",
                           "the interval (in milliseconds) for statistics updates")]

        xdcr_create = [("--create", "create a new xdcr configuration")]
        xdcr_edit = [("--edit", "modify existed xdcr configuration")]
        xdcr_delete = [("--delete", "delete existed xdcr configuration")]
        xdcr_list = [("--list", "list all xdcr configurations")]
        remote_cluster_name = [("--xdcr-cluster-name=CLUSTERNAME",
                                "remote cluster to replicate to")]
        xdcr_pause = [("--pause", "pause the replication")]
        xdcr_resume = [("--resume", "resume the replication")]
        xdcr_settings = [("--settings", "update settings for the replication")]
        remote_hostname = [("--xdcr-hostname=HOSTNAME",
                            "remote host name to connect to")]
        remote_admin = [("--xdcr-username=USERNAME",
                         "remote cluster admin username")]
        remote_pwd = [("--xdcr-password=PASSWORD",
                       "remote cluster admin password")]
        encrypt = [("--xdcr-demand-encryption=[0|1]",
                    "allow data encrypted using ssl")]
        certificate = [("--xdcr-certificate=CERTIFICATE",
                        ("pem-encoded certificate. "
                        "Need be present if xdcr-demand-encryption is true"))]
        replicator_id = [("--xdcr-replicator=REPLICATOR", "replication id")]
        from_bucket = [("--xdcr-from-bucket=BUCKET",
                       "local bucket name to replicate from")]
        to_bucket = [("--xdcr-to-bucket=BUCKETNAME",
                      "remote bucket to replicate to")]
        replication_mode = [("--xdcr-replication-mode=[xmem|capi]",
                             "replication protocol, either capi or xmem.")]

        regex = [("--filterExpression=[REGEX]",
                  "regular expression to filter replication streams")]
        if cmd == "setting-xdcr":
            return (chkpoint_interval + worker_bat_size +
                    doc_bat_size + failure_restart + opt_rep_theshold +
                    src_nozzle_node + tgt_nozzle_node + max_replication_log +
                    timeout_perc_cap + log_level + stats_interval)
        elif cmd == "xdcr-setup":
            return (xdcr_create + xdcr_edit + xdcr_list + xdcr_delete +
                    remote_cluster_name + remote_hostname + remote_admin +
                    remote_pwd + encrypt + certificate)
        elif cmd == "xdcr-replicate":
            return (xdcr_create + xdcr_delete + xdcr_list + xdcr_pause + xdcr_resume +
                    xdcr_settings + replicator_id + from_bucket + to_bucket +
                    chkpoint_interval + worker_bat_size + doc_bat_size + failure_restart +
                    opt_rep_theshold + src_nozzle_node + tgt_nozzle_node + max_replication_log +
                    timeout_perc_cap + log_level + stats_interval + replication_mode+ regex)
        else:
            return None

