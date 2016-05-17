"""
  Implementation for rebalance, add, remove, stop rebalance.
"""

import cluster_manager
import time
import os
import sys
import util_cli as util
import socket
import re
import urlparse
import json

from usage import command_error
from restclient import *
from listservers import *
from _csv import reader

try:
    import pump_bfd2
    IS_ENTERPRISE= True
except ImportError:
    IS_ENTERPRISE = False

MAX_LEN_PASSWORD = 24

# the rest commands and associated URIs for various node operations

rest_cmds = {
    'rebalance'         :'/controller/rebalance',
    'rebalance-stop'    :'/controller/stopRebalance',
    'rebalance-status'  :'/pools/default/rebalanceProgress',
    'server-add'        :'/controller/addNode',
    'server-readd'      :'/controller/reAddNode',
    'failover'          :'/controller/failOver',
    'recovery'          :'/controller/setRecoveryType',
    'cluster-init'      :'/settings/web',
    'cluster-edit'      :'/settings/web',
    'node-init'         :'/nodes/self/controller/settings',
    'setting-cluster'   :'/pools/default',
    'setting-compaction'    :'/controller/setAutoCompaction',
    'setting-notification'  :'/settings/stats',
    'setting-autofailover'  :'/settings/autoFailover',
    'setting-alert'         :'/settings/alerts',
    'setting-audit'         :'/settings/audit',
    'setting-ldap'          :'/settings/saslauthdAuth',
    'user-manage'           :'/settings/readOnlyUser',
    'setting-index'         :'/settings/indexes',
    'group-manage'          :'/pools/default/serverGroups',
    'ssl-manage'            :'/pools/default/certificate',
    'collect-logs-start'  : '/controller/startLogsCollection',
    'collect-logs-stop'   : '/controller/cancelLogsCollection',
    'collect-logs-status' : '/pools/default/tasks',
    'admin-role-manage'   : '/settings/rbac/users',
}

server_no_remove = [
    'rebalance-stop',
    'rebalance-status',
    'server-add',
    'server-readd',
    'failover',
    'recovery',
]
server_no_add = [
    'rebalance-stop',
    'rebalance-status',
    'failover',
    'recovery',
]

# Map of operations and the HTTP methods used against the REST interface

methods = {
    'rebalance'         :'POST',
    'rebalance-stop'    :'POST',
    'rebalance-status'  :'GET',
    'eject-server'      :'POST',
    'server-add'        :'POST',
    'server-readd'      :'POST',
    'failover'          :'POST',
    'recovery'          :'POST',
    'cluster-init'      :'POST',
    'cluster-edit'      :'POST',
    'node-init'         :'POST',
    'setting-cluster'   :'POST',
    'setting-compaction'    :'POST',
    'setting-notification'  :'POST',
    'setting-autofailover'  :'POST',
    'setting-alert'         :'POST',
    'setting-audit'         :'POST',
    'setting-ldap'          :'POST',
    'setting-index'         :'POST',
    'user-manage'           :'POST',
    'group-manage'          :'POST',
    'ssl-manage'            :'GET',
    'collect-logs-start'  : 'POST',
    'collect-logs-stop'   : 'POST',
    'collect-logs-status' : 'GET',
    'admin-role-manage'   : 'PUT',
}

bool_to_str = lambda value: str(bool(int(value))).lower()

# Map of HTTP success code, success message and error message for
# handling HTTP response properly

class Node:
    SEP = ","
    def __init__(self):
        self.rest_cmd = rest_cmds['rebalance-status']
        self.method = 'GET'
        self.debug = False
        self.server = ''
        self.port = ''
        self.user = ''
        self.password = ''
        self.ssl = False

        self.ro_username = ''
        self.ro_password = ''
        self.params = {}
        self.output = 'standard'
        self.password_new = None
        self.username_new = None
        self.sa_username = None
        self.sa_password = None
        self.port_new = None
        self.per_node_quota = None
        self.cluster_index_ramsize = None
        self.cluster_fts_ramsize = None
        self.index_storage_setting = None
        self.cluster_name = None
        self.data_path = None
        self.index_path = None
        self.hostname = None
        self.enable_auto_failover = None
        self.enable_notification = None
        self.autofailover_timeout = None
        self.enable_email_alert = None

        #compaction related settings
        self.compaction_db_percentage = None
        self.compaction_db_size = None
        self.compaction_view_percentage = None
        self.compaction_view_size = None
        self.compaction_period_from = None
        self.compaction_period_to = None
        self.enable_compaction_abort = None
        self.enable_compaction_parallel = None
        self.purge_interval = None

        #alert settings
        self.email_recipient = None
        self.email_sender = None
        self.email_user = None
        self.email_password = None
        self.email_host = None
        self.email_port = None
        self.email_enable_encrypt = None
        self.autofailover_node = None
        self.autofailover_max_reached = None
        self.autofailover_node_down = None
        self.autofailover_cluster_small = None
        self.autofailover_disabled = None
        self.alert_ip_changed = None
        self.alert_disk_space = None
        self.alert_meta_overhead = None
        self.alert_meta_oom = None
        self.alert_write_failed = None
        self.alert_audit_dropped = None

        #group management
        self.group_name = None
        self.server_list = []
        self.from_group = None
        self.to_group = None
        self.group_rename = None

        #SSL certificate management
        self.certificate_file = None
        self.extended = False
        self.cmd = None

        self.hard_failover = None
        self.recovery_type = None
        self.recovery_buckets = None

        # Collect logs
        self.nodes = None
        self.all_nodes = None
        self.upload = False
        self.upload_host = None
        self.customer = None
        self.ticket = ""

        #auditing
        self.audit_enabled = None
        self.audit_log_path = None
        self.audit_log_rotate_interval = None

        #ldap
        self.ldap_enabled = None
        self.ldap_admins = ''
        self.ldap_roadmins = ''
        self.ldap_default = "none"

        #index
        self.max_rollback_points = None
        self.stable_snapshot_interval = None
        self.memory_snapshot_interval = None
        self.index_threads = None
        self.services = None
        self.log_level = None

        #set-roles / delete-roles
        self.roles = None
        self.my_roles = False
        self.get_roles = None
        self.set_users = None
        self.set_names = None
        self.delete_users = None

    def runCmd(self, cmd, server, port,
               user, password, ssl, opts):
        self.rest_cmd = rest_cmds[cmd]
        self.method = methods[cmd]
        self.server = server
        self.port = int(port)
        self.user = user
        self.password = password
        self.ssl = ssl

        servers = self.processOpts(cmd, opts)
        if self.debug:
            print "INFO: servers %s" % servers

        if cmd == 'server-add' and not servers['add']:
            command_error("please list one or more --server-add=HOST[:PORT],"
                  " or use -h for more help.")

        if cmd == 'server-readd' and not servers['add']:
            command_error("please list one or more --server-add=HOST[:PORT],"
                  " or use -h for more help.")

        if cmd in ('server-add', 'rebalance'):
            if len(servers['add']) > 0:
                print "Warning: Adding server from group-manage is deprecated"
                self.groupAddServers()
            if cmd == 'rebalance':
                self.rebalance(servers)

        elif cmd == 'server-readd':
            self.reAddServers(servers)

        elif cmd == 'rebalance-status':
            output_result = self.rebalanceStatus()
            print output_result

        elif cmd == 'rebalance-stop':
            output_result = self.rebalanceStop()
            print output_result

        elif cmd == 'failover':
            if len(servers['failover']) <= 0:
                command_error("please list one or more --server-failover=HOST[:PORT];"
                      " or use -h for more help.")

            self.failover(servers)

        elif cmd == 'recovery':
            if len(servers['recovery']) <= 0:
                command_error("please list one or more --server-recovery=HOST[:PORT];"
                      " or use -h for more help.")
            self.recovery(servers)

        elif cmd in ('cluster-init', 'cluster-edit'):
            self.clusterInit(cmd)

        elif cmd == 'node-init':
            self.nodeInit()

        elif cmd == 'setting-cluster':
            self.clusterSetting()

        elif cmd == 'setting-compaction':
            self.compaction()

        elif cmd == 'setting-notification':
            self.notification()

        elif cmd == 'setting-alert':
            self.alert()

        elif cmd == 'setting-autofailover':
            self.autofailover()

        elif cmd == 'setting-audit':
            self.audit()

        elif cmd == 'setting-ldap':
            self.ldap()

        elif cmd == 'setting-index':
            self.index()

        elif cmd == 'user-manage':
            self.userManage()

        elif cmd == 'group-manage':
            self.groupManage()

        elif cmd == 'ssl-manage':
            self.retrieveCert()

        elif cmd == 'collect-logs-start':
            self.collectLogsStart(servers)

        elif cmd == 'collect-logs-stop':
            self.collectLogsStop()

        elif cmd == 'collect-logs-status':
            self.collectLogsStatus()

        elif cmd == 'admin-role-manage':
            self.alterRoles()

    def clusterInit(self, cmd):
        # We need to ensure that creating the REST username/password is the
        # last REST API that is called because once that API succeeds the
        # cluster is initialized and cluster-init cannot be run again.

        cm = cluster_manager.ClusterManager(self.server, self.port, self.user,
                                            self.password, self.ssl)

        if cmd == 'cluster-init':
            data, errors = cm.pools()
            _exitIfErrors(errors)
            if data['pools'] and len(data['pools']) > 0:
                print "Error: cluster is already initialized, use cluster-edit to change settings"
                return

        err, services = self.process_services(False)
        if err:
            print err
            return

        #set memory quota
        if cmd == 'cluster-init':
            if 'kv' in services.split(',') and not self.per_node_quota:
                print "ERROR: option cluster-ramsize is not specified"
                return
            elif 'index' in services.split(','):
                if not self.cluster_index_ramsize:
                    print "ERROR: option cluster-index-ramsize is not specified"
                    return
                param = self.index_storage_to_param(self.index_storage_setting)
                if not param:
                    print "ERROR: invalid index storage setting `%s`. Must be [default, memopt]" \
                        % self.index_storage_setting
                    return

                if self.index_storage_setting:
                    _, errors = cm.set_index_settings(param)
                    _exitIfErrors(errors)
            elif 'fts' in services.split(',') and not self.cluster_fts_ramsize:
                print "ERROR: option fts-index-ramsize is not specified"
                return

        opts = {
            "error_msg": "unable to set memory quota",
            "success_msg": "set memory quota successfully"
        }
        rest = util.restclient_factory(self.server,
                                       self.port,
                                       {'debug':self.debug},
                                       self.ssl)
        if self.per_node_quota:
            rest.setParam('memoryQuota', self.per_node_quota)
        if self.cluster_index_ramsize:
            rest.setParam('indexMemoryQuota', self.cluster_index_ramsize)
        if self.cluster_fts_ramsize:
            rest.setParam('ftsMemoryQuota', self.cluster_fts_ramsize)
        if rest.params:
            output_result = rest.restCmd(self.method,
                                         '/pools/default',
                                         self.user,
                                         self.password,
                                         opts)

        #setup services
        if cmd == "cluster-init":
            opts = {
                "error_msg": "unable to setup services",
                "success_msg": "setup service successfully"
            }
            rest = util.restclient_factory(self.server,
                                           self.port,
                                           {'debug':self.debug},
                                           self.ssl)
            rest.setParam('services', services)
            output_result = rest.restCmd(self.method,
                                         '/node/controller/setupServices',
                                         self.user,
                                         self.password,
                                         opts)

        # setup REST credentials/REST port
        if cmd == 'cluster-init' or self.username_new or self.password_new or self.port_new:
            self.enable_notification = "true"
            self.notification(False)
            rest = util.restclient_factory(self.server,
                                         self.port,
                                         {'debug':self.debug},
                                         self.ssl)
            if self.port_new:
                rest.setParam('port', self.port_new)
            else:
                rest.setParam('port', 'SAME')
            rest.setParam('initStatus', 'done')
            if self.username_new:
                rest.setParam('username', self.username_new)
            else:
                rest.setParam('username', self.user)
            if self.password_new:
                rest.setParam('password', self.password_new)
            else:
                rest.setParam('password', self.password)

            if not (rest.getParam('username') and rest.getParam('password')):
                print "ERROR: Both username and password are required."
                return

            if len(rest.getParam('password')) > MAX_LEN_PASSWORD:
                print "ERROR: Password length %s exceeds maximum number of characters allowed, which is %s" \
                      % (len(rest.getParam('password')), MAX_LEN_PASSWORD)
                return

            opts = {
                "error_msg": "unable to init/modify %s" % self.server,
                "success_msg": "init/edit %s" % self.server
            }

            output_result = rest.restCmd(self.method,
                                         self.rest_cmd,
                                         self.user,
                                         self.password,
                                         opts)
        print output_result

    def index_storage_to_param(self, value):
        if not value or value == "default":
            return "forestdb"
        if value == "memopt":
            return "memory_optimized"
        return None

    def process_services(self, data_required):
        if not self.services:
            self.services = "data"
        sep = Node.SEP
        if self.services.find(sep) < 0:
            #backward compatible when using ";" as separator
            sep = ";"
        svc_list = list(set([w.strip() for w in self.services.split(sep)]))
        svc_candidate = ["data", "index", "query", "fts"]
        for svc in svc_list:
            if svc not in svc_candidate:
                return "ERROR: invalid service: %s" % svc, None
        if data_required and "data" not in svc_list:
            svc_list.append("data")
        if not IS_ENTERPRISE:
            if len(svc_list) != len(svc_candidate):
                if len(svc_list) != 1 or "data" not in svc_list:
                    return "ERROR: Community Edition requires that all nodes provision all services or data service only", None

        services = ",".join(svc_list)
        for old, new in [[";", ","], ["data", "kv"], ["query", "n1ql"]]:
            services = services.replace(old, new)
        return None, services

    def nodeInit(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if self.data_path:
            rest.setParam('path', self.data_path)

        if self.index_path:
            rest.setParam('index_path', self.index_path)

        opts = {
            "error_msg": "unable to init %s" % self.server,
            "success_msg": "init %s" % self.server
        }

        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        if self.hostname:
            rest = util.restclient_factory(self.server,
                                         self.port,
                                         {'debug':self.debug},
                                         self.ssl)
            if self.hostname:
                rest.setParam('hostname', self.hostname)

            opts = {
                "error_msg": "unable to set hostname for %s" % self.server,
                "success_msg": "set hostname for %s" % self.server
            }

            output_result = rest.restCmd('POST',
                                         '/node/controller/rename',
                                         self.user,
                                         self.password,
                                         opts)
        print output_result

    def compaction(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)

        if self.compaction_db_percentage:
            rest.setParam('databaseFragmentationThreshold[percentage]', self.compaction_db_percentage)
        if self.compaction_db_size:
            self.compaction_db_size = int(self.compaction_db_size) * 1024**2
            rest.setParam('databaseFragmentationThreshold[size]', self.compaction_db_size)
        if self.compaction_view_percentage:
            rest.setParam('viewFragmentationThreshold[percentage]', self.compaction_view_percentage)
        if self.compaction_view_size:
            self.compaction_view_size = int(self.compaction_view_size) * 1024**2
            rest.setParam('viewFragmentationThreshold[size]', self.compaction_view_size)
        if self.compaction_period_from:
            hour, minute = self.compaction_period_from.split(':')
            if (int(hour) not in range(24)) or (int(minute) not in range(60)):
                print "ERROR: invalid hour or minute value for compaction period"
                return
            else:
                rest.setParam('allowedTimePeriod[fromHour]', int(hour))
                rest.setParam('allowedTimePeriod[fromMinute]', int(minute))
        if self.compaction_period_to:
            hour, minute = self.compaction_period_to.split(':')
            if (int(hour) not in range(24)) or (int(minute) not in range(60)):
                print "ERROR: invalid hour or minute value for compaction"
                return
            else:
                rest.setParam('allowedTimePeriod[toHour]', hour)
                rest.setParam('allowedTimePeriod[toMinute]', minute)
        if self.enable_compaction_abort:
            rest.setParam('allowedTimePeriod[abortOutside]', self.enable_compaction_abort)
        if self.enable_compaction_parallel:
            rest.setParam('parallelDBAndViewCompaction', self.enable_compaction_parallel)
        else:
            self.enable_compaction_parallel = bool_to_str(0)
            rest.setParam('parallelDBAndViewCompaction', self.enable_compaction_parallel)

        if self.compaction_period_from or self.compaction_period_to or self.enable_compaction_abort:
            if not (self.compaction_period_from and self.compaction_period_to and \
                    self.enable_compaction_abort):
                print "ERROR: compaction-period-from, compaction-period-to and enable-compaction-abort have to be specified at the same time"
                return
        if self.purge_interval:
            rest.setParam('purgeInterval', self.purge_interval)

        opts = {
            "error_msg": "unable to set compaction settings",
            "success_msg": "set compaction settings"
        }
        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def clusterSetting(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if self.per_node_quota:
            rest.setParam('memoryQuota', self.per_node_quota)
        if self.cluster_name is not None:
            rest.setParam('clusterName', self.cluster_name)
        if self.cluster_index_ramsize:
            rest.setParam('indexMemoryQuota', self.cluster_index_ramsize)
        if self.cluster_fts_ramsize:
            rest.setParam('ftsMemoryQuota', self.cluster_fts_ramsize)
        opts = {
            "error_msg": "unable to set cluster configurations",
            "success_msg": "set cluster settings"
        }
        if rest.params:
            output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
            print output_result
        else:
            print "Error: No parameters specified"

    def notification(self, print_status=True):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if self.enable_notification:
            rest.setParam('sendStats', self.enable_notification)

        opts = {
            "error_msg": "unable to set notification settings",
            "success_msg": "set notification settings"
        }
        output_result = rest.restCmd(self.method,
                                     '/settings/stats',
                                     self.user,
                                     self.password,
                                     opts)

        if print_status:
            print output_result

    def alert(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        alert_opts = ''
        if self.enable_email_alert:
            rest.setParam('enabled', self.enable_email_alert)
        if self.email_recipient:
            rest.setParam('recipients', self.email_recipient)
        if self.email_sender:
            rest.setParam('sender', self.email_sender)
        if self.email_user:
            rest.setParam('emailUser', self.email_user)
        if self.email_password:
            rest.setParam('emailPass', self.email_password)
        if self.email_host:
            rest.setParam('emailHost', self.email_host)
        if self.email_port:
            rest.setParam('emailPort', self.email_port)
        if self.email_enable_encrypt:
            rest.setParam('emailEncrypt', self.email_enable_encrypt)
        if self.autofailover_node:
            alert_opts = alert_opts + 'auto_failover_node,'
        if self.autofailover_max_reached:
            alert_opts = alert_opts + 'auto_failover_maximum_reached,'
        if self.autofailover_node_down:
            alert_opts = alert_opts + 'auto_failover_other_nodes_down,'
        if self.autofailover_cluster_small:
            alert_opts = alert_opts + 'auto_failover_cluster_too_small,'
        if self.autofailover_disabled:
            alert_opts = alert_opts + 'auto_failover_disabled,'
        if self.alert_ip_changed:
            alert_opts = alert_opts + 'ip,'
        if self.alert_disk_space:
            alert_opts = alert_opts + 'disk,'
        if self.alert_meta_overhead:
            alert_opts = alert_opts + 'overhead,'
        if self.alert_meta_oom:
            alert_opts = alert_opts + 'ep_oom_errors,'
        if self.alert_write_failed:
            alert_opts = alert_opts + 'ep_item_commit_failed,'
        if self.alert_audit_dropped:
            alert_opts = alert_opts + 'audit_dropped_events,'

        if alert_opts:
            # remove last separator
            alert_opts = alert_opts[:-1]
            rest.setParam('alerts', alert_opts)

        opts = {
            "error_msg": "unable to set alert settings",
            "success_msg": "set alert settings"
        }
        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def autofailover(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if self.autofailover_timeout:
            if int(self.autofailover_timeout) < 30:
                print "ERROR: Timeout value must be larger than 30 second."
                return
            else:
                rest.setParam('timeout', self.autofailover_timeout)

        if self.enable_auto_failover:
            rest.setParam('enabled', self.enable_auto_failover)

        opts = {
            "error_msg": "unable to set auto failover settings",
            "success_msg": "set auto failover settings"
        }
        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def audit(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if self.audit_enabled:
            rest.setParam('auditdEnabled', self.audit_enabled)

        if self.audit_log_path:
            rest.setParam('logPath', self.audit_log_path)
        elif self.audit_enabled == "true":
             rest.setParam('logPath', "/opt/couchbase/var/lib/couchbase/logs")
        if self.audit_log_rotate_interval:
            rest.setParam('rotateInterval', int(self.audit_log_rotate_interval)*60)
        elif self.audit_enabled == "true":
            rest.setParam('rotateInterval', 86400)

        opts = {
            "error_msg": "unable to set audit settings",
            "success_msg": "set audit settings"
        }
        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def ldap(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if self.ldap_enabled == 'true':
            rest.setParam('enabled', 'true')
            if self.ldap_default == 'admins':
                rest.setParam('roAdmins', self.ldap_roadmins.replace(Node.SEP, "\n"))
            elif self.ldap_default == 'roadmins':
                rest.setParam('admins', self.ldap_admins.replace(Node.SEP, "\n"))
            else:
                rest.setParam('admins', self.ldap_admins.replace(Node.SEP,"\n"))
                rest.setParam('roAdmins', self.ldap_roadmins.replace(Node.SEP, "\n"))
        else:
            rest.setParam('enabled', 'false')

        opts = {
            "error_msg": "unable to set LDAP auth settings",
            "success_msg": "set LDAP auth settings"
        }
        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    # Role-Based Access Control
    def alterRoles(self):
        cm = cluster_manager.ClusterManager(self.server, self.port, self.user,
                                            self.password, self.ssl)

        # need to check arguments
        if (self.my_roles == None and self.get_roles == None and \
            self.set_users == None and self.delete_users == None):
            print "ERROR: You must specify either '--my-roles', '--get-roles', " \
                "'--set-users', or '--delete-users'"
            return

        if self.my_roles and (self.get_roles or self.set_users or self.roles or self.delete_users):
            print "ERROR: The 'my-roles' option may not be used with any other option."
            return

        if self.get_roles and (self.my_roles or self.set_users or self.roles or self.delete_users):
            print "ERROR: The 'get-roles' option may not be used with any other option."
            return

        if (self.set_users and self.roles == None) or (self.set_users == None and self.roles):
            print "ERROR: You must specify lists of both users and roles for those users.\n  --set-users=[comma delimited user list] --roles=[comma-delimited list of one or more from full_admin, readonly_admin, cluster_admin, replication_admin, bucket_admin(opt bucket name), view_admin]"
            return

        # my_roles
        if self.my_roles:
            data, errors = cm.myRoles()
            if errors == None:
                print "SUCCESS: my roles:"

        # get_roles
        elif self.get_roles:
            data, errors = cm.getRoles()
            if errors == None:
                print "SUCCESS: user/role list:"

        # set_users
        elif self.set_users:
            data, errors = cm.setRoles(self.set_users,self.roles,self.set_names)
            if errors == None:
                print "SUCCESS: set roles for ",self.set_users,". New user/role list:"

        # delete_users
        else:
            data, errors = cm.deleteRoles(self.delete_users)
            if errors == None:
                print "SUCCESS: removed users ", self.delete_users, ". New user/role list:"

        _exitIfErrors(errors)
        print json.dumps(data,indent=2)

    def index(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if self.max_rollback_points:
            rest.setParam("maxRollbackPoints", self.max_rollback_points)
        if self.stable_snapshot_interval:
            rest.setParam("stableSnapshotInterval", self.stable_snapshot_interval)
        if self.memory_snapshot_interval:
            rest.setParam("memorySnapshotInterval", self.memory_snapshot_interval)
        if self.index_threads:
            rest.setParam("indexerThreads", self.index_threads)
        if self.log_level:
            rest.setParam("logLevel", self.log_level)

        opts = {
            "error_msg": "unable to set index settings",
            "success_msg": "set index settings"
        }
        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def processOpts(self, cmd, opts):
        """ Set standard opts.
            note: use of a server key keeps optional
            args aligned with server.
            """
        servers = {
            'add': {},
            'remove': {},
            'failover': {},
            'recovery': {},
            'log': {},
        }

        # don't allow options that don't correspond to given commands

        for o, a in opts:
            command_error_msg = "option '%s' is not used with command '%s'" % (o, cmd)

            if o in ( "-r", "--server-remove"):
                if cmd in server_no_remove:
                    command_error(command_error_msg)
            elif o in ( "-a", "--server-add",
                        "--server-add-username",
                        "--server-add-password"):
                if cmd in server_no_add:
                    command_error(command_error_msg)

        server = None
        for o, a in opts:
            if o in ("-a", "--server-add"):
                if a == "self":
                    a = socket.gethostbyname(socket.getfqdn())
                server = "%s:%d" % util.hostport(a)
                servers['add'][server] = { 'user':'', 'password':''}
                self.server_list.append(server)
            elif o == "--server-add-username":
                if server:
                    servers['add'][server]['user'] = a
                self.sa_username = a
            elif o == "--server-add-password":
                if server:
                    servers['add'][server]['password'] = a
                self.sa_password = a
            elif o in ( "-r", "--server-remove"):
                server = "%s:%d" % util.hostport(a)
                servers['remove'][server] = True
                server = None
            elif o in ( "--server-failover"):
                server = "%s:%d" % util.hostport(a)
                servers['failover'][server] = True
                server = None
            elif o in ( "--server-recovery"):
                server = "%s:%d" % util.hostport(a)
                servers['recovery'][server] = True
                server = None
            elif o == "--nodes":
                for server in self.normalize_servers(a):
                    servers['log'][server] = True
            elif o in ('-o', '--output'):
                if a == 'json':
                    self.output = a
                server = None
            elif o in ('-d', '--debug'):
                self.debug = True
                server = None
            elif o in ('--cluster-init-password', '--cluster-password'):
                self.password_new = a
            elif o in ('--cluster-init-username', '--cluster-username'):
                self.username_new = a
            elif o in ('--cluster-init-port', '--cluster-port'):
                self.port_new = a
            elif o in ('--cluster-init-ramsize', '--cluster-ramsize'):
                self.per_node_quota = a
            elif o == '--cluster-index-ramsize':
                self.cluster_index_ramsize = a
            elif o == '--cluster-fts-ramsize':
                self.cluster_fts_ramsize = a
            elif o == '--index-storage-setting':
                self.index_storage_setting = a
            elif o == '--cluster-name':
                self.cluster_name = a
            elif o == '--enable-auto-failover':
                self.enable_auto_failover = bool_to_str(a)
            elif o == '--enable-notification':
                self.enable_notification = bool_to_str(a)
            elif o == '--auto-failover-timeout':
                self.autofailover_timeout = a
            elif o == '--compaction-db-percentage':
                self.compaction_db_percentage = a
            elif o == '--compaction-db-size':
                self.compaction_db_size = a
            elif o == '--compaction-view-percentage':
                self.compaction_view_percentage = a
            elif o == '--compaction-view-size':
                self.compaction_view_size = a
            elif o == '--compaction-period-from':
                self.compaction_period_from = a
            elif o == '--compaction-period-to':
                self.compaction_period_to = a
            elif o == '--enable-compaction-abort':
                self.enable_compaction_abort = bool_to_str(a)
            elif o == '--enable-compaction-parallel':
                self.enable_compaction_parallel = bool_to_str(a)
            elif o == '--enable-email-alert':
                self.enable_email_alert = bool_to_str(a)
            elif o == '--node-init-data-path':
                self.data_path = a
            elif o == '--node-init-index-path':
                self.index_path = a
            elif o == '--node-init-hostname':
                self.hostname = a
            elif o == '--email-recipients':
                self.email_recipient = a
            elif o == '--email-sender':
                self.email_sender = a
            elif o == '--email-user':
                self.email_user = a
            elif o == '--email-password':
                self.email_password = a
            elif o == '--email-host':
                self.email_host = a
            elif o == '--email-port':
                self.email_port = a
            elif o == '--enable-email-encrypt':
                self.email_enable_encrypt = bool_to_str(a)
            elif o == '--alert-auto-failover-node':
                self.autofailover_node = True
            elif o == '--alert-auto-failover-max-reached':
                self.autofailover_max_reached = True
            elif o == '--alert-auto-failover-node-down':
                self.autofailover_node_down = True
            elif o == '--alert-auto-failover-cluster-small':
                self.autofailover_cluster_small = True
            elif o == '--alert-auto-failover-disabled':
                self.autofailover_disabled = True
            elif o == '--alert-ip-changed':
                self.alert_ip_changed = True
            elif o == '--alert-disk-space':
                self.alert_disk_space = True
            elif o == '--alert-meta-overhead':
                self.alert_meta_overhead = True
            elif o == '--alert-meta-oom':
                self.alert_meta_oom = True
            elif o == '--alert-write-failed':
                self.alert_write_failed = True
            elif o == '--alert-audit-msg-dropped':
                self.alert_audit_dropped = True
            elif o == '--create':
                self.cmd = 'create'
            elif o == '--list':
                self.cmd = 'list'
            elif o == '--delete':
                self.cmd = 'delete'
            elif o == '--set':
                self.cmd = 'set'
            elif o == '--ro-username':
                self.ro_username = a
            elif o == '--ro-password':
                self.ro_password = a
            elif o == '--metadata-purge-interval':
                self.purge_interval = a
            elif o == '--group-name':
                self.group_name = a
            elif o == '--add-servers':
                self.server_list = self.normalize_servers(a)
                self.cmd = 'add-servers'
            elif o == '--remove-servers':
                self.server_list = self.normalize_servers(a)
                self.cmd = 'remove-servers'
            elif o == '--move-servers':
                self.server_list = self.normalize_servers(a)
                self.cmd = 'move-servers'
            elif o == '--from-group':
                self.from_group = a
            elif o == '--to-group':
                self.to_group = a
            elif o == '--rename':
                self.group_rename = a
                self.cmd = 'rename'
            elif o == '--retrieve-cert':
                self.cmd = 'retrieve'
                self.certificate_file = a
            elif o == '--regenerate-cert':
                self.cmd = 'regenerate'
                self.certificate_file = a
            elif o == '--cluster-cert-info':
                self.cmd = 'cluster-cert-info'
            elif o == '--extended':
                self.extended = True
            elif o == '--node-cert-info':
                self.cmd = 'node-cert-info'
            elif o == '--upload-cluster-ca':
                self.cmd = 'upload-cluster-ca'
                self.certificate_file = a
            elif o == '--set-node-certificate':
                self.cmd = 'set-node-certificate'
            elif o == '--force':
                self.hard_failover = True
            elif o == '--recovery-type':
                self.recovery_type = a
            elif o == '--recovery-buckets':
                self.recovery_buckets = a
            elif o == '--nodes':
                self.nodes = a
            elif o == '--all-nodes':
                self.all_nodes = True
            elif o == '--upload':
                self.upload = True
            elif o == '--upload-host':
                self.upload_host = a
            elif o == '--customer':
                self.customer = a
            elif o == '--ticket':
                self.ticket = a
            elif o == '--services':
                self.services = a
            elif o == '--audit-log-rotate-interval':
                self.audit_log_rotate_interval = a
            elif o == '--audit-log-path':
                self.audit_log_path = a
            elif o == '--audit-enabled':
                self.audit_enabled = bool_to_str(a)
            elif o == '--ldap-enabled':
                self.ldap_enabled = bool_to_str(a)
            elif o == '--ldap-admins':
                self.ldap_admins = a
            elif o == '--ldap-roadmins':
                self.ldap_roadmins = a
            elif o == '--ldap-default':
                self.ldap_default = a
            elif o == '--index-max-rollback-points':
                self.max_rollback_points = a
            elif o == '--index-stable-snapshot-interval':
                self.stable_snapshot_interval = a
            elif o == '--index-memory-snapshot-interval':
                self.memory_snapshot_interval = a
            elif o == '--index-threads':
                self.index_threads = a
            elif o == '--index-log-level':
                self.log_level = a

            elif o == '--roles':
                self.roles = a
            elif o == '--my-roles':
                self.my_roles = True
            elif o == '--get-roles':
                self.get_roles = True
            elif o == '--set-users':
                self.set_users = a
            elif o == '--set-names':
                self.set_names = a
            elif o == '--delete-users':
                self.delete_users = a

        return servers

    def normalize_servers(self, server_list):
        slist = []
        sep = Node.SEP
        if server_list.find(sep) < 0:
            #backward compatible with ";" as separator
            sep = ";"
        for server in server_list.split(sep):
            hostport = "%s:%d" % util.hostport(server)
            slist.append(hostport)
        return slist

    def reAddServers(self, servers):
        known_otps, eject_otps, failover_otps, readd_otps, _ = \
            self.getNodeOtps(to_readd=servers['add'])

        for readd_otp in readd_otps:
            rest = util.restclient_factory(self.server,
                                         self.port,
                                         {'debug':self.debug},
                                         self.ssl)
            rest.setParam('otpNode', readd_otp)

            opts = {
                'error_msg': "unable to re-add %s" % readd_otp,
                'success_msg': "re-add %s" % readd_otp
            }
            output_result = rest.restCmd('POST',
                                         rest_cmds['server-readd'],
                                         self.user,
                                         self.password,
                                         opts)
            print output_result

    def getNodeOtps(self, to_eject=[], to_failover=[], to_readd=[]):
        """ Convert known nodes into otp node id's.
            """
        listservers = ListServers()
        known_nodes_list = listservers.getNodes(
                                listservers.getData(self.server,
                                                    self.port,
                                                    self.user,
                                                    self.password))
        known_otps = []
        eject_otps = []
        failover_otps = []
        readd_otps = []
        hostnames = []

        for node in known_nodes_list:
            if node.get('otpNode') is None:
                raise Exception("could not access node")
            known_otps.append(node['otpNode'])
            hostnames.append(node['hostname'])
            if node['hostname'] in to_eject:
                eject_otps.append(node['otpNode'])
            if node['hostname'] in to_failover:
                if node['clusterMembership'] != 'active':
                    raise Exception('node %s is not active' % node['hostname'])
                else:
                    failover_otps.append((node['otpNode'], node['status']))
            _, host = node['otpNode'].split('@')
            hostport = "%s:%d" % util.hostport(host)
            if node['hostname'] in to_readd or hostport in to_readd:
                readd_otps.append(node['otpNode'])

        return (known_otps, eject_otps, failover_otps, readd_otps, hostnames)

    def recovery(self, servers):
        known_otps, eject_otps, failover_otps, readd_otps, _ = \
            self.getNodeOtps(to_readd=servers['recovery'])
        for readd_otp in readd_otps:
            rest = util.restclient_factory(self.server,
                                         self.port,
                                         {'debug':self.debug},
                                         self.ssl)
            opts = {
                'error_msg': "unable to setRecoveryType for node %s" % readd_otp,
                'success_msg': "setRecoveryType for node %s" % readd_otp
            }
            rest.setParam('otpNode', readd_otp)
            if self.recovery_type:
                rest.setParam('recoveryType', self.recovery_type)
            else:
                rest.setParam('recoveryType', 'delta')
            output_result = rest.restCmd('POST',
                                         '/controller/setRecoveryType',
                                         self.user,
                                         self.password,
                                         opts)
            print output_result

    def rebalance(self, servers):
        known_otps, eject_otps, failover_otps, readd_otps, _ = \
            self.getNodeOtps(to_eject=servers['remove'])
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        rest.setParam('knownNodes', ','.join(known_otps))
        rest.setParam('ejectedNodes', ','.join(eject_otps))
        if self.recovery_buckets:
            rest.setParam('requireDeltaRecoveryBuckets', self.recovery_buckets)
        opts = {
            'success_msg': 'rebalanced cluster',
            'error_msg': 'unable to rebalance cluster'
        }
        output_result = rest.restCmd('POST',
                                     rest_cmds['rebalance'],
                                     self.user,
                                     self.password,
                                     opts)
        if self.debug:
            print "INFO: rebalance started: %s" % output_result

        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

        print "INFO: rebalancing",

        status, error = self.rebalanceStatus(prefix='\n')
        while status in['running', 'unknown']:
            print ".",
            time.sleep(0.5)
            try:
                status, error = self.rebalanceStatus(prefix='\n')
            except socket.error:
                time.sleep(2)
                status, error = self.rebalanceStatus(prefix='\n')

        if error:
            print '\n' + error
            sys.exit(1)
        else:
            print '\n' + output_result

    def rebalanceStatus(self, prefix=''):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)

        opts = {
            'error_msg': "unable to obtain rebalance status",
            'success_msg': "retrieve replication status successfully"
        }
        output_result = rest.restCmd('GET',
                                     '/pools/default/tasks',
                                     self.user,
                                     self.password,
                                     opts)
        tasks = rest.getJson(output_result)
        for task in tasks:
            error_message = None
            if "errorMessage" in task:
                error_message = task['errorMessage']

            if task["type"] == "rebalance":
                if task["status"] == "running":
                    return task["status"], error_message
                if task["status"] == "notRunning":
                    if task.has_key("statusIsStale"):
                        if task["statusIsStale"] or task["statusIsStale"] == "true":
                            return "unknown", error_message

                return task["status"], error_message

        return "unknown", error_message

    def rebalanceStop(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)

        opts = {
            'success_msg': 'rebalance cluster stopped',
            'error_msg': 'unable to stop rebalance'
        }
        output_result = rest.restCmd('POST',
                                     rest_cmds['rebalance-stop'],
                                     self.user,
                                     self.password,
                                     opts)
        return output_result


    def failover(self, servers):
        known_otps, eject_otps, failover_otps, readd_otps, _ = \
            self.getNodeOtps(to_failover=servers['failover'])

        if len(failover_otps) <= 0:
            command_error("specified servers are not part of the cluster: %s" %
                  servers['failover'].keys())

        for failover_otp, node_status in failover_otps:
            rest = util.restclient_factory(self.server,
                                         self.port,
                                         {'debug':self.debug},
                                         self.ssl)
            opts = {
                'error_msg': "unable to failover %s" % failover_otp,
                'success_msg': "failover %s" % failover_otp
            }
            rest.setParam('otpNode', failover_otp)
            if self.hard_failover or node_status != 'healthy':
                output_result = rest.restCmd('POST',
                                             rest_cmds['failover'],
                                             self.user,
                                             self.password,
                                             opts)
                print output_result
            else:
                output_result = rest.restCmd('POST',
                                             '/controller/startGracefulFailover',
                                             self.user,
                                             self.password,
                                             opts)
                if self.debug:
                    print "INFO: rebalance started: %s" % output_result

                sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

                print "INFO: graceful failover",

                status, error = self.rebalanceStatus(prefix='\n')
                while status == 'running':
                    print ".",
                    time.sleep(0.5)
                    try:
                        status, error = self.rebalanceStatus(prefix='\n')
                    except socket.error:
                        time.sleep(2)
                        status, error = self.rebalanceStatus(prefix='\n')

                if error:
                    print '\n' + error
                else:
                    print '\n' + output_result

    def userManage(self):
        if self.cmd == 'list':
            self.roUserList()
        elif self.cmd == 'delete':
            self.roUserDelete()
        elif self.cmd == 'set':
            self.roUserSet()

    def roUserList(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        opts = { 'error_msg':'not any read only user defined'}
        try:
            output_result = rest.restCmd('GET',
                                         '/settings/readOnlyAdminName',
                                         self.user,
                                         self.password,
                                         opts)
            json = rest.getJson(output_result)
            print json
        except:
            pass

    def roUserDelete(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)

        opts = {
            'success_msg': 'readOnly user deleted',
            'error_msg': 'unable to delete readOnly user'
        }
        output_result = rest.restCmd('DELETE',
                                     "/settings/readOnlyUser",
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def roUserSet(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        try:
            output_result = rest.restCmd('GET',
                                         '/settings/readOnlyAdminName',
                                         self.user,
                                         self.password)
            json = rest.getJson(output_result)
            print "ERROR: readonly user %s exist already. Delete it before creating a new one" % json
            return
        except:
            pass

        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        if self.ro_username:
            rest.setParam('username', self.ro_username)
        if self.ro_password:
            rest.setParam('password', self.ro_password)
        opts = {
            'success_msg': 'readOnly user created',
            'error_msg': 'fail to create readOnly user'
        }
        output_result = rest.restCmd('POST',
                                     "/settings/readOnlyUser",
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def groupManage(self):
        if self.cmd == 'move-servers':
            self.groupMoveServer()
        elif self.cmd == 'list':
             self.groupList()
        else:
            if self.group_name is None:
                command_error("please specify --group-name for the operation")
            elif self.cmd == 'delete':
                self.groupDelete()
            elif self.cmd == 'create':
                self.groupCreate()
            elif self.cmd == 'add-servers':
                self.groupAddServers()
            elif self.cmd == 'rename':
                self.groupRename()
            else:
                print "Unknown group command:%s" % self.cmd

    def getGroupUri(self, groupName):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        output_result = rest.restCmd('GET',
                                     '/pools/default/serverGroups',
                                     self.user,
                                     self.password)
        groups = rest.getJson(output_result)
        for group in groups["groups"]:
            if groupName == group["name"]:
                return group["uri"]
        return None

    def getServerGroups(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        output_result = rest.restCmd('GET',
                                     '/pools/default/serverGroups',
                                     self.user,
                                     self.password)
        return rest.getJson(output_result)

    def groupList(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        output_result = rest.restCmd('GET',
                                     '/pools/default/serverGroups',
                                     self.user,
                                     self.password)
        groups = rest.getJson(output_result)
        found = False
        for group in groups["groups"]:
            if self.group_name is None or self.group_name == group['name']:
                found = True
                print '%s' % group['name']
                for node in group['nodes']:
                    print ' server: %s' % node["hostname"]
        if not found and self.group_name:
            print "Invalid group name: %s" % self.group_name

    def groupCreate(self):
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        rest.setParam('name', self.group_name)
        opts = {
            'error_msg': "unable to create group %s" % self.group_name,
            'success_msg': "group created %s" % self.group_name
        }
        output_result = rest.restCmd('POST',
                                     '/pools/default/serverGroups',
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def groupRename(self):
        uri = self.getGroupUri(self.group_name)
        if uri is None:
            command_error("invalid group name:%s" % self.group_name)
        if self.group_rename is None:
            command_error("invalid group name:%s" % self.group_name)

        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        rest.setParam('name', self.group_rename)
        opts = {
            'error_msg': "unable to rename group %s" % self.group_name,
            'success_msg': "group renamed %s" % self.group_name
        }
        output_result = rest.restCmd('PUT',
                                     uri,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def groupDelete(self):
        uri = self.getGroupUri(self.group_name)
        if uri is None:
            command_error("invalid group name:%s" % self.group_name)

        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        rest.setParam('name', self.group_name)
        opts = {
            'error_msg': "unable to delete group %s" % self.group_name,
            'success_msg': "group deleted %s" % self.group_name
        }
        output_result = rest.restCmd('DELETE',
                                     uri,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def groupAddServers(self):
        # If this is the first index node added then we need to make sure to
        # set the index storage setting.
        indexStorageParam = self.index_storage_to_param(self.index_storage_setting)
        if not indexStorageParam:
            print "ERROR: invalid index storage setting `%s`. Must be [default, memopt]" \
                % self.index_storage_setting
            return

        cm = cluster_manager.ClusterManager(self.server, self.port, self.user,
                                            self.password, self.ssl)

        settings, errors = cm.index_settings()
        _exitIfErrors(errors)

        if not settings:
            print "Error: unable to infer the current index storage mode"
            return

        if settings['storageMode'] == "":
            _, errors = cm.set_index_settings(indexStorageParam)
            if errors:
                _exitIfErrors(errors)
        elif settings['storageMode'] != self.index_storage_setting and \
             self.index_storage_setting:
            print "Error: Cannot change index storage mode from `%s` to `%s`" % \
                (settings['storageMode'], self.index_storage_setting)
            return

        err, services = self.process_services(False)
        if err:
            print err
            return
        for server in self.server_list:
            _, errors = cm.add_server(server, self.group_name, self.sa_username,
                                           self.sa_password, services)
            if errors:
                _exitIfErrors(errors, "Error: Failed to add server %s: " % server)

            if self.group_name:
                print "Server %s added to group %s" % (server, self.group_name)
            else:
                print "Server %s added" % server

    def groupMoveServer(self):
        groups = self.getServerGroups()
        node_info = {}
        for group in groups["groups"]:
            if self.from_group == group['name']:
                for server in self.server_list:
                    for node in group["nodes"]:
                        if server == node["hostname"]:
                            node_info[server] = node
                            group["nodes"].remove(node)
        if not node_info:
            print "No servers removed from group '%s'" % self.from_group
            return

        for group in groups["groups"]:
            if self.to_group == group['name']:
                for server in self.server_list:
                    found = False
                    for node in group["nodes"]:
                        if server == node["hostname"]:
                            found = True
                            break
                    if not found:
                        group["nodes"].append(node_info[server])

        payload = json.dumps(groups)
        rest = util.restclient_factory(self.server,
                                     self.port,
                                     {'debug':self.debug},
                                     self.ssl)
        rest.setPayload(payload)

        opts = {
            'error_msg': "unable to move servers from group '%s' to group '%s'" % (self.from_group, self.to_group),
            'success_msg': "move servers from group '%s' to group '%s'" % (self.from_group, self.to_group)
        }
        output_result = rest.restCmd('PUT',
                                     groups["uri"],
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def retrieveCert(self):
        if self.cmd in ['retrieve', 'regenerate', 'upload-cluster-ca'] and self.certificate_file is None:
            command_error("please specify certificate file name for the operation")

        cm = cluster_manager.ClusterManager(self.server, self.port, self.user, self.password, self.ssl)

        if self.cmd == 'retrieve':
            print "Warning --retrieve-cert is deprecated, use --cluster-cert-info"
            certificate, errors = cm.retrieve_cluster_certificate()
            _exitIfErrors(errors)
            _exitOnFileWriteFailure(self.certificate_file, certificate)
            print "SUCCESS: %s certificate to '%s'" % (self.cmd, self.certificate_file)
        elif self.cmd  == 'regenerate':
            certificate, errors = cm.regenerate_cluster_certificate()
            _exitIfErrors(errors)
            _exitOnFileWriteFailure(self.certificate_file, certificate)
            print "SUCCESS: %s certificate to '%s'" % (self.cmd, self.certificate_file)
        elif self.cmd == 'cluster-cert-info':
            certificate, errors = cm.retrieve_cluster_certificate(self.extended)
            _exitIfErrors(errors)
            if isinstance(certificate, dict):
                print json.dumps(certificate, sort_keys=True, indent=2)
            else:
                print certificate
        elif self.cmd == 'node-cert-info':
            certificate, errors = cm.retrieve_node_certificate('%s:%d' % (self.server, self.port))
            _exitIfErrors(errors)
            print json.dumps(certificate, sort_keys=True, indent=2)
        elif self.cmd == 'upload-cluster-ca':
            certificate = _exitOnFileReadFailure(self.certificate_file)
            _, errors = cm.upload_cluster_certificate(certificate)
            _exitIfErrors(errors)
            print "SUCCESS: uploaded cluster certificate to %s:%d" % (self.server, self.port)
        elif self.cmd == 'set-node-certificate':
            _, errors = cm.set_node_certificate()
            _exitIfErrors(errors)
            print "SUCCESS: node certificate set"
        else:
            print "ERROR: unknown request:", self.cmd

    def collectLogsStart(self, servers):
        """Starts a cluster-wide log collection task"""
        if (servers['log'] is None) and (self.all_nodes is not True):
            command_error("please specify a list of nodes to collect logs from, " +
                  " or 'all-nodes'")

        rest = util.restclient_factory(self.server, self.port,
                                     {'debug': self.debug}, self.ssl)
        if self.all_nodes:
            rest.setParam("nodes", "*")
        else:
            known_otps, eject_otps, failover_otps, readd_otps, hostnames = \
                self.getNodeOtps(to_readd=servers['log'])
            if not len(readd_otps):
                msg = ",".join(hostnames)
                command_error("invalid node name specified for collecting logs, available nodes are:\n"+msg)

            nodelist = ",".join(readd_otps)
            rest.setParam("nodes", nodelist)
            print "NODES:", nodelist

        if self.upload:
            if self.upload_host is None:
                command_error("please specify an upload-host when using --upload")

            rest.setParam("uploadHost", self.upload_host)

            if not self.customer:
                command_error("please specify a value for --customer when using" +
                      " --upload")

            rest.setParam("customer", self.customer)
            rest.setParam("ticket", self.ticket)

        opts = {
            'error_msg': "unable to start log collection:",
            'success_msg': "Log collection started"
        }

        output_result = rest.restCmd(self.method, self.rest_cmd, self.user,
                                     self.password, opts)
        print output_result

    def collectLogsStop(self):
        """Stops a cluster-wide log collection task"""
        rest = util.restclient_factory(self.server, self.port,
                                     {'debug': self.debug}, self.ssl)

        opts = {
            'success_msg': 'collect logs successfully stopped',
            'error_msg': 'unable to stop collect logs'
        }
        output_result = rest.restCmd(self.method, self.rest_cmd, self.user,
                                     self.password, opts)
        print output_result

    def collectLogsStatus(self):
        """Shows the current status of log collection task"""
        rest = util.restclient_factory(self.server, self.port,
                                     {'debug': self.debug}, self.ssl)

        opts = {
            'error_msg': 'unable to obtain collect logs status'
        }
        output_result = rest.restCmd(self.method, self.rest_cmd, self.user,
                                     self.password, opts)

        output_json = rest.getJson(output_result)

        for e in output_json:
            if ((type(e) == type(dict()) and ('type' in e) and
                (e['type'] == 'clusterLogsCollection'))):
                print "Status:   %s" % e['status']
                if 'perNode' in e:
                    print "Details:"
                    for node, ns in e["perNode"].iteritems():
                        print '\tNode:', node
                        print '\tStatus:', ns['status']
                        for f in ["path", "statusCode", "url", "uploadStatusCode", "uploadOutput"]:
                            if f in ns:
                                print '\t', f, ":", ns[f]
                        print
                return

    def getCommandSummary(self, cmd):
        """Return one-line summary info for each supported command"""
        command_summary = {
            "server-list" :"list all servers in a cluster",
            "server-info" :"show details on one server",
            "server-add" :"add one or more servers to the cluster",
            "server-readd" :"readd a server that was failed over",
            "group-manage" :"manage server groups",
            "rebalance" :"start a cluster rebalancing",
            "rebalance-stop" :"stop current cluster rebalancing",
            "rebalance-status" :"show status of current cluster rebalancing",
            "failover" :"failover one or more servers",
            "recovery" :"recover one or more servers",
            "setting-cluster" : "set cluster settings",
            "setting-compaction" : "set auto compaction settings",
            "setting-notification" : "set notification settings",
            "setting-alert" : "set email alert settings",
            "setting-autofailover" : "set auto failover settings",
            "collect-logs-start" : "start a cluster-wide log collection",
            "collect-logs-stop" : "stop a cluster-wide log collection",
            "collect-logs-status" : "show the status of cluster-wide log collection",
            "cluster-init" : "set the username,password and port of the cluster",
            "cluster-edit" : "modify cluster settings",
            "node-init" : "set node specific parameters",
            "ssl-manage" : "manage cluster certificate",
            "user-manage" : "manage read only user",
            "setting-index" : "set index settings",
            "setting-ldap" : "set ldap settings",
            "setting-audit" : "set audit settings",
            "admin-role-manage" : "set access-control roles for users"
        }
        if cmd in command_summary:
            return command_summary[cmd]
        else:
            return None

    def getCommandHelp(self, cmd):
        """ Obtain detailed parameter help for Node commands
        Returns a list of pairs (arg1, arg1-information) or None if there's
        no help or cmd is unknown.
        """

        # Some common flags for server- commands
        server_common = [("--server-add=HOST[:PORT]", "server to be added,"),
                         ("--server-add-username=USERNAME",
                          "admin username for the server to be added"),
                         ("--server-add-password=PASSWORD",
                          "admin password for the server to be added"),
                         ("--group-name=GROUPNAME", "group that server belongs")]

        services = [("--services=data,index,query,fts",
                     "services that server runs")]

        if cmd == "server-add" or cmd == "rebalance":
            return [("--index-storage-setting=SETTING", "index storage type [default, memopt]")] \
            + server_common + services
        elif cmd == "server-readd":
            return server_common
        elif cmd == "group-manage":
            return [
            ("--group-name=GROUPNAME", "group name"),
            ("--create", "create a new group"),
            ("--delete", "delete an empty group"),
            ("--list", "show group/server relationship map"),
            ("--rename=NEWGROUPNAME", "rename group to new name"),
            ("--add-servers=HOST[:PORT],HOST[:PORT]",
             "add a list of servers to group"),
            ("--move-servers=HOST[:PORT],HOST[:PORT]",
             "move a list of servers from group"),
            ("--from-group=GROUPNAME", "group name to move servers from"),
            ("--to-group=GROUPNAME", "group name to move servers into"),
            ("--index-storage-setting=SETTING", "index storage type [default, memopt]")] + services
        elif cmd == "cluster-init" or cmd == "cluster-edit":
            return [
            ("--cluster-username=USER", "new admin username"),
            ("--cluster-password=PASSWORD", "new admin password"),
            ("--cluster-port=PORT", "new cluster REST/http port"),
            ("--cluster-ramsize=RAMSIZEMB", "per node data service ram quota in MB"),
            ("--cluster-index-ramsize=RAMSIZEMB", "per node index service ram quota in MB"),
            ("--cluster-fts-ramsize=RAMSIZEMB", "per node fts service ram quota in MB"),
            ("--index-storage-setting=SETTING", "index storage type [default, memopt]")] + services
        elif cmd == "node-init":
            return [
            ("--node-init-data-path=PATH", "data path for database files"),
            ("--node-init-index-path=PATH", "index path for view data")]
        elif cmd == "failover":
            return [
            ("--server-failover=HOST[:PORT]", "server to failover"),
            ("--force", "failover node from cluster right away")]
        elif cmd == "recovery":
            return [
            ("--server-recovery=HOST[:PORT]", "server to recover"),
            ("--recovery-type=TYPE[delta|full]",
             "type of recovery to be performed for a node")]
        elif cmd == "user-manage":
            return [
            ("--set", "create/modify a read only user"),
            ("--list", "list any read only user"),
            ("--delete", "delete read only user"),
            ("--ro-username=USERNAME", "readonly user name"),
            ("--ro-password=PASSWORD", "readonly user password")]
        elif cmd == "setting-alert":
            return [
            ("--enable-email-alert=[0|1]", "allow email alert"),
            ("--email-recipients=RECIPIENT",
             "email recipients, separate addresses with , or ;"),
            ("--email-sender=SENDER", "sender email address"),
            ("--email-user=USER", "email server username"),
            ("--email-password=PWD", "email server password"),
            ("--email-host=HOST", "email server host"),
            ("--email-port=PORT", "email server port"),
            ("--enable-email-encrypt=[0|1]", "email encrypt"),
            ("--alert-auto-failover-node", "node was auto failover"),
            ("--alert-auto-failover-max-reached",
             "maximum number of auto failover nodes was reached"),
            ("--alert-auto-failover-node-down",
             "node wasn't auto failover as other nodes are down at the same time"),
            ("--alert-auto-failover-cluster-small",
             "node wasn't auto fail over as cluster was too small"),
            ("--alert-auto-failover-disabled",
             "node was not auto-failed-over as auto-failover for one or more services running on the node is disabled"),
            ("--alert-ip-changed", "node ip address has changed unexpectedly"),
            ("--alert-disk-space",
             "disk space used for persistent storgage has reached at least 90% capacity"),
            ("--alert-meta-overhead",
             "metadata overhead is more than 50%"),
            ("--alert-meta-oom",
             "bucket memory on a node is entirely used for metadata"),
            ("--alert-write-failed",
             "writing data to disk for a specific bucket has failed"),
            ("--alert-audit-msg-dropped", "writing event to audit log has failed")]
        elif cmd == "setting-cluster":
            return [("--cluster-name=[CLUSTERNAME]", "cluster name"),
                    ("--cluster-ramsize=[RAMSIZEMB]", "per node data service ram quota in MB"),
                    ("--cluster-index-ramsize=[RAMSIZEMB]","per node index service ram quota in MB"),
                    ("--cluster-fts-ramsize=RAMSIZEMB", "per node fts service ram quota in MB")]
        elif cmd == "setting-notification":
            return [("--enable-notification=[0|1]", "allow notification")]
        elif cmd == "setting-autofailover":
            return [("--enable-auto-failover=[0|1]", "allow auto failover"),
                    ("--auto-failover-timeout=TIMEOUT (>=30)",
                     "specify timeout that expires to trigger auto failover")]
        elif cmd == "ssl-manage":
            return [("--cluster-cert-info", "prints cluster certificate info"),
                    ("--node-cert-info", "prints node certificate info"),
                    ("--retrieve-cert=CERTIFICATE",
                     "retrieve cluster certificate AND save to a pem file"),
                    ("--regenerate-cert=CERTIFICATE",
                     "regenerate cluster certificate AND save to a pem file"),
                    ("--set-node-certificate", "sets the node certificate"),
                    ("--upload-cluster-ca", "uploads a new cluster certificate")]
        elif cmd == "setting-audit":
            return [
            ("--audit-log-rotate-interval=[MINUTES]", "log rotation interval"),
            ("--audit-log-path=[PATH]", "target log directory"),
            ("--audit-enabled=[0|1]", "enable auditing or not")]
        elif cmd == "setting-ldap":
            return [
            ("--ldap-admins=", "full admins, separated by comma"),
            ("--ldap-roadmins=", "read only admins, separated by comma"),
            ("--ldap-enabled=[0|1]", "using LDAP protocol for authentication"),
            ("--ldap-default=[admins|roadmins|none]", "set default ldap accounts")]
        elif cmd == "setting-index":
            return [
            ("--index-max-rollback-points=[5]", "max rollback points"),
            ("--index-stable-snapshot-interval=SECONDS", "stable snapshot interval"),
            ("--index-memory-snapshot-interval=SECONDS", "in memory snapshot interval"),
            ("--index-threads=[4]", "indexer threads"),
            ("--index-log-level=[debug|silent|fatal|error|warn|info|verbose|timing|trace]", "indexer log level")]
        elif cmd == "collect-logs-start":
            return [
            ("--all-nodes", "Collect logs from all accessible cluster nodes"),
            ("--nodes=HOST[:PORT],HOST[:PORT]",
             "Collect logs from the specified subset of cluster nodes"),
            ("--upload", "Upload collects logs to specified host"),
            ("--upload-host=HOST",
             "Host to upload logs to (Manditory when --upload specified)"),
            ("--customer=CUSTOMER",
             "Customer name to use when uploading logs (Mandatory when --upload specified)"),
            ("--ticket=TICKET_NUMBER",
             "Ticket number to associate the uploaded logs with")]
        elif cmd == "admin-role-manage":
            return [
            ("--my-roles", "Return a list of roles for the current user."),
            ("--get-roles", "Return list of users and roles."),
            ("--set-users", "A comma-delimited list of user ids to set acess-control roles for"),
            ("--set-names", "A optional quoted, comma-delimited list names, one for each specified user id"),
            ("--roles", "A comma-delimited list of roles to set for users, one or more from full_admin, readonly_admin, cluster_admin, replication_admin, bucket_admin[opt bucket name], view_admin"),
            ("--delete-users", "A comma-delimited list of users to remove from access control")
            ]
        else:
            return None

    def getCommandExampleHelp(self, cmd):
        """ Obtain detailed example help for command
        Returns a list of command examples to illustrate how to use command
        or None if there's no example help or cmd is unknown.
        """

        if cmd == "cluster-init":
            return [("Set data service ram quota and index ram quota",
"""
    couchbase-cli cluster-init -c 192.168.0.1:8091 \\
       --cluster-username=Administrator \\
       --cluster-password=password \\
       --cluster-port=8080 \\
       --services=data,index \\
       --cluster-ramsize=300 \\
       --cluster-index-ramsize=256\\
       --index-storage-setting=memopt""")]
        elif cmd == "cluster-edit":
            return [("Change the cluster username, password, port and data service ram quota",
"""
    couchbase-cli cluster-edit -c 192.168.0.1:8091 \\
       --cluster-username=Administrator1 \\
       --cluster-password=password1 \\
       --cluster-port=8080 \\
       --cluster-ramsize=300 \\
       -u Administrator -p password""")]
        elif cmd == "node-init":
            return [("Set data path and hostname for an unprovisioned cluster",
"""
    couchbse-cli node-init -c 192.168.0.1:8091 \\
       --node-init-data-path=/tmp/data \\
       --node-init-index-path=/tmp/index \\
       --node-init-hostname=myhostname \\
       -u Administrator -p password"""),
                    ("Change the data path",
"""
     couchbase-cli node-init -c 192.168.0.1:8091 \\
       --node-init-data-path=/tmp \\
       -u Administrator -p password""")]
        elif cmd == "server-add":
            return [("Add a node to a cluster, but do not rebalance",
"""
    couchbase-cli server-add -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091 \\
       --server-add-username=Administrator1 \\
       --server-add-password=password1 \\

       --group-name=group1 \\
       --index-storage-setting=memopt \\
       -u Administrator -p password"""),
                    ("Add a node to a cluster, but do not rebalance",
"""
    couchbase-cli server-add -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091 \\
       --server-add-username=Administrator1 \\
       --server-add-password=password1 \\
       --group-name=group1 \\
       -u Administrator -p password""")]
        elif cmd == "rebalance":
            return [("Add a node to a cluster and rebalance",
"""
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091 \\
       --server-add-username=Administrator1 \\
       --server-add-password=password1 \\
       --group-name=group1 \\
       -u Administrator -p password"""),
                    ("Add a node to a cluster and rebalance",
"""
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
       --server-add=192.168.0.2:8091 \\
       --server-add-username=Administrator1 \\
       --server-add-password=password1 \\
       --group-name=group1 \\
       -u Administrator -p password"""),
                    ("Remove a node from a cluster and rebalance",
"""
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
       --server-remove=192.168.0.2:8091 \\
       -u Administrator -p password"""),
                    ("Remove and add nodes from/to a cluster and rebalance",
"""
    couchbase-cli rebalance -c 192.168.0.1:8091 \\
      --server-remove=192.168.0.2 \\
      --server-add=192.168.0.4 \\
      --server-add-username=Administrator1 \\
      --server-add-password=password1 \\
      --group-name=group1 \\
      -u Administrator -p password""")
       ]
        elif cmd == "rebalance-stop":
            return [("Stop the current rebalancing",
"""
    couchbase-cli rebalance-stop -c 192.168.0.1:8091 \\
       -u Administrator -p password""")]
        elif cmd == "recovery":
            return [("Set recovery type to a server",
"""
    couchbase-cli recovery -c 192.168.0.1:8091 \\
       --server-recovery=192.168.0.2 \\
       --recovery-type=full \\
       -u Administrator -p password""")]
        elif cmd == "failover":
            return [("Set a failover, readd, recovery and rebalance sequence operations",
"""
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
       -u Administrator -p password""")]
        elif cmd == "user-manage":
            return [("List read only user in a cluster",
"""
    couchbase-cli user-manage --list -c 192.168.0.1:8091 \\
           -u Administrator -p password"""),
                ("Delete a read only user in a cluster",
"""
    couchbase-cli user-manage -c 192.168.0.1:8091 \\
        --delete --ro-username=readonlyuser \\
        -u Administrator -p password"""),
                ("Create/modify a read only user in a cluster",
"""
    couchbase-cli user-manage -c 192.168.0.1:8091 \\
        --set --ro-username=readonlyuser --ro-password=readonlypassword \\
        -u Administrator -p password""")]
        elif cmd == "group-manage":
            return [("Create a new group",
"""
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --create --group-name=group1 -u Administrator -p password"""),
                ("Delete an empty group",
"""
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --delete --group-name=group1 -u Administrator -p password"""),
                ("Rename an existed group",
"""
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --rename=newgroup --group-name=group1 -u Administrator -p password"""),
                ("Show group/server map",
"""
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --list -u Administrator -p password"""),
                ("Add a server to a group",
"""
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --add-servers=10.1.1.1:8091,10.1.1.2:8091 \\
        --group-name=group1 \\
        --server-add-username=Administrator1 \\
        --server-add-password=password1 \\
        --services=data,index,query,fts \\
        -u Administrator -p password"""),
                ("Move list of servers from group1 to group2",
"""
    couchbase-cli group-manage -c 192.168.0.1:8091 \\
        --move-servers=10.1.1.1:8091,10.1.1.2:8091 \\
        --from-group=group1 \\
        --to-group=group2 \\
        -u Administrator -p password""")]
        elif cmd == "ssl-manage":
            return [("Download a cluster certificate",
"""
    couchbase-cli ssl-manage -c 192.168.0.1:8091 \\
        --retrieve-cert=/tmp/test.pem \\
        -u Administrator -p password

    couchbase-cli ssl-manage -c 192.168.0.1:8091 \\
        --cluster-cert-info \\
        -u Administrator -p password"""),
                ("Regenerate AND download a cluster certificate",
"""
    couchbase-cli ssl-manage -c 192.168.0.1:8091 \\
        --regenerate-cert=/tmp/test.pem \\
        -u Administrator -p password"""),
                ("Download the extended cluster certificate",
"""
    couchbase-cli ssl-manage -c 192.168.0.1:8091 \\
        --cluster-cert-info --extended \\
        -u Administrator -p password"""),
                ("Download the current node certificate",
"""
    couchbase-cli ssl-manage -c 192.168.0.1:8091 \\
        --node-cert-info \\
        -u Administrator -p password"""),
                ("Upload a new cluster certificate",
"""
    couchbase-cli ssl-manage -c 192.168.0.1:8091 \\
        --upload-cluster-ca=/tmp/test.pem \\
        -u Administrator -p password"""),
                ("Set the new node certificate",
"""
    couchbase-cli ssl-manage -c 192.168.0.1:8091 \\
        --set-node-certificate \\
        -u Administrator -p password""")]
        elif cmd == "collect-logs-start":
            return [("Start cluster-wide log collection for whole cluster",
"""
    couchbase-cli collect-logs-start -c 192.168.0.1:8091 \\
        -u Administrator -p password \\
        --all-nodes --upload --upload-host=host.upload.com \\
        --customer="example inc" --ticket=12345"""),
                ("Start cluster-wide log collection for selected nodes",
"""
    couchbase-cli collect-logs-start -c 192.168.0.1:8091 \\
        -u Administrator -p password \\
        --nodes=10.1.2.3:8091,10.1.2.4 --upload --upload-host=host.upload.com \\
        --customer="example inc" --ticket=12345""")]
        elif cmd == "collect-logs-stop":
            return [("Stop cluster-wide log collection",
"""
    couchbase-cli collect-logs-stop -c 192.168.0.1:8091 \\
        -u Administrator -p password""")]
        elif cmd == "collect-logs-status":
            return [("Show status of cluster-wide log collection",
"""
    couchbase-cli collect-logs-status -c 192.168.0.1:8091 \\
        -u Administrator -p password""")]
        elif cmd == "setting-ldap":
            return [("Enable LDAP with None default",
"""
    couchbase-cli setting-ldap -c 192.168.0.1:8091 \\
        --ldap-enabled=1 --ldap-admins=u1,u2 --ldap-roadmins=u3,u3,u5 \\
        -u Administrator -p password"""),
                ("Enable LDAP with full admin default",
"""
    couchbase-cli setting-ldap -c 192.168.0.1:8091 \\
        --ldap-enabled=1 --ldap-default=admins --ldap-roadmins=u3,u3,u5 \\
        -u Administrator -p password"""),
                ("Enable LDAP with read only default",
"""
    couchbase-cli setting-ldap -c 192.168.0.1:8091 \\
        --ldap-enabled=1 --ldap-default=roadmins --ldap-admins=u1,u2 \\
        -u Administrator -p password"""),
                ("Disable LDAP",
"""
    couchbase-cli setting-ldap -c 192.168.0.1:8091 \\
        --ldap-enabled=0  -u Administrator -p password""")]
        elif cmd == "setting-audit":
            return [("Enable audit",
"""
    couchbase-cli setting-audit -c 192.168.0.1:8091 \\
        --audit-enabled=1 --audit-log-rotate-interval=900 \\
        --audit-log-path="/opt/couchbase/var/lib/couchbase/logs"
        -u Administrator -p password"""),
                ("Disable audit",
"""
    couchbase-cli setting-audit -c 192.168.0.1:8091 \\
        --audit-enabled=0 -u Administrator -p password""")]
        elif cmd == "setting-index":
            return [("Indexer setting",
"""
    couchbase-cli setting-index  -c 192.168.0.1:8091 \\
        --index-max-rollback-points=5 \\
        --index-stable-snapshot-interval=5000 \\
        --index-memory-snapshot-interval=200 \\
        --index-threads=5 \\
        --index-log-level=debug \\
        -u Administrator -p password""")]

        elif cmd == "admin-role-manage":
            return [("Show the current users roles.",
"""
    couchbase-cli admin-role-manage -c 192.168.0.1:8091 --my-roles
            """),
            ("Get a list of all users and roles",
"""
    couchbase-cli admin-role-manage -c 192.168.0.1:8091 --get-roles
            """),
                    ("Make bob and mary cluster_admins, and bucket admins for the default bucket",
"""
    couchbase-cli admin-role-manage -c 192.168.0.1:8091 \\
        --set-users=bob,mary --set-names="Bob Smith,Mary Jones" --roles=cluster_admin,bucket_admin[default]
            """),
                    ("Make jen bucket admins for all buckets",
"""
    couchbase-cli admin-role-manage -c 192.168.0.1:8091 \\
        --set-users=jen --roles=bucket_admin
            """),
            ("Remove all roles for bob",
"""
    couchbase-cli admin-role-manage -c 192.168.0.1:8091 --delete-users=bob
            """)
            ]

        else:
            return None

def _exitIfErrors(errors, prefix=""):
    if errors:
        for error in errors:
            print prefix + error
        sys.exit(1)

def _exitOnFileWriteFailure(fname, bytes):
    try:
        fp = open(fname, 'w')
        fp.write(bytes)
        fp.close()
    except IOError, error:
        print "ERROR:", error
        sys.exit(1)

def _exitOnFileReadFailure(fname):
    try:
        fp = open(fname, 'r')
        bytes = fp.read()
        fp.close()
        return bytes
    except IOError, error:
        print "ERROR:", error
        sys.exit(1)
