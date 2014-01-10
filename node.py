"""
  Implementation for rebalance, add, remove, stop rebalance.
"""

import time
import os
import sys
import util_cli as util
import socket
import simplejson as json

from usage import usage
from restclient import *
from listservers import *

# the rest commands and associated URIs for various node operations

rest_cmds = {
    'rebalance'         :'/controller/rebalance',
    'rebalance-stop'    :'/controller/stopRebalance',
    'rebalance-status'  :'/pools/default/rebalanceProgress',
    'server-add'        :'/controller/addNode',
    'server-readd'      :'/controller/reAddNode',
    'failover'          :'/controller/failOver',
    'cluster-init'      :'/settings/web',
    'cluster-edit'      :'/settings/web',
    'node-init'         :'/nodes/self/controller/settings',
    'setting-compaction'    :'/controller/setAutoCompaction',
    'setting-notification'  :'/settings/stats',
    'setting-autofailover'  :'/settings/autoFailover',
    'setting-alert'         :'/settings/alerts',
    'user-manage'           :'/settings/readOnlyUser',
    'group-manage'          :'/pools/default/serverGroups',
    'ssl-manage'            :'/pools/default/certificate',
}

server_no_remove = [
    'rebalance-stop',
    'rebalance-status',
    'server-add',
    'server-readd',
    'failover'
]
server_no_add = [
    'rebalance-stop',
    'rebalance-status',
    'failover',
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
    'cluster-init'      :'POST',
    'cluster-edit'      :'POST',
    'node-init'         :'POST',
    'setting-compaction'    :'POST',
    'setting-notification'  :'POST',
    'setting-autofailover'  :'POST',
    'setting-alert'         :'POST',
    'user-manage'           :'POST',
    'group-manage'          :'POST',
    'ssl-manage'            :'GET',
}

bool_to_str = lambda value: str(bool(int(value))).lower()

# Map of HTTP success code, success message and error message for
# handling HTTP response properly

class Node:
    SEP = ";"
    def __init__(self):
        self.rest_cmd = rest_cmds['rebalance-status']
        self.method = 'GET'
        self.debug = False
        self.server = ''
        self.port = ''
        self.user = ''
        self.password = ''
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
        self.data_path = None
        self.index_path = None
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
        self.alert_ip_changed = None
        self.alert_disk_space = None
        self.alert_meta_overhead = None
        self.alert_meta_oom = None
        self.alert_write_failed = None

        #group management
        self.group_name = None
        self.server_list = []
        self.from_group = None
        self.to_group = None
        self.group_rename = None

        #SSL certificate management
        self.certificate_file = None
        self.cmd = None

    def runCmd(self, cmd, server, port,
               user, password, opts):

        self.rest_cmd = rest_cmds[cmd]
        self.method = methods[cmd]
        self.server = server
        self.port = int(port)
        self.user = user
        self.password = password
        servers = self.processOpts(cmd, opts)

        if self.debug:
            print "INFO: servers %s" % servers

        if cmd == 'server-add' and not servers['add']:
            usage("please list one or more --server-add=HOST[:PORT];"
                  " or use -h for more help.")

        if cmd == 'server-readd' and not servers['add']:
            usage("please list one or more --server-add=HOST[:PORT];"
                  " or use -h for more help.")

        if cmd in ('server-add', 'rebalance'):
            if not self.group_name:
                self.addServers(servers['add'])
            else:
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
                usage("please list one or more --server-failover=HOST[:PORT];"
                      " or use -h for more help.")

            self.failover(servers)

        elif cmd in ('cluster-init', 'cluster-edit'):
            self.clusterInit(cmd)

        elif cmd == 'node-init':
            self.nodeInit()

        elif cmd == 'setting-compaction':
            self.compaction()

        elif cmd == 'setting-notification':
            self.notification()

        elif cmd == 'setting-alert':
            self.alert()

        elif cmd == 'setting-autofailover':
            self.autofailover()

        elif cmd == 'user-manage':
            self.userManage()

        elif cmd == 'group-manage':
            self.groupManage()

        elif cmd == 'ssl-manage':
            self.retrieveCert()

    def clusterInit(self, cmd):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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

        opts = {
            "error_msg": "unable to init/modify %s" % self.server,
            "success_msg": "init %s" % self.server
        }

        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        # per node quota unfortunately runs against a different location
        if cmd == "cluster-init" and not self.per_node_quota:
            print "ERROR: option cluster-init-ramsize is not specified"
            return
        if self.port_new:
            self.port = int(self.port_new)
        if self.username_new:
            self.user = self.username_new
        if self.password_new:
            self.password = self.password_new

        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        if self.per_node_quota:
            rest.setParam('memoryQuota', self.per_node_quota)

        output_result = rest.restCmd(self.method,
                                     '/pools/default',
                                     self.user,
                                     self.password,
                                     opts)
        print output_result


    def nodeInit(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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
        print output_result

    def compaction(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})

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

        if self.compaction_period_from and self.compaction_period_to:
            if self.compaction_period_from >= self.compaction_period_to:
                print "ERROR: compaction from time period cannot be late than to time period"
                return
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

    def notification(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        if self.enable_notification:
            rest.setParam('sendStats', self.enable_notification)

        opts = {
            "error_msg": "unable to set notification settings",
            "success_msg": "set notification settings"
        }
        output_result = rest.restCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     opts)
        print output_result

    def alert(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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

    def processOpts(self, cmd, opts):
        """ Set standard opts.
            note: use of a server key keeps optional
            args aligned with server.
            """
        servers = {
            'add': {},
            'remove': {},
            'failover': {}
        }

        # don't allow options that don't correspond to given commands

        for o, a in opts:
            usage_msg = "option '%s' is not used with command '%s'" % (o, cmd)

            if o in ( "-r", "--server-remove"):
                if cmd in server_no_remove:
                    usage(usage_msg)
            elif o in ( "-a", "--server-add",
                        "--server-add-username",
                        "--server-add-password"):
                if cmd in server_no_add:
                    usage(usage_msg)

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
            elif o == 'email-port':
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

        return servers

    def normalize_servers(self, server_list):
        slist = []
        for server in server_list.split(Node.SEP):
            hostport = "%s:%d" % util.hostport(server)
            slist.append(hostport)
        return slist

    def addServers(self, servers):
        for server in servers:
            user = servers[server]['user']
            password = servers[server]['password']
            output_result = self.serverAdd(server,
                                           user,
                                           password)
            print output_result

    def serverAdd(self, add_server, add_with_user, add_with_password):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        rest.setParam('hostname', add_server)
        if add_with_user and add_with_password:
            rest.setParam('user', add_with_user)
            rest.setParam('password', add_with_password)

        opts = {
            'error_msg': "unable to server-add %s" % add_server,
            'success_msg': "server-add %s" % add_server
        }
        output_result = rest.restCmd('POST',
                                     rest_cmds['server-add'],
                                     self.user,
                                     self.password,
                                     opts)
        return output_result

    def reAddServers(self, servers):
        known_otps, eject_otps, failover_otps, readd_otps = \
            self.getNodeOtps(to_readd=servers['add'])

        for readd_otp in readd_otps:
            rest = restclient.RestClient(self.server,
                                         self.port,
                                         {'debug':self.debug})
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

        for node in known_nodes_list:
            if node.get('otpNode') is None:
                raise Exception("could not access node;" +
                                " please check your username (-u) and password (-p)")

            known_otps.append(node['otpNode'])
            if node['hostname'] in to_eject:
                eject_otps.append(node['otpNode'])
            if node['hostname'] in to_failover:
                if node['clusterMembership'] != 'active':
                    raise Exception('node %s is not active' % node['hostname'])
                else:
                    failover_otps.append(node['otpNode'])
            if node['hostname'] in to_readd:
                readd_otps.append(node['otpNode'])

        return (known_otps, eject_otps, failover_otps, readd_otps)

    def rebalance(self, servers):
        known_otps, eject_otps, failover_otps, readd_otps = \
            self.getNodeOtps(to_eject=servers['remove'])

        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        rest.setParam('knownNodes', ','.join(known_otps))
        rest.setParam('ejectedNodes', ','.join(eject_otps))

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
            sys.exit(1)
        else:
            print '\n' + output_result

    def rebalanceStatus(self, prefix=''):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        opts = { 'error_msg':'unable to obtain rebalance status'}

        output_result = rest.restCmd('GET',
                                     rest_cmds['rebalance-status'],
                                     self.user,
                                     self.password,
                                     opts)

        json = rest.getJson(output_result)
        if type(json) == type(list()):
            print prefix + ("ERROR: %s" % json[0])
            sys.exit(1)

        if 'errorMessage' in json:
            error_message = json['errorMessage']
        else:
            error_message = None

        return json['status'],error_message

    def rebalanceStop(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})

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
        known_otps, eject_otps, failover_otps, readd_otps = \
            self.getNodeOtps(to_failover=servers['failover'])

        if len(failover_otps) <= 0:
            usage("specified servers are not part of the cluster: %s" %
                  servers['failover'].keys())

        for failover_otp in failover_otps:
            rest = restclient.RestClient(self.server,
                                         self.port,
                                         {'debug':self.debug})
            rest.setParam('otpNode', failover_otp)

            opts = {
                'error_msg': "unable to failover %s" % failover_otp,
                'success_msg': "failover %s" % failover_otp
            }
            output_result = rest.restCmd('POST',
                                         rest_cmds['failover'],
                                         self.user,
                                         self.password,
                                         opts)
            print output_result

    def userManage(self):
        if self.cmd == 'list':
            self.roUserList()
        elif self.cmd == 'delete':
            self.roUserDelete()
        elif self.cmd == 'set':
            self.roUserSet()

    def roUserList(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})

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
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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

        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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
                usage("please specify --group-name for the operation")
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
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        output_result = rest.restCmd('GET',
                                     '/pools/default/serverGroups',
                                     self.user,
                                     self.password)
        return rest.getJson(output_result)

    def groupList(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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
            usage("invalid group name:%s" % self.group_name)
        if self.group_rename is None:
            usage("invalid group name:%s" % self.group_name)

        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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
            usage("invalid group name:%s" % self.group_name)

        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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

        uri = self.getGroupUri(self.group_name)
        if uri is None:
            usage("invalid group name:%s" % self.group_name)
        uri = "%s/addNode" % uri
        groups = self.getServerGroups()
        for server in self.server_list:
            rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
            rest.setParam('hostname', server)
            if self.sa_username:
                rest.setParam('user', self.sa_username)
            if self.sa_password:
                rest.setParam('password', self.sa_password)

            opts = {
                'error_msg': "unable to add server '%s' to group '%s'" % (server, self.group_name),
                'success_msg': "add server '%s' to group '%s'" % (server, self.group_name)
            }
            output_result = rest.restCmd('POST',
                                     uri,
                                     self.user,
                                     self.password,
                                     opts)
            print output_result

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
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
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
        if self.certificate_file is None:
            usage("please specify certificate file name for the operation")

        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        output_result = ''
        if self.cmd == 'retrieve':
            opts = {
                'error_msg': "unable to %s certificate" % self.cmd,
                'success_msg': "Successfully %s certificate" % self.cmd
            }
            output_result = rest.restCmd('GET',
                                         '/pools/default/certificate',
                                        self.user,
                                        self.password,
                                        opts)
        elif self.cmd  == 'regenerate':
            opts = {
                'error_msg': "unable to %s certificate" % self.cmd,
                'success_msg': None
            }
            output_result = rest.restCmd('POST',
                                         '/controller/regenerateCertificate',
                                        self.user,
                                        self.password,
                                        opts)
        else:
            print "ERROR: unknown request:", self.cmd
            return

        try:
            fp = open(self.certificate_file, 'w')
            fp.write(output_result)
            fp.close()
            print "SUCCESS: %s certificate to '%s'" % (self.cmd, self.certificate_file)
        except IOError, error:
            print "ERROR:", error
