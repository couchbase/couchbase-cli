
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
from _csv import reader

try:
    import pump_bfd2
    IS_ENTERPRISE= True
except ImportError:
    IS_ENTERPRISE = False

# the rest commands and associated URIs for various node operations

rest_cmds = {
    'server-readd'      :'/controller/reAddNode',
    'recovery'          :'/controller/setRecoveryType',
    'node-init'         :'/nodes/self/controller/settings',
    'setting-compaction'    :'/controller/setAutoCompaction',
    'group-manage'          :'/pools/default/serverGroups',
    'ssl-manage'            :'/pools/default/certificate',
    'collect-logs-start'  : '/controller/startLogsCollection',
    'collect-logs-stop'   : '/controller/cancelLogsCollection',
    'collect-logs-status' : '/pools/default/tasks',
    'admin-role-manage'   : '/settings/rbac/users',
}

server_no_remove = [
    'server-readd',
    'recovery',
]
server_no_add = [
    'recovery',
]

# Map of operations and the HTTP methods used against the REST interface

methods = {
    'eject-server'      :'POST',
    'server-readd'      :'POST',
    'recovery'          :'POST',
    'node-init'         :'POST',
    'setting-compaction'    :'POST',
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
        self.rest_cmd = None
        self.method = 'GET'
        self.debug = False
        self.server = ''
        self.port = ''
        self.user = ''
        self.password = ''
        self.ssl = False

        self.params = {}
        self.output = 'standard'
        self.sa_username = None
        self.sa_password = None
        self.index_storage_setting = None
        self.data_path = None
        self.index_path = None
        self.hostname = None
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

        self.recovery_type = None
        self.recovery_buckets = None

        # Collect logs
        self.nodes = None
        self.all_nodes = None
        self.upload = False
        self.upload_host = None
        self.customer = None
        self.ticket = ""

        #index
        self.services = None

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

        if cmd == 'server-readd' and not servers['add']:
            command_error("please list one or more --server-add=HOST[:PORT],"
                  " or use -h for more help.")

        elif cmd == 'server-readd':
            self.reAddServers(servers)

        elif cmd == 'recovery':
            if len(servers['recovery']) <= 0:
                command_error("please list one or more --server-recovery=HOST[:PORT];"
                      " or use -h for more help.")
            self.recovery(servers)

        elif cmd == 'node-init':
            self.nodeInit()

        elif cmd == 'setting-compaction':
            self.compaction()

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
            print "ERROR: You must specify lists of both users and roles for those users.\n  --set-users=[comma delimited user list] --roles=[comma-delimited list of one or more from admin, ro_admin, cluster_admin, replication_admin, bucket_admin[bucket name or '*'], views_admin[bucket name or '*']"
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
            elif o == '--index-storage-setting':
                self.index_storage_setting = a
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
            elif o == '--create':
                self.cmd = 'create'
            elif o == '--list':
                self.cmd = 'list'
            elif o == '--delete':
                self.cmd = 'delete'
            elif o == '--set':
                self.cmd = 'set'
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
        cm = cluster_manager.ClusterManager(self.server, self.port, self.user,
                                            self.password, self.ssl)
        result, errors = cm.pools('default')
        _exitIfErrors(errors)

        known_nodes_list = result["nodes"]
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

        if self.cmd  == 'regenerate':
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
            "server-readd" :"readd a server that was failed over",
            "group-manage" :"manage server groups",
            "recovery" :"recover one or more servers",
            "setting-compaction" : "set auto compaction settings",
            "collect-logs-start" : "start a cluster-wide log collection",
            "collect-logs-stop" : "stop a cluster-wide log collection",
            "collect-logs-status" : "show the status of cluster-wide log collection",
            "node-init" : "set node specific parameters",
            "ssl-manage" : "manage cluster certificate",
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

        if cmd == "server-readd":
            return server_common
        elif cmd == "group-manage":
            return [
            ("--group-name=GROUPNAME", "group name"),
            ("--create", "create a new group"),
            ("--delete", "delete an empty group"),
            ("--list", "show group/server relationship map"),
            ("--rename=NEWGROUPNAME", "rename group to new name"),
            ("--move-servers=HOST[:PORT],HOST[:PORT]",
             "move a list of servers from group"),
            ("--from-group=GROUPNAME", "group name to move servers from"),
            ("--to-group=GROUPNAME", "group name to move servers into")]
        elif cmd == "node-init":
            return [
            ("--node-init-data-path=PATH", "data path for database files"),
            ("--node-init-index-path=PATH", "index path for view data")]
        elif cmd == "recovery":
            return [
            ("--server-recovery=HOST[:PORT]", "server to recover"),
            ("--recovery-type=TYPE[delta|full]",
             "type of recovery to be performed for a node")]
        elif cmd == "ssl-manage":
            return [("--cluster-cert-info", "prints cluster certificate info"),
                    ("--node-cert-info", "prints node certificate info"),
                    ("--regenerate-cert=CERTIFICATE",
                     "regenerate cluster certificate AND save to a pem file"),
                    ("--set-node-certificate", "sets the node certificate"),
                    ("--upload-cluster-ca", "uploads a new cluster certificate")]
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
            ("--roles", "A comma-delimited list of roles to set for users, one or more from admin, ro_admin, cluster_admin, replication_admin, bucket_admin[bucket name or '*'], views_admin[bucket name or '*']"),
            ("--delete-users", "A comma-delimited list of users to remove from access control")
            ]
        else:
            return None

    def getCommandExampleHelp(self, cmd):
        """ Obtain detailed example help for command
        Returns a list of command examples to illustrate how to use command
        or None if there's no example help or cmd is unknown.
        """

        if cmd == "node-init":
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
        elif cmd == "recovery":
            return [("Set recovery type to a server",
"""
    couchbase-cli recovery -c 192.168.0.1:8091 \\
       --server-recovery=192.168.0.2 \\
       --recovery-type=full \\
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
        --set-users=jen --roles=bucket_admin[*]
            """),
            ("Remove all roles for bob",
"""
    couchbase-cli admin-role-manage -c 192.168.0.1:8091 --delete-users=bob
            """)
            ]

        else:
            return None

def _warning(msg):
    print "WARNING: " + msg

def _exitIfErrors(errors):
    if errors:
        for error in errors:
            print "ERROR: " + error
        sys.exit(1)

def _exitOnFileWriteFailure(fname, bytes):
    try:
        fp = open(fname, 'w')
        fp.write(bytes)
        fp.close()
    except IOError, error:
        print "ERROR: ", error
        sys.exit(1)

def _exitOnFileReadFailure(fname):
    try:
        fp = open(fname, 'r')
        bytes = fp.read()
        fp.close()
        return bytes
    except IOError, error:
        print "ERROR: ", error
        sys.exit(1)

def isInt(s):
    if s is None:
        return False
    try:
        int(s)
        return True
    except ValueError:
        return False
