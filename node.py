"""
  Implementation for rebalance, add, remove, stop rebalance.
"""

import time
import os
import sys
import util

from usage import usage
from restclient import *
from listservers import *

# the rest commands and associated URIs for various node operations

rest_cmds = {
    'rebalance'         :'/controller/rebalance',
    'rebalance-stop'    :'/controller/stopRebalance',
    'rebalance-status'  :'/pools/default/rebalanceProgress',
    'eject-server'      :'/controller/ejectNode',
    'server-add'        :'/controller/addNode',
    're-add-server'     :'/controller/addNode',
    'failover'          :'/controller/failOver',
}

server_no_remove = [
    'server-add',
    're-add-server',
    'rebalance-status',
    'rebalance-stop',
]
server_no_add = [
    'rebalance-status',
    'rebalance-stop',
    'failover',
]

# Map of operations and the HTTP methods used against the REST interface

methods = {
    'rebalance'         :'POST',
    'rebalance-stop'    :'POST',
    'rebalance-status'  :'GET',
    'eject-server'      :'POST',
    'server-add'        :'POST',
    'failover'          :'POST',
    're-add-server'     :'POST',
}

# Map of HTTP success code, success message and error message for
# handling HTTP response properly

class Node:
    def __init__(self):
        self.rest_cmd = rest_cmds['rebalance-status']
        self.method = 'GET'
        self.debug = False
        self.server = ''
        self.port = ''
        self.user = ''
        self.password = ''
        self.params = {}
        self.output = 'standard'

    def runCmd(self, cmd, server, port,
               user, password, opts):
        output_result = ''

        self.rest_cmd = rest_cmds[cmd]
        self.method = methods[cmd]
        self.server = server
        self.port = int(port)
        self.user = user
        self.password = password

        servers = self.processOpts(cmd, opts)

        if self.debug:
            print "INFO: servers %s" % servers

        if cmd in ('server-add', 'rebalance'):
            self.addServers(servers['add'])
            if cmd == 'rebalance':
                self.rebalance(servers)
        elif cmd == 'rebalance-status':
            output_result = self.rebalanceStatus()
            print output_result

    def processOpts(self, cmd, opts):
        """ Set standard opts.
            note: use of a server key keeps optional
            args aligned with server.
            """
        servers = {
            'add': {},
            'remove': {}
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
                server = "%s:%d" % util.hostport(a)
                servers['add'][server] = { 'user':'', 'password':''}
            elif o == "--server-add-username":
                if server is None:
                    usage("please specify --server-add" +
                          " before --server-add-username")
                servers['add'][server]['user'] = a
            elif o == "--server-add-password":
                if server is None:
                    usage("please specify --server-add" +
                          " before --server-add-password")
                servers['add'][server]['password'] = a
            elif o in ( "-r", "--server-remove"):
                server = "%s:%d" % util.hostport(a)
                servers['remove'][a] = True
                server = None
            elif o in ('-o', '--output'):
                if a == 'json':
                    self.output = a
                server = None
            elif o in ('-d', '--debug'):
                self.debug = True
                server = None

        return servers

    def addServers(self, servers):
        for server in servers:
            user = servers[server]['user']
            password = servers[server]['password']
            output_result = self.serverAdd(server,
                                           user,
                                           password)
            print output_result

    def serverAdd(self, add_server, add_with_user, add_with_password):
        opts = {}
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        rest.setParam('hostname', add_server)
        if add_with_user and add_with_password:
            rest.setParam('user', add_with_user)
            rest.setParam('password', add_with_password)

        opts['error_msg'] = "unable to add %s" % add_server
        opts['success_msg'] = "added %s" % add_server

        output_result = rest.restCmd('POST',
                                     rest_cmds['server-add'],
                                     self.user,
                                     self.password,
                                     opts)
        return output_result

    def getNodeOtps(self, to_eject):
        """ Obtains the list of known nodes to be passed to a rebalance
            """
        listservers = ListServers()
        known_nodes_list = listservers.getNodes(
                                listservers.getData(self.server,
                                                    self.port,
                                                    self.user,
                                                    self.password))

        eject_otps = []
        known_otps = []

        for node in known_nodes_list:
            known_otps.append(node['otpNode'])
            if node['hostname'] in to_eject:
                eject_otps.append(node['otpNode'])

        return({ 'ejectedNodes' : ','.join(eject_otps),
                 'knownNodes'   : ','.join(known_otps) })

    def rebalance(self, servers):
        opts = {}
        nodes = self.getNodeOtps(servers['remove'])

        # POST response will be handled except server-add because that is
        # handled in a loop per server in command line list
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        rest.setParam('knownNodes', nodes['knownNodes'])
        rest.setParam('ejectedNodes', nodes['ejectedNodes'])

        opts['success_msg'] = 'rebalanced cluster'
        opts['error_msg'] = 'unable to reblance cluster'

        output_result = rest.restCmd('POST',
                                     rest_cmds['rebalance'],
                                     self.user,
                                     self.password,
                                     opts)
        if self.debug:
            print "INFO: rebalance started: %s" % output_result

        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

        print "INFO: rebalancing",

        while self.rebalanceStatus(prefix='\n') == 'running':
            print ".",
            time.sleep(0.5)

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

        return json['status']

    def setParam(self, param, value):
        self.params[param] = value
