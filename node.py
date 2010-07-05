"""
  Node class

  This class implements methods that will pertain to
  rebalance, add, remove, stop rebalance

"""

import time
import os
import sys
from membase_info import usage
from restclient import *

# Note: I would like to do this differently. I'm thinking
# listservers should be moved into node since it does pertain
# to nodes. TBD

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
        """
            constructor
            """
        # defaults

        self.rest_cmd = rest_cmds['rebalance-status']
        self.method = 'GET'
        self.debug = False
        self.verbose = False
        self.server = ''
        self.port = ''
        self.user = ''
        self.password = ''
        self.params = {}
        self.output = 'standard'

    def runCmd(self,
               cmd,
               server,
               port,
               user,
               password,
               opts):
        """
            runCmd - perform the operation. This is where all the
            functionaly for the given operation is implemented

            """

        output_result = ''
        self.rest_cmd = rest_cmds[cmd]
        self.method = methods[cmd]
        self.server = server
        self.port = int(port)
        self.user = user
        self.password = password

        servers = self.processOpts(cmd, opts)

        if self.debug:
            print "servers ", servers

        if cmd in ('server-add', 'rebalance'):
            self.addServers(servers['add'])
            if cmd == 'rebalance':
                self.rebalance(servers)
        elif cmd == 'rebalance-status':
            output_result = self.rebalanceStatus()
            print output_result

    def processOpts(self, cmd, opts):
        """
            processOpts()

            This method sets standard opts
            note: use of a server key keeps optional
            args aligned with server
            """

        servers = {}
        servers['add'] = {}
        servers['remove'] = {}

        # don't allow options that don't correspond to given commands

        for o, a in opts:
            usage_msg = "You cannot specify the %s option with '%s'"\
                % (o, cmd)
            if o in ( "-r", "--server-remove", "--server-remove-username", \
                        "--server-remove-password"):
                if cmd in server_no_remove:
                    print "cmd %s usage_msg %s" % (cmd, usage_msg)
                    usage(usage_msg)
            elif o in ( "-a", "--server-add", "--server-add-username", \
                        "--server-add-password"):
                if cmd in server_no_add:
                    print "cmd %s usage_msg %s" % (cmd, usage_msg)
                    usage(usage_msg)

        for o, a in opts:
            if o in ("-a","--server-add"):
                server = a
                servers['add'][server] = { 'user':'', 'password':''}
            elif o in ( "-r", "--server-remove"):
                server = a
                servers['remove'][server] = { 'user':'', 'password':''}
            elif o == "--server-add-username":
                servers['add'][server]['user'] = a
            elif o == "--server-add-password":
                servers['add'][server]['password'] = a
            elif o == "--server-remove-username":
                servers['remove'][server]['user'] = a
            elif o == "--server-remove-password":
                servers['remove'][server]['password'] = a
            elif o in ('-o', '--output'):
                if a == 'json':
                    self.output = a
            elif o in ('-d', '--debug'):
                self.debug = True
            elif o in ('-v', '--verbose'):
                self.verbose = True

        return servers

    def getEjectList(self, servers):
        ejectlist = []
        for server in servers['remove']:
            ejectlist.append(server)
        return ejectlist

    def addServers(self, servers):
        for server in servers:
            user = servers[server]['user']
            password = servers[server]['password']
            output_result = self.serverAdd(server,
                                           user,
                                           password)
            print output_result

    def serverAdd(self, server, user='', password=''):
        opts = {}
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        rest.setParam('hostname', server)
        if user and password:
            rest.setParam('user', user)
            rest.setParam('password', password)
        opts['error_msg'] = "Unable to add %s" % server
        opts['success_msg'] = "Added %s" % server
        output_result = rest.restCmd('POST',
                                     rest_cmds['server-add'],
                                     user,
                                     password,
                                     opts)
        return output_result

    def setNodes(self, ejectlist):
        """
            setNodes - this obtains the list of nodes
            in order from knownNodes to be passed to a rebalance

            """

        known_nodes = ''
        eject_nodes = ''
        listservers = Listservers()
        known_nodes_list = listservers.getNodes(
                                listservers.getData(self.server,
                                                    self.port,
                                                    self.user,
                                                    self.password))

        # I would like to do something like this

        nodes = []
        ejectnodes = []

        for node in known_nodes_list:
            nodes.append(node['otpNode'])
            for ejectee in ejectlist:
                if ejectee == node['hostname']:
                    ejectnodes.append(node['otpNode'])

        eject_nodes = eject_nodes.join(',').join(ejectnodes)
        known_nodes = known_nodes.join(',').join(nodes)

        # a list of ejectNodes and knownNodes is needed (comma-separated)

        return({ 'knownNodes':known_nodes, 'ejectedNodes':eject_nodes})

    def rebalance(self, servers):
        opts = {}
        ejectlist = self.getEjectList(servers)
        nodes = self.setNodes(ejectlist)

        # POST response will be handled except server-add because that is
        # handled in a loop per server in command line list
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        rest.setParam('knownNodes', nodes['knownNodes'])
        rest.setParam('ejectedNodes', nodes['ejectedNodes'])

        opts['success_msg'] = 'Rebalanced cluster'
        opts['error_msg'] = 'Unable to reblance cluster'

        output_result = rest.restCmd('POST',
                                     rest_cmds['rebalance'],
                                     self.user,
                                     self.password,
                                     opts)
        if self.debug:
            print "rebalance POST response: %s" % output_result

        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
        print "Start rebalance",

        while self.rebalanceStatus() == 'running':
            print ".",
            time.sleep(1)
        print output_result

        return 0

    def rebalanceStatus(self):
        rest = restclient.RestClient(self.server,
                                     self.port,
                                     {'debug':self.debug})
        opts = { 'error_msg':'Unable to obtain rebalance status'}
        output_result = rest.restCmd('GET',
                                     rest_cmds['rebalance-status'],
                                     self.user,
                                     self.password,
                                     opts)
        json = rest.getJson(output_result)
        if type(json) == type(list()):
            return "ERROR: %s" % json[0]
        return json['status']

    def setParam(self, param, value):
        """
            setParam - sets the param dictionary which
            is used for POST

            """

        self.params[param] = value
