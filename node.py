"""
  Node class

  This class implements methods that will pertain to
  rebalance, add, remove, stop rebalance

"""

from membase_info import *
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
    'failover'          :'/controller/failOver',
}

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

response_dict = {
    'server-add'    : { 'success_code'  : 200,
    'success_msg'   :'server added.',
    'error_msg'     :'unable to add server',
    },
    'rebalance'     : { 'success_code'  : 200,
    'success_msg'   :'rebalance started.',
    'error_msg'     :'unable to start rebalance',
    },
    'rebalance-stop': { 'success_code'  : 200,
    'success_msg'   :'rebalance stopped.',
    'error_msg'     :'unable to stop rebalance',
    },
}

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
        self.params = {}

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

        output = 'standard'
        output_result = ''
        servers = {}
        servers['add'] = {}
        servers['remove'] = {}
        ejectlist = []
        ejectees = ''
        self.cmd = cmd
        self.rest_cmd = rest_cmds[cmd]
        self.method = methods[cmd]
        self.server = server
        self.port = int(port)
        self.user = user
        self.password = password

        # set standard opts
        # note: use of a server key keeps optional
        # args aligned with server

        for o, a in opts:
            if o in ("-a","--server-add"):
                server = a
                servers['add'][server] = { 'user':'','password':''}
            elif o in ( "-r", "--server-remove"):
                server = a
                servers['remove'][server] = { 'user':'','password':''}
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
                    output = a
            elif o in ('-d', '--debug'):
                self.debug = True
            elif o in ('-v', '--verbose'):
                self.verbose = True

        if self.debug:
            print "servers ", servers

        # set the parameters for each server

        if methods[cmd] == 'POST':
            if cmd in ('server-add','rebalance'):
                for action in ('add', 'remove'):
                    for server in servers[action]:
                        if action == 'remove' and cmd == 'rebalance':
                            ejectlist.append(server)
                        else:
                            self.setParam('hostname', server)

                        if servers[action][server]['user'] and \
                                servers[action][server]['password'] :
                            self.setParam('user', servers[action][server]['user'])
                            self.setParam('password', servers[action][server]['password'])

                        # both rebalance and server-add call addServer()

                        if servers['add']:
                            print self.handlePostResponse('server-add')

                # if rebalance, we have to prepare the knownNodes
                # and ejectedNodes, and then rebalance after adding any servers

                if cmd == 'rebalance':
                    self.setNodes(ejectlist)

            # POST response will be handled except server-add because that is
            # handled in a loop per server in command line list

            if cmd != 'server-add':
                output_result = self.handlePostResponse(cmd)

        else:
            output_result = self.handleGetRequest(cmd)

        if output == 'standard' and self.method == 'GET':
            json = self.rest.getJson(output_result)
            output_result = "Rebalance status: %s" % json['status']

        # print the results, GET, SET or DELETE

        print output_result


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
        # known_nodes = known_nodes.join(',').join([node['hostname'] for node in nodes])

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

        self.setParam('knownNodes', known_nodes);
        self.setParam('ejectedNodes', eject_nodes);

    def handleGetRequest(self, cmd):

        self.rest = RestClient(self.server,
                               self.port)
        response = self.rest.sendCmd(self.method,
                                     self.rest_cmd,
                                     self.user,
                                     self.password,
                                     self.params)
        return response.read()

    def handlePostResponse(self, cmd):
        """
            handlePostResponse - this handles the response
            from a POST according to the operation/cmd run

            """

        self.rest = RestClient(self.server,
                               self.port)

        response = self.rest.sendCmd(methods[cmd],
                                     rest_cmds[cmd],
                                     self.user,
                                     self.password,
                                     self.params)

        if response.status == response_dict[cmd]['success_code']:
            data = 'SUCCESS: %s.' % response_dict[cmd]['success_msg']
        else:
            data = 'ERROR: %s.' % response_dict[cmd]['error_msg']

        return data

    def setParam(self, param, value):
        """
            setParam - sets the param dictionary which
            is used for POST

            """

        self.params[param] = value

    def delParam(self, param):
        """
            delParam - removes a parameter from the param dictionary
            which is used for POST

            """
        del self.params[param]
