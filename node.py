"""
  Nodemgr class

  This class implements methods that will pertain to 
  adding/removing servers to/from the cluster and rebalancing 

"""

import pprint
from membase_info import *
from restclient import *

rest_cmds = { 
    'rebalance'         :'/controller/rebalance',
    'rebalance-stop'    :'/controller/stopRebalance',
    'rebalance-status'  :'/pools/default/rebalanceProgress',
    'eject-server'      :'/controller/ejectNode',
    'add-server'        :'/controller/addNode',
    'failover'          :'/controller/failOver',
}
# [{addNode, {struct, [{uri, <<"/controller/addNode">>}]}},
# {rebalance, {struct, [{uri, <<"/controller/rebalance">>}]}},
# {failOver, {struct, [{uri, <<"/controller/failOver">>}]}},
# {reAddNode, {struct, [{uri, <<"/controller/reAddNode">>}]}},
# {ejectNode, {struct, [{uri, <<"/controller/ejectNode">>}]}},
methods = { 
    'rebalance'         :'POST',
    'rebalance-stop'    :'POST',
    'rebalance-status'  :'GET',
    'eject-server'      :'POST',
    'add-server'        :'POST',
    'failover'          :'POST',
}

class Node:
    def __init__(self):
        """ 
            constructor
            """
        # default
        self.rest_cmd = rest_cmds['rebalance-status']

        # default
        self.method = 'GET'
    
    def runCmd(self, cmd, cluster, user, password, opts):
        # default
        standard_result = ''
        servers = {}
        servers['add'] = {}
        servers['remove'] = {}
        self.rest_cmd = rest_cmds[cmd]

        # set standard opts
        # note: use of a server key keeps optional 
        # args aligned with server 
        for o, a in opts:
            if o in ("-a","--server-add"):
                server = a
                servers['add'][server] = {} 
            elif o == "--server-add-username":
                servers['add'][server]['username'] = a
            elif o == "--server-add-password":
                servers['add'][server]['password'] = a
            elif o in ("-r","--server-remove"):
                servers['remove'][server]= {} 
            elif o == "--server-remove-username":
                servers['remove'][server]['username'] = a
            elif o == "--server-remove-password":
                servers['remove'][server]['password'] = a

        # allow user to be lazy and not specify port
        cluster, port = cluster.split(':')
        if not port:
            port = "8080";

        rest = RestClient(cluster, port) 

        # get the parameters straight
        #if methods[cmd] == 'POST':
        #  for server in server_add_list:
        #    rest.setParam('name', server)
        #    if creds['add'][server]['username']: 
        #      rest.setParam('user', creds['add'][server]['username'])
        #    if creds['add'][server]['password']: 
        #      rest.setParam('password', creds['add'][server]['password'])
        #  
        #  for server in server_remove_list:
        #    rest.setParam('name', server)
        #    if creds['remove'][server]['username']: 
        #      rest.setParam('user', creds['remove'][server]['username'])
        #    if creds['remove'][server]['password']: 
        #      rest.setParam('password', creds['remove'][server]['password'])
    
        print "DEBUG:"
        print servers

        standard_result= "This code will deal with cluster rebalance"

        print standard_result

