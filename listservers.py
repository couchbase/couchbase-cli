#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  listservers class

  This class implements methods that will list servers within a
  membase cluster

"""

import pprint
import restclient
from membase_info import usage


class Listservers:

    def __init__(self):
        """
      constructor
    """

        self.rest_cmd = '/pools/default'
        self.method = 'GET'
        self.debug = False
        self.verbose = False
        self.output = 'standard'
        self.user = ''
        self.password = ''
        self.error = ''

    def runCmd(
        self,
        cmd,
        server,
        port,
        user,
        password,
        opts,
        ):
        """
        runCmd()

        This method is the primary method, defined for all command
        modules that performs the primary task, in this case, listing
        the servers that are part of a Membase cluster
    """


        self.server = server
        self.port = port
        self.user = user
        self.password = password

        for (o, a) in opts:
            if o in  ('-o', '--output'):
                self.output = a
            if o in  ('-d', '--debug'):
                self.debug = True
            if o in  ('-v', '--verbose'):
                self.verbose = True

        data = self.getData(self.server,
                            self.port,
                            self.user,
                            self.password)

        if (self.output == 'json'):
            print data
        else:
            if self.verbose:
                print 'List of servers within the server %s:%s' % (server, port)
            # obtain dict of nodes. If not dict, is error message
            nodes = self.getNodes(data)
            if type(nodes) == type(list()):
                self.printNodes(nodes)
            else:
                print self.error

    def getData(self, server, port, user, password):
        """
        getData()

        This method obtains the raw json output from the server
        The reason for passing arguments which could be obtained
        from 'self' is because getData() must be callable externally

    """

        self.rest = restclient.RestClient(server,
                                          port,
                                          {'debug':self.debug})

        opts = {'error_msg':"Unable to obtain server list"}
        data = self.rest.restCmd('GET',
                                 self.rest_cmd,
                                 user,
                                 password)

        return data

    def getNodes(self, data):
        """
        This method deserializes the raw json output and
        returns only the nodes
    """

        json = self.rest.getJson(data)
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(json['nodes'])
        # this means an error occured
        if type(json) == type(unicode()):
            self.error = json
            return None
        elif type(json) == type(list()):
            self.error = json[0]
            return None
        return json['nodes']

    def printNodes(self, nodes):
        """
        This method prints the nodes
    """
        for node in nodes:
            print '%s %s %s' % (node['otpNode'],
                                node['status'],
                                node['clusterMembership'])
