#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  listservers class

  This class implements methods that will list servers within a
  membase cluster

"""

import pprint
from membase_info import usage
from restclient import sendCmd, getJson


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

        self.user = user
        self.password = password

        for (o, a) in opts:
            if o in  ('-o', '--output'):
                self.output = a
            if o in  ('-d', '--debug'):
                self.debug = True
            if o in  ('-v', '--verbose'):
                self.verbose = True

        data = self.getData(server,
                            port,
                            user,
                            password)

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



    def getData(self,
                server,
                port,
                user='',
                password=''):
        """
        get the raw json output from the server
    """

        if not user and not password:
            user = self.user
            password = self.password

        self.rest = RestClient(server, port, {'debug':self.debug})
        response = self.rest.sendCmd(self.method,
                                     self.rest_cmd,
                                     user,
                                     password)

        if response.status == 200:
            data = response.read()
        else:
            data = '"ERROR: %d %s"' % (response.status, response.reason)

        return data

    def getNodes(self, data):
        """
        deserialize the raw json output and return just the nodes
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
        print the nodes
    """
        for node in nodes:
            print '%s\t%s' % (node['otpNode'], node['status' ])
