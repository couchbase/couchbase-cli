#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  listservers class

  This class implements methods that will list servers within a
  membase cluster

"""

import pprint
from membase_info import *
from restclient import *


class Listservers:

    def __init__(self):
        """
      constructor
    """

        self.rest_cmd = '/pools/default'
        self.method = 'GET'
        self.debug = False

    def runCmd(
        self,
        cmd,
        cluster,
        user,
        password,
        opts,
        ):

        output= 'default'
        for (o, a) in opts:
            if o in  ('-o', '--output'):
                output = a
            if o in  ('-d', '--debug'):
                self.debug = 1

        (cluster, port) = cluster.split(':')
        if not port:
            port = '8080'

        data = self.getData(cluster,port)
        if (output == 'json'):
            print data
        else:
            print 'List of servers within the cluster %s:%s' % (cluster, port)
            self.printNodes(self.getNodes(data))


    def getData(self,
                cluster,
                port):
        """
        get the raw json output from the server
    """

        self.rest = RestClient(cluster, port, {'debug':self.debug})
        response = self.rest.sendCmd(self.method, self.rest_cmd)

        if response.status == 200:
            data = response.read()
        else:
            data = '"Error! ' + response.status + response.reason + '"'

        return data

    def getNodes(self, data):
        """
        deserialize the raw json output and return just the nodes
    """

        json = self.rest.getJson(data)
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(json['nodes'])
        return json['nodes']

    def printNodes(self, nodes):
        """
        print the nodes
    """

        for node in nodes:
            print '\t%s\t%s\t%s' % (node['hostname'],
                node['otpNode'], node['status' ])
