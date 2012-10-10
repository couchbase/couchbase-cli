#!/usr/bin/env python
# -*- coding: utf-8 -*-
import restclient


class ListServers:
    def __init__(self):
        self.rest_cmd = '/pools/default'
        self.method = 'GET'
        self.output = 'standard'
        self.debug = False
        self.user = ''
        self.password = ''
        self.error = ''

    def runCmd(self, cmd, server, port,
               user, password, opts,):
        self.cmd = cmd
        self.server = server
        self.port = port
        self.user = user
        self.password = password

        for (o, a) in opts:
            if o in  ('-o', '--output'):
                self.output = a
            if o in  ('-d', '--debug'):
                self.debug = True

        data = self.getData(self.server,
                            self.port,
                            self.user,
                            self.password)
        if (self.output == 'return'):
            return self.getNodes(data)
        elif (self.output == 'json'):
            print data
        else:
            # obtain dict of nodes. If not dict, is error message
            nodes = self.getNodes(data)
            if type(nodes) == type(list()):
                self.printNodes(nodes)
            else:
                print self.error

    def getData(self, server, port, user, password):
        """
        getData()

        Obtain the raw json output from the server
        The reason for passing arguments which could be obtained
        from 'self' is because getData() must be callable externally
    """
        self.rest = restclient.RestClient(server, port,
                                          {'debug':self.debug})
        return self.rest.restCmd('GET', self.rest_cmd,
                                 user, password)

    def getNodes(self, data):
        """
        Deserialize json into nodes.
    """
        json = self.rest.getJson(data)
        if type(json) == type(unicode()):
            self.error = json
            return None
        elif type(json) == type(list()):
            self.error = json[0]
            return None
        return json['nodes']

    def printNodes(self, nodes):
        for node in nodes:
            if self.cmd == "host-list":
                print node['hostname']
            else:
                if node.get('otpNode') is None:
                    raise Exception("could not access node;" +
                                    " please check your username (-u) and password (-p)")

                print '%s %s %s %s' % (node['otpNode'],
                                       node['hostname'],
                                       node['status'],
                                       node['clusterMembership'])
