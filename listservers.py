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

        (cluster, port) = cluster.split(':')
        if not port:
            port = '8080'

        rest = RestClient(cluster, port)

        response = rest.sendCmd(self.method, self.rest_cmd)

        if response.status == 200:
            data = response.read()
        else:
            print 'Error! ', response.status, response.reason
            sys.exit(2)

        if (output == 'json'):
            print data
        else:
            json = rest.getJson(data)

            i = 1
            print 'List of servers within the cluster %s:%s' % (cluster, port)
            for node in json['nodes']:
                print '\t[%d]: %s\t%s\t%s' % (i, node['hostname'],
                    node['otpNode'], node['status' ])
                i = i + 1

        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(json)
