#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
  listservers class

  This class implements methods that will list servers within a 
  membase cluster

"""

import pprint
from membase_info import *
from membase_cli_rest_client import *


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

        (cluster, port) = cluster.split(':')
        if not port:
            port = '8080'

        rest = MembaseCliRestClient(cluster, port)

        response = rest.sendCmd(self.method, self.rest_cmd)

        if response.status == 200:
            data = response.read()
        else:
            print 'Error! ', response.status, response.reason
            sys.exit(2)

        json = rest.getJson(data)

        i = 1
        print 'List of servers within the cluster %s:%s' % (cluster,
                port)
        for node in json['nodes']:
            print '\t[%d]: %s\t%s' % (i, node['hostname'], node['status'
                    ])
            i = i + 1


    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(json)
