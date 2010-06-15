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

rest_cmds = {
    'bucket-list': '/pools/default/buckets',
    'bucket-flush': '/pools/default/buckets/',
    'bucket-delete': '/pools/default/buckets/',
    'bucket-create': '/pools/default/buckets/',
    }
methods = {
    'bucket-list': 'GET',
    'bucket-delete': 'DELETE',
    'bucket-create': 'POST',
    'bucket-flush': 'POST',
    'bucket-stats': 'GET',
    }


class Buckets:

    def __init__(self):
        """
      constructor
    """
        self.debug = False

    # default

        self.rest_cmd = rest_cmds['bucket-list']

    # default

        self.method = 'GET'

    def runCmd(
        self,
        cmd,
        server,
        port,
        user,
        password,
        opts,
        ):

        # default

        bucketname = ''
        cachesize = ''
        standard_result = ''
        self.user = user
        self.password = password

        # set standard opts

        output= 'default'
        for (o, a) in opts:
            if o == '-b' or o == '--buckets':
                bucketname = a
            if o == '-d' or o == '--debug':
                self.debug = True
            if o in  ('-o', '--output'):
                output = a
            if o == '-s' or o == '--size':
                cachesize = a

        # allow user to be lazy and not specify port

        rest = RestClient(server, port, {'debug':self.debug})
        self.rest_cmd = rest_cmds[cmd]

        # get the parameters straight

        if cmd == 'bucket-delete' or cmd == 'bucket-create' or cmd \
            == 'bucket-flush':
            if len(bucketname):
                rest.setParam('name', bucketname)
            if len(cachesize):
                rest.setParam('cacheSize', cachesize)
            self.rest_cmd = self.rest_cmd + bucketname
            if cmd == 'bucket-flush':
                self.rest_cmd = self.rest_cmd + '/controller/doFlush'

        response = rest.sendCmd(methods[cmd],
                                self.rest_cmd,
                                self.user,
                                self.password)

        if cmd == 'bucket-flush':
            if response.status == 204:
                data = 'Success! %s flushed' % bucketname
            else:
                data = 'Error: unable to flush %s' % bucketname
        else:
            if response.status == 200:
                data = response.read()
            else:
                print 'Error! ', response.status, response.reason
                sys.exit(2)

        if methods[cmd] == 'GET':
            if output == 'json':
                print data
            else:
                json = rest.getJson(data)
                i = 1
                print 'List of buckets within the server %s:%s' \
                    % (server, port)
                for bucket in json:
                    print '\t[%d]: %s' % (i, bucket['name'])
                    i = i + 1
        else:
            if output == 'json':
                print rest.jsonMessage(data)
            else:
                print data

    # debug
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(json)
