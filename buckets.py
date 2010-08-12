#!/usr/bin/env python
# -*- coding: utf-8 -*-

from usage import usage

import restclient

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
        self.debug = False
        self.rest_cmd = rest_cmds['bucket-list']
        self.method = 'GET'

    def runCmd(self, cmd, server, port,
               user, password, opts):
        self.user = user
        self.password = password

        bucketname = ''
        cachesize = ''
        output = 'default'

        for (o, a) in opts:
            if o == '-b' or o == '--bucket':
                bucketname = a
            if o == '-d' or o == '--debug':
                self.debug = True
            if o in  ('-o', '--output'):
                output = a
            if o == '-s' or o == '--size':
                cachesize = a

        self.rest_cmd = rest_cmds[cmd]

        rest = restclient.RestClient(server, port, {'debug':self.debug})

        # get the parameters straight

        if cmd in ('bucket-delete', 'bucket-create', 'bucket-flush'):
            if len(bucketname):
                rest.setParam('name', bucketname)
            if len(cachesize):
                rest.setParam('cacheSize', cachesize)
            self.rest_cmd = self.rest_cmd + bucketname
            if cmd == 'bucket-flush':
                self.rest_cmd = self.rest_cmd + '/controller/doFlush'

        data = rest.restCmd(methods[cmd], self.rest_cmd,
                            self.user, self.password, opts)

        if methods[cmd] == 'GET':
            if output == 'json':
                print data
            else:
                json = rest.getJson(data)
                for bucket in json:
                    print '%s' % bucket['name']
        else:
            if output == 'json':
                print rest.jsonMessage(data)
            else:
                print data
