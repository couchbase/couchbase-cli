#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  Provides info about a particular server.
"""

from usage import usage

import restclient
import simplejson

class Info:
    def __init__(self):
        self.debug = False

    def runCmd(self, cmd, server, port,
               user, password, opts):
        for (o, a) in opts:
            if o == '-d' or o == '--debug':
                self.debug = True

        rest = restclient.RestClient(server, port, {'debug':self.debug})
        opts = {'error_msg': 'server-info error'}

        data = rest.restCmd('GET', '/nodes/self',
                            user, password, opts)

        json = rest.getJson(data)

        for x in ['license', 'licenseValid', 'licenseValidUntil']:
            if x in json:
                del(json[x])

        print simplejson.dumps(json, sort_keys=True, indent=2)
